"""PulseGuard AI entry point.

    python Backend\\main.py probe -> Check read endpoints and response schemas.
    python Backend\\main.py run   -> Run the bot in configured safety mode.

Pre-flight heartbeats are sent only when a valid preflight_tick arrives
from the Kafka market.ticks topic.
"""

import json
import logging
import sys
import time
from logging.handlers import RotatingFileHandler

from config import load_settings
from execution import Executor
from market_data import KafkaFeed, MarketBook, parse_history
from platform_client import (
    PlatformClient,
    PlatformError,
    parse_active_tickers,
    parse_cash,
    parse_clock,
    parse_confirmed_shorts,
    parse_equity,
    parse_orders,
    parse_positions,
    parse_text_state,
    pick,
)
from risk_manager import RiskManager
from strategy import generate_signal


log = logging.getLogger("pulseguard")


def setup_logging(s):
    """Configure console and rotating file logging."""
    s.log_dir.mkdir(parents=True, exist_ok=True)

    fmt = logging.Formatter(
        "%(asctime)s %(levelname)-7s %(name)s: %(message)s"
    )

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    # Avoid adding duplicate handlers if setup_logging is called again.
    if root.handlers:
        return

    handlers = [
        logging.StreamHandler(),
        RotatingFileHandler(
            s.log_dir / "pulseguard.log",
            maxBytes=2_000_000,
            backupCount=3,
            encoding="utf-8",
        ),
    ]

    for handler in handlers:
        handler.setFormatter(fmt)
        root.addHandler(handler)


def tickers_from(body):
    """Extract ticker symbols from a stocks API response."""
    items = (
        body
        if isinstance(body, list)
        else pick(body, ("stocks", "data", "tickers"))
    )

    out = []

    for item in items if isinstance(items, list) else []:
        ticker = (
            item
            if isinstance(item, str)
            else pick(item, ("ticker", "symbol"))
        )

        if ticker:
            out.append(str(ticker).upper())

    return out


def probe(s, client):
    """Check read endpoints without placing orders or sending heartbeats."""
    print(
        f"Base URL: {s.rest_base_url} | "
        f"token: {s.masked_token()} | "
        f"auth: {s.auth_header}: {s.auth_scheme} <token>"
    )

    calls = [
        ("stocks", client.stocks),
        ("stage", client.stage),
        ("clock", client.clock),
        ("cash", client.cash),
        ("portfolio", client.portfolio),
        ("orders", client.orders),
    ]

    stocks = None

    for name, function in calls:
        try:
            body = function()

            print(
                f"\n[OK ] {name}:\n"
                f"{json.dumps(body, indent=2)[:1500]}"
            )

            if name == "stocks":
                stocks = body

        except PlatformError as error:
            print(f"\n[FAIL] {name}: {error}")

    tickers = tickers_from(stocks)

    if tickers:
        try:
            body = client.history(tickers[0], 5)

            print(
                f"\n[OK ] history({tickers[0]}, 5):\n"
                f"{json.dumps(body, indent=2)[:1000]}"
            )

        except PlatformError as error:
            print(f"\n[FAIL] history: {error}")

    print(
        "\nHeartbeat is not sent by probe. "
        "Run the bot to acknowledge actual pre-flight ticks."
    )


def trading_allowed(s, client):
    """Allow trading only in explicitly configured stages."""
    try:
        response = client.stage()

        stage = parse_text_state(
            response,
            ("stage", "status", "state", "phase"),
        )

    except PlatformError as error:
        return False, f"stage unavailable: {error}"

    if stage is None:
        return False, "stage response not recognised (run probe)"

    allowed = stage.strip().lower() in s.trading_stages

    return allowed, stage


_last_acknowledged_seq = 0


def acknowledge_preflight(client, seq):
    """Acknowledge an authentic pre-flight tick using its positive sequence number.

    Guarantees:
    - Rejects invalid / non-positive / boolean seq.
    - Prevents duplicate acknowledgement of identical or older sequence numbers.
    - Logs response status and sanitized error message without exposing credentials.
    - Distinguishes success from failure.
    """
    global _last_acknowledged_seq
    if (
        isinstance(seq, bool)
        or not isinstance(seq, int)
        or seq <= 0
    ):
        log.warning("Ignoring invalid pre-flight sequence: %r", seq)
        return False, "invalid_seq"

    if seq <= _last_acknowledged_seq:
        log.debug("Skipping already acknowledged pre-flight sequence: %d (last: %d)", seq, _last_acknowledged_seq)
        return False, "already_acknowledged"

    try:
        # The sequence must come from an authentic preflight_tick.
        resp = client.heartbeat({"seq": seq})
        _last_acknowledged_seq = max(_last_acknowledged_seq, seq)
        log.info("Pre-flight heartbeat acknowledged: seq=%s (resp=%.200s)", seq, resp)
        return True, resp

    except PlatformError as error:
        log.error("Pre-flight heartbeat failed for seq=%s (status=%s): %s", seq, error.status, error)
        return False, str(error)

    except Exception as exc:
        log.exception("Unexpected heartbeat error for seq=%s: %s", seq, exc)
        return False, str(exc)


def run(s, client):
    """Start the market feed and run the trading loop."""
    if not s.token:
        sys.exit("BOT_ACCESS_TOKEN missing in .env")

    log.info(
        "Starting PulseGuard | dry_run=%s | token=%s",
        s.dry_run,
        s.masked_token(),
    )

    # Verify authentication and obtain canonical active tradable universe.
    stage_data = None
    try:
        stage_data = client.stage()
    except Exception as err:
        log.warning("Could not pre-fetch stage info: %s", err)

    stocks = client.stocks()
    active_universe = parse_active_tickers(stage_data, stocks)
    if not active_universe:
        active_universe = frozenset(tickers_from(stocks))

    tickers = [t for t in (list(s.watchlist) or active_universe) if t in active_universe]

    if not tickers:
        sys.exit(
            "No valid active tickers found: verify active dataset with "
            "/stage and /stocks."
        )

    log.info("Active Tradable Universe (Verified): %s", sorted(active_universe))

    # Validate market data and order flows against the active tradable universe
    book = MarketBook(universe=active_universe)
    risk = RiskManager(s, active_universe=active_universe)
    executor = Executor(s, client, risk, active_universe=active_universe)

    # Seed indicators with historical closing prices.
    for ticker in tickers:
        try:
            closes = parse_history(
                client.history(ticker, s.history_days)
            )

            book.seed(ticker, closes)

            log.info(
                "History %s: %d prices",
                ticker,
                len(closes),
            )

        except PlatformError as error:
            log.warning(
                "History %s failed: %s",
                ticker,
                error,
            )

    # The callback acknowledges only actual pre-flight ticks.
    def on_preflight_tick(seq):
        acknowledge_preflight(client, seq)

    feed = KafkaFeed(
        s,
        book,
        on_preflight_tick=on_preflight_tick,
    )

    feed.start()

    log.info(
        "Kafka feed started | ticks=%s | news=%s",
        s.topic_ticks,
        s.topic_news,
    )

    try:
        while True:
            try:
                cycle(
                    s,
                    client,
                    book,
                    risk,
                    executor,
                    tickers,
                    feed,
                )

            except PlatformError as error:
                log.error(
                    "Trading cycle stopped due to platform error: %s",
                    error,
                )

            except Exception:
                log.exception("Unexpected error during trading cycle")

            time.sleep(s.loop_interval)

    except KeyboardInterrupt:
        log.info("Stopped by user")

    finally:
        feed.stop()
        feed.join(timeout=5)
        log.info("PulseGuard stopped")


_stage_cache = {"allowed": None, "stage": None, "tickers": None, "expires": 0.0}
_clock_cache = {"clock": None, "expires": 0.0}


def get_cached_trading_allowed(s, client, ttl=15.0):
    """Cache stage check briefly to reduce redundant REST overhead."""
    now = time.time()
    if now < _stage_cache["expires"] and _stage_cache["allowed"] is not None:
        return _stage_cache["allowed"], _stage_cache["stage"]
    try:
        response = client.stage()
        stage = parse_text_state(
            response,
            ("stage", "status", "state", "phase"),
        )
        active_ticks = parse_active_tickers(response)
        if active_ticks:
            _stage_cache["tickers"] = active_ticks
    except PlatformError as error:
        return False, f"stage unavailable: {error}"

    if stage is None:
        return False, "stage response not recognised (run probe)"

    allowed = stage.strip().lower() in s.trading_stages
    _stage_cache["allowed"] = allowed
    _stage_cache["stage"] = stage
    _stage_cache["expires"] = now + ttl
    return allowed, stage


def get_cached_clock(client, ttl=30.0):
    """Cache simulation clock to prevent polling overhead."""
    now = time.time()
    if now < _clock_cache["expires"] and _clock_cache["clock"] is not None:
        return _clock_cache["clock"]
    try:
        body = client.clock()
        c = parse_clock(body)
        _clock_cache["clock"] = c
        _clock_cache["expires"] = now + ttl
        return c
    except Exception as err:
        log.debug("Clock check unavailable: %s", err)
        return None


def cycle(s, client, book, risk, executor, tickers, feed):
    """Evaluate signals and execute only permitted trading decisions."""
    allowed, stage = get_cached_trading_allowed(s, client)

    if not allowed:
        log.info("NO TRADING: stage=%s", stage)
        return

    # Keep active universe in sync across components if stage updated it
    cached_tickers = _stage_cache.get("tickers")
    if cached_tickers:
        book.set_universe(cached_tickers)
        risk.set_active_universe(cached_tickers)
        executor.set_active_universe(cached_tickers)

    clock_info = get_cached_clock(client)
    if clock_info and clock_info.get("date"):
        log.info(
            "SIM CLOCK | Date: %s | Day: %s/%s (Remaining: %s)",
            clock_info.get("date"),
            clock_info.get("current_day") or "?",
            clock_info.get("total_days") or "?",
            clock_info.get("remaining_days") or "?",
        )

    try:
        cash = parse_cash(client.cash())
        portfolio = client.portfolio()

    except PlatformError as error:
        log.error(
            "NO TRADING: cannot read cash/portfolio: %s",
            error,
        )
        return

    positions = parse_positions(portfolio)

    if cash is None or positions is None:
        log.error(
            "NO TRADING: cash/portfolio response not recognised"
        )
        return

    # Estimate gross exposure using the latest available market prices.
    gross = sum(
        abs(quantity)
        * (
            book.last(ticker)[1]
            if book.last(ticker)
            else 0
        )
        for ticker, quantity in positions.items()
    )

    equity = parse_equity(portfolio)

    if equity is None:
        equity = cash + sum(
            quantity
            * (
                book.last(ticker)[1]
                if book.last(ticker)
                else 0
            )
            for ticker, quantity in positions.items()
        )

    for ticker in tickers:
        # Ticker Universe Guard: skip symbols not in the verified active tradable universe
        if executor.active_universe is not None and ticker not in executor.active_universe:
            log.warning(
                "[STAGE 3: SKIPPED] Ticker %r not in active universe %s",
                ticker,
                sorted(executor.active_universe),
            )
            continue

        signal = generate_signal(
            ticker,
            book.series(ticker),
            book.news_for(ticker),
            positions.get(ticker, 0),
            s.signal_threshold,
            s.allow_short,
            valid_universe=executor.active_universe,
        )

        last = book.last(ticker)

        decision = risk.check(
            signal,
            last[1] if last else None,
            book.age(ticker),
            cash,
            positions,
            equity,
            gross,
            True,
        )

        if signal.action == "HOLD" or not decision.allowed:
            log.info(
                "DO NOTHING %s | %s | risk: %s",
                ticker,
                signal.explain(),
                decision.reason,
            )
            continue

        log.info(
            "DECISION %s | qty=%d | %s",
            signal.explain(),
            decision.quantity,
            decision.reason,
        )

        executor.execute(
            ticker,
            signal.action,
            decision.quantity,
            signal.explain(),
        )


if __name__ == "__main__":
    settings = load_settings()
    setup_logging(settings)

    client = PlatformClient(settings)

    mode = sys.argv[1] if len(sys.argv) > 1 else "run"

    try:
        if mode == "probe":
            probe(settings, client)
        elif mode == "run":
            run(settings, client)
        else:
            sys.exit(
                f"Unknown mode: {mode}. Use 'probe' or 'run'."
            )

    except PlatformError as error:
        log.error("Fatal platform error: %s", error)
        sys.exit(1)
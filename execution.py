"""Order submission with dry-run, de-duplication and reconciliation after uncertain results."""
import logging
import time

from platform_client import PlatformError

log = logging.getLogger("pulseguard.exec")
ACTIONS = {"BUY", "SELL", "SHORT", "COVER"}


def build_payload(template, ticker, action, quantity):
    """Fill the organizer-provided template. Whole-string placeholders keep their type (qty -> int)."""
    values = {"{ticker}": ticker, "{action}": action, "{quantity}": int(quantity)}

    def fill(x):
        if isinstance(x, dict):
            return {k: fill(v) for k, v in x.items()}
        if isinstance(x, list):
            return [fill(v) for v in x]
        if isinstance(x, str):
            if x in values:
                return values[x]
            for k, v in values.items():
                x = x.replace(k, str(v))
        return x
    return fill(template)


class Executor:
    def __init__(self, settings, client, risk, active_universe=None):
        self.s, self.client, self.risk = settings, client, risk
        self.active_universe = {str(t).upper() for t in active_universe} if active_universe else None
        self.recent = {}   # (ticker, action) -> ts
        self.recent_ticker = {}  # ticker -> ts
        self.pending_orders = {}  # ticker -> dict(action, qty, time)
        self.order_log = []  # audit trail of executions

    def set_active_universe(self, active_universe):
        self.active_universe = {str(t).upper() for t in active_universe} if active_universe else None

    def live_ready(self):
        if self.s.dry_run:
            return False, "DRY_RUN=true"
        if not self.s.order_schema_verified:
            return False, "ORDER_SCHEMA_VERIFIED=false (verify order body with organizers first)"
        if not self.s.order_payload_template:
            return False, "ORDER_PAYLOAD_TEMPLATE empty"
        return True, "ok"

    def execute(self, ticker, action, qty, why, now=None):
        now = now or time.time()
        norm_ticker = str(ticker).upper() if ticker else ""
        ready, reason = self.live_ready()

        # Stage 4 Diagnostic Log: immediately before any submission/dry-run check
        log.info("[STAGE 4: PRE-SUBMIT] ticker=%s action=%s qty=%d why=%s live_ready=%s", norm_ticker, action, qty, why, ready)

        # Active Universe Guard: Reject immediately if not in verified active universe
        if self.active_universe is not None and norm_ticker not in self.active_universe:
            log.error("[STAGE 4: SUBMIT BLOCKED] Invalid ticker %r rejected: not in active universe %s", norm_ticker, sorted(self.active_universe))
            return "invalid-ticker"

        if action not in ACTIONS or qty < 1:
            log.warning("[STAGE 4: SUBMIT BLOCKED] Invalid order %s %s qty=%s", norm_ticker, action, qty)
            return "skipped"

        # COVER Order Guard: Only generate a COVER order when platform confirms an active short position
        if action == "COVER":
            try:
                portfolio = self.client.portfolio()
                from platform_client import parse_confirmed_shorts
                confirmed_shorts = parse_confirmed_shorts(portfolio)
            except Exception as e:
                log.error("[STAGE 4: COVER BLOCKED] Cannot verify platform short position for %s: %s", norm_ticker, e)
                return "skipped"
            held_short = confirmed_shorts.get(norm_ticker, 0)
            if held_short <= 0:
                log.error("[STAGE 4: COVER BLOCKED] Cannot cover %s: no active short position held on platform (confirmed_shorts=%s)", norm_ticker, confirmed_shorts)
                return "no-short-position"
            if qty > held_short:
                log.warning("Clamping COVER %s qty from %d to confirmed short %d", norm_ticker, qty, int(held_short))
                qty = int(held_short)

        key = (norm_ticker, action)
        if now - self.recent.get(key, 0) < self.s.ticker_cooldown_sec:
            log.info("[STAGE 4: SUBMIT BLOCKED] Duplicate %s %s within cooldown", norm_ticker, action)
            return "duplicate"
        if now - self.recent_ticker.get(norm_ticker, 0) < self.s.ticker_cooldown_sec:
            log.info("[STAGE 4: SUBMIT BLOCKED] Ticker %s still in cooldown", norm_ticker)
            return "duplicate"

        # Check for in-flight pending order
        if norm_ticker in self.pending_orders:
            pending = self.pending_orders[norm_ticker]
            if now - pending.get("time", 0) < self.s.ticker_cooldown_sec:
                log.info("[STAGE 4: SUBMIT BLOCKED] Pending order already in-flight for %s", norm_ticker)
                return "pending"
            del self.pending_orders[norm_ticker]

        if not ready:
            log.info("[STAGE 4: SUBMIT BLOCKED] live_ready=False (%s) - order not sent to gateway", reason)
            log.info("DRY-RUN would submit %s %s x%d (%s) [not live: %s]", action, norm_ticker, qty, why, reason)
            self.recent[key] = now
            self.recent_ticker[norm_ticker] = now
            self.pending_orders[norm_ticker] = {"action": action, "qty": qty, "time": now}
            self.risk.record_order(norm_ticker, now)
            self.order_log.append({"ticker": norm_ticker, "action": action, "qty": qty, "status": "dry-run", "time": now})
            return "dry-run"

        payload = build_payload(self.s.order_payload_template, norm_ticker, action, qty)
        try:
            before = self._snapshot()
        except PlatformError as e:
            log.error("SKIP %s %s: cannot snapshot orders before submit (%s)", action, norm_ticker, e)
            return "skipped"

        self.recent[key] = now          # set before sending: blocks any re-send, even on ambiguity
        self.recent_ticker[norm_ticker] = now
        self.pending_orders[norm_ticker] = {"action": action, "qty": qty, "time": now}
        self.risk.record_order(norm_ticker, now)
        try:
            resp = self.client.place_order(payload)
            status = "submitted"
            if isinstance(resp, dict):
                platform_status = str(resp.get("status", "")).upper()
                if platform_status == "REJECTED":
                    reason = resp.get("rejection_reason") or resp.get("rejection_message")
                    log.error("ORDER REJECTED by platform %s %s x%d: %s", action, norm_ticker, qty, reason)
                    self.pending_orders.pop(norm_ticker, None)
                    self.order_log.append({"ticker": norm_ticker, "action": action, "qty": qty, "status": "rejected", "reason": reason, "time": now})
                    return "rejected"
                elif platform_status == "FILLED":
                    status = "filled"
            log.info("ORDER %s %s %s x%d -> %.300s", status.upper(), action, norm_ticker, qty, resp)
            self.pending_orders.pop(norm_ticker, None)
            self.order_log.append({"ticker": norm_ticker, "action": action, "qty": qty, "status": status, "time": now})
            return status
        except PlatformError as e:
            if e.ambiguous:
                res = self._reconcile(norm_ticker, action, qty, before, e)
                if res != "unknown":
                    self.pending_orders.pop(norm_ticker, None)
                self.order_log.append({"ticker": norm_ticker, "action": action, "qty": qty, "status": res, "time": now})
                return res
            log.error("ORDER REJECTED %s %s x%d: %s", action, norm_ticker, qty, e)
            self.pending_orders.pop(norm_ticker, None)
            self.order_log.append({"ticker": norm_ticker, "action": action, "qty": qty, "status": "rejected", "time": now})
            return "rejected"

    def _snapshot(self):
        body = self.client.orders()
        items = body if isinstance(body, list) else (body.get("data") or body.get("orders") if isinstance(body, dict) else None)
        if not isinstance(items, list):
            raise PlatformError("unrecognised orders response")
        return len(items)

    def _reconcile(self, ticker, action, qty, before, err):
        log.warning("ORDER UNCERTAIN %s %s x%d (%s); checking platform", action, ticker, qty, err)
        time.sleep(1)
        try:
            after = self._snapshot()
        except PlatformError as e:
            log.error("Reconcile failed (%s); treating as possibly submitted, NOT retrying", e)
            return "unknown"
        if after > before:
            log.info("Reconciled: order count %d -> %d; order was accepted", before, after)
            return "submitted"
        log.warning("Reconciled: order not found; will not auto-retry until cooldown passes")
        return "not-found"

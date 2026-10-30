
"""Kafka consumers (market.ticks, market.news) and an in-memory price/news book.

Message field names are not documented to us, so extraction is tolerant.
Unparseable data is never traded on.
"""

import json
import logging
import threading
import time
from collections import defaultdict, deque

from platform_client import pick, to_float

log = logging.getLogger("pulseguard.market")


def extract_news(msg, valid_universe=None):
    """Extract a ticker and news text without duplicating nested fields.
    Optionally filters by verified active universe.
    """
    if isinstance(msg, str):
        return None, msg.strip() or None

    if not isinstance(msg, dict):
        return None, None

    # Platform news messages may store fields inside "data".
    data = msg.get("data")
    if not isinstance(data, dict):
        data = {}

    ticker = pick(data, ("ticker", "symbol", "stock"))
    if ticker is None:
        ticker = pick(msg, ("ticker", "symbol", "stock"))

    keys = ("headline", "title", "summary", "body", "text", "content")

    # Prefer nested data if it contains news fields.
    # Otherwise, use the top-level message.
    source = data if any(data.get(key) for key in keys) else msg

    parts = []
    seen = set()

    for key in keys:
        value = pick(source, (key,))

        if value is None:
            continue

        value = str(value).strip()

        if value and value not in seen:
            parts.append(value)
            seen.add(value)

    text = " ".join(parts).strip()
    norm_ticker = str(ticker).upper() if ticker else None

    if valid_universe is not None and norm_ticker and norm_ticker not in valid_universe:
        log.debug("[STAGE 2: NEWS FILTERED] Ticker %r not in active universe", norm_ticker)
        return None, None

    return norm_ticker, (text or None)


def parse_history(body):
    """Return closing prices (oldest first) from a history response."""
    items = (
        body
        if isinstance(body, list)
        else pick(body, ("history", "prices", "data", "candles"))
    )

    if not isinstance(items, list):
        return []

    prices = []

    for item in items:
        value = (
            to_float(item)
            if not isinstance(item, dict)
            else to_float(pick(item, ("close", "price", "last")))
        )

        if value and value > 0:
            prices.append(value)

    return prices


def extract_tick(msg, valid_universe=None):
    """Extract a ticker and valid positive price from a market message.
    Stage 2 Diagnostic: Logs parsed tick and enforces active universe.
    """
    if not isinstance(msg, dict):
        return None

    data = msg.get("data")
    if not isinstance(data, dict):
        data = {}

    ticker = pick(data, ("ticker", "symbol", "stock"))
    if ticker is None:
        ticker = pick(msg, ("ticker", "symbol", "stock"))

    price = to_float(
        pick(data, ("price", "last", "last_price", "close", "ltp"))
    )

    if price is None:
        price = to_float(
            pick(msg, ("price", "last", "last_price", "close", "ltp"))
        )

    if ticker is None or price is None or price <= 0:
        return None

    norm_ticker = str(ticker).upper()
    if valid_universe is not None and norm_ticker not in valid_universe:
        log.warning("[STAGE 2: TICK REJECTED] Ticker %r not in active universe %s", norm_ticker, sorted(valid_universe))
        return None

    log.debug("[STAGE 2: TICK PARSED] ticker=%s price=%s (valid_universe=%s)", norm_ticker, price, bool(valid_universe))
    return norm_ticker, price


def extract_preflight_seq(msg):
    """Extract a valid positive integer sequence number from a pre-flight tick.

    Supports flat and nested payloads:
      {"event_type": "preflight_tick", "seq": 123}
      {"event_type": "preflight_tick", "data": {"seq": 123}}
      {"event": "preflight_tick", "seq": 123}
    Returns a positive integer seq or None.
    """
    if not isinstance(msg, dict):
        return None

    data = msg.get("data") if isinstance(msg.get("data"), dict) else {}
    event_type = (
        msg.get("event_type")
        or msg.get("event")
        or data.get("event_type")
        or data.get("event")
    )
    if str(event_type).lower() != "preflight_tick":
        return None

    seq = msg.get("seq")
    if seq is None:
        seq = data.get("seq")

    if isinstance(seq, bool) or not isinstance(seq, int) or seq <= 0:
        return None

    return seq


class MarketBook:
    """Thread-safe in-memory storage for market prices and news."""

    def __init__(self, maxlen=500, universe=None):
        self._lock = threading.Lock()
        self.prices = defaultdict(lambda: deque(maxlen=maxlen))
        self.news = deque(maxlen=200)
        self.bad = {"ticks": 0, "news": 0}
        self._seen_news = set()
        self.universe = {str(t).upper() for t in universe} if universe else None

    def set_universe(self, universe):
        with self._lock:
            self.universe = {str(t).upper() for t in universe} if universe else None

    def add_tick(self, ticker, price, ts=None):
        t = str(ticker).upper() if ticker else None
        if not t or (self.universe is not None and t not in self.universe):
            return
        with self._lock:
            self.prices[t].append((ts or time.time(), price))

    def seed(self, ticker, closes):
        t = str(ticker).upper() if ticker else None
        if not t:
            return
        with self._lock:
            for close in closes:
                self.prices[t].append((0.0, close))

    def add_news(self, ticker, text):
        if not text or not str(text).strip():
            return
        t = str(ticker).upper() if ticker else None
        if self.universe is not None and t is not None and t not in self.universe:
            return
        clean_text = str(text).strip()
        key = (t, clean_text)
        with self._lock:
            if key in self._seen_news:
                return
            if len(self._seen_news) >= 500:
                self._seen_news.clear()
            self._seen_news.add(key)
            self.news.append((time.time(), t, clean_text))

    def series(self, ticker):
        with self._lock:
            return [price for _, price in self.prices.get(ticker, [])]

    def last(self, ticker):
        with self._lock:
            prices = self.prices.get(ticker)
            return prices[-1] if prices else None

    def age(self, ticker):
        last = self.last(ticker)

        if not last or last[0] == 0.0:
            return None

        return time.time() - last[0]

    def news_for(self, ticker, max_age=900):
        """Return recent news matching a ticker."""
        cutoff = time.time() - max_age

        with self._lock:
            return [
                text
                for ts, news_ticker, text in self.news
                if ts >= cutoff
                and (
                    news_ticker == ticker
                    or (
                        news_ticker is None
                        and ticker.lower() in text.lower()
                    )
                )
            ]


class KafkaFeed(threading.Thread):
    """Consume market ticks and news from Kafka."""

    def __init__(self, settings, book, on_preflight_tick=None):
        super().__init__(daemon=True, name="kafka-feed")

        self.s = settings
        self.book = book
        self._halt = threading.Event()
        self.on_preflight_tick = on_preflight_tick
        self.connected = False
        self._samples = 0

    def stop(self):
        self._halt.set()

    @staticmethod
    def _decode(raw):
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception:
            try:
                return raw.decode("utf-8", "replace")
            except Exception:
                return None

    def _handle(self, topic, msg):
        """Process a Kafka message, including preflight heartbeat ticks.
        Stage 1 Diagnostic: logs receipt of Kafka messages.
        """
        raw_ticker = None
        if isinstance(msg, dict):
            raw_ticker = pick(msg.get("data") if isinstance(msg.get("data"), dict) else msg, ("ticker", "symbol", "stock"))
        log.debug("[STAGE 1: KAFKA RECV] topic=%s ticker=%s raw_msg=%.200s", topic, raw_ticker, msg)

        if self._samples < 3:
            self._samples += 1
            log.info("Kafka sample [%s]: %.300s", topic, msg)

        if topic == self.s.topic_ticks:

            # Diagnostic: sample tick event type and sequence.
            if isinstance(msg, dict):
                if self._samples <= 10:
                    log.info(
                        "Tick diagnostic | event_type=%r | event=%r | seq=%r",
                        msg.get("event_type"),
                        msg.get("event"),
                        msg.get("seq"),
                    )
                else:
                    log.debug(
                        "Tick diagnostic | event_type=%r | event=%r | seq=%r",
                        msg.get("event_type"),
                        msg.get("event"),
                        msg.get("seq"),
                    )

            # Acknowledge each preflight tick using its actual sequence number.
            preflight_seq = extract_preflight_seq(msg)
            if preflight_seq is not None:
                if self.on_preflight_tick is not None:
                    try:
                        self.on_preflight_tick(preflight_seq)
                    except Exception:
                        log.exception(
                            "Preflight heartbeat callback failed for seq=%s",
                            preflight_seq,
                        )
                else:
                    log.warning(
                        "Preflight tick received, but no heartbeat "
                        "handler is configured"
                    )
                # Never treat a preflight message as a market price tick.
                return
            elif isinstance(msg, dict):
                evt = msg.get("event_type") or msg.get("event")
                if str(evt).lower() == "preflight_tick":
                    log.warning("Preflight tick has an invalid seq: %.200s", msg)
                    return

            result = extract_tick(msg, valid_universe=self.book.universe)

            if result:
                ts = None
                if isinstance(msg, dict):
                    data = msg.get("data") if isinstance(msg.get("data"), dict) else {}
                    raw_ts = to_float(pick(data, ("timestamp", "time", "ts"))) or to_float(pick(msg, ("timestamp", "time", "ts")))
                    if raw_ts and raw_ts > 0:
                        ts = raw_ts / 1000.0 if raw_ts > 1e11 else raw_ts
                self.book.add_tick(result[0], result[1], ts=ts)
            else:
                self.book.bad["ticks"] += 1

                if self.book.bad["ticks"] <= 3:
                    log.warning("Unparseable tick: %.200s", msg)

        elif topic == self.s.topic_news:
            ticker, news_text = extract_news(msg, valid_universe=self.book.universe)

            if news_text:
                self.book.add_news(ticker, news_text)

                log.info(
                    "News processed | ticker=%s | text=%.200s",
                    ticker or "UNKNOWN",
                    news_text,
                )
            else:
                self.book.bad["news"] += 1

                if self.book.bad["news"] <= 3:
                    log.warning("Unparseable news: %.200s", msg)

    def run(self):
        from kafka import KafkaConsumer

        while not self._halt.is_set():
            consumer = None

            try:
                consumer = KafkaConsumer(
                    self.s.topic_ticks,
                    self.s.topic_news,
                    bootstrap_servers=self.s.kafka_bootstrap,
                    group_id="group-MN2627",
                    auto_offset_reset="latest",
                    enable_auto_commit=False,
                    value_deserializer=self._decode,
                    request_timeout_ms=15000,
                    consumer_timeout_ms=1000,
                )

                self.connected = True

                log.info(
                    "Kafka connected: %s topics=%s,%s",
                    self.s.kafka_bootstrap,
                    self.s.topic_ticks,
                    self.s.topic_news,
                )

                while not self._halt.is_set():
                    for record in consumer:
                        self._handle(record.topic, record.value)

                        if self._halt.is_set():
                            break

            except Exception as exc:
                self.connected = False
                log.error("Kafka error: %s (retrying in 5s)", exc)

            finally:
                self.connected = False

                if consumer is not None:
                    try:
                        consumer.close()
                    except Exception:
                        pass

            if not self._halt.is_set():
                self._halt.wait(5)

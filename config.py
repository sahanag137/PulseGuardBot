"""Configuration from .env. No secrets are hardcoded."""
import json
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")


def _s(name, default=""):
    return os.getenv(name, default).strip()


def _f(name, default):
    try:
        return float(_s(name, str(default)))
    except ValueError:
        raise SystemExit(f"Invalid number for {name} in .env")


def _b(name, default):
    return _s(name, str(default)).lower() in ("1", "true", "yes", "on")


def _json(name):
    raw = _s(name)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        raise SystemExit(f"{name} in .env is not valid JSON")


@dataclass(frozen=True)
class Settings:
    token: str
    rest_base_url: str
    kafka_bootstrap: str
    topic_ticks: str
    topic_news: str
    auth_header: str
    auth_scheme: str
    dry_run: bool
    order_schema_verified: bool
    order_payload_template: dict
    heartbeat_body: dict
    heartbeat_interval: float
    trading_stages: frozenset
    max_position_pct: float
    max_exposure_pct: float
    min_cash_reserve_pct: float
    max_daily_loss_pct: float
    max_orders_per_min: int
    ticker_cooldown_sec: float
    allow_short: bool
    stale_seconds: float
    signal_threshold: float
    loop_interval: float
    history_days: int
    request_timeout: float
    watchlist: tuple
    log_dir: Path

    def masked_token(self):
        t = self.token
        return "<missing>" if not t else t[:3] + "***" + t[-2:] if len(t) > 8 else "***"


def load_settings() -> Settings:
    return Settings(
        token=_s("BOT_ACCESS_TOKEN"),
        rest_base_url=_s("REST_BASE_URL", "http://192.168.65.157:3002").rstrip("/"),
        kafka_bootstrap=_s("KAFKA_BOOTSTRAP", "192.168.65.157:9094"),
        topic_ticks=_s("TOPIC_TICKS", "market.ticks"),
        topic_news=_s("TOPIC_NEWS", "market.news"),
        auth_header=_s("AUTH_HEADER", "Authorization"),
        auth_scheme=_s("AUTH_SCHEME", "Bearer"),
        dry_run=_b("DRY_RUN", True),
        order_schema_verified=_b("ORDER_SCHEMA_VERIFIED", False),
        order_payload_template=_json("ORDER_PAYLOAD_TEMPLATE"),
        heartbeat_body=_json("HEARTBEAT_BODY"),
        heartbeat_interval=_f("HEARTBEAT_INTERVAL", 10),
        trading_stages=frozenset(x.strip().lower() for x in _s("TRADING_STAGES", "RUNNING").split(",") if x.strip()),
        max_position_pct=_f("MAX_POSITION_PCT", 0.05),
        max_exposure_pct=_f("MAX_EXPOSURE_PCT", 0.50),
        min_cash_reserve_pct=_f("MIN_CASH_RESERVE_PCT", 0.10),
        max_daily_loss_pct=_f("MAX_DAILY_LOSS_PCT", 0.03),
        max_orders_per_min=int(_f("MAX_ORDERS_PER_MIN", 6)),
        ticker_cooldown_sec=_f("TICKER_COOLDOWN_SEC", 60),
        allow_short=_b("ALLOW_SHORT", False),
        stale_seconds=_f("STALE_SECONDS", 30),
        signal_threshold=_f("SIGNAL_THRESHOLD", 2.0),
        loop_interval=_f("LOOP_INTERVAL", 5),
        history_days=int(_f("HISTORY_DAYS", 30)),
        request_timeout=_f("REQUEST_TIMEOUT", 10),
        watchlist=tuple(x.strip().upper() for x in _s("WATCHLIST").split(",") if x.strip()),
        log_dir=ROOT / "logs",
    )

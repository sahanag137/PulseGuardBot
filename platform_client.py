"""REST client for the organizers' API.

Response/request field names are NOT assumed to be known: parsers below are tolerant
and return None when they cannot find data, and the caller must then skip trading.
Run `python Backend\\main.py probe` to see the real payloads and adjust.
"""
import logging
import time

import requests

log = logging.getLogger("pulseguard.platform")


class PlatformError(Exception):
    def __init__(self, msg, status=None, body=None, ambiguous=False):
        super().__init__(msg)
        self.status, self.body, self.ambiguous = status, body, ambiguous


def pick(obj, keys):
    """Case-insensitive lookup of the first matching key (also looks one level into 'data')."""
    if not isinstance(obj, dict):
        return None
    lowered = {str(k).lower(): v for k, v in obj.items()}
    for k in keys:
        if k.lower() in lowered and lowered[k.lower()] is not None:
            return lowered[k.lower()]
    inner = lowered.get("data")
    if isinstance(inner, dict):
        return pick(inner, keys)
    return None


def to_float(v):
    try:
        f = float(v)
        return f if f == f else None
    except (TypeError, ValueError):
        return None


def parse_cash(body):
    if isinstance(body, (int, float)):
        return float(body)
    return to_float(pick(body, ("cash", "available_cash", "availableCash", "balance", "available")))


def parse_equity(body):
    return to_float(pick(body, (
        "total_portfolio_value", "totalPortfolioValue",
        "total_wealth", "totalWealth",
        "total_value", "totalValue",
        "portfolio_value", "portfolioValue",
        "equity", "net_worth", "netWorth"
    )))

def parse_positions(body):
    """Return {ticker: signed_qty} or None if the structure is not recognised. Shorts = negative."""
    items = body if isinstance(body, list) else pick(body, ("positions", "holdings", "stocks", "portfolio"))
    if isinstance(items, dict):
        items = [{"ticker": k, "quantity": v} if not isinstance(v, dict) else {"ticker": k, **v} for k, v in items.items()]
    if not isinstance(items, list):
        return None
    out = {}
    for it in items:
        if not isinstance(it, dict):
            return None
        t = pick(it, ("ticker", "symbol"))
        q = to_float(pick(it, ("quantity", "qty", "shares", "position")))
        if t is None or q is None:
            return None
        side = str(pick(it, ("side", "type", "direction")) or "").upper()
        is_short = it.get("is_short") is True or "SHORT" in side
        if is_short and q > 0:
            q = -q
        out[str(t).upper()] = out.get(str(t).upper(), 0) + q
    return out


def parse_confirmed_shorts(body):
    """Return {ticker: abs_short_qty} ONLY for confirmed active short positions.

    A position is a confirmed short if and only if:
      - is_short is True (and quantity != 0), OR
      - quantity < 0
    Returns a dict mapping uppercase ticker -> positive float/int quantity.
    """
    items = body if isinstance(body, list) else pick(body, ("positions", "holdings", "stocks", "portfolio"))
    if isinstance(items, dict):
        items = [{"ticker": k, **v} if isinstance(v, dict) else {"ticker": k, "quantity": v} for k, v in items.items()]
    if not isinstance(items, list):
        return {}
    shorts = {}
    for it in items:
        if not isinstance(it, dict):
            continue
        t = pick(it, ("ticker", "symbol"))
        if not t:
            continue
        q = to_float(pick(it, ("quantity", "qty", "shares", "position"))) or 0.0
        side = str(pick(it, ("side", "type", "direction")) or "").upper()
        is_short = it.get("is_short") is True or "SHORT" in side
        if (is_short and q != 0) or q < 0:
            shorts[str(t).upper()] = abs(q)
    return shorts


def parse_active_tickers(stage_body, stocks_body=None):
    """Extract canonical active tradable universe from stage and stocks responses."""
    tickers = set()
    if isinstance(stage_body, dict):
        raw_tickers = stage_body.get("tickers") or (stage_body.get("data", {}).get("tickers") if isinstance(stage_body.get("data"), dict) else None)
        if isinstance(raw_tickers, list) and raw_tickers:
            for t in raw_tickers:
                if t and str(t).strip():
                    tickers.add(str(t).strip().upper())
    if not tickers and stocks_body is not None:
        items = stocks_body if isinstance(stocks_body, list) else pick(stocks_body, ("stocks", "data", "tickers"))
        for it in items if isinstance(items, list) else []:
            sym = it if isinstance(it, str) else pick(it, ("ticker", "symbol"))
            if sym:
                tickers.add(str(sym).strip().upper())
    return frozenset(tickers)


def parse_text_state(body, keys):
    if isinstance(body, str):
        return body.strip()
    v = pick(body, keys)
    return str(v).strip() if v is not None else None


def parse_clock(body):
    """Extract canonical simulation clock information."""
    if not isinstance(body, dict):
        return None
    data = body.get("data") if isinstance(body.get("data"), dict) else body
    date_val = pick(data, ("date", "simulated_date", "simulation_date", "current_date"))
    current_day = pick(data, ("current_day", "day", "day_index", "step"))
    total_days = pick(data, ("total_days", "days", "max_days"))
    remaining_days = pick(data, ("remaining_days", "remaining", "days_left"))
    return {
        "date": str(date_val).strip() if date_val else None,
        "current_day": int(current_day) if current_day is not None and str(current_day).isdigit() else None,
        "total_days": int(total_days) if total_days is not None and str(total_days).isdigit() else None,
        "remaining_days": int(remaining_days) if remaining_days is not None and str(remaining_days).isdigit() else None,
    }


def parse_orders(body):
    """Return a list of parsed order dicts or empty list."""
    items = body if isinstance(body, list) else pick(body, ("orders", "data", "history"))
    if not isinstance(items, list):
        return []
    out = []
    for it in items:
        if not isinstance(it, dict):
            continue
        order_id = pick(it, ("id", "order_id", "orderId"))
        ticker = pick(it, ("ticker", "symbol", "stock"))
        action = pick(it, ("action", "side", "type", "direction"))
        quantity = to_float(pick(it, ("quantity", "qty", "shares", "amount")))
        price = to_float(pick(it, ("price", "execution_price", "avg_price", "fill_price")))
        status = pick(it, ("status", "state"))
        ts = pick(it, ("timestamp", "created_at", "time"))
        out.append({
            "order_id": str(order_id) if order_id is not None else None,
            "ticker": str(ticker).upper() if ticker else None,
            "action": str(action).upper() if action else None,
            "quantity": quantity,
            "price": price,
            "status": str(status).upper() if status else "UNKNOWN",
            "timestamp": ts,
        })
    return out


class PlatformClient:
    def __init__(self, settings):
        self.s = settings
        self.http = requests.Session()
        if settings.token:
            scheme = f"{settings.auth_scheme} " if settings.auth_scheme else ""
            self.http.headers[settings.auth_header] = f"{scheme}{settings.token}"
        self.http.headers["Accept"] = "application/json"

    def _request(self, method, path, params=None, json_body=None, retries=0):
        url = self.s.rest_base_url + path
        last = None
        for attempt in range(retries + 1):
            try:
                r = self.http.request(method, url, params=params, json=json_body, timeout=self.s.request_timeout)
            except (requests.Timeout, requests.ConnectionError) as e:
                last = PlatformError(f"{method} {path} network error: {e}", ambiguous=True)
                if attempt < retries:
                    time.sleep(1 + attempt)
                    continue
                raise last
            try:
                body = r.json()
            except ValueError:
                body = r.text
            if r.status_code >= 500:
                last = PlatformError(f"{method} {path} -> {r.status_code}", r.status_code, body, ambiguous=True)
                if attempt < retries:
                    time.sleep(1 + attempt)
                    continue
                raise last
            if r.status_code >= 400:
                raise PlatformError(f"{method} {path} -> {r.status_code}: {body}", r.status_code, body)
            return body
        raise last  # pragma: no cover

    # Reads are safe to retry; POSTs are never auto-retried.
    def stocks(self): return self._request("GET", "/api/v1/stocks", retries=2)
    def history(self, ticker, days): return self._request("GET", "/api/v1/stocks/history", {"ticker": ticker, "days": days}, retries=2)
    def stage(self): return self._request("GET", "/api/v1/stage", retries=2)
    def clock(self): return self._request("GET", "/api/v1/simulation/clock", retries=2)
    def orders(self): return self._request("GET", "/api/v1/orders", retries=2)
    def portfolio(self): return self._request("GET", "/api/v1/portfolio", retries=2)
    def cash(self): return self._request("GET", "/api/v1/portfolio/cash", retries=2)
    def heartbeat(self, body=None): return self._request("POST", "/api/v1/bot/heartbeat", json_body=body)
    def place_order(self, payload): return self._request("POST", "/api/v1/orders", json_body=payload)

"""Pre-trade risk checks. Every order must pass through RiskManager.check()."""
import datetime
import logging
import math
import time
from collections import deque
from dataclasses import dataclass

log = logging.getLogger("pulseguard.risk")
OPENING = {"BUY", "SHORT"}


@dataclass
class RiskDecision:
    allowed: bool
    quantity: int
    reason: str


class RiskManager:
    def __init__(self, s, active_universe=None):
        self.s = s
        self.active_universe = {str(t).upper() for t in active_universe} if active_universe else None
        self.order_times = deque()
        self.last_by_ticker = {}
        self.start_equity = None
        self.start_date = None

    def set_active_universe(self, active_universe):
        self.active_universe = {str(t).upper() for t in active_universe} if active_universe else None

    def record_order(self, ticker, now=None):
        now = now or time.time()
        self.order_times.append(now)
        self.last_by_ticker[ticker] = now

    def check(self, signal, price, age, cash, positions, equity, gross_exposure, trading_ok, now=None):
        now = now or time.time()
        a, t = signal.action, str(signal.ticker).upper() if signal.ticker else ""
        no = lambda why: RiskDecision(False, 0, why)

        if a == "HOLD":
            return no("signal is HOLD")
        if not trading_ok:
            return no("simulation not in a trading stage")
        if self.active_universe is not None and t not in self.active_universe:
            return no(f"ticker {t} not in verified active universe {sorted(self.active_universe)}")
        if price is None or price <= 0 or not math.isfinite(price):
            return no("missing price")
        if age is None or age > self.s.stale_seconds:
            return no(f"market data stale/unavailable (age={age})")
        if cash is None or positions is None or equity is None or equity <= 0 or not math.isfinite(equity) or not math.isfinite(cash):
            return no("missing cash/portfolio/equity data")

        try:
            current_date = datetime.date.fromtimestamp(now) if now else datetime.date.today()
        except (ValueError, OSError):
            current_date = datetime.date.today()

        if self.start_date is None:
            self.start_date = current_date
            if self.start_equity is None:
                self.start_equity = equity
        elif self.start_date != current_date:
            self.start_date = current_date
            self.start_equity = equity
        elif self.start_equity is None:
            self.start_equity = equity

        loss = (self.start_equity - equity) / self.start_equity
        if a in OPENING and loss >= self.s.max_daily_loss_pct:
            return no(f"loss limit hit ({loss:.1%} >= {self.s.max_daily_loss_pct:.1%}); no new positions")

        while self.order_times and now - self.order_times[0] > 60:
            self.order_times.popleft()
        if len(self.order_times) >= self.s.max_orders_per_min:
            return no("order rate limit reached")
        if now - self.last_by_ticker.get(t, 0) < self.s.ticker_cooldown_sec:
            return no("ticker cooldown active")

        held = positions.get(t, 0)
        if a == "SELL":
            return RiskDecision(True, int(held), "close long") if held > 0 else no("no long position to SELL")
        if a == "COVER":
            return RiskDecision(True, int(-held), "close short") if held < 0 else no("no short position to COVER")
        if a == "SHORT" and not self.s.allow_short:
            return no("shorting disabled")
        if a == "BUY" and held < 0:
            return no("short open; COVER first")
        if a == "SHORT" and held > 0:
            return no("long open; SELL first")

        room_pos = equity * self.s.max_position_pct - abs(held) * price
        room_exp = equity * self.s.max_exposure_pct - gross_exposure
        budget = min(room_pos, room_exp)
        if a == "BUY":
            budget = min(budget, cash - equity * self.s.min_cash_reserve_pct)
        qty = math.floor(budget / price) if budget > 0 else 0
        if qty < 1:
            return no(f"no budget left (position/exposure/cash limits), budget={budget:.2f}")
        return RiskDecision(True, qty, "within limits")


def calculate_trade_metrics(trades, starting_equity=100000.0, total_charges=0.0):
    """Compute quantitative risk-adjusted performance metrics from closed trades.

    Calculates: completed round trips, win rate, profit factor, avg win/loss,
    gross P&L, net P&L after charges, and turnover.
    """
    if not trades:
        return {
            "completed_round_trips": 0,
            "win_rate": 0.0,
            "gross_realized_pnl": 0.0,
            "total_charges": total_charges,
            "net_realized_pnl": -total_charges,
            "profit_factor": 0.0,
            "avg_win": 0.0,
            "avg_loss": 0.0,
            "turnover": 0.0,
        }
    completed = len(trades)
    winning = [t for t in trades if t.get("net_pnl", 0) > 0]
    losing = [t for t in trades if t.get("net_pnl", 0) <= 0]
    win_rate = (len(winning) / completed * 100) if completed > 0 else 0.0
    gross_gains = sum(t.get("gross_pnl", 0) for t in trades if t.get("gross_pnl", 0) > 0)
    gross_losses = abs(sum(t.get("gross_pnl", 0) for t in trades if t.get("gross_pnl", 0) < 0))
    profit_factor = (gross_gains / gross_losses) if gross_losses > 0 else (99.0 if gross_gains > 0 else 0.0)
    avg_win = (sum(t.get("net_pnl", 0) for t in winning) / len(winning)) if winning else 0.0
    avg_loss = (sum(t.get("net_pnl", 0) for t in losing) / len(losing)) if losing else 0.0
    gross_pnl = sum(t.get("gross_pnl", 0) for t in trades)
    net_pnl = sum(t.get("net_pnl", 0) for t in trades)
    turnover = sum(t.get("turnover", 0) for t in trades)
    return {
        "completed_round_trips": completed,
        "win_rate": win_rate,
        "gross_realized_pnl": gross_pnl,
        "total_charges": total_charges,
        "net_realized_pnl": net_pnl,
        "profit_factor": profit_factor,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "turnover": turnover,
    }

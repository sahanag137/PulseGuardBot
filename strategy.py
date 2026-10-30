"""Indicators + explainable signal generation. Pure functions, no network."""
import logging
from dataclasses import dataclass, field

log = logging.getLogger("pulseguard.strategy")

POS_WORDS = {"beat", "beats", "surge", "surges", "growth", "profit", "record", "upgrade", "strong", "gain", "gains",
             "rally", "wins", "approval", "bullish", "expands", "soars", "outperform"}
NEG_WORDS = {"miss", "misses", "plunge", "plunges", "loss", "losses", "downgrade", "weak", "fraud", "lawsuit", "probe",
             "recall", "bearish", "falls", "drops", "cuts", "default", "bankruptcy", "underperform", "slump"}


def sma(prices, n):
    return sum(prices[-n:]) / n if len(prices) >= n else None


def rsi(prices, period=14):
    if len(prices) < period + 1:
        return None
    gains = losses = 0.0
    for a, b in zip(prices[-period - 1:-1], prices[-period:]):
        d = b - a
        gains += max(d, 0)
        losses += max(-d, 0)
    if losses == 0:
        return 100.0 if gains > 0 else 50.0
    rs = (gains / period) / (losses / period)
    return 100 - 100 / (1 + rs)


def momentum(prices, n=10):
    if len(prices) <= n or prices[-n - 1] == 0:
        return None
    return prices[-1] / prices[-n - 1] - 1


def sentiment(texts):
    """Crude keyword score in [-1, 1]; 0 when no news."""
    pos = neg = 0
    for t in texts:
        words = {w.strip(".,!?:;()\"'").lower() for w in t.split()}
        pos += len(words & POS_WORDS)
        neg += len(words & NEG_WORDS)
    return 0.0 if pos + neg == 0 else (pos - neg) / (pos + neg)


@dataclass
class Signal:
    ticker: str
    action: str            # BUY SELL SHORT COVER HOLD
    score: float
    reasons: list = field(default_factory=list)

    def explain(self):
        return f"{self.ticker} {self.action} score={self.score:+.2f} | " + "; ".join(self.reasons)


def generate_signal(ticker, prices, news_texts, position_qty, threshold=2.0, allow_short=False,
                    fast=5, slow=20, rsi_period=14, mom_n=10, min_score_margin=0.0,
                    valid_universe=None, entry_price=None, take_profit_pct=None, stop_loss_pct=None):
    norm_ticker = str(ticker).upper()
    if valid_universe is not None and norm_ticker not in valid_universe:
        sig = Signal(norm_ticker, "HOLD", 0.0, [f"[STAGE 3: REJECTED] Ticker '{norm_ticker}' not in active tradable universe {sorted(valid_universe)}"])
        log.warning("[STAGE 3: SIGNAL BLOCKED] %s", sig.explain())
        return sig

    need = max(slow, rsi_period + 1, mom_n + 1)
    if not prices or len(prices) < need:
        sig = Signal(norm_ticker, "HOLD", 0.0, [f"insufficient data ({len(prices) if prices else 0}/{need} prices)"])
        log.debug("[STAGE 3: SIGNAL GEN] %s", sig.explain())
        return sig
    if any(p is None or p <= 0 or p != p for p in prices[-need:]):
        sig = Signal(norm_ticker, "HOLD", 0.0, ["invalid or non-positive price in required window"])
        log.debug("[STAGE 3: SIGNAL GEN] %s", sig.explain())
        return sig
    f, s = sma(prices, fast), sma(prices, slow)
    r, m, sent = rsi(prices, rsi_period), momentum(prices, mom_n), sentiment(news_texts)
    reasons, score = [], 0.0

    trend = 1.0 if f > s else -1.0 if f < s else 0.0
    score += trend
    reasons.append(f"SMA{fast}={f:.2f} {'>' if trend > 0 else '<' if trend < 0 else '='} SMA{slow}={s:.2f} ({trend:+.0f})")

    rc = 1.0 if r < 30 else -1.0 if r > 70 else 0.0
    score += rc
    reasons.append(f"RSI={r:.1f} ({rc:+.0f}{', oversold' if rc > 0 else ', overbought' if rc < 0 else ''})")

    mc = max(-1.0, min(1.0, m / 0.02))
    score += mc
    reasons.append(f"momentum({mom_n})={m * 100:+.2f}% ({mc:+.2f})")

    score += sent
    reasons.append(f"news sentiment={sent:+.2f} from {len(news_texts)} item(s)")

    # Optional Profit Management for open positions
    if position_qty > 0 and entry_price and entry_price > 0:
        pnl_pct = (prices[-1] - entry_price) / entry_price
        if take_profit_pct and pnl_pct >= take_profit_pct:
            sig = Signal(norm_ticker, "SELL", score, reasons + [f"TAKE PROFIT triggered: +{pnl_pct * 100:.2f}% >= +{take_profit_pct * 100:.1f}%"])
            log.info("[STAGE 3: SIGNAL GEN] %s", sig.explain())
            return sig
        if stop_loss_pct and pnl_pct <= -stop_loss_pct:
            sig = Signal(norm_ticker, "SELL", score, reasons + [f"STOP LOSS triggered: {pnl_pct * 100:.2f}% <= -{stop_loss_pct * 100:.1f}%"])
            log.info("[STAGE 3: SIGNAL GEN] %s", sig.explain())
            return sig
    elif position_qty < 0 and entry_price and entry_price > 0:
        pnl_pct = (entry_price - prices[-1]) / entry_price
        if take_profit_pct and pnl_pct >= take_profit_pct:
            sig = Signal(norm_ticker, "COVER", score, reasons + [f"TAKE PROFIT short triggered: +{pnl_pct * 100:.2f}% >= +{take_profit_pct * 100:.1f}%"])
            log.info("[STAGE 3: SIGNAL GEN] %s", sig.explain())
            return sig
        if stop_loss_pct and pnl_pct <= -stop_loss_pct:
            sig = Signal(norm_ticker, "COVER", score, reasons + [f"STOP LOSS short triggered: {pnl_pct * 100:.2f}% <= -{stop_loss_pct * 100:.1f}%"])
            log.info("[STAGE 3: SIGNAL GEN] %s", sig.explain())
            return sig

    if score >= threshold:
        if position_qty < 0:
            action = "COVER"
        elif position_qty > 0:
            action = "HOLD"
            reasons.append("already long; skip duplicate BUY")
        elif min_score_margin > 0 and score < threshold + min_score_margin:
            action = "HOLD"
            reasons.append(f"score {score:.2f} below cost-adjusted entry threshold {threshold + min_score_margin:.2f}")
        else:
            action = "BUY"
    elif score <= -threshold:
        if position_qty > 0:
            action = "SELL"
        elif position_qty < 0:
            action = "HOLD"
            reasons.append("already short; skip duplicate SHORT")
        elif not allow_short:
            action, reasons = "HOLD", reasons + ["bearish but no long position and shorting disabled"]
        elif min_score_margin > 0 and abs(score) < threshold + min_score_margin:
            action = "HOLD"
            reasons.append(f"|score| {abs(score):.2f} below cost-adjusted entry threshold {threshold + min_score_margin:.2f}")
        else:
            action = "SHORT"
    else:
        action = "HOLD"
        reasons.append(f"|score| below threshold {threshold}")

    sig = Signal(norm_ticker, action, score, reasons)
    log.debug("[STAGE 3: SIGNAL GEN] %s", sig.explain())
    return sig

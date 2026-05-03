"""Provider-neutral market bar quality helpers."""

from __future__ import annotations

from typing import Protocol


DEFAULT_BREAK_THRESHOLD = 0.5
PRE2018_CUTOFF = "2018-01-01"


class DailyBarLike(Protocol):
    date: str
    open: float
    high: float
    low: float
    close: float
    adj_close: float | None
    div_cash: float
    split_factor: float


def check_adjustment_applied(bars: list[DailyBarLike]) -> bool:
    """Return True when adjusted close differs from close on enough rows."""
    if not bars:
        return False
    comparable = 0
    same = 0
    for bar in bars:
        if bar.adj_close is None:
            continue
        comparable += 1
        if abs(bar.adj_close - bar.close) < 1e-9:
            same += 1
    if comparable == 0:
        return False
    return (same / comparable) < 0.9


def detect_history_breaks(
    bars: list[DailyBarLike],
    *,
    threshold: float = DEFAULT_BREAK_THRESHOLD,
) -> list[str]:
    """Detect adjusted-close jumps above threshold on ordinary trading days."""
    break_dates: list[str] = []
    prev_adj: float | None = None
    for bar in bars:
        if bar.adj_close is None:
            prev_adj = None
            continue
        if (bar.split_factor or 1.0) != 1.0 or (bar.div_cash or 0.0) > 0.0:
            prev_adj = bar.adj_close
            continue
        if prev_adj is None or prev_adj == 0:
            prev_adj = bar.adj_close
            continue
        change = abs((bar.adj_close - prev_adj) / prev_adj)
        if change > threshold:
            break_dates.append(bar.date)
        prev_adj = bar.adj_close
    return break_dates


def check_ohlc_sanity(bar: DailyBarLike) -> bool:
    """Return True when OHLC values are positive and internally ordered."""
    if min(bar.open, bar.high, bar.low, bar.close) <= 0:
        return False
    return bar.low <= bar.open <= bar.high and bar.low <= bar.close <= bar.high

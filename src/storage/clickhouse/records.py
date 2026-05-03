"""Record dataclasses for the ClickHouse market lane (issue #118 P2).

Mirror the schema in ``storage/clickhouse/schema.py`` row-for-row. Kept
separate from ``storage/models/market.py`` (SQLite-shaped records) so
the two backends evolve independently — the SQLite records carry
quality flags / segment ids that have no CH analogue, and the CH
records carry split-out raw + adjusted OHLCV that the SQLite bar packs
into a single row plus auxiliary flags.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date as Date, datetime


@dataclass(frozen=True)
class CHBar:
    """One row of ``market.bars_1d``.

    ``time`` is the bar's session-close moment in UTC — for daily NYSE
    bars that's the regular-hours close (20:00 UTC standard / 21:00
    DST), 18:00 UTC for half-days, 5pm NY for FX, vendor-specific for
    crypto. The store does not compute this; the caller passes the
    canonical close instant for the venue.

    Adjusted OHL+V are derived at write time from
    ``factor = adjusted_close / close`` (volume divides by factor); the
    caller is expected to have already computed them so this dataclass
    can stay storage-shaped rather than transform-shaped.
    """

    instrument_id: str
    ticker: str
    exchange: str
    time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    adjusted_open: float
    adjusted_high: float
    adjusted_low: float
    adjusted_close: float
    adjusted_volume: float
    fetched_at: datetime


@dataclass(frozen=True)
class CHDividend:
    """One row of ``market.dividends``.

    ``cash_amount`` is EODHD's already-split-adjusted ``value`` (per-
    ticker endpoint) or ``dividend`` (bulk endpoint).
    ``unadjusted_amount`` is EODHD's ``unadjustedValue`` — the cash
    actually paid at the time, required for total-return backtests.
    Declaration / record / payment dates may be ``None`` (bulk endpoint
    leaves them unfilled for most non-major tickers).
    """

    instrument_id: str
    ticker: str
    ex_date: Date
    declaration_date: Date | None
    record_date: Date | None
    payment_date: Date | None
    period: str
    cash_amount: float
    unadjusted_amount: float
    currency: str
    fetched_at: datetime
    content_hash: str


@dataclass(frozen=True)
class CHSplit:
    """One row of ``market.splits``.

    ``to_factor`` / ``from_factor`` are the numerator / denominator of
    EODHD's ``split`` string ``'7.000000/1.000000'``. Stored as
    ``Float64`` (not ``Int``) because non-integer ratios occur
    (``5/4`` ascending, ``1/17`` reverse). Adjustment factor for prices
    pre-event is ``from_factor / to_factor``.
    """

    instrument_id: str
    ticker: str
    execution_date: Date
    to_factor: float
    from_factor: float
    fetched_at: datetime
    content_hash: str


@dataclass(frozen=True)
class CHInstrument:
    """One row of ``market.instruments``.

    ``last_seen`` drives the ``ReplacingMergeTree`` collapse — the most
    recently seen row per ``instrument_id`` wins. ``metadata`` is a
    JSON string for free-form provider extras (e.g. exchange-specific
    sector codes, EODHD ``Type`` field).
    """

    instrument_id: str
    isin: str
    figi: str
    composite_figi: str
    ticker: str
    exchange: str
    asset_class: str
    currency: str
    name: str
    list_date: Date | None
    is_active: bool
    last_seen: datetime
    metadata: str

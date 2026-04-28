"""Storage records — EODHD fundamentals (issue #68 slice 1).

Mirrors ``MarketCorpActionsRawRecord`` shape for the raw audit lane plus
three typed projections (company / financials / highlights) and a
schema-only forward-estimates record (population deferred to a later
slice). Same content-hash + observed-at PIT discipline as ``cal_corp_*``:
the raw lane never overwrites; projections only update when the
incoming ``observed_at_epoch_ms`` is at least as recent as the stored
value, so a late-arriving older snapshot cannot clobber a newer view.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FundamentalsRawRecord:
    """One snapshot of the full ``/api/fundamentals/{TICKER}.{EX}`` payload.

    ``content_hash`` keys revision history — re-fetching the same
    payload is a no-op; a single field change anywhere in the snapshot
    flips the hash and lands a new row.
    """

    provider: str             # "eodhd"
    ticker: str               # full provider ticker e.g. "AAPL.US"
    snapshot_epoch_ms: int    # when WE fetched this snapshot
    content_hash: str         # sha256 over the canonical-JSON full payload
    payload_json: str         # raw response, verbatim
    fetched_at: str           # ISO-8601 UTC


@dataclass(frozen=True)
class FundamentalsCompanyRecord:
    """Static-ish profile from the ``General`` section.

    One row per ticker. Updated whenever upstream changes any tracked
    field (sector reclassification, fiscal-year-end amendment, etc.).
    """

    provider: str
    ticker: str
    name: str
    asset_type: str           # General.Type — "Common Stock" / "ETF" / …
    sector: str
    industry: str
    fiscal_year_end: str      # e.g. "December"
    listing_exchange: str
    currency_code: str
    country_iso: str
    isin: str
    cusip: str
    payload_json: str         # full ``General`` block, sorted-keys JSON
    content_hash: str
    observed_at_epoch_ms: int


@dataclass(frozen=True)
class FundamentalsFinancialsRecord:
    """One financial-statement period.

    Grain: ``(ticker, period_end, period_type, statement)`` — three
    statement types (``IS`` / ``BS`` / ``CF``) × two period types
    (``Q`` / ``A``) per fiscal date. Typed columns cover the eight
    highest-traffic line items; ``payload_json`` retains the full
    upstream period dict so unmodelled fields stay queryable.
    """

    provider: str
    ticker: str
    period_end: str           # ISO date "YYYY-MM-DD"
    period_type: str          # "Q" or "A"
    statement: str            # "IS" / "BS" / "CF"
    currency: str
    filing_date: str          # ISO date if EODHD provides it; else ""
    revenue: float | None              # IS
    net_income: float | None           # IS
    eps_basic: float | None            # IS
    total_assets: float | None         # BS
    total_equity: float | None         # BS
    total_liabilities: float | None    # BS
    cash_from_ops: float | None        # CF
    capex: float | None                # CF
    payload_json: str
    content_hash: str
    observed_at_epoch_ms: int


@dataclass(frozen=True)
class FundamentalsHighlightsRecord:
    """``Highlights`` + ``Valuation`` + ``SharesStats`` block.

    EODHD updates these ~daily — keyed by ``as_of_date`` so revisions
    on the same calendar day overwrite via observed-at, while a fresh
    day always lands a new row.
    """

    provider: str
    ticker: str
    as_of_date: str           # ISO date — local snapshot date in UTC
    market_cap: float | None
    pe_ratio: float | None
    eps_ttm: float | None
    dividend_yield: float | None
    book_value: float | None
    shares_outstanding: float | None
    payload_json: str
    content_hash: str
    observed_at_epoch_ms: int


@dataclass(frozen=True)
class FundamentalsEstimatesRecord:
    """Forward-looking analyst estimate point.

    Schema landed in slice 1 so the table exists for migrations;
    the projector skips it until a later slice wires the ``Earnings``
    / ``AnalystRatings`` parsers (see issue #68 scope §1).
    """

    provider: str
    ticker: str
    period_end: str           # ISO date — forward fiscal period
    period_type: str          # "Q" or "A"
    metric: str               # e.g. "eps_avg" / "revenue_avg"
    value: float | None
    analyst_count: int | None
    payload_json: str
    content_hash: str
    observed_at_epoch_ms: int

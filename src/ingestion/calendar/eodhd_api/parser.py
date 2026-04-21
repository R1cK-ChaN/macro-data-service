"""Parse EODHD calendar rows into ``cal_corp_*`` storage records.

Pure functions — no DB, no HTTP. Each ``parse_*_row`` function maps a
single EODHD row dict to ``(CalendarCorpRawRecord, CalendarCorpEventRecord)``.

Identity synthesis per issue #8 body:

    provider_event_id = sha256(provider + "|" + subtype + "|" + code +
                               "|" + primary_date + "|" + subtype_key)

``primary_date`` is the subtype-specific event anchor (report_date,
start_date, split_date, ex-dividend date, trend date). ``subtype_key``
varies per subtype to keep rows stable across the EODHD lifecycle (e.g.,
an IPO row flipping from ``Filed`` → ``Priced`` keeps the same ID when
its ``start_date`` stabilizes but gets a fresh ID if EODHD's
``start_date`` itself changes — the raw-lane revision history preserves
both snapshots either way).

``content_hash`` is subtype-specific and covers the fields EODHD can
revise between snapshots. Observational-time fields (``date`` for the
dividend ex-date, numeric values, lifecycle state) all live inside the
hash; identity fields (``code``, primary date at first sight) are out of
scope because they already shape ``provider_event_id``.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal

PROVIDER = "eodhd"

SUBTYPES = frozenset({"earnings", "earnings_trend", "ipo", "split", "dividend"})

# Per-subtype mutable field sets. Changing any field listed here flips the
# content_hash and lands a new cal_corp_raw row. Ordering inside a set is
# irrelevant; ordering inside the *hash input* is enforced by sorting in
# :func:`_content_hash_for_fields`.
_EARNINGS_MUTABLE: frozenset[str] = frozenset({
    "report_date", "date", "before_after_market",
    "actual", "estimate", "difference", "percent", "currency",
})
_TREND_MUTABLE: frozenset[str] = frozenset({
    "date", "period", "growth",
    "earningsEstimateAvg", "earningsEstimateLow", "earningsEstimateHigh",
    "earningsEstimateYearAgoEps", "earningsEstimateNumberOfAnalysts",
    "earningsEstimateGrowth",
    "revenueEstimateAvg", "revenueEstimateLow", "revenueEstimateHigh",
    "revenueEstimateYearAgoEps", "revenueEstimateNumberOfAnalysts",
    "revenueEstimateGrowth",
    "epsTrendCurrent", "epsTrend7daysAgo", "epsTrend30daysAgo",
    "epsTrend60daysAgo", "epsTrend90daysAgo",
    "epsRevisionsUpLast7days", "epsRevisionsUpLast30days",
    "epsRevisionsDownLast30days",
})
_IPO_MUTABLE: frozenset[str] = frozenset({
    "start_date", "filing_date", "amended_date",
    "price_from", "price_to", "offer_price",
    "shares", "deal_type", "exchange", "currency", "name",
})
_SPLIT_MUTABLE: frozenset[str] = frozenset({
    "split_date", "old_shares", "new_shares", "optionable",
})
_DIVIDEND_MUTABLE: frozenset[str] = frozenset({
    # /api/calendar/dividends is a discovery-only feed: rows carry just
    # ``symbol`` + ``date`` (ex-dividend) — no value, period, or
    # declaration/record/payment dates. The extended fields live on the
    # per-ticker ``/api/div/{TICKER}.{EXCHANGE}`` endpoint (see
    # :func:`parse_dividend_detail_row`). The only mutable field on the
    # calendar feed itself is ``date`` (an upstream ex-date correction).
    "date",
})
_DIVIDEND_DETAIL_MUTABLE: frozenset[str] = frozenset({
    # /api/div/{TICKER}.{EXCHANGE} enrichment feed. Covers the rich row
    # fields that the calendar discovery feed omits. Extended-format
    # fields (declarationDate / recordDate / paymentDate / unadjustedValue
    # / currency) only arrive for major US + European tickers; for
    # smaller symbols only ``date`` and ``value`` are present, and the
    # missing-key normalisation in :func:`_content_hash_for_fields` keeps
    # the hash stable when fields stay absent.
    "date", "value", "unadjustedValue", "currency",
    "declarationDate", "recordDate", "paymentDate", "period",
})


@dataclass(frozen=True)
class CalendarCorpRawRecord:
    """One row destined for ``cal_corp_raw``."""

    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class CalendarCorpEventRecord:
    """One row destined for ``cal_corp_event`` (PIT projection)."""

    provider: str
    provider_event_id: str
    event_subtype: Literal["earnings", "earnings_trend", "ipo", "split", "dividend"]
    event_time_utc: str
    event_time_precision: Literal["datetime", "date", "approximate"]
    ticker: str
    exchange: str
    currency: str
    currency_reporting: str
    title: str
    reference_date: str | None
    source_url: str
    content_hash: str
    payload_json: str
    observed_at_epoch_ms: int


def synthesize_provider_event_id(
    *,
    subtype: str,
    code: str,
    primary_date: str,
    subtype_key: str,
) -> str:
    """Stable id per ``sha256(provider|subtype|code|primary_date|subtype_key)``.

    All inputs are stringified and separated by ``|`` to keep component
    boundaries unambiguous — two rows that agree on every field produce
    the same id; changing any one component flips the id.
    """
    joined = "|".join([PROVIDER, subtype, code or "", primary_date or "", subtype_key or ""])
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def _content_hash_for_fields(row: dict[str, Any], fields: frozenset[str]) -> str:
    """SHA256 over ``fields`` in deterministic order, joined by ``|``.

    Missing keys and JSON ``null`` both normalize to ``""`` so a field
    flipping between absent and null doesn't register as a revision.
    """
    parts = []
    for key in sorted(fields):
        value = row.get(key)
        parts.append("" if value is None else str(value))
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def _split_code(code: str) -> tuple[str, str]:
    """Split ``AAPL.US`` into ``("AAPL", "US")``. No-dot codes yield
    empty exchange."""
    if not code:
        return "", ""
    code = str(code).strip()
    if "." in code:
        ticker, exchange = code.split(".", 1)
        return ticker, exchange
    return code, ""


def _fetched_at(snapshot_epoch_ms: int) -> str:
    return datetime.fromtimestamp(snapshot_epoch_ms / 1000, tz=timezone.utc).isoformat()


def _stable_json(row: dict[str, Any]) -> str:
    return json.dumps(row, sort_keys=True, ensure_ascii=False)


def _require_code(row: dict[str, Any]) -> str:
    """Dividends use ``symbol``; the other four endpoints use ``code``."""
    for key in ("code", "symbol"):
        value = row.get(key)
        if value:
            return str(value)
    raise ValueError("EODHD row missing code/symbol")


def _build_raw(
    *,
    provider_event_id: str,
    row: dict[str, Any],
    content_hash: str,
    snapshot_epoch_ms: int,
) -> CalendarCorpRawRecord:
    return CalendarCorpRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=_stable_json(row),
        fetched_at=_fetched_at(snapshot_epoch_ms),
    )


# ──────────────────────────────────────────────────────────────────────────
# Earnings
# ──────────────────────────────────────────────────────────────────────────


def parse_earnings_row(
    row: dict[str, Any],
    *,
    snapshot_epoch_ms: int,
    observed_at_epoch_ms: int | None = None,
) -> tuple[CalendarCorpRawRecord, CalendarCorpEventRecord]:
    """Map a ``/calendar/earnings`` row.

    Anchors on ``report_date`` (falls back to ``date``) because EODHD
    uses ``report_date`` as the scheduled announcement day. The
    ``before_after_market`` marker enters the event id so that a ticker
    moving between "BeforeMarket" and "AfterMarket" is treated as a new
    event rather than a silent revision (EODHD re-uses the same row in
    that case, which would otherwise hide the schedule shift)."""
    code = _require_code(row)
    ticker, exchange = _split_code(code)
    report_date = _primary_date_earnings(row)
    if not report_date:
        raise ValueError("EODHD earnings row missing report_date / date")
    baf = (row.get("before_after_market") or "").strip()

    provider_event_id = synthesize_provider_event_id(
        subtype="earnings", code=code, primary_date=report_date, subtype_key=baf,
    )
    content_hash = _content_hash_for_fields(row, _EARNINGS_MUTABLE)
    observed = observed_at_epoch_ms if observed_at_epoch_ms is not None else snapshot_epoch_ms

    title = f"{ticker} earnings" if ticker else "earnings"
    if baf:
        title = f"{title} ({baf})"

    event = CalendarCorpEventRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        event_subtype="earnings",
        event_time_utc=report_date,
        event_time_precision="date",
        ticker=ticker,
        exchange=exchange,
        currency=str(row.get("currency") or ""),
        currency_reporting="",
        title=title,
        reference_date=_opt_str(row.get("date")),
        source_url="",
        content_hash=content_hash,
        payload_json=_stable_json(row),
        observed_at_epoch_ms=observed,
    )
    raw = _build_raw(
        provider_event_id=provider_event_id,
        row=row,
        content_hash=content_hash,
        snapshot_epoch_ms=snapshot_epoch_ms,
    )
    return raw, event


def _primary_date_earnings(row: dict[str, Any]) -> str:
    return str(row.get("report_date") or row.get("date") or "").strip()


# ──────────────────────────────────────────────────────────────────────────
# Earnings trends
# ──────────────────────────────────────────────────────────────────────────


def parse_trend_row(
    row: dict[str, Any],
    *,
    snapshot_epoch_ms: int,
    observed_at_epoch_ms: int | None = None,
) -> tuple[CalendarCorpRawRecord, CalendarCorpEventRecord]:
    """Map a ``/calendar/trends`` row.

    EODHD returns one row per (ticker, period) combination (e.g. ``0q``,
    ``+1q``, ``0y``, ``+1y``). ``period`` is the subtype_key so each
    horizon lands as its own event; otherwise four rows per ticker would
    collide on the same provider_event_id.
    """
    code = _require_code(row)
    ticker, exchange = _split_code(code)
    date_str = str(row.get("date") or "").strip()
    if not date_str:
        raise ValueError("EODHD trend row missing date")
    period = str(row.get("period") or "").strip()

    provider_event_id = synthesize_provider_event_id(
        subtype="earnings_trend", code=code, primary_date=date_str, subtype_key=period,
    )
    content_hash = _content_hash_for_fields(row, _TREND_MUTABLE)
    observed = observed_at_epoch_ms if observed_at_epoch_ms is not None else snapshot_epoch_ms

    title = f"{ticker} EPS trend" if ticker else "EPS trend"
    if period:
        title = f"{title} ({period})"

    event = CalendarCorpEventRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        event_subtype="earnings_trend",
        event_time_utc=date_str,
        event_time_precision="date",
        ticker=ticker,
        exchange=exchange,
        currency="",
        currency_reporting="",
        title=title,
        reference_date=period or None,
        source_url="",
        content_hash=content_hash,
        payload_json=_stable_json(row),
        observed_at_epoch_ms=observed,
    )
    raw = _build_raw(
        provider_event_id=provider_event_id,
        row=row,
        content_hash=content_hash,
        snapshot_epoch_ms=snapshot_epoch_ms,
    )
    return raw, event


# ──────────────────────────────────────────────────────────────────────────
# IPOs
# ──────────────────────────────────────────────────────────────────────────


def parse_ipo_row(
    row: dict[str, Any],
    *,
    snapshot_epoch_ms: int,
    observed_at_epoch_ms: int | None = None,
) -> tuple[CalendarCorpRawRecord, CalendarCorpEventRecord]:
    """Map a ``/calendar/ipos`` row.

    EODHD's lifecycle is ``Filed → Expected → Amended → Priced``. The
    event id anchors on ``filing_date`` (the one date that's set once
    and does not drift) so a row picking up a later ``start_date`` still
    lands on the same ``cal_corp_event`` row rather than forking into a
    new event and leaving the filing-only row as a ghost. The projected
    ``event_time_utc`` still prefers ``start_date`` → ``filing_date`` →
    ``amended_date`` so the calendar surface shows the best-known
    listing date.

    ``deal_type`` is out of the id so the row does not fork on a simple
    lifecycle advance; it still lands in content_hash so raw-lane
    revisions capture the transition.
    """
    code = _require_code(row)
    ticker, exchange_from_code = _split_code(code)
    exchange = str(row.get("exchange") or exchange_from_code or "").strip()
    primary_date = _primary_date_ipo(row)
    if not primary_date:
        raise ValueError("EODHD ipo row missing all of start_date/filing_date/amended_date")
    identity_date = _identity_date_ipo(row) or primary_date
    deal_type = str(row.get("deal_type") or "").strip()

    provider_event_id = synthesize_provider_event_id(
        subtype="ipo", code=code, primary_date=identity_date, subtype_key="",
    )
    content_hash = _content_hash_for_fields(row, _IPO_MUTABLE)
    observed = observed_at_epoch_ms if observed_at_epoch_ms is not None else snapshot_epoch_ms

    name = str(row.get("name") or "").strip()
    title = f"{ticker or name} IPO" if (ticker or name) else "IPO"
    if deal_type:
        title = f"{title} ({deal_type})"

    precision: Literal["datetime", "date", "approximate"] = (
        "date" if deal_type.lower() in {"priced"} else "approximate"
    )

    event = CalendarCorpEventRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        event_subtype="ipo",
        event_time_utc=primary_date,
        event_time_precision=precision,
        ticker=ticker,
        exchange=exchange,
        currency=str(row.get("currency") or ""),
        currency_reporting="",
        title=title,
        reference_date=_opt_str(row.get("filing_date")),
        source_url="",
        content_hash=content_hash,
        payload_json=_stable_json(row),
        observed_at_epoch_ms=observed,
    )
    raw = _build_raw(
        provider_event_id=provider_event_id,
        row=row,
        content_hash=content_hash,
        snapshot_epoch_ms=snapshot_epoch_ms,
    )
    return raw, event


def _primary_date_ipo(row: dict[str, Any]) -> str:
    """Best-known listing date for the projected ``event_time_utc``."""
    for key in ("start_date", "filing_date", "amended_date"):
        value = row.get(key)
        if value:
            text = str(value).strip()
            if text:
                return text
    return ""


def _identity_date_ipo(row: dict[str, Any]) -> str:
    """Lifecycle-stable anchor for ``provider_event_id``.

    ``filing_date`` is set at the SEC / exchange filing step and does
    not shift as the IPO progresses through Expected / Amended /
    Priced. Falling back to ``amended_date`` covers older rows that
    omit the filing date; ``start_date`` is the last resort because it
    moves during the lifecycle."""
    for key in ("filing_date", "amended_date", "start_date"):
        value = row.get(key)
        if value:
            text = str(value).strip()
            if text:
                return text
    return ""


# ──────────────────────────────────────────────────────────────────────────
# Splits
# ──────────────────────────────────────────────────────────────────────────


def parse_split_row(
    row: dict[str, Any],
    *,
    snapshot_epoch_ms: int,
    observed_at_epoch_ms: int | None = None,
) -> tuple[CalendarCorpRawRecord, CalendarCorpEventRecord]:
    """Map a ``/calendar/splits`` row.

    subtype_key encodes the ratio so a ticker that has two splits on the
    same day (rare, but happens for multi-class shares) lands as two
    events. A ratio re-statement (e.g. EODHD correcting ``1:5`` to
    ``2:5``) still flips content_hash.
    """
    code = _require_code(row)
    ticker, exchange = _split_code(code)
    split_date = str(row.get("split_date") or "").strip()
    if not split_date:
        raise ValueError("EODHD split row missing split_date")
    old_shares = row.get("old_shares")
    new_shares = row.get("new_shares")
    subtype_key = f"{old_shares}:{new_shares}" if (old_shares and new_shares) else ""

    provider_event_id = synthesize_provider_event_id(
        subtype="split", code=code, primary_date=split_date, subtype_key=subtype_key,
    )
    content_hash = _content_hash_for_fields(row, _SPLIT_MUTABLE)
    observed = observed_at_epoch_ms if observed_at_epoch_ms is not None else snapshot_epoch_ms

    title = f"{ticker} split" if ticker else "split"
    if subtype_key:
        title = f"{title} ({subtype_key})"

    event = CalendarCorpEventRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        event_subtype="split",
        event_time_utc=split_date,
        event_time_precision="date",
        ticker=ticker,
        exchange=exchange,
        currency="",
        currency_reporting="",
        title=title,
        reference_date=None,
        source_url="",
        content_hash=content_hash,
        payload_json=_stable_json(row),
        observed_at_epoch_ms=observed,
    )
    raw = _build_raw(
        provider_event_id=provider_event_id,
        row=row,
        content_hash=content_hash,
        snapshot_epoch_ms=snapshot_epoch_ms,
    )
    return raw, event


# ──────────────────────────────────────────────────────────────────────────
# Dividends
# ──────────────────────────────────────────────────────────────────────────


def parse_dividend_row(
    row: dict[str, Any],
    *,
    snapshot_epoch_ms: int,
    observed_at_epoch_ms: int | None = None,
) -> tuple[CalendarCorpRawRecord, CalendarCorpEventRecord]:
    """Map a ``/calendar/dividends`` row.

    EODHD's calendar dividends feed is *discovery-only*: each row is
    ``{"symbol": "...", "date": "YYYY-MM-DD"}`` (the date is the
    ex-dividend date). There is no value, period, currency, or
    declaration/record/payment dates — those fields live on
    ``/api/div/{TICKER}.{EXCHANGE}`` and will be wired by a later slice.

    Identity is ``(symbol, date)`` — the (symbol, ex-date) tuple is
    naturally unique for a single dividend event. The ``subtype_key``
    stays empty because ``period`` isn't available on this feed.

    :func:`_require_code` accepts both ``code`` and ``symbol`` so the
    same parser works if EODHD later harmonises the key name.
    """
    code = _require_code(row)
    ticker, exchange = _split_code(code)
    ex_date = str(row.get("date") or "").strip()
    if not ex_date:
        raise ValueError("EODHD dividend row missing date")

    provider_event_id = synthesize_provider_event_id(
        subtype="dividend", code=code, primary_date=ex_date, subtype_key="",
    )
    content_hash = _content_hash_for_fields(row, _DIVIDEND_MUTABLE)
    observed = observed_at_epoch_ms if observed_at_epoch_ms is not None else snapshot_epoch_ms

    title = f"{ticker} dividend" if ticker else "dividend"

    event = CalendarCorpEventRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        event_subtype="dividend",
        event_time_utc=ex_date,
        event_time_precision="date",
        ticker=ticker,
        exchange=exchange,
        # Calendar feed doesn't carry currency — left empty; populated
        # later when the per-ticker /api/div endpoint is integrated.
        currency="",
        currency_reporting="",
        title=title,
        reference_date=None,
        source_url="",
        content_hash=content_hash,
        payload_json=_stable_json(row),
        observed_at_epoch_ms=observed,
    )
    raw = _build_raw(
        provider_event_id=provider_event_id,
        row=row,
        content_hash=content_hash,
        snapshot_epoch_ms=snapshot_epoch_ms,
    )
    return raw, event


def parse_dividend_detail_row(
    row: dict[str, Any],
    *,
    code: str,
    snapshot_epoch_ms: int,
    observed_at_epoch_ms: int | None = None,
) -> tuple[CalendarCorpRawRecord, CalendarCorpEventRecord]:
    """Map a ``/api/div/{TICKER}.{EXCHANGE}`` row (enrichment feed).

    Shares identity with :func:`parse_dividend_row` — both anchor on
    ``sha256(eodhd|dividend|{code}|{ex_date}|"")`` so the rich detail
    snapshot upserts the same ``cal_corp_event`` row discovery wrote.
    ``code`` is passed in by the caller because ``/api/div`` carries the
    ticker on the URL path, not inside the row body.

    Extended-format fields (``declarationDate`` / ``recordDate`` /
    ``paymentDate`` / ``unadjustedValue`` / ``currency``) are optional —
    EODHD only returns them for major US + European tickers. Missing
    fields stay absent from the event row (empty string / None).

    Revision trade-off: ``cal_corp_event`` upserts obey
    "latest ``observed_at`` wins". A later discovery-feed refresh will
    overwrite the enriched ``currency`` with ``""`` — re-run detail to
    re-enrich. This matches how all other subtypes behave.
    """
    code = str(code or "").strip()
    if not code:
        raise ValueError("EODHD dividend detail row requires a code (from URL path)")
    ticker, exchange = _split_code(code)
    ex_date = str(row.get("date") or "").strip()
    if not ex_date:
        raise ValueError("EODHD dividend detail row missing date")

    provider_event_id = synthesize_provider_event_id(
        subtype="dividend", code=code, primary_date=ex_date, subtype_key="",
    )
    content_hash = _content_hash_for_fields(row, _DIVIDEND_DETAIL_MUTABLE)
    observed = observed_at_epoch_ms if observed_at_epoch_ms is not None else snapshot_epoch_ms

    title = f"{ticker} dividend" if ticker else "dividend"
    period = _opt_str(row.get("period"))

    event = CalendarCorpEventRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        event_subtype="dividend",
        event_time_utc=ex_date,
        event_time_precision="date",
        ticker=ticker,
        exchange=exchange,
        currency=str(row.get("currency") or ""),
        currency_reporting="",
        title=title,
        reference_date=period,
        source_url="",
        content_hash=content_hash,
        payload_json=_stable_json(row),
        observed_at_epoch_ms=observed,
    )
    raw = _build_raw(
        provider_event_id=provider_event_id,
        row=row,
        content_hash=content_hash,
        snapshot_epoch_ms=snapshot_epoch_ms,
    )
    return raw, event


def _opt_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None

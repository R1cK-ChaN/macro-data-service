"""EODHD calendar validation — planner, runner, diff helpers.

Nine-probe budget covers the five corporate calendar subtypes
(earnings, IPOs, splits, dividends, dividend-detail enrichment) and
exercises both the JSON:API envelope and the top-level-array
``/api/div/{TICKER}`` shape.
"""

from __future__ import annotations

from collections import Counter
from typing import Any

from ingestion.calendar.eodhd_api import (
    EODHDAPIClient,
    parse_dividend_detail_row,
    parse_dividend_row,
    parse_earnings_row,
    parse_ipo_row,
    parse_split_row,
    parse_trend_row,
)

from scripts.validate._shared import (
    Probe,
    ProbeResult,
    RowDiff,
    days_ago_iso,
    days_ahead_iso,
    render_params,
    today_iso,
)


# EODHD parser-reads per subtype (grep src/ingestion/calendar/eodhd_api/
# parser.py — these are the keys each parse_*_row function accesses or
# hashes). Compared against the observed row to surface UNKNOWN_OBSERVED
# (EODHD added a field we don't see) and MISSING_EXPECTED (our parser
# reads something upstream didn't return).

EODHD_EARNINGS_READS: frozenset[str] = frozenset({
    "code", "report_date", "date", "before_after_market",
    "currency", "actual", "estimate", "difference", "percent",
})

EODHD_TREND_READS: frozenset[str] = frozenset({
    "code", "date", "period", "growth",
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

EODHD_IPO_READS: frozenset[str] = frozenset({
    "code", "name", "exchange", "currency",
    "start_date", "filing_date", "amended_date",
    "price_from", "price_to", "offer_price",
    "shares", "deal_type",
})

EODHD_SPLIT_READS: frozenset[str] = frozenset({
    "code", "split_date", "optionable", "old_shares", "new_shares",
})

# /calendar/dividends is discovery-only: (symbol, date) pairs, no
# value/period/currency/declaration/record/payment dates. Validated
# against live EODHD 2026-04-21 for AAPL.US and filter[date_eq]=<recent>.
# Richer per-ticker dividend details live on /api/div/{TICKER} and are
# consumed by :func:`parse_dividend_detail_row` (subtype
# ``dividend_detail`` below).
EODHD_DIVIDEND_READS: frozenset[str] = frozenset({
    "symbol", "date",
})

# /api/div/{TICKER}.{EXCHANGE} enrichment feed. Extended fields arrive
# only for major US + European tickers; smaller tickers return just
# {date, value}.
EODHD_DIVIDEND_DETAIL_READS: frozenset[str] = frozenset({
    "date", "value", "unadjustedValue", "currency",
    "declarationDate", "recordDate", "paymentDate", "period",
})

EODHD_SUBTYPE_READS: dict[str, frozenset[str]] = {
    "earnings":         EODHD_EARNINGS_READS,
    "earnings_trend":   EODHD_TREND_READS,
    "ipo":              EODHD_IPO_READS,
    "split":            EODHD_SPLIT_READS,
    "dividend":         EODHD_DIVIDEND_READS,
    "dividend_detail":  EODHD_DIVIDEND_DETAIL_READS,
}

EODHD_SUBTYPE_PARSERS: dict[str, Any] = {
    "earnings":       parse_earnings_row,
    "earnings_trend": parse_trend_row,
    "ipo":            parse_ipo_row,
    "split":          parse_split_row,
    "dividend":       parse_dividend_row,
}


# Enum-style fields tallied per subtype. Different subtypes have different
# interesting vocabularies (deal_type for IPOs, period for dividends/
# trends, before_after_market for earnings, etc.).
EODHD_ENUM_FIELDS_BY_SUBTYPE: dict[str, tuple[str, ...]] = {
    "earnings":         ("before_after_market", "currency"),
    "earnings_trend":   ("period",),
    "ipo":              ("deal_type", "exchange", "currency"),
    "split":            ("optionable",),
    # /calendar/dividends is discovery-only (symbol/date) — no enum
    # fields available on this feed. Period/currency arrive via
    # dividend_detail (/api/div/{TICKER}) instead.
    "dividend":         (),
    "dividend_detail":  ("period", "currency"),
}


def plan_eodhd_probes() -> list[Probe]:
    """Eight probes covering all five corporate subtypes.

    Two dividend variants exercise the ``filter[symbol]`` vs
    ``filter[date_eq]`` paths, and two trend variants confirm whether
    ``/calendar/trends`` really returns ``[[...]]`` for multi-symbol
    requests (and what it does for a single-symbol request).
    """
    today = today_iso()
    week_ahead = days_ahead_iso(7)
    month_ahead = days_ahead_iso(30)
    week_ago = days_ago_iso(7)
    yesterday = days_ago_iso(1)
    return [
        Probe(
            name="earnings_date_window",
            path="/api/calendar/earnings",
            description="earnings shape over a week-ahead window",
            expected_shape="{earnings: [row, row, ...]}",
            params={"from": today, "to": week_ahead},
            rows_key="earnings",
            subtype="earnings",
            expected_fields=EODHD_EARNINGS_READS,
        ),
        Probe(
            name="earnings_symbols",
            path="/api/calendar/earnings",
            description="earnings scoped to AAPL.US + MSFT.US",
            expected_shape="{earnings: [row, row, ...]}",
            params={"symbols": "AAPL.US,MSFT.US"},
            rows_key="earnings",
            subtype="earnings",
            expected_fields=EODHD_EARNINGS_READS,
        ),
        Probe(
            name="ipos_date_window",
            path="/api/calendar/ipos",
            description="IPO shape + deal_type vocabulary over 30-day window",
            expected_shape="{ipos: [row, row, ...]}",
            params={"from": today, "to": month_ahead},
            rows_key="ipos",
            subtype="ipo",
            expected_fields=EODHD_IPO_READS,
        ),
        Probe(
            name="splits_date_window",
            path="/api/calendar/splits",
            description="split shape over 30-day window",
            expected_shape="{splits: [row, row, ...]}",
            params={"from": today, "to": month_ahead},
            rows_key="splits",
            subtype="split",
            expected_fields=EODHD_SPLIT_READS,
        ),
        Probe(
            name="dividends_symbol_filter",
            path="/api/calendar/dividends",
            description="dividend shape under filter[symbol] — JSON:API envelope",
            expected_shape="{meta, data: [row, row, ...], links}",
            params={"filter[symbol]": "AAPL.US"},
            rows_key="data",
            subtype="dividend",
            expected_fields=EODHD_DIVIDEND_READS,
        ),
        Probe(
            name="dividends_date_eq_filter",
            path="/api/calendar/dividends",
            description="dividend shape under filter[date_eq]=<yesterday>",
            expected_shape="{meta, data: [row, row, ...], links}",
            params={"filter[date_eq]": yesterday},
            rows_key="data",
            subtype="dividend",
            expected_fields=EODHD_DIVIDEND_READS,
        ),
        Probe(
            name="trends_single_symbol",
            path="/api/calendar/trends",
            description="trend shape for 1 symbol — is outer list wrapped?",
            expected_shape="{trends: [[row, ...]]} (or flat [row, ...] if single-symbol)",
            params={"symbols": "AAPL.US"},
            rows_key="trends",
            subtype="earnings_trend",
            expected_fields=EODHD_TREND_READS,
        ),
        Probe(
            name="trends_multi_symbol",
            path="/api/calendar/trends",
            description="trend shape for 2 symbols — confirm [[…]] nesting",
            expected_shape="{trends: [[row, ...], [row, ...]]}",
            params={"symbols": "AAPL.US,MSFT.US"},
            rows_key="trends",
            subtype="earnings_trend",
            expected_fields=EODHD_TREND_READS,
        ),
        Probe(
            # Enrichment feed — populates amount / currency / declaration /
            # record / payment dates that /calendar/dividends can't carry.
            # AAPL.US is the canonical "major US ticker" test case: it
            # should always return the extended fields.
            name="dividend_details_aapl",
            path="/api/div/AAPL.US",
            description="per-ticker dividend extended fields — amount / currency / D/R/P dates",
            expected_shape="list[row] (top-level array, not enveloped)",
            params={"from": days_ago_iso(180), "to": today_iso()},
            rows_key="",
            subtype="dividend_detail",
            expected_fields=EODHD_DIVIDEND_DETAIL_READS,
            top_level_array=True,
        ),
    ]


def _flatten_rows(rows: list[Any]) -> list[dict[str, Any]]:
    """/calendar/trends responses wrap per-symbol row groups in lists;
    every other endpoint returns a flat list. This flattens one level
    without altering already-flat payloads."""
    flat: list[dict[str, Any]] = []
    for row in rows:
        if isinstance(row, list):
            flat.extend(r for r in row if isinstance(r, dict))
        elif isinstance(row, dict):
            flat.append(row)
    return flat


def diff_eodhd_row(row: dict[str, Any], expected_fields: frozenset[str]) -> RowDiff:
    """Per-subtype field diff for an EODHD row.

    ``expected_fields`` is the per-subtype EODHD_*_READS set. The union
    of all subtype reads functions as the "known fields" reference so
    UNKNOWN_OBSERVED only fires when upstream added something we've
    never heard of in any subtype.
    """
    observed = set(row.keys())
    known_any_subtype: frozenset[str] = frozenset().union(*EODHD_SUBTYPE_READS.values())
    diff = RowDiff(
        observed_fields=sorted(observed),
        read_by_parser=sorted(observed & expected_fields),
        ignored_by_parser=sorted((known_any_subtype & observed) - expected_fields),
        unknown_observed=sorted(observed - known_any_subtype),
        missing_expected=sorted(expected_fields - observed),
    )

    # Type spot-checks — nothing is P0 fatal, just surface surprises.
    for numeric_field in ("actual", "estimate", "difference", "percent",
                          "value", "unadjustedValue",
                          "price_from", "price_to", "offer_price",
                          "shares", "old_shares", "new_shares"):
        v = row.get(numeric_field)
        if v is not None and isinstance(v, str):
            diff.type_warnings.append(
                f"{numeric_field}={v!r} arrives as string — content_hash "
                f"uses str(), so string↔number revisions flip the hash"
            )
    return diff


def try_parse_eodhd(row: dict[str, Any], subtype: str, *, code: str = "") -> tuple[bool, str]:
    if subtype == "dividend_detail":
        try:
            raw, event = parse_dividend_detail_row(
                row, code=code, snapshot_epoch_ms=1_700_000_000_000,
            )
            return True, f"ok subtype=dividend_detail event_id={raw.provider_event_id[:10]}…"
        except Exception as exc:
            return False, f"{type(exc).__name__}: {exc}"
    parser = EODHD_SUBTYPE_PARSERS.get(subtype)
    if parser is None:
        return False, f"no parser registered for subtype={subtype!r}"
    try:
        raw, event = parser(row, snapshot_epoch_ms=1_700_000_000_000)
        tag = getattr(event, "event_subtype", subtype)
        return True, f"ok subtype={tag} event_id={raw.provider_event_id[:10]}…"
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def run_eodhd_probe(client: EODHDAPIClient, probe: Probe) -> ProbeResult:
    result = ProbeResult(probe=probe, status="skipped")
    result.request_path = f"{probe.path}?{render_params(probe.params)}"

    try:
        if probe.top_level_array:
            # /api/div/{code} returns the row list at payload root — bypass
            # the envelope extractor and use payload directly.
            call = client.get(probe.path, params=probe.params)
        else:
            call = client.get_rows(probe.path, params=probe.params, rows_key=probe.rows_key)
    except Exception as exc:
        result.status = "http_error"
        result.notes.append(f"{type(exc).__name__}: {exc}")
        return result

    result.status = "ok"
    result.http_elapsed_ms = call.elapsed_ms
    if probe.top_level_array:
        raw_rows = call.payload if isinstance(call.payload, list) else []
    else:
        raw_rows = call.rows
    # Client's `rows` may contain inner lists for /calendar/trends — flatten.
    flat_rows = _flatten_rows(raw_rows)
    result.row_count = len(flat_rows)
    if probe.subtype == "earnings_trend" and any(isinstance(r, list) for r in raw_rows):
        result.notes.append(f"trends payload wrapped [[…]] ({len(raw_rows)} outer groups → {len(flat_rows)} rows)")

    if flat_rows:
        result.sample_row = flat_rows[0]
        result.field_diff = diff_eodhd_row(flat_rows[0], probe.expected_fields)

        enum_fields = EODHD_ENUM_FIELDS_BY_SUBTYPE.get(probe.subtype, ())
        counters: dict[str, Counter] = {k: Counter() for k in enum_fields}
        for row in flat_rows:
            for key in enum_fields:
                counters[key][repr(row.get(key))] += 1
        result.enum_counters = counters

        # /api/div/{code} carries the ticker on the URL, not in the row;
        # pass it to the parser so the identity hash can be produced.
        parse_code = ""
        if probe.subtype == "dividend_detail":
            parse_code = probe.path.rsplit("/", 1)[-1]

        sample_n = min(10, len(flat_rows))
        result.parse_attempts = sample_n
        for row in flat_rows[:sample_n]:
            ok, msg = try_parse_eodhd(row, probe.subtype, code=parse_code)
            if ok:
                result.parse_successes += 1
            else:
                if len(result.parse_error_samples) < 3:
                    result.parse_error_samples.append(msg)
    return result


__all__ = [
    "EODHDAPIClient",
    "EODHD_DIVIDEND_DETAIL_READS",
    "EODHD_DIVIDEND_READS",
    "EODHD_EARNINGS_READS",
    "EODHD_ENUM_FIELDS_BY_SUBTYPE",
    "EODHD_IPO_READS",
    "EODHD_SPLIT_READS",
    "EODHD_SUBTYPE_PARSERS",
    "EODHD_SUBTYPE_READS",
    "EODHD_TREND_READS",
    "diff_eodhd_row",
    "plan_eodhd_probes",
    "run_eodhd_probe",
    "try_parse_eodhd",
]

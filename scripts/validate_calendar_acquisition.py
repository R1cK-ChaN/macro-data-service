#!/usr/bin/env python3
"""Live validation of the calendar *acquisition layer* against upstream APIs.

Scope: only the ``获取`` (fetch + parse) step. Storage and downstream API
are under our control and can be adjusted once we know the upstream
shape is understood correctly.

Default is ``--dry-run`` (plans and prints requests, no HTTP). Pass
``--execute`` to actually hit the upstream. The script prompts once
before spending requests unless ``--yes`` is passed.

Usage::

    # Dry run — see what would happen, zero HTTP
    PYTHONPATH=src python3 scripts/validate_calendar_acquisition.py \\
        --provider te

    # Live run — hits TE with ~5–6 requests
    PYTHONPATH=src python3 scripts/validate_calendar_acquisition.py \\
        --provider te --execute

Output: a markdown report under ``docs/validation/`` with per-probe
field diffs, enum observations, and parser dry-parse results.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

# Scaffold under test — we import and exercise exactly what production uses.
from ingestion.calendar.te_api import TEAPIClient, parse_calendar_row  # noqa: E402
from ingestion.calendar.te_api.client import TECallResult  # noqa: E402
from ingestion.calendar.te_api.parser import ALL_TE_FIELDS, MUTABLE_FIELDS  # noqa: E402

from ingestion.calendar.eodhd_api import (  # noqa: E402
    EODHDAPIClient,
    parse_dividend_detail_row,
    parse_dividend_row,
    parse_earnings_row,
    parse_ipo_row,
    parse_split_row,
    parse_trend_row,
)

from ingestion.calendar.bls_api import (  # noqa: E402
    INDICATOR_REGISTRY as BLS_INDICATOR_REGISTRY,
    parse_observation as parse_bls_observation,
)
from ingestion.timeseries.scrapers.bls import BLSClient  # noqa: E402


# Fields the TE parser actively reads (grep parser.py — these are the
# only row.get() keys that flow into CalendarEventRecord). Anything
# observed outside this set is ignored at parse time.
TE_PARSER_READS: frozenset[str] = frozenset({
    "CalendarId", "Date", "Country", "Category", "Event",
    "Reference", "ReferenceDate", "Source", "SourceURL",
    "Actual", "Previous", "Forecast", "TEForecast", "Revised",
    "Importance", "Currency", "Unit", "Ticker", "LastUpdate",
})

# /calendar/updates returns a reduced pointer shape by design. Flagging
# the missing value/classification fields as "MISSING_EXPECTED" against
# the full 22-field read set is noise — check only the pointer set for
# those probes.
TE_UPDATES_POINTER_READS: frozenset[str] = frozenset({
    "CalendarId", "Country", "Event", "LastUpdate",
})


# ──────────────────────────────────────────────────────────────────────────
# EODHD parser-reads per subtype (grep src/ingestion/calendar/eodhd_api/
# parser.py — these are the keys each parse_*_row function accesses or
# hashes). Compared against the observed row to surface UNKNOWN_OBSERVED
# (EODHD added a field we don't see) and MISSING_EXPECTED (our parser
# reads something upstream didn't return).
# ──────────────────────────────────────────────────────────────────────────

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


# ──────────────────────────────────────────────────────────────────────────
# Probe definitions
# ──────────────────────────────────────────────────────────────────────────


@dataclass
class Probe:
    """One planned upstream request.

    ``expected_fields`` differs per probe (TE pointer shape has fewer
    fields than full; each EODHD subtype has its own read set).
    ``params`` + ``rows_key`` + ``subtype`` are EODHD-only; TE probes
    embed everything in ``path`` and ignore these.
    """

    name: str
    path: str
    description: str
    expected_shape: str  # human-readable
    expected_fields: frozenset[str] = TE_PARSER_READS
    # EODHD-only knobs.
    params: dict[str, Any] = field(default_factory=dict)
    rows_key: str = ""
    subtype: str = ""
    # True for EODHD endpoints whose payload is itself the row list
    # (``/api/div/{TICKER}``); False for envelope-style calendar endpoints
    # that wrap rows under a subtype-specific key.
    top_level_array: bool = False


def _today_iso() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _days_ago_iso(n: int) -> str:
    return (datetime.now(timezone.utc).date() - timedelta(days=n)).isoformat()


def plan_te_probes() -> list[Probe]:
    """Fixed 5-probe budget for round 1. Extend only with evidence."""
    return [
        Probe(
            name="country_all_last_7d",
            path=f"/calendar/country/All/{_days_ago_iso(7)}/{_today_iso()}",
            description="baseline 22-field shape over a dense recent window",
            expected_shape="list[22-field dict]",
        ),
        Probe(
            name="country_all_2024_01",
            path="/calendar/country/All/2024-01-01/2024-01-07",
            description="older era — shape drift vs recent window",
            expected_shape="list[22-field dict]",
        ),
        Probe(
            # Country name with a space exercises URL encoding.
            name="country_us_last_7d",
            path=f"/calendar/country/{quote('United States')}/{_days_ago_iso(7)}/{_today_iso()}",
            description="country-scoped + URL encoding on spaces",
            expected_shape="list[22-field dict]",
        ),
        Probe(
            name="updates_pointer",
            path="/calendar/updates",
            description="pointer shape: is it really 4 fields?",
            expected_shape="list[pointer dict (CalendarId/Country/Event/LastUpdate)]",
            expected_fields=TE_UPDATES_POINTER_READS,
        ),
        Probe(
            # The 3 ids to rehydrate come from probe #1 at runtime — see
            # resolve_dynamic_probes below. If probe #1 returns nothing
            # we skip this probe entirely.
            name="calendarid_rehydrate",
            path="/calendar/calendarid/{ids}",  # template; filled at runtime
            description="rehydration shape vs /country/All full-row shape",
            expected_shape="list[22-field dict]",
        ),
    ]


# ──────────────────────────────────────────────────────────────────────────
# Diff helpers
# ──────────────────────────────────────────────────────────────────────────


@dataclass
class RowDiff:
    """Field-level audit of one observed row vs parser expectations."""

    observed_fields: list[str] = field(default_factory=list)
    read_by_parser: list[str] = field(default_factory=list)
    ignored_by_parser: list[str] = field(default_factory=list)
    unknown_observed: list[str] = field(default_factory=list)
    missing_expected: list[str] = field(default_factory=list)
    type_warnings: list[str] = field(default_factory=list)


@dataclass
class ProbeResult:
    probe: Probe
    status: str  # "skipped" | "ok" | "http_error" | "auth_missing"
    request_path: str = ""
    http_elapsed_ms: float = 0.0
    row_count: int = 0
    truncated: bool = False
    sample_row: dict[str, Any] | None = None
    # First N CalendarIds captured from this probe's rows — feeds the
    # calendarid_rehydrate probe so we hit real ids that were just
    # returned by /country/All.
    dynamic_ids_sample: list[str] = field(default_factory=list)
    field_diff: RowDiff | None = None
    parse_attempts: int = 0
    parse_successes: int = 0
    parse_error_samples: list[str] = field(default_factory=list)
    enum_counters: dict[str, Counter] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)


def diff_te_row(row: dict[str, Any], expected_fields: frozenset[str]) -> RowDiff:
    """Compare one observed TE row to the parser's known field set.

    ``expected_fields`` is the probe-specific subset parser reads
    (full 22-field for /country/All and /calendarid, pointer-only for
    /calendar/updates). Using one global set would over-report
    MISSING_EXPECTED on the updates probe.
    """
    observed = set(row.keys())
    diff = RowDiff(
        observed_fields=sorted(observed),
        read_by_parser=sorted(observed & expected_fields),
        # Known-to-TE fields we saw but parser never reads (e.g. DateSpan,
        # URL, Symbol). Informational — not a bug, but worth surfacing.
        ignored_by_parser=sorted((ALL_TE_FIELDS & observed) - expected_fields),
        unknown_observed=sorted(observed - ALL_TE_FIELDS),
        missing_expected=sorted(expected_fields - observed),
    )

    # Type spot-checks — the fields parser does explicit type handling on.
    imp = row.get("Importance")
    if imp is not None and not isinstance(imp, int):
        diff.type_warnings.append(
            f"Importance is {type(imp).__name__}={imp!r} — parser expects int "
            f"(falls through to None → importance column always null)"
        )
    cid = row.get("CalendarId")
    if cid is None:
        diff.type_warnings.append("CalendarId is None — parser raises ValueError on this row")
    elif not isinstance(cid, (int, str)):
        diff.type_warnings.append(f"CalendarId is {type(cid).__name__}={cid!r}")

    # Note: TE Date/LastUpdate strings arrive without timezone markers
    # (no Z / no offset suffix), but the values are already UTC —
    # verified against the NFIB release schedule on 2026-04-21. The
    # earlier "missing tz marker" heuristic was a false positive and has
    # been removed.
    return diff


def try_parse(row: dict[str, Any]) -> tuple[bool, str]:
    try:
        raw, event = parse_calendar_row(row, snapshot_epoch_ms=1_700_000_000_000)
        return True, f"ok provider_event_id={raw.provider_event_id[:10]}… country={event.country_code}"
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


# Enum-type fields we want to tally to surface the real vocabulary.
TE_ENUM_FIELDS: tuple[str, ...] = ("Importance", "Currency", "Country", "Category")


# ──────────────────────────────────────────────────────────────────────────
# Runner
# ──────────────────────────────────────────────────────────────────────────


def run_probe(
    client: TEAPIClient,
    probe: Probe,
    *,
    dynamic_ids: list[str] | None = None,
) -> ProbeResult:
    result = ProbeResult(probe=probe, status="skipped")

    path = probe.path
    if probe.name == "calendarid_rehydrate":
        if not dynamic_ids:
            result.notes.append("skipped — no ids from probe 1 to rehydrate")
            return result
        path = f"/calendar/calendarid/{','.join(dynamic_ids)}"
    result.request_path = path

    try:
        call: TECallResult = client.get(path)
    except Exception as exc:
        result.status = "http_error"
        result.notes.append(f"{type(exc).__name__}: {exc}")
        return result

    result.status = "ok"
    result.http_elapsed_ms = call.elapsed_ms
    result.row_count = call.row_count
    result.truncated = call.truncated

    if call.rows:
        rows = call.rows
        result.sample_row = rows[0]
        result.field_diff = diff_te_row(rows[0], probe.expected_fields)
        # Capture up to 3 CalendarIds for downstream rehydrate probe.
        for row in rows[:50]:
            cid = row.get("CalendarId")
            if cid is not None:
                result.dynamic_ids_sample.append(str(cid))
                if len(result.dynamic_ids_sample) >= 3:
                    break

        # Enum counters — tally across all rows to see real vocabulary.
        counters: dict[str, Counter] = {k: Counter() for k in TE_ENUM_FIELDS}
        for row in rows:
            for key in TE_ENUM_FIELDS:
                counters[key][repr(row.get(key))] += 1
        result.enum_counters = counters

        # Dry-parse up to 10 rows — enough to catch systematic breakage
        # without re-scanning 1000-row responses.
        sample_n = min(10, len(rows))
        result.parse_attempts = sample_n
        for row in rows[:sample_n]:
            ok, msg = try_parse(row)
            if ok:
                result.parse_successes += 1
            else:
                if len(result.parse_error_samples) < 3:
                    result.parse_error_samples.append(msg)
    return result


def resolve_dynamic_ids(prior: list[ProbeResult]) -> list[str]:
    """Pull up to 3 CalendarIds from the baseline recent-window probe."""
    for pr in prior:
        if pr.status == "ok" and pr.probe.name == "country_all_last_7d":
            return list(pr.dynamic_ids_sample)
    return []


# ──────────────────────────────────────────────────────────────────────────
# EODHD probe plan + runner
# ──────────────────────────────────────────────────────────────────────────


def _days_ahead_iso(n: int) -> str:
    return (datetime.now(timezone.utc).date() + timedelta(days=n)).isoformat()


def plan_eodhd_probes() -> list[Probe]:
    """Eight probes covering all five corporate subtypes.

    Two dividend variants exercise the ``filter[symbol]`` vs
    ``filter[date_eq]`` paths, and two trend variants confirm whether
    ``/calendar/trends`` really returns ``[[...]]`` for multi-symbol
    requests (and what it does for a single-symbol request).
    """
    today = _today_iso()
    week_ahead = _days_ahead_iso(7)
    month_ahead = _days_ahead_iso(30)
    week_ago = _days_ago_iso(7)
    yesterday = _days_ago_iso(1)
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
            params={"from": _days_ago_iso(180), "to": _today_iso()},
            rows_key="",
            subtype="dividend_detail",
            expected_fields=EODHD_DIVIDEND_DETAIL_READS,
            top_level_array=True,
        ),
    ]


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
    result.request_path = f"{probe.path}?{_render_params(probe.params)}"

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


def _render_params(params: dict[str, Any]) -> str:
    if not params:
        return ""
    return "&".join(f"{k}={v}" for k, v in params.items())


# ──────────────────────────────────────────────────────────────────────────
# Report writer
# ──────────────────────────────────────────────────────────────────────────


def _json_pretty(obj: Any) -> str:
    return json.dumps(obj, indent=2, ensure_ascii=False, sort_keys=True, default=str)


def render_report(
    results: list[ProbeResult], *, requests_spent: int, provider: str,
) -> str:
    today = datetime.now(timezone.utc).date().isoformat()
    provider_label = {
        "te": "TE", "eodhd": "EODHD", "bls": "BLS",
    }.get(provider, provider.upper())
    budget_line = {
        "te":    "- TE basic-plan monthly cap: 1000",
        "eodhd": "- EODHD All-in-One plan: per-call consumption (no tight cap)",
        "bls":   "- BLS Public Data API v2 free-tier daily cap: 500",
    }.get(provider, f"- {provider_label} plan: unknown cap")
    lines: list[str] = [
        f"# {provider_label} Calendar Acquisition Validation — {today}",
        "",
        "Scope: verifies the **acquisition** step only (fetch + parse).",
        "Storage and downstream API are under our control and may be "
        "adjusted once upstream shape is understood correctly.",
        "",
        "## Budget",
        "",
        f"- Requests spent this run: **{requests_spent}**",
        budget_line,
        f"- Probes planned: {len(results)} / executed: {sum(1 for r in results if r.status == 'ok')}",
        "",
        "## Probes",
        "",
    ]

    for idx, r in enumerate(results, 1):
        lines.extend(_render_probe_section(idx, r))

    lines.append("## Summary")
    lines.append("")
    any_unknown = any(r.field_diff and r.field_diff.unknown_observed for r in results if r.field_diff)
    any_missing = any(r.field_diff and r.field_diff.missing_expected for r in results if r.field_diff)
    any_type = any(r.field_diff and r.field_diff.type_warnings for r in results if r.field_diff)
    any_parse_err = any(r.parse_successes < r.parse_attempts for r in results)
    lines.append(f"- Unknown-observed fields: {'⚠️ found' if any_unknown else '✓ none'}")
    lines.append(f"- Missing-expected fields: {'⚠️ found' if any_missing else '✓ none'}")
    lines.append(f"- Type mismatches: {'⚠️ found' if any_type else '✓ none'}")
    lines.append(f"- Parse failures in sample: {'⚠️ found' if any_parse_err else '✓ none'}")
    lines.append("")
    lines.append("### Action items")
    lines.append("")
    if not (any_unknown or any_missing or any_type or any_parse_err):
        lines.append("- Acquisition layer matches parser expectations. No scaffold changes required.")
    else:
        if any_unknown:
            lines.append("- Review UNKNOWN_OBSERVED fields per probe — may be new TE columns "
                         "worth reading or ignoring explicitly.")
        if any_missing:
            lines.append("- Review MISSING_EXPECTED — parser reads fields that never arrived. "
                         "Either defensive defaults are masking it or we're overspec'd.")
        if any_type:
            lines.append("- Review type warnings — type coercion quirks that silently corrupt "
                         "event rows.")
        if any_parse_err:
            lines.append("- Parser dry-parse failed on real rows. Check error samples per probe.")
    lines.append("")
    return "\n".join(lines)


def _render_probe_section(idx: int, r: ProbeResult) -> list[str]:
    lines: list[str] = [
        f"### Probe {idx} — `{r.probe.name}`",
        "",
        f"- Purpose: {r.probe.description}",
        f"- Expected shape: `{r.probe.expected_shape}`",
        f"- Request path: `{r.request_path or r.probe.path}`",
        f"- Status: **{r.status}**",
    ]
    if r.status == "ok":
        lines.append(f"- HTTP elapsed: {r.http_elapsed_ms:.0f} ms")
        lines.append(f"- Row count: {r.row_count}{' (⚠️ truncated at 1000)' if r.truncated else ''}")
    if r.notes:
        for note in r.notes:
            lines.append(f"- Note: {note}")
    lines.append("")

    if r.field_diff is not None:
        d = r.field_diff
        lines.append("#### Field diff (first row)")
        lines.append("")
        lines.append(f"- Observed: {_fmt_field_list(d.observed_fields)}")
        lines.append(f"- Read by parser: {_fmt_field_list(d.read_by_parser)}")
        lines.append(f"- Ignored by parser (known-but-unread): {_fmt_field_list(d.ignored_by_parser)}")
        if d.unknown_observed:
            lines.append(f"- ⚠️ **UNKNOWN_OBSERVED**: {_fmt_field_list(d.unknown_observed)}")
        else:
            lines.append("- UNKNOWN_OBSERVED: ✓ none")
        if d.missing_expected:
            lines.append(f"- ⚠️ **MISSING_EXPECTED**: {_fmt_field_list(d.missing_expected)}")
        else:
            lines.append("- MISSING_EXPECTED: ✓ none")
        if d.type_warnings:
            lines.append("- ⚠️ **Type warnings**:")
            for w in d.type_warnings:
                lines.append(f"  - {w}")
        lines.append("")

    if r.enum_counters:
        lines.append("#### Enum observations (all rows)")
        lines.append("")
        for key, counter in r.enum_counters.items():
            if not counter:
                continue
            top = counter.most_common(8)
            rendered = ", ".join(f"{k}={v}" for k, v in top)
            more = f" (+{len(counter) - len(top)} more values)" if len(counter) > len(top) else ""
            lines.append(f"- `{key}`: {rendered}{more}")
        lines.append("")

    if r.parse_attempts:
        lines.append(
            f"#### Parser dry-parse: {r.parse_successes}/{r.parse_attempts} rows parsed"
        )
        if r.parse_error_samples:
            lines.append("")
            for sample in r.parse_error_samples:
                lines.append(f"- {sample}")
        lines.append("")

    if r.sample_row is not None:
        lines.append("<details><summary>Sample row JSON</summary>")
        lines.append("")
        lines.append("```json")
        lines.append(_json_pretty(r.sample_row))
        lines.append("```")
        lines.append("")
        lines.append("</details>")
        lines.append("")

    return lines


def _fmt_field_list(fields: list[str]) -> str:
    if not fields:
        return "(none)"
    return ", ".join(f"`{f}`" for f in fields)


# ──────────────────────────────────────────────────────────────────────────
# BLS probe plan + runner
# ──────────────────────────────────────────────────────────────────────────

# Field set each BLSObservation carries in its ``raw`` dict — this is what
# the BLS API returns per observation row inside ``Results.series[].data[]``.
# The parser reads ``year`` + ``period`` to compute ``date``, ``value`` for
# the numeric, and keeps the entire dict in ``BLSObservation.raw`` so
# footnote-only revisions register a new content hash on the calendar side.
BLS_OBS_EXPECTED_FIELDS: frozenset[str] = frozenset({
    "year", "period", "periodName", "value", "footnotes", "latest",
})

# Structural assertions on a parsed BLSObservation:
#   - period matches M01..M13 (monthly) / Q01..Q05 (quarterly) / A01 / S01..S02.
#   - value is numeric (the client's _parse_observation already coerces).
#   - date is ISO YYYY-MM-DD.
_BLS_PERIOD_RE = re.compile(r"^(M0[1-9]|M1[0-3]|Q0[1-5]|A01|S0[1-2])$")


@dataclass
class BLSProbe:
    """One BLS live-validation probe — a single series + year window.

    Separate dataclass from :class:`Probe` because BLS probes drive
    ``BLSClient.get_series_single`` rather than a raw-path HTTP call;
    the generic Probe shape (``path``, ``params``, ``rows_key``,
    ``subtype``) doesn't fit cleanly.
    """

    name: str
    series_id: str
    indicator: str          # canonical token (``"CPI"``, ``"NFP"``)
    description: str
    start_year: int
    end_year: int


def plan_bls_probes() -> list[BLSProbe]:
    """Two probes covering the P1 anchor indicators — CPI + NFP.

    Window is the current year plus the prior year, which guarantees
    at least 12 months of observations for any call after January
    while staying well under the BLS 500-requests-per-day budget
    (the two probes consume 2 requests total).
    """
    now_year = datetime.now(timezone.utc).year
    return [
        BLSProbe(
            name="cpi_two_year_window",
            series_id="CUUR0000SA0",
            indicator="CPI",
            description=(
                "CPI-U, All Items, NSA — headline US inflation print "
                "(highest trader-impact BLS release)"
            ),
            start_year=now_year - 1,
            end_year=now_year,
        ),
        BLSProbe(
            name="nfp_two_year_window",
            series_id="CES0000000001",
            indicator="NFP",
            description=(
                "Total nonfarm employment, SA (thousands) — the BLS "
                "Employment Situation headline"
            ),
            start_year=now_year - 1,
            end_year=now_year,
        ),
    ]


def _diff_bls_observation(raw_row: dict[str, Any]) -> RowDiff:
    """Field diff for one BLS observation row (the ``raw`` dict)."""
    observed = set(raw_row.keys())
    diff = RowDiff(
        observed_fields=sorted(observed),
        read_by_parser=sorted(observed & BLS_OBS_EXPECTED_FIELDS),
        ignored_by_parser=sorted(
            (BLS_OBS_EXPECTED_FIELDS & observed) - BLS_OBS_EXPECTED_FIELDS
        ),  # empty by construction — kept for shape symmetry
        unknown_observed=sorted(observed - BLS_OBS_EXPECTED_FIELDS),
        missing_expected=sorted(BLS_OBS_EXPECTED_FIELDS - observed),
    )

    period = raw_row.get("period", "")
    if period and not _BLS_PERIOD_RE.match(str(period)):
        diff.type_warnings.append(
            f"period={period!r} doesn't match M01-M13 / Q01-Q05 / A01 / "
            f"S01-S02 shape"
        )
    value = raw_row.get("value")
    if value is None:
        diff.type_warnings.append("value missing — parser would drop this row")
    elif not isinstance(value, str):
        diff.type_warnings.append(
            f"value is {type(value).__name__}={value!r} — parser expects str"
        )
    return diff


def _try_parse_bls(obs) -> tuple[bool, str]:
    """Dry-run the calendar-side BLS projector on one observation."""
    spec = BLS_INDICATOR_REGISTRY.get(obs.series_id)
    if spec is None:
        return False, f"no BLS calendar spec for series_id={obs.series_id!r}"
    try:
        raw, event = parse_bls_observation(
            obs, snapshot_epoch_ms=1_700_000_000_000, spec=spec,
        )
        return True, (
            f"ok indicator={spec.indicator} event_id={raw.provider_event_id[:10]}…"
        )
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def run_bls_probe(client: BLSClient, probe: BLSProbe) -> ProbeResult:
    """Execute one BLS probe and return a populated :class:`ProbeResult`.

    Runs via ``BLSClient.get_series_single`` — reuses the production
    transport layer so the probe exercises exactly the path a recurring
    fetch would. Rate-limit / auth errors surface as ``http_error``;
    empty responses surface as ``ok`` with ``row_count=0`` so the
    report still prints the probe card.
    """
    # BLS uses a POST body rather than a GET path; the audit trail
    # needs to describe both the wire shape (endpoint) and the
    # body-level identity (series id + year window) so a reader can
    # reproduce the call from the report alone.
    generic = Probe(
        name=probe.name,
        path="POST https://api.bls.gov/publicAPI/v2/timeseries/data/",
        description=probe.description,
        expected_shape="list[BLSObservation] after client parse",
        expected_fields=BLS_OBS_EXPECTED_FIELDS,
    )
    result = ProbeResult(probe=generic, status="skipped")
    result.request_path = (
        f"{generic.path} "
        f"seriesid=[{probe.series_id}] "
        f"startyear={probe.start_year} endyear={probe.end_year}"
    )

    if not client.api_key:
        result.status = "auth_missing"
        result.notes.append("BLS_API_KEY not set — probe skipped")
        return result

    import time as _time
    t0 = _time.monotonic()
    try:
        observations = client.get_series_single(
            probe.series_id,
            start_year=probe.start_year,
            end_year=probe.end_year,
        )
    except Exception as exc:
        result.status = "http_error"
        result.notes.append(f"{type(exc).__name__}: {exc}")
        return result
    result.http_elapsed_ms = (_time.monotonic() - t0) * 1000

    result.status = "ok"
    result.row_count = len(observations)
    if not observations:
        result.notes.append("BLS returned zero observations for this window")
        return result

    # Sort newest-first so the sample row + diff reflect the most recent
    # release — that's the one whose shape we care about for schedule
    # adherence audits.
    observations = sorted(observations, key=lambda o: o.date, reverse=True)
    sample = observations[0]
    result.sample_row = sample.raw or {
        "series_id": sample.series_id,
        "date": sample.date,
        "value": sample.value,
        "period": sample.period,
    }
    if sample.raw:
        result.field_diff = _diff_bls_observation(sample.raw)

    # period / periodName tallies — surfaces mixed-frequency responses
    # (monthly + annual average rows) when the client passes
    # annual_average=False but BLS attaches A01 anyway.
    period_counter: Counter = Counter()
    for obs in observations:
        period_counter[repr(obs.period)] += 1
    result.enum_counters = {"period": period_counter}

    sample_n = min(10, len(observations))
    result.parse_attempts = sample_n
    for obs in observations[:sample_n]:
        ok, msg = _try_parse_bls(obs)
        if ok:
            result.parse_successes += 1
        else:
            if len(result.parse_error_samples) < 3:
                result.parse_error_samples.append(msg)
    return result


# ──────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────


_OFFICIAL_PROVIDERS: frozenset[str] = frozenset({"bls", "bea", "fed", "ecb", "nbs"})
_OFFICIAL_PROVIDERS_WITH_PROBES: frozenset[str] = frozenset({"bls"})


def parse_args(argv: list[str]) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument(
        "--provider",
        choices=["te", "eodhd", "bls", "bea", "fed", "ecb", "nbs"],
        default="te",
        help=(
            "which acquisition lane to validate. "
            "bls/bea/fed/ecb/nbs are scaffold-only in issue #9 P0 — "
            "probe bodies land in subsequent phases."
        ),
    )
    ap.add_argument(
        "--execute", action="store_true",
        help="actually hit the upstream; default is dry-run (plan only)",
    )
    ap.add_argument(
        "--yes", action="store_true",
        help="skip the budget-confirmation prompt",
    )
    ap.add_argument(
        "--report-dir",
        default=str(REPO_ROOT / "docs" / "validation"),
        help="where to write the markdown report (default: docs/validation/)",
    )
    return ap.parse_args(argv)


def confirm_budget(probes: list[Probe]) -> bool:
    print(f"Planned probes ({len(probes)}):")
    for i, p in enumerate(probes, 1):
        print(f"  {i}. {p.name} — {p.path}")
    print(f"Estimated upstream requests: {len(probes)}")
    resp = input("Proceed with live run? [y/N] ").strip().lower()
    return resp in {"y", "yes"}


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    # Official-source connectors with no probe body yet land on the
    # scaffold-only stub. BLS now has a live probe; BEA / Fed / ECB /
    # NBS still report back as scaffold-registered.
    unwired = _OFFICIAL_PROVIDERS - _OFFICIAL_PROVIDERS_WITH_PROBES
    if args.provider in unwired:
        print(
            f"--provider {args.provider}: scaffold registered (issue #9 P0). "
            f"Probe bodies land in the phase that ships the {args.provider} "
            f"connector live probe. No HTTP, no report written."
        )
        return 0

    if args.provider == "bls":
        bls_probes = plan_bls_probes()
        return _run_bls(args, bls_probes)

    probes = plan_te_probes() if args.provider == "te" else plan_eodhd_probes()

    if not args.execute:
        print(f"DRY RUN ({args.provider}) — pass --execute to actually hit upstream.")
        print()
        for i, p in enumerate(probes, 1):
            print(f"{i}. {p.name}")
            query = _render_params(p.params)
            print(f"   path: {p.path}{'?' + query if query else ''}")
            print(f"   purpose: {p.description}")
            print(f"   expected shape: {p.expected_shape}")
        print()
        print(f"Total planned requests: {len(probes)}")
        return 0

    if not args.yes and not confirm_budget(probes):
        print("Aborted.")
        return 1

    results: list[ProbeResult] = []
    if args.provider == "te":
        with TEAPIClient() as client:
            for probe in probes:
                if probe.name == "calendarid_rehydrate":
                    dynamic_ids = resolve_dynamic_ids(results)
                    result = run_probe(client, probe, dynamic_ids=dynamic_ids)
                else:
                    result = run_probe(client, probe)
                results.append(result)
                _print_probe_summary(result)
    else:
        with EODHDAPIClient() as client:
            for probe in probes:
                result = run_eodhd_probe(client, probe)
                results.append(result)
                _print_probe_summary(result)

    report = render_report(
        results,
        requests_spent=sum(1 for r in results if r.status == "ok"),
        provider=args.provider,
    )
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / (
        f"calendar_acquisition_{args.provider}_"
        f"{datetime.now(timezone.utc).date().isoformat()}.md"
    )
    report_path.write_text(report, encoding="utf-8")
    print()
    print(f"Report written: {report_path}")
    return 0


def _run_bls(args: argparse.Namespace, probes: list[BLSProbe]) -> int:
    """Dispatch the BLS probe flow — dry-run plan vs --execute live run.

    BLS doesn't need the TE / EODHD budget-confirm prompt because the
    probe set is small (2 requests for P1) and BLS_API_KEY is a free-
    tier key with a 500-req-daily budget. Still honours ``--yes`` to
    match the other providers' muscle memory.
    """
    if not args.execute:
        print(f"DRY RUN (bls) — pass --execute to actually hit upstream.")
        print()
        for i, p in enumerate(probes, 1):
            print(f"{i}. {p.name}")
            print(f"   series: {p.series_id} ({p.indicator})")
            print(f"   window: {p.start_year}-{p.end_year}")
            print(f"   purpose: {p.description}")
        print()
        print(f"Total planned requests: {len(probes)}")
        return 0

    if not args.yes:
        print(f"Planned BLS probes ({len(probes)}):")
        for i, p in enumerate(probes, 1):
            print(f"  {i}. {p.name} — {p.series_id} {p.start_year}-{p.end_year}")
        print(f"Estimated upstream requests: {len(probes)}")
        resp = input("Proceed with live run? [y/N] ").strip().lower()
        if resp not in {"y", "yes"}:
            print("Aborted.")
            return 1

    results: list[ProbeResult] = []
    client = BLSClient()
    for probe in probes:
        result = run_bls_probe(client, probe)
        results.append(result)
        _print_probe_summary(result)

    report = render_report(
        results,
        requests_spent=sum(1 for r in results if r.status == "ok"),
        provider="bls",
    )
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / (
        f"calendar_acquisition_bls_"
        f"{datetime.now(timezone.utc).date().isoformat()}.md"
    )
    report_path.write_text(report, encoding="utf-8")
    print()
    print(f"Report written: {report_path}")
    return 0


def _print_probe_summary(r: ProbeResult) -> None:
    tag = {"ok": "✓", "skipped": "-", "http_error": "✗", "auth_missing": "✗"}.get(r.status, "?")
    row_info = f"{r.row_count} rows" if r.status == "ok" else r.status
    print(f"  {tag} {r.probe.name}: {row_info}")
    if r.field_diff and r.field_diff.unknown_observed:
        print(f"      ⚠️ unknown fields: {r.field_diff.unknown_observed}")
    if r.field_diff and r.field_diff.missing_expected:
        print(f"      ⚠️ missing fields: {r.field_diff.missing_expected}")
    if r.field_diff and r.field_diff.type_warnings:
        for w in r.field_diff.type_warnings[:2]:
            print(f"      ⚠️ {w}")


if __name__ == "__main__":
    sys.exit(main())

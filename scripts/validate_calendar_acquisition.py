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

from ingestion.calendar.bea_api import (  # noqa: E402
    INDICATOR_REGISTRY as BEA_INDICATOR_REGISTRY,
    parse_observation as parse_bea_observation,
)
from ingestion.timeseries.scrapers.bea import BEAClient  # noqa: E402

from ingestion.calendar.ecb_api import (  # noqa: E402
    INDICATOR_REGISTRY as ECB_INDICATOR_REGISTRY,
    parse_observation as parse_ecb_observation,
)
from ingestion.calendar.ecb_api.fetcher import (  # noqa: E402
    _collapse_to_rate_changes as _ecb_collapse_to_rate_changes,
)
from ingestion.timeseries.sdmx._types import SDMXObservation  # noqa: E402
from ingestion.timeseries.sdmx.providers.ecb import ECBClient  # noqa: E402

from ingestion.calendar.fed_api import (  # noqa: E402
    FED_CALENDAR_JSON_URL,
    FOMC_CALENDAR_URL,
    FomcMeetingEntry,
    FedReleaseEntry,
    INDICATOR_REGISTRY as FED_INDICATOR_REGISTRY,
    fetch_fed_calendar_json,
    fetch_fomc_calendar_html,
    meeting_entry_to_records,
    parse_fed_calendar_json,
    parse_fomc_calendar_html,
    release_entry_to_records,
)

from ingestion.calendar.nbs_api import (  # noqa: E402
    INDICATOR_REGISTRY as NBS_INDICATOR_REGISTRY,
    NBSReleaseEntry,
    discover_nbs_calendar_url,
    fetch_nbs_calendar_index_html,
    fetch_nbs_yearly_calendar_html,
    parse_nbs_calendar_html,
    release_entry_to_records as nbs_release_entry_to_records,
)


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
        "te": "TE", "eodhd": "EODHD", "bls": "BLS", "bea": "BEA",
        "ecb": "ECB", "fed": "Fed", "nbs": "NBS",
    }.get(provider, provider.upper())
    budget_line = {
        "te":    "- TE basic-plan monthly cap: 1000",
        "eodhd": "- EODHD All-in-One plan: per-call consumption (no tight cap)",
        "bls":   "- BLS Public Data API v2 free-tier daily cap: 500",
        "bea":   "- BEA REST API free-tier daily cap: 1000",
        "ecb":   "- ECB Data Portal: no auth, unspecified rate limit (polite)",
        "fed":   "- federalreserve.gov: no auth, HTML scrape (browser-UA required)",
        "nbs":   "- stats.gov.cn: no auth, HTTP-only, flaky from non-CN IPs",
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
    # Probes that http-errored or were skipped for missing auth never
    # populate ``field_diff`` / ``parse_attempts``, so the field-diff
    # counters alone can't tell the acquisition-layer-clean story. Without
    # this guard the summary claims "No scaffold changes required" on
    # runs where every probe 404'd (observed on the 2026-04-22 P4b-live
    # Fed run). Split the signal: good-run iff every probe returned ``ok``.
    failed_probes = [r for r in results if r.status != "ok"]
    lines.append(f"- Unknown-observed fields: {'⚠️ found' if any_unknown else '✓ none'}")
    lines.append(f"- Missing-expected fields: {'⚠️ found' if any_missing else '✓ none'}")
    lines.append(f"- Type mismatches: {'⚠️ found' if any_type else '✓ none'}")
    lines.append(f"- Parse failures in sample: {'⚠️ found' if any_parse_err else '✓ none'}")
    lines.append(
        f"- Probe-level failures: "
        f"{'⚠️ ' + str(len(failed_probes)) + ' of ' + str(len(results)) if failed_probes else '✓ none'}"
    )
    lines.append("")
    lines.append("### Action items")
    lines.append("")
    if not (any_unknown or any_missing or any_type or any_parse_err or failed_probes):
        lines.append("- Acquisition layer matches parser expectations. No scaffold changes required.")
    else:
        if failed_probes:
            lines.append(
                f"- {len(failed_probes)} probe(s) failed outright "
                f"(status ≠ ``ok``). Each probe's Note lines carry the "
                f"error — upstream drift (URL / DOM / payload shape) is "
                f"the most common cause. Resolve before treating the "
                f"remaining field-diff signal as authoritative."
            )
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
    """One probe per entry in the BLS calendar INDICATOR_REGISTRY.

    P1c expanded the whitelist from 2 anchors (CPI + NFP) to 11
    indicators covering the full BLS headline set. Each probe hits
    one series with a two-year window — the current year plus the
    prior year — which guarantees coverage across the monthly /
    quarterly release cadences the whitelist mixes. Total BLS API
    usage per live run is one request per series (~11), well under
    the 500-requests-per-day budget.
    """
    now_year = datetime.now(timezone.utc).year
    probes: list[BLSProbe] = []
    for series_id, spec in BLS_INDICATOR_REGISTRY.items():
        token = _bls_probe_token(spec.indicator)
        probes.append(
            BLSProbe(
                name=f"{token}_two_year_window",
                series_id=series_id,
                indicator=token.upper(),
                description=spec.title,
                start_year=now_year - 1,
                end_year=now_year,
            )
        )
    return probes


def _bls_probe_token(indicator_label: str) -> str:
    """Slugify an indicator label into a stable probe-name token."""
    token = indicator_label.strip().lower()
    token = re.sub(r"[^a-z0-9]+", "_", token).strip("_")
    return token or "indicator"


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
# BEA probe plan + runner
# ──────────────────────────────────────────────────────────────────────────

# Fields each BEAObservation carries in its ``raw`` dict — keys the parser
# hashes on (``_HASH_FIELDS = (DataValue, LineDescription, NoteRef)``) plus
# the identity fields (``TimePeriod``, ``LineNumber``). Anything observed
# outside this set surfaces as UNKNOWN_OBSERVED — the prompt to decide
# whether a new upstream column should be read or explicitly ignored.
BEA_OBS_EXPECTED_FIELDS: frozenset[str] = frozenset({
    "TimePeriod", "DataValue", "LineNumber", "LineDescription", "NoteRef",
})


@dataclass
class BEAProbe:
    """One BEA live-validation probe — a single series + year window.

    Mirrors :class:`BLSProbe`. BEA's query surface differs from BLS
    (``GET /api/data`` with ``DatasetName`` / ``TableName`` / ``Frequency``
    / ``Year`` rather than a POST body), so this dataclass carries the
    call coordinates the runner needs; the generic :class:`Probe` shape
    still carries the report metadata on the :class:`ProbeResult`.
    """

    name: str
    series_id: str
    indicator: str          # canonical token (``"GDP"``, ``"PERSONAL_INCOME"``)
    dataset: str            # BEA dataset name (``"NIPA"``, ``"ITA"``)
    table: str              # BEA table code (``"T10101"``, ``"T20600"``)
    line_number: str        # BEA line within the table (``"1"``)
    frequency: str          # BEA frequency code (``"Q"``, ``"M"``, ``"A"``)
    description: str
    start_year: int
    end_year: int


def plan_bea_probes() -> list[BEAProbe]:
    """One probe per entry in the BEA calendar ``INDICATOR_REGISTRY``.

    Each probe hits one ``(dataset, table, frequency)`` coordinate with
    a two-year window — current year plus prior — which guarantees
    coverage across the monthly + quarterly cadences the whitelist
    mixes. The runner filters the returned rows down to the probe's
    ``line_number`` after the response lands (BEA returns every line
    of the requested table in a single call).

    Total BEA API usage per live run is one request per indicator
    (currently 2 — GDP + Personal Income), well under the 1000-per-day
    free-tier budget. The registry includes entries with
    ``api_fetch=False`` (GDP, schedule-only) — the probe still exercises
    their API shape so an operator can verify the parser's expectations
    before enabling an API lane.
    """
    now_year = datetime.now(timezone.utc).year
    probes: list[BEAProbe] = []
    for series_id, spec in BEA_INDICATOR_REGISTRY.items():
        token = _bea_probe_token(spec.indicator)
        probes.append(
            BEAProbe(
                name=f"{token}_two_year_window",
                series_id=series_id,
                indicator=spec.indicator,
                dataset=spec.dataset,
                table=spec.table,
                line_number=spec.line_number,
                frequency=spec.frequency,
                description=spec.title,
                start_year=now_year - 1,
                end_year=now_year,
            )
        )
    return probes


def _bea_probe_token(indicator_label: str) -> str:
    """Slugify an indicator label into a stable probe-name token."""
    token = indicator_label.strip().lower()
    token = re.sub(r"[^a-z0-9]+", "_", token).strip("_")
    return token or "indicator"


def _diff_bea_observation(raw_row: dict[str, Any]) -> RowDiff:
    """Field diff for one BEA observation row (the ``raw`` dict).

    Parser reads ``TimePeriod`` (→ reference date) and ``DataValue``
    (→ numeric); hashes on ``DataValue`` + ``LineDescription`` +
    ``NoteRef`` (methodology / release-stage flags). ``LineNumber`` is
    the identity coordinate on the line within the table. Any other
    field upstream returns — ``SeriesCode``, ``CL_UNIT``,
    ``UNIT_MULT``, ``METRIC_NAME`` are documented possibilities on some
    BEA datasets — surfaces as UNKNOWN_OBSERVED so the operator decides
    whether to read or ignore it.
    """
    observed = set(raw_row.keys())
    diff = RowDiff(
        observed_fields=sorted(observed),
        read_by_parser=sorted(observed & BEA_OBS_EXPECTED_FIELDS),
        ignored_by_parser=sorted(
            (BEA_OBS_EXPECTED_FIELDS & observed) - BEA_OBS_EXPECTED_FIELDS
        ),  # empty by construction — symmetry with BLS shape
        unknown_observed=sorted(observed - BEA_OBS_EXPECTED_FIELDS),
        missing_expected=sorted(BEA_OBS_EXPECTED_FIELDS - observed),
    )

    time_period = raw_row.get("TimePeriod")
    if time_period is None or str(time_period).strip() == "":
        diff.type_warnings.append(
            "TimePeriod missing — client would synthesize an empty reference date"
        )
    data_value = raw_row.get("DataValue")
    if data_value is None:
        diff.type_warnings.append("DataValue missing — parser would write null actual")
    elif not isinstance(data_value, str):
        # BEA documents DataValue as a string ("21,542.5" / "(NA)" /
        # "(D)") so the client's ``_parse_data_value`` can run its
        # suppressed-value detection. A non-string would bypass the
        # suppressed-value check and land as ``None``.
        diff.type_warnings.append(
            f"DataValue is {type(data_value).__name__}={data_value!r} — "
            f"parser expects str"
        )
    line_number = raw_row.get("LineNumber")
    if line_number is None:
        diff.type_warnings.append(
            "LineNumber missing — line filter + series-id synthesis both break"
        )
    return diff


def _try_parse_bea(obs) -> tuple[bool, str]:
    """Dry-run the calendar-side BEA projector on one observation."""
    spec = BEA_INDICATOR_REGISTRY.get(obs.series_id)
    if spec is None:
        return False, f"no BEA calendar spec for series_id={obs.series_id!r}"
    try:
        raw, event = parse_bea_observation(
            obs, snapshot_epoch_ms=1_700_000_000_000, spec=spec,
        )
        return True, (
            f"ok indicator={spec.indicator} event_id={raw.provider_event_id[:10]}…"
        )
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def run_bea_probe(client: BEAClient, probe: BEAProbe) -> ProbeResult:
    """Execute one BEA probe and return a populated :class:`ProbeResult`.

    Runs via ``BEAClient.get_data`` — reuses the production transport so
    throttling + error handling behave identically to a recurring fetch.
    BEA returns every line for the requested table; the runner filters
    down to the probe's ``line_number`` after the call lands. An empty
    filtered set lands as ``ok`` with ``row_count=0`` and a note that
    records the full-table row count — the signal that the coordinate
    drifted upstream vs the registry.
    """
    generic = Probe(
        name=probe.name,
        path=(
            f"GET https://apps.bea.gov/api/data?DatasetName={probe.dataset}"
            f"&TableName={probe.table}"
        ),
        description=probe.description,
        expected_shape="list[BEAObservation] after client parse",
        expected_fields=BEA_OBS_EXPECTED_FIELDS,
    )
    result = ProbeResult(probe=generic, status="skipped")
    year_param = ",".join(
        str(y) for y in range(probe.start_year, probe.end_year + 1)
    )
    result.request_path = (
        f"{generic.path}"
        f"&Frequency={probe.frequency}&Year={year_param} "
        f"(filter LineNumber={probe.line_number})"
    )

    if not client.api_key:
        result.status = "auth_missing"
        result.notes.append("BEA_API_KEY not set — probe skipped")
        return result

    import time as _time
    t0 = _time.monotonic()
    try:
        observations = client.get_data(
            probe.dataset,
            TableName=probe.table,
            Frequency=probe.frequency,
            Year=year_param,
        )
    except Exception as exc:
        result.status = "http_error"
        result.notes.append(f"{type(exc).__name__}: {exc}")
        return result
    result.http_elapsed_ms = (_time.monotonic() - t0) * 1000

    # BEA returns every line of the table; filter down to the probe's
    # line before shape-diffing so the sample row reflects the indicator
    # we actually care about rather than an arbitrary neighbour line.
    filtered = [o for o in observations if o.line_number == probe.line_number]

    result.status = "ok"
    result.row_count = len(filtered)
    if not filtered:
        result.notes.append(
            f"BEA returned zero observations for LineNumber={probe.line_number} "
            f"(table payload had {len(observations)} rows across other lines) "
            f"— coordinate may have drifted upstream"
        )
        return result

    # BEA ``obs.date`` is end-of-period ISO (quarterly/annual) or first-
    # of-month (monthly) — lexical sort descending puts the newest
    # observation first for both.
    filtered = sorted(filtered, key=lambda o: o.date, reverse=True)
    sample = filtered[0]
    result.sample_row = sample.raw or {
        "series_id": sample.series_id,
        "date": sample.date,
        "value": sample.value,
        "line_number": sample.line_number,
    }
    if sample.raw:
        result.field_diff = _diff_bea_observation(sample.raw)

    # TimePeriod tally — surfaces mixed-frequency responses (e.g. an
    # annual row landing inside a quarterly ``Frequency=Q`` query) that
    # would otherwise silently inflate the event stream.
    period_counter: Counter = Counter()
    for obs in filtered:
        period_counter[repr(obs.raw.get("TimePeriod") if obs.raw else None)] += 1
    result.enum_counters = {"TimePeriod": period_counter}

    sample_n = min(10, len(filtered))
    result.parse_attempts = sample_n
    for obs in filtered[:sample_n]:
        ok, msg = _try_parse_bea(obs)
        if ok:
            result.parse_successes += 1
        else:
            if len(result.parse_error_samples) < 3:
                result.parse_error_samples.append(msg)
    return result


# ──────────────────────────────────────────────────────────────────────────
# ECB probe plan + runner
# ──────────────────────────────────────────────────────────────────────────

# SDMX observations arrive as a typed dataclass (:class:`SDMXObservation`)
# rather than a raw upstream dict — the SDMX parser consumes the JSON
# envelope at the boundary. "Field diff" therefore reports the parser-
# facing attributes rather than upstream keys; upstream schema drift
# surfaces as empty-results or type warnings, not UNKNOWN_OBSERVED.
ECB_OBS_EXPECTED_FIELDS: frozenset[str] = frozenset({
    "series_id", "date", "value", "dataflow",
})


@dataclass
class ECBProbe:
    """One ECB SDMX live-validation probe — a single series + date window.

    Mirrors :class:`BLSProbe` / :class:`BEAProbe`. ECB's data path is
    ``GET /service/data/{dataflow}/{key}`` — no auth, `jsondata` format,
    ``startPeriod`` / ``endPeriod`` / ``lastNObservations`` query params.
    """

    name: str
    series_id: str
    dataflow_id: str
    series_key: str
    indicator: str
    description: str
    start_period: str     # ISO date
    end_period: str       # ISO date


def plan_ecb_probes() -> list[ECBProbe]:
    """One probe per entry in the ECB calendar ``INDICATOR_REGISTRY``.

    Each probe pulls roughly two years of business-daily observations
    for the series — enough to cover multiple Governing Council
    decisions in any realistic recent window. The runner reports both
    the raw count (~500/year of business days) and the collapsed rate-
    change count (~3–6 per window) so an operator can eyeball whether
    the fetcher would project the expected signal from the firehose.

    ECB requires no auth, so there's no ``api_key`` guard to bypass.
    Three probes per run — under the endpoint's unspecified but
    generous rate limit (we throttle via the SDMX client's retry
    shape regardless).
    """
    today = datetime.now(timezone.utc).date()
    # Calendar-day rollback rather than calendar-year so a leap year
    # doesn't change the window size; 730 days fully covers two GC
    # cycles (~8 scheduled decisions per year).
    two_years_ago = today - timedelta(days=730)
    probes: list[ECBProbe] = []
    for series_id, spec in ECB_INDICATOR_REGISTRY.items():
        probes.append(
            ECBProbe(
                name=f"{spec.indicator.lower()}_two_year_window",
                series_id=series_id,
                dataflow_id=spec.dataflow_id,
                series_key=spec.series_key,
                indicator=spec.indicator,
                description=spec.title,
                start_period=two_years_ago.isoformat(),
                end_period=today.isoformat(),
            )
        )
    return probes


def _diff_ecb_observation(obs: SDMXObservation) -> RowDiff:
    """Field diff for one SDMX observation.

    "Observed" means the attributes present + non-null after the SDMX
    parser has run — ECB's JSON envelope is already consumed at that
    boundary. Missing-field / type warnings surface parser mis-
    mappings rather than upstream schema drift; a fully empty
    response from ECB (series key retired upstream) is caught at the
    runner level, not here.
    """
    present = {
        "series_id": obs.series_id,
        "date":      obs.date,
        "value":     obs.value,
        "dataflow":  obs.dataflow,
    }
    observed = {k for k, v in present.items() if v not in (None, "")}
    diff = RowDiff(
        observed_fields=sorted(observed),
        read_by_parser=sorted(observed & ECB_OBS_EXPECTED_FIELDS),
        ignored_by_parser=[],   # no ambient upstream fields — dataclass is closed
        unknown_observed=[],    # likewise
        missing_expected=sorted(ECB_OBS_EXPECTED_FIELDS - observed),
    )
    if obs.value is None:
        diff.type_warnings.append(
            "value=None — rate-level series should always carry a numeric"
        )
    elif not isinstance(obs.value, (int, float)):
        diff.type_warnings.append(
            f"value is {type(obs.value).__name__}={obs.value!r} — "
            f"SDMX parser expects numeric"
        )
    if not obs.date:
        diff.type_warnings.append("date empty — parser would drop this row")
    return diff


def _try_parse_ecb(obs: SDMXObservation) -> tuple[bool, str]:
    """Dry-run the calendar-side ECB projector on one observation."""
    spec = ECB_INDICATOR_REGISTRY.get(obs.series_id)
    if spec is None:
        return False, f"no ECB calendar spec for series_id={obs.series_id!r}"
    try:
        raw, event = parse_ecb_observation(
            obs, snapshot_epoch_ms=1_700_000_000_000, spec=spec,
        )
        return True, (
            f"ok indicator={spec.indicator} event_id={raw.provider_event_id[:10]}…"
        )
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def run_ecb_probe(client: ECBClient, probe: ECBProbe) -> ProbeResult:
    """Execute one ECB probe and return a populated :class:`ProbeResult`.

    Runs via ``ECBClient.get_data`` — reuses the production SDMX
    transport so the probe exercises exactly the path a recurring
    fetch would. An empty response lands as ``ok`` with ``row_count=0``
    plus a note that the series key may have been retired upstream
    (the signal we need for P6 parity not to silently drift).
    """
    generic = Probe(
        name=probe.name,
        path=(
            f"GET https://data-api.ecb.europa.eu/service/data/"
            f"{probe.dataflow_id}/{probe.series_key}"
        ),
        description=probe.description,
        expected_shape="list[SDMXObservation] after client parse",
        expected_fields=ECB_OBS_EXPECTED_FIELDS,
    )
    result = ProbeResult(probe=generic, status="skipped")
    result.request_path = (
        f"{generic.path}?format=jsondata"
        f"&startPeriod={probe.start_period}"
        f"&endPeriod={probe.end_period}"
    )

    import time as _time
    t0 = _time.monotonic()
    try:
        observations = client.get_data(
            probe.dataflow_id,
            probe.series_key,
            series_id=probe.series_id,
            start_period=probe.start_period,
            end_period=probe.end_period,
            limit=0,  # no lastNObservations cap
        )
    except Exception as exc:
        result.status = "http_error"
        result.notes.append(f"{type(exc).__name__}: {exc}")
        return result
    result.http_elapsed_ms = (_time.monotonic() - t0) * 1000

    result.status = "ok"
    result.row_count = len(observations)
    if not observations:
        result.notes.append(
            "ECB returned zero observations for this series + window "
            "— series key may have been retired upstream"
        )
        return result

    # FM-lane publishes the same level every business day until the
    # Governing Council moves it. Expose both counts so the report
    # makes the ~500-per-2-years business-daily noise vs the ~8-per-
    # 2-years policy-move signal explicit.
    changes = _ecb_collapse_to_rate_changes(observations, prior_value=None)
    result.notes.append(
        f"raw observations: {len(observations)} | rate changes after "
        f"collapse: {len(changes)}"
    )

    # Newest-first via lexical date sort (ISO YYYY-MM-DD).
    sorted_obs = sorted(observations, key=lambda o: o.date, reverse=True)
    sample = sorted_obs[0]
    result.sample_row = {
        "series_id": sample.series_id,
        "date":      sample.date,
        "value":     sample.value,
        "dataflow":  sample.dataflow,
    }
    result.field_diff = _diff_ecb_observation(sample)

    # Unique rate-level tally — makes the policy-move cadence visible
    # inside the window, and flags oddities (sub-zero prints, unusual
    # precision) the parser should be ready for.
    value_counter: Counter = Counter()
    for obs in observations:
        value_counter[repr(obs.value)] += 1
    result.enum_counters = {"value": value_counter}

    # Dry-parse the collapsed-changes subset rather than the business-
    # daily firehose — the fetcher projects only these rows, so
    # they're the set whose parse failure would matter.
    target = changes if changes else sorted_obs
    sample_n = min(10, len(target))
    result.parse_attempts = sample_n
    for obs in target[:sample_n]:
        ok, msg = _try_parse_ecb(obs)
        if ok:
            result.parse_successes += 1
        else:
            if len(result.parse_error_samples) < 3:
                result.parse_error_samples.append(msg)
    return result


# ──────────────────────────────────────────────────────────────────────────
# Fed probe plan + runner
# ──────────────────────────────────────────────────────────────────────────

# Fed publishes no authenticated calendar API. ``fomccalendars.htm``
# is an HTML scrape; ``/json/calendar.json`` (Beige Book / H.4.1 / H.8
# alongside FOMC meetings and speeches) is a JSON feed. The probe
# runner doesn't do a per-field observation diff — the parser consumes
# the payload at the boundary. Upstream drift surfaces as zero parsed
# entries (parser raises) or non-empty ``row_issues`` (partial row-
# level failures after a title match).


@dataclass
class FedProbe:
    """One Fed live-validation probe — a single HTML surface.

    Fed has two scrape surfaces, each driven by its own fetcher +
    parser. ``source`` selects the dispatch branch inside
    :func:`run_fed_probe`; ``url`` is informational (printed in the
    dry-run plan + the report's request-path line).
    """

    name: str
    source: str          # "fomc_calendar" | "releasedates"
    url: str
    description: str


def plan_fed_probes() -> list[FedProbe]:
    """Two probes — one per Fed calendar surface.

    The FOMC HTML calendar carries ~48 meetings across a 6-year rolling
    window (3 years past + current + 2 years forward) in a stable
    panel-per-year layout; ``/json/calendar.json`` carries the full
    rolling calendar — weekly H.4.1 / H.8 (one entry per month, days
    comma-separated), ~8/year Beige Book, plus FOMC meetings, speeches,
    and testimony we filter out. Each probe issues exactly one HTTP
    request per run — no fan-out.
    """
    return [
        FedProbe(
            name="fomc_calendar",
            source="fomc_calendar",
            url=FOMC_CALENDAR_URL,
            description=(
                "FOMC meeting calendar — 6-year rolling panel of meeting "
                "dates + SEP markers"
            ),
        ),
        FedProbe(
            name="fed_releasedates",
            source="releasedates",
            url=FED_CALENDAR_JSON_URL,
            description=(
                "Fed calendar JSON — rolling Beige Book / H.4.1 / H.8 "
                "schedule"
            ),
        ),
    ]


def _try_project_fomc_entry(entry: FomcMeetingEntry) -> tuple[bool, str]:
    """Dry-run the FOMC meeting → calendar-record projection."""
    try:
        raw, event = meeting_entry_to_records(
            entry, snapshot_epoch_ms=1_700_000_000_000,
        )
        return True, (
            f"ok indicator=FOMC_RATE closing={entry.closing_date.isoformat()} "
            f"event_id={raw.provider_event_id[:10]}…"
        )
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def _try_project_release_entry(entry: FedReleaseEntry) -> tuple[bool, str]:
    """Dry-run the release-entry → calendar-record projection."""
    try:
        raw, event = release_entry_to_records(
            entry, snapshot_epoch_ms=1_700_000_000_000,
        )
        return True, (
            f"ok indicator={entry.series_id} date={entry.release_date} "
            f"event_id={raw.provider_event_id[:10]}…"
        )
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def _run_fomc_probe(result: ProbeResult, fomc_fetcher) -> ProbeResult:
    import time as _time
    t0 = _time.monotonic()
    try:
        html = fomc_fetcher()
        entries = parse_fomc_calendar_html(html)
    except Exception as exc:
        result.status = "http_error"
        result.notes.append(f"{type(exc).__name__}: {exc}")
        return result
    result.http_elapsed_ms = (_time.monotonic() - t0) * 1000

    result.row_count = len(entries)
    if not entries:
        # ``fetch_fed_calendar`` raises ``FomcCalendarParseError`` on
        # this exact condition — a 200 interstitial or DOM drift that
        # leaves ``parse_fomc_calendar_html`` with no matched rows.
        # The probe must mirror that loud-fail semantics, otherwise a
        # drifted upstream produces a green ``ok`` card and the report
        # summary can claim the acquisition layer matches expectations
        # while production would refuse to fetch.
        result.status = "http_error"
        result.notes.append(
            "zero FOMC meetings parsed — DOM drift or access-denied "
            "interstitial (production fetcher raises on this path)"
        )
        return result
    result.status = "ok"

    # Newest-first: closing_date is a date; sort desc.
    ordered = sorted(entries, key=lambda e: e.closing_date, reverse=True)
    sample = ordered[0]
    result.sample_row = {
        "year":         sample.year,
        "month_name":   sample.month_name,
        "date_cell":    sample.date_cell,
        "closing_date": sample.closing_date.isoformat(),
        "has_sep":      sample.has_sep,
    }

    # Year-span + SEP tally — report the coverage surface so an
    # operator eyeballing the card can see at a glance whether the
    # panel still spans the expected ~6-year window and how many SEP
    # (dot-plot) meetings sit inside it.
    years = sorted({e.year for e in entries})
    sep_count = sum(1 for e in entries if e.has_sep)
    result.notes.append(
        f"year span: {years[0]} → {years[-1]} "
        f"({len(years)} distinct years) | SEP meetings: {sep_count}"
    )

    year_counter: Counter = Counter()
    for e in entries:
        year_counter[repr(e.year)] += 1
    result.enum_counters = {"year": year_counter}

    sample_n = min(10, len(ordered))
    result.parse_attempts = sample_n
    for entry in ordered[:sample_n]:
        ok, msg = _try_project_fomc_entry(entry)
        if ok:
            result.parse_successes += 1
        else:
            if len(result.parse_error_samples) < 3:
                result.parse_error_samples.append(msg)
    return result


def _run_releasedates_probe(result: ProbeResult, releasedates_fetcher) -> ProbeResult:
    import time as _time
    t0 = _time.monotonic()
    row_issues: list[str] = []
    try:
        text = releasedates_fetcher()
        entries = parse_fed_calendar_json(text, row_issues=row_issues)
    except Exception as exc:
        result.status = "http_error"
        result.notes.append(f"{type(exc).__name__}: {exc}")
        return result
    result.http_elapsed_ms = (_time.monotonic() - t0) * 1000

    # ``parse_fed_calendar_json`` already raises on both empty-match
    # and all-matches-failed paths (see the module for the exact
    # guards), so ``entries`` is guaranteed non-empty once we reach
    # here — the general exception branch above carries the loud-
    # fail signal into ``status=http_error`` for both failure shapes.
    result.status = "ok"
    result.row_count = len(entries)
    if row_issues:
        # Surface partial-row failures exactly as the fetcher's summary
        # does — the probe report is the debugging surface for whoever
        # investigates a drift signal, so it shouldn't hide this.
        for issue in row_issues[:5]:
            result.notes.append(f"row_issue: {issue}")
        if len(row_issues) > 5:
            result.notes.append(
                f"... {len(row_issues) - 5} more row_issues suppressed"
            )

    ordered = sorted(entries, key=lambda e: e.release_date, reverse=True)
    sample = ordered[0]
    result.sample_row = {
        "series_id":          sample.series_id,
        "release_title":      sample.release_title,
        "release_date":       sample.release_date,
        "release_time_local": sample.release_time_local,
        "event_time_utc":     sample.event_time_utc,
    }

    by_indicator: Counter = Counter()
    for e in entries:
        by_indicator[e.series_id] += 1
    result.enum_counters = {"series_id": by_indicator}
    result.notes.append(
        "entries by indicator: "
        + ", ".join(f"{k}={v}" for k, v in by_indicator.most_common())
    )

    sample_n = min(10, len(ordered))
    result.parse_attempts = sample_n
    for entry in ordered[:sample_n]:
        ok, msg = _try_project_release_entry(entry)
        if ok:
            result.parse_successes += 1
        else:
            if len(result.parse_error_samples) < 3:
                result.parse_error_samples.append(msg)
    return result


def run_fed_probe(
    probe: FedProbe,
    *,
    fomc_fetcher=fetch_fomc_calendar_html,
    releasedates_fetcher=fetch_fed_calendar_json,
) -> ProbeResult:
    """Execute one Fed probe and return a populated :class:`ProbeResult`.

    Unlike the API-based probes, Fed consumes public web responses
    locally — HTML for FOMC calendar, JSON for the release feed. The
    runner drives the production fetcher + parser; ``fomc_fetcher``
    and ``releasedates_fetcher`` are seams tests inject to feed fixture
    payloads without hitting ``federalreserve.gov``. ``auth_missing``
    is not reachable — both surfaces are public.
    """
    generic = Probe(
        name=probe.name,
        path=f"GET {probe.url}",
        description=probe.description,
        expected_shape="list[FomcMeetingEntry] / list[FedReleaseEntry]",
        expected_fields=frozenset(),  # HTML/JSON surface — diff not meaningful
    )
    result = ProbeResult(probe=generic, status="skipped")
    result.request_path = generic.path

    if probe.source == "fomc_calendar":
        return _run_fomc_probe(result, fomc_fetcher)
    if probe.source == "releasedates":
        return _run_releasedates_probe(result, releasedates_fetcher)
    raise ValueError(f"unknown Fed probe source: {probe.source!r}")


# ──────────────────────────────────────────────────────────────────────────
# NBS probe plan + runner
# ──────────────────────────────────────────────────────────────────────────

# Single-page HTML scrape per run — the NBS yearly calendar is one
# article listing every registered indicator's 12-month (or
# quarterly) release schedule. Probe coverage is therefore breadth
# (how many indicators matched) rather than depth (per-indicator
# surfaces). NBS is documented as the highest-risk upstream on this
# issue: HTTP-only, HTML-fragile, frequent timeouts from non-CN IPs.
# The probe runner catches any exception cleanly — a single-probe
# timeout surfaces as a loud ``http_error`` card rather than crashing
# the whole run; on the current single-indicator registry that's
# also the only probe in flight, but the tolerance shape is in place
# for when P5c-live expands the whitelist.


@dataclass
class NBSProbe:
    """One NBS yearly-calendar live-validation probe."""

    name: str
    year: int
    description: str


def plan_nbs_probes() -> list[NBSProbe]:
    """One probe per run — the yearly calendar article for the current
    UTC year. The article lists every registered indicator together,
    so a single fetch validates the breadth of
    :data:`INDICATOR_REGISTRY` coverage.
    """
    year = datetime.now(timezone.utc).year
    return [
        NBSProbe(
            name=f"nbs_yearly_calendar_{year}",
            year=year,
            description=(
                f"NBS yearly release calendar article for {year} — covers "
                f"every registered indicator in one fetch"
            ),
        )
    ]


def _try_project_nbs_entry(entry: NBSReleaseEntry) -> tuple[bool, str]:
    """Dry-run the NBS calendar projection on one entry."""
    try:
        raw, event = nbs_release_entry_to_records(
            entry,
            snapshot_epoch_ms=1_700_000_000_000,
            calendar_url="https://example.test/nbs_fixture",
        )
        return True, (
            f"ok indicator={entry.indicator} "
            f"date={entry.year}-{entry.month:02d}-{entry.day:02d} "
            f"event_id={raw.provider_event_id[:10]}…"
        )
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def run_nbs_probe(
    probe: NBSProbe,
    *,
    index_fetcher=fetch_nbs_calendar_index_html,
    html_fetcher=fetch_nbs_yearly_calendar_html,
) -> ProbeResult:
    """Execute one NBS probe and return a populated :class:`ProbeResult`.

    Auto-discovers the calendar URL via the index page, fetches the
    article, parses entries, and dry-projects each through
    ``release_entry_to_records``. ``index_fetcher`` + ``html_fetcher``
    seams let tests feed fixture HTML without touching
    ``stats.gov.cn``.

    NBS upstream is notorious for intermittent timeouts from outside
    CN; the single ``except Exception`` branch carries every failure
    mode (index timeout, article timeout, parse raise) into a
    ``http_error`` card rather than letting the exception propagate
    and kill a multi-probe run (forward-compatible for P5c-live's
    whitelist expansion).
    """
    generic = Probe(
        name=probe.name,
        path=(
            f"GET http://www.stats.gov.cn/english/PressRelease/"
            f"ReleaseCalendar/ → {probe.year} article"
        ),
        description=probe.description,
        expected_shape="list[NBSReleaseEntry] after HTML parse",
        expected_fields=frozenset(),  # HTML surface — diff not meaningful
    )
    result = ProbeResult(probe=generic, status="skipped")

    import time as _time
    t0 = _time.monotonic()
    try:
        calendar_url = discover_nbs_calendar_url(
            probe.year, index_fetcher=index_fetcher,
        )
        result.request_path = f"GET {calendar_url}"
        html = html_fetcher(calendar_url)
        entries = parse_nbs_calendar_html(html, year_override=probe.year)
    except Exception as exc:
        result.status = "http_error"
        result.notes.append(f"{type(exc).__name__}: {exc}")
        result.notes.append(
            "NBS upstream is the highest-risk source on this issue "
            "(HTTP-only, HTML-fragile, frequent non-CN timeouts). "
            "Transient failures here are expected — retry the probe "
            "before treating it as a genuine drift signal."
        )
        return result
    result.http_elapsed_ms = (_time.monotonic() - t0) * 1000

    result.row_count = len(entries)
    if not entries:
        # The production fetcher raises ``NBSCalendarParseError`` on
        # this exact path; mirror the loud-fail shape per the Codex
        # review lesson from P4b — a green card on zero entries would
        # let an interstitial slip through.
        result.status = "http_error"
        result.notes.append(
            "zero NBS entries parsed — DOM drift or interstitial "
            "(production fetcher raises on this path)"
        )
        return result
    result.status = "ok"

    # Newest-first by (year, month, day).
    ordered = sorted(
        entries,
        key=lambda e: (e.year, e.month, e.day),
        reverse=True,
    )
    sample = ordered[0]
    result.sample_row = {
        "year":               sample.year,
        "month":              sample.month,
        "day":                sample.day,
        "indicator":          sample.indicator,
        "release_time_local": sample.release_time_local,
        "date_cell":          sample.date_cell,
    }

    # Per-indicator tally. The yearly calendar is the one place every
    # registered indicator converges, so the count-per-indicator is
    # the best drift signal: if PPI / GDP / etc. (P5c-live additions)
    # land with zero matches, their label_fragment drifted off the
    # page's actual headers.
    by_indicator: Counter = Counter()
    for e in entries:
        by_indicator[e.indicator] += 1
    result.enum_counters = {"indicator": by_indicator}
    result.notes.append(
        "entries by indicator: "
        + ", ".join(f"{k}={v}" for k, v in by_indicator.most_common())
    )

    # Dry-project up to 10 newest entries — the set whose projection
    # failure would matter most for an imminent release.
    sample_n = min(10, len(ordered))
    result.parse_attempts = sample_n
    for entry in ordered[:sample_n]:
        ok, msg = _try_project_nbs_entry(entry)
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
_OFFICIAL_PROVIDERS_WITH_PROBES: frozenset[str] = frozenset(
    {"bls", "bea", "ecb", "fed", "nbs"}
)


def parse_args(argv: list[str]) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument(
        "--provider",
        choices=["te", "eodhd", "bls", "bea", "fed", "ecb", "nbs"],
        default="te",
        help=(
            "which acquisition lane to validate. "
            "bls / bea / ecb / fed / nbs all have live probes (P1b / P2b "
            "/ P3b / P4b / P5c)."
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
    # scaffold-only stub. After P5c, every official provider has a
    # live probe; ``unwired`` is empty on the current tree but the
    # branch is kept as a safety net for any future provider seeded
    # into ``_OFFICIAL_PROVIDERS`` before its probe ships.
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

    if args.provider == "bea":
        bea_probes = plan_bea_probes()
        return _run_bea(args, bea_probes)

    if args.provider == "ecb":
        ecb_probes = plan_ecb_probes()
        return _run_ecb(args, ecb_probes)

    if args.provider == "fed":
        fed_probes = plan_fed_probes()
        return _run_fed(args, fed_probes)

    if args.provider == "nbs":
        nbs_probes = plan_nbs_probes()
        return _run_nbs(args, nbs_probes)

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


def _run_bea(args: argparse.Namespace, probes: list[BEAProbe]) -> int:
    """Dispatch the BEA probe flow — same shape as :func:`_run_bls`.

    BEA's 1000-req-daily free tier makes the 2-probe P2b run cheap; the
    confirm prompt mirrors BLS muscle memory and honours ``--yes``.
    """
    if not args.execute:
        print("DRY RUN (bea) — pass --execute to actually hit upstream.")
        print()
        for i, p in enumerate(probes, 1):
            print(f"{i}. {p.name}")
            print(
                f"   coordinate: {p.dataset} {p.table} line={p.line_number} "
                f"({p.indicator})"
            )
            print(f"   window: {p.start_year}-{p.end_year} freq={p.frequency}")
            print(f"   purpose: {p.description}")
        print()
        print(f"Total planned requests: {len(probes)}")
        return 0

    if not args.yes:
        print(f"Planned BEA probes ({len(probes)}):")
        for i, p in enumerate(probes, 1):
            print(
                f"  {i}. {p.name} — {p.dataset} {p.table} line={p.line_number} "
                f"{p.start_year}-{p.end_year}"
            )
        print(f"Estimated upstream requests: {len(probes)}")
        resp = input("Proceed with live run? [y/N] ").strip().lower()
        if resp not in {"y", "yes"}:
            print("Aborted.")
            return 1

    results: list[ProbeResult] = []
    client = BEAClient()
    for probe in probes:
        result = run_bea_probe(client, probe)
        results.append(result)
        _print_probe_summary(result)

    report = render_report(
        results,
        requests_spent=sum(1 for r in results if r.status == "ok"),
        provider="bea",
    )
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / (
        f"calendar_acquisition_bea_"
        f"{datetime.now(timezone.utc).date().isoformat()}.md"
    )
    report_path.write_text(report, encoding="utf-8")
    print()
    print(f"Report written: {report_path}")
    return 0


def _run_ecb(args: argparse.Namespace, probes: list[ECBProbe]) -> int:
    """Dispatch the ECB probe flow — same shape as :func:`_run_bls`.

    ECB Data Portal requires no auth, so the ``api_key`` bail-out
    branch is not applicable. Three probes (MRO / DFR / MLFR) make
    for a cheap live run; honours ``--yes`` for muscle memory.
    """
    if not args.execute:
        print("DRY RUN (ecb) — pass --execute to actually hit upstream.")
        print()
        for i, p in enumerate(probes, 1):
            print(f"{i}. {p.name}")
            print(f"   series: {p.series_id} ({p.indicator})")
            print(f"   window: {p.start_period} → {p.end_period}")
            print(f"   purpose: {p.description}")
        print()
        print(f"Total planned requests: {len(probes)}")
        return 0

    if not args.yes:
        print(f"Planned ECB probes ({len(probes)}):")
        for i, p in enumerate(probes, 1):
            print(f"  {i}. {p.name} — {p.series_id} {p.start_period}..{p.end_period}")
        print(f"Estimated upstream requests: {len(probes)}")
        resp = input("Proceed with live run? [y/N] ").strip().lower()
        if resp not in {"y", "yes"}:
            print("Aborted.")
            return 1

    results: list[ProbeResult] = []
    client = ECBClient()
    for probe in probes:
        result = run_ecb_probe(client, probe)
        results.append(result)
        _print_probe_summary(result)

    report = render_report(
        results,
        requests_spent=sum(1 for r in results if r.status == "ok"),
        provider="ecb",
    )
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / (
        f"calendar_acquisition_ecb_"
        f"{datetime.now(timezone.utc).date().isoformat()}.md"
    )
    report_path.write_text(report, encoding="utf-8")
    print()
    print(f"Report written: {report_path}")
    return 0


def _run_fed(args: argparse.Namespace, probes: list[FedProbe]) -> int:
    """Dispatch the Fed probe flow — same shape as :func:`_run_ecb`.

    Fed pages are public + require no auth, so the ``api_key`` bail-
    out branch is not applicable. Two probes (FOMC calendar + release
    dates) — a cheap, lightweight live run.
    """
    if not args.execute:
        print("DRY RUN (fed) — pass --execute to actually hit upstream.")
        print()
        for i, p in enumerate(probes, 1):
            print(f"{i}. {p.name}")
            print(f"   source: {p.source}")
            print(f"   url: {p.url}")
            print(f"   purpose: {p.description}")
        print()
        print(f"Total planned requests: {len(probes)}")
        return 0

    if not args.yes:
        print(f"Planned Fed probes ({len(probes)}):")
        for i, p in enumerate(probes, 1):
            print(f"  {i}. {p.name} — {p.url}")
        print(f"Estimated upstream requests: {len(probes)}")
        resp = input("Proceed with live run? [y/N] ").strip().lower()
        if resp not in {"y", "yes"}:
            print("Aborted.")
            return 1

    results: list[ProbeResult] = []
    for probe in probes:
        result = run_fed_probe(probe)
        results.append(result)
        _print_probe_summary(result)

    report = render_report(
        results,
        requests_spent=sum(1 for r in results if r.status == "ok"),
        provider="fed",
    )
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / (
        f"calendar_acquisition_fed_"
        f"{datetime.now(timezone.utc).date().isoformat()}.md"
    )
    report_path.write_text(report, encoding="utf-8")
    print()
    print(f"Report written: {report_path}")
    return 0


def _run_nbs(args: argparse.Namespace, probes: list[NBSProbe]) -> int:
    """Dispatch the NBS probe flow — same shape as :func:`_run_fed`.

    NBS is the highest-risk upstream (HTTP-only, HTML-fragile, non-CN
    timeouts). One probe covers every registered indicator in a single
    article fetch; the runner's ``except Exception`` branch absorbs
    transient network failures cleanly rather than crashing the run.
    """
    if not args.execute:
        print("DRY RUN (nbs) — pass --execute to actually hit upstream.")
        print()
        for i, p in enumerate(probes, 1):
            print(f"{i}. {p.name}")
            print(f"   year: {p.year}")
            print(f"   purpose: {p.description}")
        print()
        print(f"Total planned requests: {len(probes)} (+ 1 index-page fetch)")
        return 0

    if not args.yes:
        print(f"Planned NBS probes ({len(probes)}):")
        for i, p in enumerate(probes, 1):
            print(f"  {i}. {p.name} — yearly calendar for {p.year}")
        print(f"Estimated upstream requests: {len(probes)} (+ 1 index-page fetch)")
        resp = input("Proceed with live run? [y/N] ").strip().lower()
        if resp not in {"y", "yes"}:
            print("Aborted.")
            return 1

    results: list[ProbeResult] = []
    for probe in probes:
        result = run_nbs_probe(probe)
        results.append(result)
        _print_probe_summary(result)

    # Each NBS probe expends two upstream requests on the happy path —
    # the index-page discovery plus the yearly article fetch. The
    # dry-run summary advertises this ("+ 1 index-page fetch"); the
    # Budget section should match. When discovery fails the article
    # fetch never runs, so the probe's ``request_path`` (set only
    # after ``discover_nbs_calendar_url`` returns) acts as the
    # proxy for whether 1 or 2 requests were spent.
    def _nbs_requests_spent(r: ProbeResult) -> int:
        if r.status == "skipped":
            return 0
        return 2 if r.request_path else 1

    report = render_report(
        results,
        requests_spent=sum(_nbs_requests_spent(r) for r in results),
        provider="nbs",
    )
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / (
        f"calendar_acquisition_nbs_"
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

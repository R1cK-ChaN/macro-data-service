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
import os
import re
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT))

from scripts.validate._shared import (  # noqa: E402
    Probe,
    ProbeResult,
    RowDiff,
    days_ago_iso,
    days_ahead_iso,
    today_iso,
)
from scripts.validate.te import (  # noqa: E402
    ALL_TE_FIELDS,
    TE_ENUM_FIELDS,
    TE_PARSER_READS,
    TE_UPDATES_POINTER_READS,
    TEAPIClient,
    diff_te_row,
    plan_te_probes,
    resolve_dynamic_ids,
    run_probe,
    try_parse,
)

# Scaffold under test — we import and exercise exactly what production uses.
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

from ingestion.calendar.census_api import (  # noqa: E402
    CensusEITSClient,
    CensusEITSObservation,
    INDICATOR_REGISTRY as CENSUS_INDICATOR_REGISTRY,
    parse_observation as parse_census_observation,
)

from ingestion.calendar.ism_api import (  # noqa: E402
    ISM_RELEASE_CALENDAR_URL,
    ISM_REPORTS_URL,
    discover_current_report_url,
    fetch_report_html as fetch_ism_report_html,
    fetch_reports_landing_html as fetch_ism_reports_landing_html,
    fetch_schedule_html as fetch_ism_schedule_html,
    parse_report_html as parse_ism_report_html,
    parse_schedule_html as parse_ism_schedule_html,
    report_value_to_records as ism_report_value_to_records,
    schedule_entry_to_records as ism_schedule_entry_to_records,
)

from ingestion.calendar.umich_api import (  # noqa: E402
    UMICH_MAIN_URL,
    UMICH_SURVEY_INFO_URL,
    UMichScheduleDocument,
    current_value_to_records as umich_current_value_to_records,
    fetch_current_results_html as fetch_umich_current_results_html,
    fetch_release_dates_document as fetch_umich_release_dates_document,
    parse_current_results_html as parse_umich_current_results_html,
    parse_release_dates_text as parse_umich_release_dates_text,
    schedule_entry_to_records as umich_schedule_entry_to_records,
)

from ingestion.calendar.conference_board_api import (  # noqa: E402
    CONFERENCE_BOARD_CALENDAR_URL,
    CONFERENCE_BOARD_CONSUMER_CONFIDENCE_URL,
    CONFERENCE_BOARD_LEADING_INDICATORS_URL,
    current_value_to_records as conference_board_current_value_to_records,
    fetch_calendar_json as fetch_conference_board_calendar_json,
    fetch_indicator_html as fetch_conference_board_indicator_html,
    parse_calendar_events_json as parse_conference_board_calendar_events_json,
    parse_current_value_html as parse_conference_board_current_value_html,
    schedule_entry_to_records as conference_board_schedule_entry_to_records,
)

from ingestion.calendar.nar_api import (  # noqa: E402
    NAR_EXISTING_HOME_SALES_URL,
    NAR_PENDING_HOME_SALES_URL,
    NAR_SCHEDULE_URL,
    current_value_to_records as nar_current_value_to_records,
    fetch_current_html as fetch_nar_current_html,
    fetch_schedule_html as fetch_nar_schedule_html,
    parse_current_value_html as parse_nar_current_value_html,
    parse_schedule_html as parse_nar_schedule_html,
    schedule_entry_to_records as nar_schedule_entry_to_records,
)

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
from ingestion.calendar.meti_api import (  # noqa: E402
    ESTAT_RELEASE_CALENDAR_URL,
    METI_RETAIL_PAGE_URL,
    fetch_iip_release_calendar_html,
    fetch_retail_schedule_html,
    parse_iip_release_calendar_html,
    parse_retail_schedule_html,
    schedule_entry_to_records as meti_schedule_entry_to_records,
)
from ingestion.calendar.stat_bureau_api import (  # noqa: E402
    ESTAT_STATS_DATA_URL,
    INDICATOR_REGISTRY as STAT_BUREAU_INDICATOR_REGISTRY,
    fetch_cpi_release_schedule_html,
    fetch_estat_value_json,
    fetch_lfs_release_schedule_html,
    parse_cpi_release_schedule_html,
    parse_estat_value_json,
    parse_lfs_release_schedule_html,
    schedule_entry_to_records as stat_bureau_schedule_entry_to_records,
    time_code_for_month,
)


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
# EODHD probe plan + runner
# ──────────────────────────────────────────────────────────────────────────


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
        "census": "Census", "ism": "ISM", "umich": "U Michigan",
        "conference-board": "Conference Board", "nar": "NAR",
        "ecb": "ECB", "fed": "Fed", "nbs": "NBS", "meti": "METI",
        "stat-bureau-jp": "Statistics Bureau JP",
    }.get(provider, provider.upper())
    budget_line = {
        "te":    "- TE basic-plan monthly cap: 1000",
        "eodhd": "- EODHD All-in-One plan: per-call consumption (no tight cap)",
        "bls":   "- BLS Public Data API v2 free-tier daily cap: 500",
        "bea":   "- BEA REST API free-tier daily cap: 1000",
        "census": "- Census EITS API: optional key, unspecified rate limit (polite)",
        "ism":    "- ISM public HTML: no auth, unspecified rate limit (polite)",
        "umich": "- U Michigan public HTML/PDF: no auth, unspecified rate limit (polite)",
        "conference-board": "- Conference Board public HTML/JSON: no auth, unspecified rate limit (polite)",
        "nar": "- NAR public HTML: no auth, unspecified rate limit (polite)",
        "ecb":   "- ECB Data Portal: no auth, unspecified rate limit (polite)",
        "fed":   "- federalreserve.gov: no auth, HTML scrape (browser-UA required)",
        "nbs":   "- stats.gov.cn: no auth, HTTP-only, flaky from non-CN IPs",
        "meti": "- meti.go.jp public XML/HTML schedules: no auth",
        "stat-bureau-jp": "- stat.go.jp schedules + e-Stat API; ESTAT_APP_ID required for value probes",
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
# Census probe plan + runner
# ──────────────────────────────────────────────────────────────────────────

CENSUS_EITS_EXPECTED_FIELDS: frozenset[str] = frozenset({
    "data_type_code",
    "seasonally_adj",
    "category_code",
    "cell_value",
    "error_data",
    "time_slot_id",
    "time_slot_name",
    "time",
    "us",
})


@dataclass
class CensusProbe:
    """One Census live-validation probe — one series in one EITS year."""

    name: str
    series_id: str
    dataset: str
    data_type_code: str
    category_code: str
    seasonally_adj: str
    time_slot_id: str
    description: str
    year: int


def plan_census_probes() -> list[CensusProbe]:
    """One probe per Census calendar registry entry."""
    now_year = datetime.now(timezone.utc).year
    probes: list[CensusProbe] = []
    for series_id, spec in CENSUS_INDICATOR_REGISTRY.items():
        token = _bls_probe_token(spec.indicator)
        probes.append(
            CensusProbe(
                name=f"{token}_{now_year}",
                series_id=series_id,
                dataset=spec.dataset,
                data_type_code=spec.data_type_code,
                category_code=spec.category_code,
                seasonally_adj=spec.seasonally_adj,
                time_slot_id=spec.time_slot_id,
                description=spec.title,
                year=now_year,
            )
        )
    return probes


def _diff_census_row(raw_row: dict[str, Any]) -> RowDiff:
    observed = set(raw_row.keys())
    diff = RowDiff(
        observed_fields=sorted(observed),
        read_by_parser=sorted(observed & CENSUS_EITS_EXPECTED_FIELDS),
        ignored_by_parser=[],
        unknown_observed=sorted(observed - CENSUS_EITS_EXPECTED_FIELDS),
        missing_expected=sorted(CENSUS_EITS_EXPECTED_FIELDS - observed),
    )
    value = raw_row.get("cell_value")
    if value in (None, ""):
        diff.type_warnings.append("cell_value missing")
    else:
        try:
            float(str(value))
        except ValueError:
            diff.type_warnings.append(f"cell_value is not numeric: {value!r}")
    return diff


def _row_matches_census_probe(row: dict[str, str], probe: CensusProbe) -> bool:
    return (
        row.get("data_type_code") == probe.data_type_code
        and row.get("seasonally_adj") == probe.seasonally_adj
        and row.get("category_code") == probe.category_code
        and row.get("time_slot_id") == probe.time_slot_id
    )


def _census_obs_from_row(row: dict[str, str], probe: CensusProbe) -> CensusEITSObservation:
    return CensusEITSObservation(
        series_id=probe.series_id,
        dataset=probe.dataset,
        time=row.get("time", ""),
        data_type_code=row.get("data_type_code", ""),
        category_code=row.get("category_code", ""),
        seasonally_adj=row.get("seasonally_adj", ""),
        time_slot_id=row.get("time_slot_id", ""),
        time_slot_name=row.get("time_slot_name", ""),
        cell_value=row.get("cell_value", ""),
        error_data=row.get("error_data", ""),
        raw=dict(row),
    )


def _try_parse_census(row: dict[str, str], probe: CensusProbe) -> tuple[bool, str]:
    spec = CENSUS_INDICATOR_REGISTRY.get(probe.series_id)
    if spec is None:
        return False, f"no Census calendar spec for series_id={probe.series_id!r}"
    try:
        obs = _census_obs_from_row(row, probe)
        raw, event = parse_census_observation(
            obs,
            snapshot_epoch_ms=1_700_000_000_000,
            spec=spec,
        )
        return True, (
            f"ok indicator={spec.indicator} event_id={raw.provider_event_id[:10]}…"
        )
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def run_census_probe(client: CensusEITSClient, probe: CensusProbe) -> ProbeResult:
    generic = Probe(
        name=probe.name,
        path=f"GET https://api.census.gov/data/timeseries/eits/{probe.dataset}",
        description=probe.description,
        expected_shape="list[dict] after EITS header-row decode",
        expected_fields=CENSUS_EITS_EXPECTED_FIELDS,
    )
    result = ProbeResult(probe=generic, status="skipped")
    result.request_path = (
        f"{generic.path}?time={probe.year}&for=us:* "
        f"(filter data_type_code={probe.data_type_code} "
        f"seasonally_adj={probe.seasonally_adj} "
        f"category_code={probe.category_code})"
    )

    import time as _time
    t0 = _time.monotonic()
    try:
        rows = client.get_dataset_year(probe.dataset, probe.year)
    except Exception as exc:
        result.status = "http_error"
        result.notes.append(f"{type(exc).__name__}: {exc}")
        return result
    result.http_elapsed_ms = (_time.monotonic() - t0) * 1000

    filtered = [row for row in rows if _row_matches_census_probe(row, probe)]
    result.status = "ok"
    result.row_count = len(filtered)
    if not filtered:
        result.notes.append(
            f"Census returned zero rows for {probe.series_id} in {probe.year} "
            f"(dataset payload had {len(rows)} rows)"
        )
        return result

    filtered = sorted(filtered, key=lambda row: row.get("time", ""), reverse=True)
    result.sample_row = filtered[0]
    result.field_diff = _diff_census_row(filtered[0])
    result.enum_counters = {
        "time": Counter(row.get("time") for row in filtered),
        "error_data": Counter(row.get("error_data") for row in filtered),
    }

    sample_n = min(10, len(filtered))
    result.parse_attempts = sample_n
    for row in filtered[:sample_n]:
        ok, msg = _try_parse_census(row, probe)
        if ok:
            result.parse_successes += 1
        else:
            if len(result.parse_error_samples) < 3:
                result.parse_error_samples.append(msg)
    return result


# ──────────────────────────────────────────────────────────────────────────
# ISM probe plan + runner
# ──────────────────────────────────────────────────────────────────────────


@dataclass
class ISMProbe:
    """One ISM live-validation probe."""

    name: str
    source: str
    url: str
    description: str


def plan_ism_probes() -> list[ISMProbe]:
    """Validate the ISM release calendar and current Manufacturing report."""
    return [
        ISMProbe(
            name="ism_release_calendar",
            source="schedule",
            url=ISM_RELEASE_CALENDAR_URL,
            description=(
                "ISM Manufacturing PMI release dates from the public "
                "release-calendar table"
            ),
        ),
        ISMProbe(
            name="ism_current_manufacturing_report",
            source="report",
            url=ISM_REPORTS_URL,
            description=(
                "ISM PMI reports hub discovery plus current Manufacturing "
                "PMI report value parse"
            ),
        ),
    ]


def _try_project_ism_schedule(entry: Any) -> tuple[bool, str]:
    try:
        raw, event = ism_schedule_entry_to_records(
            entry,
            snapshot_epoch_ms=1_700_000_000_000,
        )
        return True, (
            f"ok series={entry.series_id} ref={event.reference_date} "
            f"event_id={raw.provider_event_id[:10]}..."
        )
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def _try_project_ism_value(value: Any) -> tuple[bool, str]:
    try:
        raw, event = ism_report_value_to_records(
            value,
            snapshot_epoch_ms=1_700_000_000_000,
        )
        return True, (
            f"ok series={value.series_id} actual={event.actual} "
            f"event_id={raw.provider_event_id[:10]}..."
        )
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def _run_ism_schedule_probe(
    result: ProbeResult,
    schedule_fetcher,
) -> ProbeResult:
    import time as _time

    t0 = _time.monotonic()
    try:
        html = schedule_fetcher()
        entries = parse_ism_schedule_html(html)
    except Exception as exc:
        result.status = "http_error"
        result.notes.append(f"{type(exc).__name__}: {exc}")
        return result
    result.http_elapsed_ms = (_time.monotonic() - t0) * 1000
    result.row_count = len(entries)
    if not entries:
        result.status = "http_error"
        result.notes.append("zero ISM schedule entries parsed")
        return result
    result.status = "ok"
    ordered = sorted(entries, key=lambda e: e.release_date, reverse=True)
    sample = ordered[0]
    result.sample_row = {
        "series_id": sample.series_id,
        "reference_date": sample.reference_date,
        "release_date": sample.release_date,
        "event_time_utc": sample.event_time_utc,
    }
    by_series = Counter(entry.series_id for entry in entries)
    result.enum_counters = {"series_id": by_series}
    result.notes.append(
        "entries by series: "
        + ", ".join(f"{k}={v}" for k, v in by_series.most_common())
    )
    sample_n = min(10, len(ordered))
    result.parse_attempts = sample_n
    for entry in ordered[:sample_n]:
        ok, msg = _try_project_ism_schedule(entry)
        if ok:
            result.parse_successes += 1
        else:
            if len(result.parse_error_samples) < 3:
                result.parse_error_samples.append(msg)
    return result


def _run_ism_report_probe(
    result: ProbeResult,
    landing_fetcher,
    report_fetcher,
) -> ProbeResult:
    import time as _time

    t0 = _time.monotonic()
    try:
        landing_html = landing_fetcher()
        report_url = discover_current_report_url(landing_html)
        result.request_path = f"GET {ISM_REPORTS_URL} -> GET {report_url}"
        report_html = report_fetcher(report_url)
        value = parse_ism_report_html(report_html, source_url=report_url)
    except Exception as exc:
        result.status = "http_error"
        result.notes.append(f"{type(exc).__name__}: {exc}")
        return result
    result.http_elapsed_ms = (_time.monotonic() - t0) * 1000
    result.status = "ok"
    result.row_count = 1
    result.sample_row = {
        "series_id": value.series_id,
        "reference_date": value.reference_date,
        "actual": value.actual,
        "previous": value.previous,
        "source_url": value.source_url,
    }
    result.parse_attempts = 1
    ok, msg = _try_project_ism_value(value)
    if ok:
        result.parse_successes = 1
    else:
        result.parse_error_samples.append(msg)
    return result


def run_ism_probe(
    probe: ISMProbe,
    *,
    schedule_fetcher=fetch_ism_schedule_html,
    landing_fetcher=fetch_ism_reports_landing_html,
    report_fetcher=fetch_ism_report_html,
) -> ProbeResult:
    """Execute one ISM public-HTML probe."""
    generic = Probe(
        name=probe.name,
        path=f"GET {probe.url}",
        description=probe.description,
        expected_shape="HTML -> ISM schedule entries / report value",
        expected_fields=frozenset(),
    )
    result = ProbeResult(probe=generic, status="skipped")
    result.request_path = generic.path
    if probe.source == "schedule":
        return _run_ism_schedule_probe(result, schedule_fetcher)
    if probe.source == "report":
        return _run_ism_report_probe(result, landing_fetcher, report_fetcher)
    raise ValueError(f"unknown ISM probe source: {probe.source!r}")


# ──────────────────────────────────────────────────────────────────────────
# U Michigan probe plan + runner
# ──────────────────────────────────────────────────────────────────────────


@dataclass
class UMichProbe:
    """One U Michigan live-validation probe."""

    name: str
    source: str
    url: str
    description: str
    year: int | None = None


def plan_umich_probes() -> list[UMichProbe]:
    """Validate the U Michigan release-date PDF and current results page."""
    year = datetime.now(timezone.utc).year
    return [
        UMichProbe(
            name=f"umich_release_dates_{year}",
            source="schedule",
            url=UMICH_SURVEY_INFO_URL,
            description=(
                "U Michigan Consumer Sentiment preliminary/final release "
                f"dates for {year}"
            ),
            year=year,
        ),
        UMichProbe(
            name="umich_current_results",
            source="current",
            url=UMICH_MAIN_URL,
            description=(
                "U Michigan current Consumer Sentiment table value parse"
            ),
        ),
    ]


def _try_project_umich_schedule(entry: Any) -> tuple[bool, str]:
    try:
        raw, event = umich_schedule_entry_to_records(
            entry,
            snapshot_epoch_ms=1_700_000_000_000,
        )
        return True, (
            f"ok series={entry.series_id} stage={entry.release_stage} "
            f"ref={event.reference_date} event_id={raw.provider_event_id[:10]}..."
        )
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def _try_project_umich_value(value: Any) -> tuple[bool, str]:
    try:
        raw, event = umich_current_value_to_records(
            value,
            snapshot_epoch_ms=1_700_000_000_000,
        )
        return True, (
            f"ok series={value.series_id} stage={value.release_stage} "
            f"actual={event.actual} event_id={raw.provider_event_id[:10]}..."
        )
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def _coerce_umich_document(doc: object) -> tuple[str, str]:
    if isinstance(doc, UMichScheduleDocument):
        return doc.text, doc.source_url
    return str(doc), UMICH_SURVEY_INFO_URL


def _run_umich_schedule_probe(
    result: ProbeResult,
    probe: UMichProbe,
    document_fetcher,
) -> ProbeResult:
    import time as _time

    t0 = _time.monotonic()
    try:
        doc = document_fetcher(year=probe.year)
        text, source_url = _coerce_umich_document(doc)
        result.request_path = f"GET {UMICH_SURVEY_INFO_URL} -> GET {source_url}"
        entries = parse_umich_release_dates_text(text, source_url=source_url)
    except Exception as exc:
        result.status = "http_error"
        result.notes.append(f"{type(exc).__name__}: {exc}")
        return result
    result.http_elapsed_ms = (_time.monotonic() - t0) * 1000
    result.row_count = len(entries)
    if not entries:
        result.status = "http_error"
        result.notes.append("zero U Michigan schedule entries parsed")
        return result
    result.status = "ok"
    ordered = sorted(entries, key=lambda e: e.release_date, reverse=True)
    sample = ordered[0]
    result.sample_row = {
        "series_id": sample.series_id,
        "reference_date": sample.reference_date,
        "release_stage": sample.release_stage,
        "release_date": sample.release_date,
        "event_time_utc": sample.event_time_utc,
    }
    by_stage = Counter(entry.release_stage for entry in entries)
    result.enum_counters = {"release_stage": by_stage}
    result.notes.append(
        "entries by stage: "
        + ", ".join(f"{k}={v}" for k, v in by_stage.most_common())
    )
    sample_n = min(10, len(ordered))
    result.parse_attempts = sample_n
    for entry in ordered[:sample_n]:
        ok, msg = _try_project_umich_schedule(entry)
        if ok:
            result.parse_successes += 1
        else:
            if len(result.parse_error_samples) < 3:
                result.parse_error_samples.append(msg)
    return result


def _run_umich_current_probe(
    result: ProbeResult,
    current_fetcher,
) -> ProbeResult:
    import time as _time

    t0 = _time.monotonic()
    try:
        html = current_fetcher()
        value = parse_umich_current_results_html(html, source_url=UMICH_MAIN_URL)
    except Exception as exc:
        result.status = "http_error"
        result.notes.append(f"{type(exc).__name__}: {exc}")
        return result
    result.http_elapsed_ms = (_time.monotonic() - t0) * 1000
    result.status = "ok"
    result.row_count = 1
    result.sample_row = {
        "series_id": value.series_id,
        "reference_date": value.reference_date,
        "release_stage": value.release_stage,
        "actual": value.actual,
        "previous": value.previous,
        "source_url": value.source_url,
    }
    result.parse_attempts = 1
    ok, msg = _try_project_umich_value(value)
    if ok:
        result.parse_successes = 1
    else:
        result.parse_error_samples.append(msg)
    return result


def run_umich_probe(
    probe: UMichProbe,
    *,
    document_fetcher=fetch_umich_release_dates_document,
    current_fetcher=fetch_umich_current_results_html,
) -> ProbeResult:
    """Execute one U Michigan public HTML/PDF probe."""
    generic = Probe(
        name=probe.name,
        path=f"GET {probe.url}",
        description=probe.description,
        expected_shape="HTML/PDF -> U Michigan schedule entries / current value",
        expected_fields=frozenset(),
    )
    result = ProbeResult(probe=generic, status="skipped")
    result.request_path = generic.path
    if probe.source == "schedule":
        return _run_umich_schedule_probe(result, probe, document_fetcher)
    if probe.source == "current":
        return _run_umich_current_probe(result, current_fetcher)
    raise ValueError(f"unknown U Michigan probe source: {probe.source!r}")


# ──────────────────────────────────────────────────────────────────────────
# Conference Board probe plan + runner
# ──────────────────────────────────────────────────────────────────────────


@dataclass
class ConferenceBoardProbe:
    """One Conference Board live-validation probe."""

    name: str
    source: str
    url: str
    description: str
    series_id: str = ""
    from_epoch_ms: int | None = None
    to_epoch_ms: int | None = None


def _conference_board_window() -> tuple[int, int]:
    today = datetime.now(timezone.utc).date()
    start = today - timedelta(days=45)
    end = today + timedelta(days=180)
    start_ms = int(datetime(start.year, start.month, start.day, tzinfo=timezone.utc).timestamp() * 1000)
    end_ms = int(datetime(end.year, end.month, end.day, tzinfo=timezone.utc).timestamp() * 1000)
    return start_ms, end_ms


def plan_conference_board_probes() -> list[ConferenceBoardProbe]:
    """Validate the Conference Board calendar endpoint and current pages."""
    from_ms, to_ms = _conference_board_window()
    return [
        ConferenceBoardProbe(
            name="conference_board_release_calendar",
            source="schedule",
            url=CONFERENCE_BOARD_CALENDAR_URL,
            description=(
                "Conference Board economic-indicator calendar rows for "
                "US Consumer Confidence and US Leading Index"
            ),
            from_epoch_ms=from_ms,
            to_epoch_ms=to_ms,
        ),
        ConferenceBoardProbe(
            name="conference_board_consumer_confidence",
            source="current",
            url=CONFERENCE_BOARD_CONSUMER_CONFIDENCE_URL,
            description="Conference Board current Consumer Confidence value parse",
            series_id="TCB_CONSUMER_CONFIDENCE",
        ),
        ConferenceBoardProbe(
            name="conference_board_us_leading_index",
            source="current",
            url=CONFERENCE_BOARD_LEADING_INDICATORS_URL,
            description="Conference Board current US Leading Index monthly-change parse",
            series_id="TCB_LEADING_INDEX",
        ),
    ]


def _try_project_conference_board_schedule(entry: Any) -> tuple[bool, str]:
    try:
        raw, event = conference_board_schedule_entry_to_records(
            entry,
            snapshot_epoch_ms=1_700_000_000_000,
        )
        return True, (
            f"ok series={entry.series_id} ref={event.reference_date} "
            f"event_id={raw.provider_event_id[:10]}..."
        )
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def _try_project_conference_board_value(value: Any) -> tuple[bool, str]:
    try:
        raw, event = conference_board_current_value_to_records(
            value,
            snapshot_epoch_ms=1_700_000_000_000,
        )
        return True, (
            f"ok series={value.series_id} actual={event.actual} "
            f"event_id={raw.provider_event_id[:10]}..."
        )
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def _run_conference_board_schedule_probe(
    result: ProbeResult,
    probe: ConferenceBoardProbe,
    schedule_fetcher,
) -> ProbeResult:
    import time as _time

    from_ms, to_ms = probe.from_epoch_ms, probe.to_epoch_ms
    if from_ms is None or to_ms is None:
        from_ms, to_ms = _conference_board_window()
    result.request_path = f"GET {probe.url}?from={from_ms}&to={to_ms}"
    t0 = _time.monotonic()
    try:
        payload = schedule_fetcher(from_epoch_ms=from_ms, to_epoch_ms=to_ms)
        entries = parse_conference_board_calendar_events_json(payload)
    except Exception as exc:
        result.status = "http_error"
        result.notes.append(f"{type(exc).__name__}: {exc}")
        return result
    result.http_elapsed_ms = (_time.monotonic() - t0) * 1000
    result.row_count = len(entries)
    if not entries:
        result.status = "http_error"
        result.notes.append("zero Conference Board schedule entries parsed")
        return result
    result.status = "ok"
    ordered = sorted(entries, key=lambda e: e.release_date, reverse=True)
    sample = ordered[0]
    result.sample_row = {
        "series_id": sample.series_id,
        "calendar_event_id": sample.calendar_event_id,
        "reference_date": sample.reference_date,
        "release_date": sample.release_date,
        "event_time_utc": sample.event_time_utc,
        "source_url": sample.source_url,
    }
    by_series = Counter(entry.series_id for entry in entries)
    result.enum_counters = {"series_id": by_series}
    result.notes.append(
        "entries by series: "
        + ", ".join(f"{k}={v}" for k, v in by_series.most_common())
    )
    sample_n = min(10, len(ordered))
    result.parse_attempts = sample_n
    for entry in ordered[:sample_n]:
        ok, msg = _try_project_conference_board_schedule(entry)
        if ok:
            result.parse_successes += 1
        else:
            if len(result.parse_error_samples) < 3:
                result.parse_error_samples.append(msg)
    return result


def _run_conference_board_current_probe(
    result: ProbeResult,
    probe: ConferenceBoardProbe,
    current_fetcher,
) -> ProbeResult:
    import time as _time

    t0 = _time.monotonic()
    try:
        html = current_fetcher(probe.url)
        value = parse_conference_board_current_value_html(
            html,
            source_url=probe.url,
            series_id=probe.series_id,
        )
    except Exception as exc:
        result.status = "http_error"
        result.notes.append(f"{type(exc).__name__}: {exc}")
        return result
    result.http_elapsed_ms = (_time.monotonic() - t0) * 1000
    result.status = "ok"
    result.row_count = 1
    result.sample_row = {
        "series_id": value.series_id,
        "reference_date": value.reference_date,
        "actual": value.actual,
        "previous": value.previous,
        "index_level": value.index_level,
        "source_url": value.source_url,
    }
    result.parse_attempts = 1
    ok, msg = _try_project_conference_board_value(value)
    if ok:
        result.parse_successes = 1
    else:
        result.parse_error_samples.append(msg)
    return result


def run_conference_board_probe(
    probe: ConferenceBoardProbe,
    *,
    schedule_fetcher=fetch_conference_board_calendar_json,
    current_fetcher=fetch_conference_board_indicator_html,
) -> ProbeResult:
    """Execute one Conference Board public HTML/JSON probe."""
    generic = Probe(
        name=probe.name,
        path=f"GET {probe.url}",
        description=probe.description,
        expected_shape="JSON/HTML -> Conference Board schedule entries / current values",
        expected_fields=frozenset(),
    )
    result = ProbeResult(probe=generic, status="skipped")
    result.request_path = generic.path
    if probe.source == "schedule":
        return _run_conference_board_schedule_probe(result, probe, schedule_fetcher)
    if probe.source == "current":
        return _run_conference_board_current_probe(result, probe, current_fetcher)
    raise ValueError(f"unknown Conference Board probe source: {probe.source!r}")


# ──────────────────────────────────────────────────────────────────────────
# NAR probe plan + runner
# ──────────────────────────────────────────────────────────────────────────


@dataclass
class NARProbe:
    """One NAR live-validation probe."""

    name: str
    source: str
    url: str
    description: str
    series_id: str = ""


def plan_nar_probes() -> list[NARProbe]:
    """Validate the NAR schedule page and current housing-statistics pages."""
    return [
        NARProbe(
            name="nar_statistical_release_schedule",
            source="schedule",
            url=NAR_SCHEDULE_URL,
            description=(
                "NAR statistical release schedule rows for Existing-Home "
                "Sales and Pending Home Sales Index"
            ),
        ),
        NARProbe(
            name="nar_existing_home_sales",
            source="current",
            url=NAR_EXISTING_HOME_SALES_URL,
            description="NAR current Existing Home Sales million-SAAR parse",
            series_id="NAR_EXISTING_HOME_SALES",
        ),
        NARProbe(
            name="nar_pending_home_sales",
            source="current",
            url=NAR_PENDING_HOME_SALES_URL,
            description="NAR current Pending Home Sales MoM percent parse",
            series_id="NAR_PENDING_HOME_SALES_MOM",
        ),
    ]


def _try_project_nar_schedule(entry: Any) -> tuple[bool, str]:
    try:
        raw, event = nar_schedule_entry_to_records(
            entry,
            snapshot_epoch_ms=1_700_000_000_000,
        )
        return True, (
            f"ok series={entry.series_id} ref={event.reference_date} "
            f"event_id={raw.provider_event_id[:10]}..."
        )
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def _try_project_nar_value(value: Any) -> tuple[bool, str]:
    try:
        raw, event = nar_current_value_to_records(
            value,
            snapshot_epoch_ms=1_700_000_000_000,
        )
        return True, (
            f"ok series={value.series_id} actual={event.actual} "
            f"event_id={raw.provider_event_id[:10]}..."
        )
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def _run_nar_schedule_probe(
    result: ProbeResult,
    schedule_fetcher,
) -> ProbeResult:
    import time as _time

    t0 = _time.monotonic()
    try:
        html = schedule_fetcher()
        entries = parse_nar_schedule_html(html)
    except Exception as exc:
        result.status = "http_error"
        result.notes.append(f"{type(exc).__name__}: {exc}")
        return result
    result.http_elapsed_ms = (_time.monotonic() - t0) * 1000
    result.row_count = len(entries)
    if not entries:
        result.status = "http_error"
        result.notes.append("zero NAR schedule entries parsed")
        return result
    result.status = "ok"
    ordered = sorted(entries, key=lambda e: e.release_date, reverse=True)
    sample = ordered[0]
    result.sample_row = {
        "series_id": sample.series_id,
        "raw_title": sample.raw_title,
        "reference_date": sample.reference_date,
        "release_date": sample.release_date,
        "event_time_utc": sample.event_time_utc,
    }
    by_series = Counter(entry.series_id for entry in entries)
    result.enum_counters = {"series_id": by_series}
    result.notes.append(
        "entries by series: "
        + ", ".join(f"{k}={v}" for k, v in by_series.most_common())
    )
    sample_n = min(10, len(ordered))
    result.parse_attempts = sample_n
    for entry in ordered[:sample_n]:
        ok, msg = _try_project_nar_schedule(entry)
        if ok:
            result.parse_successes += 1
        else:
            if len(result.parse_error_samples) < 3:
                result.parse_error_samples.append(msg)
    return result


def _run_nar_current_probe(
    result: ProbeResult,
    probe: NARProbe,
    current_fetcher,
) -> ProbeResult:
    import time as _time

    t0 = _time.monotonic()
    try:
        html = current_fetcher(probe.url)
        value = parse_nar_current_value_html(
            html,
            source_url=probe.url,
            series_id=probe.series_id,
        )
    except Exception as exc:
        result.status = "http_error"
        result.notes.append(f"{type(exc).__name__}: {exc}")
        return result
    result.http_elapsed_ms = (_time.monotonic() - t0) * 1000
    result.status = "ok"
    result.row_count = 1
    result.sample_row = {
        "series_id": value.series_id,
        "reference_date": value.reference_date,
        "actual": value.actual,
        "previous": value.previous,
        "raw_change": value.raw_change,
        "source_url": value.source_url,
    }
    result.parse_attempts = 1
    ok, msg = _try_project_nar_value(value)
    if ok:
        result.parse_successes = 1
    else:
        result.parse_error_samples.append(msg)
    return result


def run_nar_probe(
    probe: NARProbe,
    *,
    schedule_fetcher=fetch_nar_schedule_html,
    current_fetcher=fetch_nar_current_html,
) -> ProbeResult:
    """Execute one NAR public HTML probe."""
    generic = Probe(
        name=probe.name,
        path=f"GET {probe.url}",
        description=probe.description,
        expected_shape="HTML -> NAR schedule entries / current values",
        expected_fields=frozenset(),
    )
    result = ProbeResult(probe=generic, status="skipped")
    result.request_path = generic.path
    if probe.source == "schedule":
        return _run_nar_schedule_probe(result, schedule_fetcher)
    if probe.source == "current":
        return _run_nar_current_probe(result, probe, current_fetcher)
    raise ValueError(f"unknown NAR probe source: {probe.source!r}")


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
# METI probes
# ──────────────────────────────────────────────────────────────────────────


def plan_meti_probes() -> list[Probe]:
    return [
        Probe(
            name="meti_iip_release_calendar",
            path=ESTAT_RELEASE_CALENDAR_URL,
            description="e-Stat IIP release calendar (toukei_cd 00550300)",
            expected_shape="HTML rows -> MetiScheduleEntry",
            params={"surface": "iip"},
        ),
        Probe(
            name="meti_retail_schedule",
            path=METI_RETAIL_PAGE_URL,
            description="METI Current Survey of Commerce next-release sentence",
            expected_shape="HTML page -> MetiScheduleEntry",
            params={"surface": "retail"},
        ),
    ]


def run_meti_probe(probe: Probe) -> ProbeResult:
    result = ProbeResult(probe=probe, status="ok", request_path=probe.path)
    started = datetime.now(timezone.utc)
    try:
        if probe.name == "meti_iip_release_calendar":
            today = started.date()
            html_text = fetch_iip_release_calendar_html(
                today - timedelta(days=14),
                today + timedelta(days=90),
            )
            entries = parse_iip_release_calendar_html(html_text)
        elif probe.name == "meti_retail_schedule":
            html = fetch_retail_schedule_html()
            entries = [parse_retail_schedule_html(html)]
        else:
            raise ValueError(f"unknown METI probe: {probe.name}")

        result.row_count = len(entries)
        if entries:
            first = entries[0]
            result.sample_row = {
                "indicator": first.indicator,
                "reference_date": first.reference_date.isoformat(),
                "release_date": first.release_date.isoformat(),
                "release_time_local": first.release_time_local,
            }
        result.enum_counters = {
            "indicator": Counter(e.indicator for e in entries),
        }
        sample_n = min(10, len(entries))
        result.parse_attempts = sample_n
        for entry in entries[:sample_n]:
            try:
                meti_schedule_entry_to_records(entry, snapshot_epoch_ms=1)
                result.parse_successes += 1
            except Exception as exc:
                if len(result.parse_error_samples) < 3:
                    result.parse_error_samples.append(str(exc))
    except Exception as exc:
        result.status = "http_error"
        result.notes.append(str(exc))
    finally:
        result.http_elapsed_ms = (
            datetime.now(timezone.utc) - started
        ).total_seconds() * 1000
    return result


# ──────────────────────────────────────────────────────────────────────────
# Statistics Bureau / e-Stat probes
# ──────────────────────────────────────────────────────────────────────────


def plan_stat_bureau_probes() -> list[Probe]:
    cpi_ref = date(2026, 3, 1)
    lfs_ref = date(2026, 2, 1)
    return [
        Probe(
            name="stat_bureau_cpi_schedule",
            path="https://www.stat.go.jp/english/data/cpi/1582.htm",
            description="CPI release schedule — Japan column",
            expected_shape="HTML table rows -> StatBureauScheduleEntry",
            params={"surface": "cpi"},
        ),
        Probe(
            name="stat_bureau_lfs_schedule",
            path="https://www.stat.go.jp/english/data/roudou/1543.htm",
            description="Labour Force Survey release schedule — Basic tabulation",
            expected_shape="HTML table rows -> StatBureauScheduleEntry",
            params={"surface": "lfs"},
        ),
        Probe(
            name="stat_bureau_core_cpi_value",
            path=ESTAT_STATS_DATA_URL,
            description="e-Stat Core CPI YoY scalar value",
            expected_shape="GET_STATS_DATA.DATA_INF.VALUE scalar",
            params={
                "statsDataId": STAT_BUREAU_INDICATOR_REGISTRY["CORE_CPI"].stats_data_id,
                "indicator": "CORE_CPI",
                "reference": cpi_ref.isoformat(),
                "cdTime": time_code_for_month(cpi_ref),
            },
        ),
        Probe(
            name="stat_bureau_unemployment_value",
            path=ESTAT_STATS_DATA_URL,
            description="e-Stat Unemployment Rate scalar value",
            expected_shape="GET_STATS_DATA.DATA_INF.VALUE scalar",
            params={
                "statsDataId": STAT_BUREAU_INDICATOR_REGISTRY["UNEMPLOYMENT_RATE"].stats_data_id,
                "indicator": "UNEMPLOYMENT_RATE",
                "reference": lfs_ref.isoformat(),
                "cdTime": time_code_for_month(lfs_ref),
            },
        ),
    ]


def run_stat_bureau_probe(probe: Probe) -> ProbeResult:
    result = ProbeResult(probe=probe, status="ok", request_path=probe.path)
    started = datetime.now(timezone.utc)
    try:
        if probe.name == "stat_bureau_cpi_schedule":
            html = fetch_cpi_release_schedule_html()
            entries = parse_cpi_release_schedule_html(html)
        elif probe.name == "stat_bureau_lfs_schedule":
            html = fetch_lfs_release_schedule_html()
            entries = parse_lfs_release_schedule_html(html)
        elif probe.name in {
            "stat_bureau_core_cpi_value",
            "stat_bureau_unemployment_value",
        }:
            app_id = os.getenv("ESTAT_APP_ID", "").strip()
            if not app_id:
                result.status = "auth_missing"
                result.notes.append("ESTAT_APP_ID not set")
                return result
            indicator = str(probe.params["indicator"])
            reference = date.fromisoformat(str(probe.params["reference"]))
            spec = STAT_BUREAU_INDICATOR_REGISTRY[indicator]
            data = fetch_estat_value_json(spec, reference, app_id=app_id)
            value = parse_estat_value_json(
                data,
                indicator=indicator,
                reference=reference,
            )
            result.row_count = 1
            result.sample_row = {
                "indicator": indicator,
                "reference_date": value.reference_date.isoformat(),
                "time_code": value.time_code,
                "actual": value.actual,
                "unit": value.unit,
                "attrs": value.attrs,
            }
            result.parse_attempts = 1
            result.parse_successes = 1
            result.enum_counters = {
                "indicator": Counter({indicator: 1}),
                "unit": Counter({value.unit: 1}),
            }
            return result
        else:
            raise ValueError(f"unknown Statistics Bureau probe: {probe.name}")

        result.row_count = len(entries)
        if entries:
            first = entries[0]
            result.sample_row = {
                "indicator": first.indicator,
                "reference_date": first.reference_date.isoformat(),
                "release_date": first.release_date.isoformat(),
                "release_time_local": first.release_time_local,
            }
        result.enum_counters = {
            "indicator": Counter(e.indicator for e in entries),
        }
        sample_n = min(10, len(entries))
        result.parse_attempts = sample_n
        for entry in entries[:sample_n]:
            try:
                stat_bureau_schedule_entry_to_records(
                    entry,
                    snapshot_epoch_ms=1,
                )
                result.parse_successes += 1
            except Exception as exc:
                if len(result.parse_error_samples) < 3:
                    result.parse_error_samples.append(str(exc))
    except Exception as exc:
        result.status = "http_error"
        result.notes.append(str(exc))
    finally:
        result.http_elapsed_ms = (
            datetime.now(timezone.utc) - started
        ).total_seconds() * 1000
    return result


# ──────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────


_OFFICIAL_PROVIDERS: frozenset[str] = frozenset(
    {
        "bls", "bea", "census", "ism", "umich", "conference-board",
        "nar", "fed", "ecb", "nbs", "stat-bureau-jp", "meti",
    }
)
_OFFICIAL_PROVIDERS_WITH_PROBES: frozenset[str] = frozenset(
    {
        "bls", "bea", "census", "ism", "umich", "conference-board",
        "nar", "ecb", "fed", "nbs", "meti", "stat-bureau-jp",
    }
)


def parse_args(argv: list[str]) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument(
        "--provider",
        choices=[
            "te", "eodhd", "bls", "bea", "census", "ism", "umich",
            "conference-board", "nar", "fed", "ecb", "nbs",
            "stat-bureau-jp", "meti",
        ],
        default="te",
        help=(
            "which acquisition lane to validate. "
            "bls / bea / census / ism / umich / conference-board / "
            "nar / ecb / fed / nbs / meti / stat-bureau-jp have live probes."
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
    # scaffold-only stub. METI ships its connector before its live
    # acquisition probe, so it stays in this set for issue #14 P5.
    unwired = _OFFICIAL_PROVIDERS - _OFFICIAL_PROVIDERS_WITH_PROBES
    if args.provider in unwired:
        print(
            f"--provider {args.provider}: scaffold registered. "
            f"Probe bodies land with the {args.provider} live-probe phase. "
            f"Dry-run stub completed."
        )
        return 0

    if args.provider == "bls":
        bls_probes = plan_bls_probes()
        return _run_bls(args, bls_probes)

    if args.provider == "bea":
        bea_probes = plan_bea_probes()
        return _run_bea(args, bea_probes)

    if args.provider == "census":
        census_probes = plan_census_probes()
        return _run_census(args, census_probes)

    if args.provider == "ism":
        ism_probes = plan_ism_probes()
        return _run_ism(args, ism_probes)

    if args.provider == "umich":
        umich_probes = plan_umich_probes()
        return _run_umich(args, umich_probes)

    if args.provider == "conference-board":
        conference_board_probes = plan_conference_board_probes()
        return _run_conference_board(args, conference_board_probes)

    if args.provider == "nar":
        nar_probes = plan_nar_probes()
        return _run_nar(args, nar_probes)

    if args.provider == "ecb":
        ecb_probes = plan_ecb_probes()
        return _run_ecb(args, ecb_probes)

    if args.provider == "fed":
        fed_probes = plan_fed_probes()
        return _run_fed(args, fed_probes)

    if args.provider == "nbs":
        nbs_probes = plan_nbs_probes()
        return _run_nbs(args, nbs_probes)

    if args.provider == "meti":
        meti_probes = plan_meti_probes()
        return _run_meti(args, meti_probes)

    if args.provider == "stat-bureau-jp":
        stat_bureau_probes = plan_stat_bureau_probes()
        return _run_stat_bureau(args, stat_bureau_probes)

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


def _run_census(args: argparse.Namespace, probes: list[CensusProbe]) -> int:
    """Dispatch the Census EITS probe flow."""
    if not args.execute:
        print("DRY RUN (census) — pass --execute to actually hit upstream.")
        print()
        for i, p in enumerate(probes, 1):
            print(f"{i}. {p.name}")
            print(
                f"   coordinate: {p.dataset} data_type={p.data_type_code} "
                f"seasonally_adj={p.seasonally_adj} category={p.category_code}"
            )
            print(f"   year: {p.year}")
            print(f"   purpose: {p.description}")
        print()
        print(f"Total planned requests: {len(probes)}")
        return 0

    if not args.yes:
        print(f"Planned Census probes ({len(probes)}):")
        for i, p in enumerate(probes, 1):
            print(
                f"  {i}. {p.name} — {p.dataset} {p.data_type_code} "
                f"{p.category_code} {p.year}"
            )
        print(f"Estimated upstream requests: {len(probes)}")
        resp = input("Proceed with live run? [y/N] ").strip().lower()
        if resp not in {"y", "yes"}:
            print("Aborted.")
            return 1

    results: list[ProbeResult] = []
    client = CensusEITSClient()
    for probe in probes:
        result = run_census_probe(client, probe)
        results.append(result)
        _print_probe_summary(result)

    report = render_report(
        results,
        requests_spent=client.requests_made,
        provider="census",
    )
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / (
        f"calendar_acquisition_census_"
        f"{datetime.now(timezone.utc).date().isoformat()}.md"
    )
    report_path.write_text(report, encoding="utf-8")
    print()
    print(f"Report written: {report_path}")
    return 0


def _run_ism(args: argparse.Namespace, probes: list[ISMProbe]) -> int:
    """Dispatch the ISM public-HTML probe flow."""
    if not args.execute:
        print("DRY RUN (ism) — pass --execute to actually hit upstream.")
        print()
        for i, p in enumerate(probes, 1):
            print(f"{i}. {p.name}")
            print(f"   url: {p.url}")
            print(f"   purpose: {p.description}")
        print()
        print(f"Total planned requests: {len(probes)}")
        return 0

    if not args.yes:
        print(f"Planned ISM probes ({len(probes)}):")
        for i, p in enumerate(probes, 1):
            print(f"  {i}. {p.name} — {p.url}")
        print("Estimated upstream requests: 3")
        resp = input("Proceed with live run? [y/N] ").strip().lower()
        if resp not in {"y", "yes"}:
            print("Aborted.")
            return 1

    results: list[ProbeResult] = []
    for probe in probes:
        result = run_ism_probe(probe)
        results.append(result)
        _print_probe_summary(result)

    report = render_report(
        results,
        requests_spent=3,
        provider="ism",
    )
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / (
        f"calendar_acquisition_ism_"
        f"{datetime.now(timezone.utc).date().isoformat()}.md"
    )
    report_path.write_text(report, encoding="utf-8")
    print()
    print(f"Report written: {report_path}")
    return 0


def _run_umich(args: argparse.Namespace, probes: list[UMichProbe]) -> int:
    """Dispatch the U Michigan public HTML/PDF probe flow."""
    if not args.execute:
        print("DRY RUN (umich) — pass --execute to actually hit upstream.")
        print()
        for i, p in enumerate(probes, 1):
            print(f"{i}. {p.name}")
            print(f"   url: {p.url}")
            print(f"   purpose: {p.description}")
        print()
        print("Total planned requests: 3")
        return 0

    if not args.yes:
        print(f"Planned U Michigan probes ({len(probes)}):")
        for i, p in enumerate(probes, 1):
            print(f"  {i}. {p.name} — {p.url}")
        print("Estimated upstream requests: 3")
        resp = input("Proceed with live run? [y/N] ").strip().lower()
        if resp not in {"y", "yes"}:
            print("Aborted.")
            return 1

    results: list[ProbeResult] = []
    for probe in probes:
        result = run_umich_probe(probe)
        results.append(result)
        _print_probe_summary(result)

    report = render_report(
        results,
        requests_spent=3,
        provider="umich",
    )
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / (
        f"calendar_acquisition_umich_"
        f"{datetime.now(timezone.utc).date().isoformat()}.md"
    )
    report_path.write_text(report, encoding="utf-8")
    print()
    print(f"Report written: {report_path}")
    return 0


def _run_conference_board(
    args: argparse.Namespace,
    probes: list[ConferenceBoardProbe],
) -> int:
    """Dispatch the Conference Board public JSON/HTML probe flow."""
    if not args.execute:
        print("DRY RUN (conference-board) — pass --execute to actually hit upstream.")
        print()
        for i, p in enumerate(probes, 1):
            print(f"{i}. {p.name}")
            print(f"   url: {p.url}")
            print(f"   purpose: {p.description}")
        print()
        print("Total planned requests: 3")
        return 0

    if not args.yes:
        print(f"Planned Conference Board probes ({len(probes)}):")
        for i, p in enumerate(probes, 1):
            print(f"  {i}. {p.name} — {p.url}")
        print("Estimated upstream requests: 3")
        resp = input("Proceed with live run? [y/N] ").strip().lower()
        if resp not in {"y", "yes"}:
            print("Aborted.")
            return 1

    results: list[ProbeResult] = []
    for probe in probes:
        result = run_conference_board_probe(probe)
        results.append(result)
        _print_probe_summary(result)

    report = render_report(
        results,
        requests_spent=3,
        provider="conference-board",
    )
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / (
        f"calendar_acquisition_conference_board_"
        f"{datetime.now(timezone.utc).date().isoformat()}.md"
    )
    report_path.write_text(report, encoding="utf-8")
    print()
    print(f"Report written: {report_path}")
    return 0


def _run_nar(args: argparse.Namespace, probes: list[NARProbe]) -> int:
    """Dispatch the NAR public HTML probe flow."""
    if not args.execute:
        print("DRY RUN (nar) — pass --execute to actually hit upstream.")
        print()
        for i, p in enumerate(probes, 1):
            print(f"{i}. {p.name}")
            print(f"   url: {p.url}")
            print(f"   purpose: {p.description}")
        print()
        print("Total planned requests: 3")
        return 0

    if not args.yes:
        print(f"Planned NAR probes ({len(probes)}):")
        for i, p in enumerate(probes, 1):
            print(f"  {i}. {p.name} — {p.url}")
        print("Estimated upstream requests: 3")
        resp = input("Proceed with live run? [y/N] ").strip().lower()
        if resp not in {"y", "yes"}:
            print("Aborted.")
            return 1

    results: list[ProbeResult] = []
    for probe in probes:
        result = run_nar_probe(probe)
        results.append(result)
        _print_probe_summary(result)

    report = render_report(
        results,
        requests_spent=3,
        provider="nar",
    )
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / (
        f"calendar_acquisition_nar_"
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


def _run_meti(args: argparse.Namespace, probes: list[Probe]) -> int:
    """Dispatch the METI probe flow."""
    if not args.execute:
        print("DRY RUN (meti) — pass --execute to actually hit upstream.")
        print()
        for i, p in enumerate(probes, 1):
            print(f"{i}. {p.name}")
            print(f"   path: {p.path}")
            print(f"   purpose: {p.description}")
            print(f"   expected shape: {p.expected_shape}")
        print()
        print(f"Total planned requests: {len(probes)}")
        return 0

    if not args.yes and not confirm_budget(probes):
        print("Aborted.")
        return 1

    results: list[ProbeResult] = []
    for probe in probes:
        result = run_meti_probe(probe)
        results.append(result)
        _print_probe_summary(result)

    report = render_report(
        results,
        requests_spent=sum(1 for r in results if r.status == "ok"),
        provider="meti",
    )
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / (
        f"calendar_acquisition_meti_"
        f"{datetime.now(timezone.utc).date().isoformat()}.md"
    )
    report_path.write_text(report, encoding="utf-8")
    print()
    print(f"Report written: {report_path}")
    return 0


def _run_stat_bureau(args: argparse.Namespace, probes: list[Probe]) -> int:
    """Dispatch the Statistics Bureau probe flow."""
    if not args.execute:
        print("DRY RUN (stat-bureau-jp) — pass --execute to actually hit upstream.")
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
    for probe in probes:
        result = run_stat_bureau_probe(probe)
        results.append(result)
        _print_probe_summary(result)

    report = render_report(
        results,
        requests_spent=sum(1 for r in results if r.status == "ok"),
        provider="stat-bureau-jp",
    )
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / (
        f"calendar_acquisition_stat_bureau_jp_"
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

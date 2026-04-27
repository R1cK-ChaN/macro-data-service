"""Trading Economics calendar validation — planner, runner, diff helpers.

Five-probe budget: baseline recent window, older era, country-scoped,
``/calendar/updates`` pointer shape, and a ``/calendar/calendarid``
rehydrate that consumes ids from the first probe.
"""

from __future__ import annotations

from collections import Counter
from typing import Any
from urllib.parse import quote

from ingestion.calendar.te_api import TEAPIClient, parse_calendar_row
from ingestion.calendar.te_api.client import TECallResult
from ingestion.calendar.te_api.parser import ALL_TE_FIELDS

from scripts.validate._shared import (
    Probe,
    ProbeResult,
    RowDiff,
    days_ago_iso,
    today_iso,
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

# Enum-type fields we want to tally to surface the real vocabulary.
TE_ENUM_FIELDS: tuple[str, ...] = ("Importance", "Currency", "Country", "Category")


def plan_te_probes() -> list[Probe]:
    """Fixed 5-probe budget for round 1. Extend only with evidence."""
    return [
        Probe(
            name="country_all_last_7d",
            path=f"/calendar/country/All/{days_ago_iso(7)}/{today_iso()}",
            description="baseline 22-field shape over a dense recent window",
            expected_shape="list[22-field dict]",
            expected_fields=TE_PARSER_READS,
        ),
        Probe(
            name="country_all_2024_01",
            path="/calendar/country/All/2024-01-01/2024-01-07",
            description="older era — shape drift vs recent window",
            expected_shape="list[22-field dict]",
            expected_fields=TE_PARSER_READS,
        ),
        Probe(
            # Country name with a space exercises URL encoding.
            name="country_us_last_7d",
            path=f"/calendar/country/{quote('United States')}/{days_ago_iso(7)}/{today_iso()}",
            description="country-scoped + URL encoding on spaces",
            expected_shape="list[22-field dict]",
            expected_fields=TE_PARSER_READS,
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
            expected_fields=TE_PARSER_READS,
        ),
    ]


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


__all__ = [
    "ALL_TE_FIELDS",
    "TEAPIClient",
    "TE_ENUM_FIELDS",
    "TE_PARSER_READS",
    "TE_UPDATES_POINTER_READS",
    "diff_te_row",
    "plan_te_probes",
    "resolve_dynamic_ids",
    "run_probe",
    "try_parse",
]

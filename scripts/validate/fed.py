"""Fed FOMC + release-feed validation — planner, runner.

Two HTML/JSON surfaces: ``fomccalendars.htm`` (HTML) and
``/json/calendar.json`` (Beige Book / H.4.1 / H.8 alongside FOMC
meetings and speeches).
"""

from __future__ import annotations

import time as _time
from collections import Counter
from dataclasses import dataclass

from ingestion.calendar.fed_api import (
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

from scripts.validate._shared import Probe, ProbeResult


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


__all__ = [
    "FED_INDICATOR_REGISTRY",
    "FedProbe",
    "plan_fed_probes",
    "run_fed_probe",
]

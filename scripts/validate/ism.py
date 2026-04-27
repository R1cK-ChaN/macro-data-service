"""ISM Manufacturing PMI calendar validation — planner, runner.

Two probes: the public release-calendar table (schedule) and the
current-report HTML page (one-shot value parse).
"""

from __future__ import annotations

import time as _time
from collections import Counter
from dataclasses import dataclass
from typing import Any

from ingestion.calendar.ism_api import (
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

from scripts.validate._shared import Probe, ProbeResult


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


__all__ = ["ISMProbe", "plan_ism_probes", "run_ism_probe"]

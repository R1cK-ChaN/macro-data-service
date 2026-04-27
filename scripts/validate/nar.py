"""NAR housing-statistics calendar validation — planner, runner.

Three probes: the statistical-release schedule page, plus current
Existing Home Sales and Pending Home Sales pages (one-shot value
parses).
"""

from __future__ import annotations

import time as _time
from collections import Counter
from dataclasses import dataclass
from typing import Any

from ingestion.calendar.nar_api import (
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

from scripts.validate._shared import Probe, ProbeResult


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


__all__ = ["NARProbe", "plan_nar_probes", "run_nar_probe"]

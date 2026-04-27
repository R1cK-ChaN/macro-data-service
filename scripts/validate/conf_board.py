"""Conference Board calendar validation — planner, runner.

Three probes: the calendar JSON endpoint (schedule rows for Consumer
Confidence + US Leading Index), and one current-value HTML probe per
indicator.
"""

from __future__ import annotations

import time as _time
from collections import Counter
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from typing import Any

from ingestion.calendar.conference_board_api import (
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

from scripts.validate._shared import Probe, ProbeResult


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


__all__ = [
    "ConferenceBoardProbe",
    "plan_conference_board_probes",
    "run_conference_board_probe",
]

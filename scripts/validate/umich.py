"""U Michigan Consumer Sentiment calendar validation — planner, runner.

Two probes: the survey-info release-dates document (PDF/HTML schedule)
and the current-results page (one-shot value parse).
"""

from __future__ import annotations

import time as _time
from collections import Counter
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Any

from ingestion.calendar.umich_api import (
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

from scripts.validate._shared import Probe, ProbeResult


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


__all__ = ["UMichProbe", "plan_umich_probes", "run_umich_probe"]

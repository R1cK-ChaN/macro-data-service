"""METI / e-Stat IIP + retail calendar validation — planner, runner.

Two probes: the e-Stat IIP release calendar (HTML) and the METI
Current Survey of Commerce next-release sentence on the retail page.
"""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone

from ingestion.calendar.meti_api import (
    ESTAT_RELEASE_CALENDAR_URL,
    METI_RETAIL_PAGE_URL,
    fetch_iip_release_calendar_html,
    fetch_retail_schedule_html,
    parse_iip_release_calendar_html,
    parse_retail_schedule_html,
    schedule_entry_to_records as meti_schedule_entry_to_records,
)

from scripts.validate._shared import Probe, ProbeResult


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


__all__ = ["plan_meti_probes", "run_meti_probe"]

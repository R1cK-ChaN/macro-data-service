"""Statistics Bureau JP / e-Stat calendar validation — planner, runner.

Four probes: CPI release schedule (HTML), Labour Force Survey
schedule (HTML), and two e-Stat scalar values (Core CPI, Unemployment
Rate) that hit the GET_STATS_DATA endpoint with an ESTAT_APP_ID.
"""

from __future__ import annotations

import os
from collections import Counter
from datetime import date, datetime, timezone

from ingestion.calendar.stat_bureau_api import (
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

from scripts.validate._shared import Probe, ProbeResult


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


__all__ = ["plan_stat_bureau_probes", "run_stat_bureau_probe"]

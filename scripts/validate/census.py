"""Census EITS calendar validation — planner, runner, diff helpers.

One probe per Census calendar registry entry; each probe drives
:class:`CensusEITSClient.get_dataset_year` against a single dataset
year and filters the returned rows down to the indicator coordinate.
"""

from __future__ import annotations

import re
import time as _time
from collections import Counter
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Any

from ingestion.calendar.census_api import (
    CensusEITSClient,
    CensusEITSObservation,
    INDICATOR_REGISTRY as CENSUS_INDICATOR_REGISTRY,
    parse_observation as parse_census_observation,
)

from scripts.validate._shared import Probe, ProbeResult, RowDiff


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
        token = _census_probe_token(spec.indicator)
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


def _census_probe_token(indicator_label: str) -> str:
    """Slugify an indicator label into a stable probe-name token."""
    token = indicator_label.strip().lower()
    token = re.sub(r"[^a-z0-9]+", "_", token).strip("_")
    return token or "indicator"


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


__all__ = [
    "CENSUS_EITS_EXPECTED_FIELDS",
    "CENSUS_INDICATOR_REGISTRY",
    "CensusEITSClient",
    "CensusProbe",
    "plan_census_probes",
    "run_census_probe",
]

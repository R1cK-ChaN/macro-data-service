"""Mocked tests for the EIA weekly-stocks calendar connector (issue #50).

Uses a fake EIAClient subclass to feed canned ``EIAObservation``
batches; no real HTTP.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from ingestion.calendar.eia_api import (
    EIAObservation,
    INDICATOR_REGISTRY,
    fetch_eia_calendar,
    observation_to_records,
)
from ingestion.calendar.eia_api.parser import PROVIDER, _next_release_datetime
from ingestion.timeseries.scrapers.eia import EIAObservation as TSObservation
from storage.sqlite import SQLiteEngineStore


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


# ── parser ───────────────────────────────────────────────────────


def test_next_release_datetime_lands_on_next_wednesday_for_petroleum() -> None:
    # April 17 (Friday) period → Wednesday April 22 10:30 ET.
    iso = _next_release_datetime(
        date(2026, 4, 17), dow=2, time_local="10:30 AM ET",
    )
    assert iso.startswith("2026-04-22T")


def test_next_release_datetime_lands_on_next_thursday_for_natural_gas() -> None:
    iso = _next_release_datetime(
        date(2026, 4, 17), dow=3, time_local="10:30 AM ET",
    )
    assert iso.startswith("2026-04-23T")


def test_observation_to_records_uses_period_as_reference_date() -> None:
    obs = EIAObservation(
        indicator="CRUDE_OIL_STOCKS",
        period="2026-04-17",
        value="465729",
        unit="MBBL",
        raw={},
    )
    raw, ev = observation_to_records(obs, snapshot_epoch_ms=1_800_000_000_000)
    assert ev.actual == "465729"
    assert ev.reference_date == "2026-04-17"
    assert ev.event_time_utc.startswith("2026-04-22T")
    assert ev.title.startswith("US Crude Oil Stocks")


def test_observation_unknown_indicator_raises() -> None:
    obs = EIAObservation(
        indicator="NOT_A_REAL_INDICATOR",
        period="2026-04-17", value="1", unit="", raw={},
    )
    with pytest.raises(KeyError):
        observation_to_records(obs, snapshot_epoch_ms=1)


# ── fetcher ──────────────────────────────────────────────────────


@dataclass
class _FakeEIAClient:
    """Minimal stub of :class:`EIAClient.get_series` for tests."""

    api_key: str = "TEST"
    response: dict = None  # type: ignore[assignment]

    def get_series(
        self, route: str, *,
        params: dict, series_id: str,
        start: str | None = None, limit: int = 100,
    ) -> list[TSObservation]:
        return self.response.get(series_id, [])


def test_fetch_eia_calendar_writes_one_event_per_observation(
    store: SQLiteEngineStore,
) -> None:
    response = {
        "WCESTUS1": [
            TSObservation(series_id="WCESTUS1", date="2026-04-10", value=463804.0, unit="MBBL"),
            TSObservation(series_id="WCESTUS1", date="2026-04-17", value=465729.0, unit="MBBL"),
        ],
        "WGTSTUS1": [
            TSObservation(series_id="WGTSTUS1", date="2026-04-17", value=228374.0, unit="MBBL"),
        ],
        "WDISTUS1": [],
        "NW2_EPG0_SWO_NUS_BCF": [
            TSObservation(
                series_id="NW2_EPG0_SWO_NUS_BCF",
                date="2026-04-17", value=2024.0, unit="BCF",
            ),
        ],
    }
    client = _FakeEIAClient(response=response)

    with store._connection(commit=True) as conn:
        summary = fetch_eia_calendar(
            conn, client,
            dry_run=False,
            start="2026-04-01",
            end="2026-04-30",
            snapshot_epoch_ms=1_800_000_000_000,
        )

    assert summary.dry_run is False
    assert summary.observations_seen == 4
    assert summary.events_upserted == 4
    assert set(summary.indicators_ok) == {
        "CRUDE_OIL_STOCKS", "GASOLINE_STOCKS", "NATURAL_GAS_STORAGE",
    }
    assert "DISTILLATE_STOCKS" in summary.indicators_empty


def test_fetch_eia_calendar_isolates_per_indicator_failure(
    store: SQLiteEngineStore,
) -> None:
    class _Boom(_FakeEIAClient):
        def get_series(self, route, *, params, series_id, start=None, limit=100):
            if series_id == "WGTSTUS1":
                raise RuntimeError("simulated 502")
            return self.response.get(series_id, [])

    response = {
        "WCESTUS1": [
            TSObservation(series_id="WCESTUS1", date="2026-04-17", value=465729.0, unit="MBBL"),
        ],
        "WDISTUS1": [],
        "NW2_EPG0_SWO_NUS_BCF": [],
    }
    client = _Boom(response=response)

    with store._connection(commit=True) as conn:
        summary = fetch_eia_calendar(
            conn, client,
            dry_run=False,
            start="2026-04-01", end="2026-04-30",
        )

    failed = {ind for ind, _ in summary.series_failed}
    assert "GASOLINE_STOCKS" in failed
    assert "CRUDE_OIL_STOCKS" in summary.indicators_ok


def test_fetch_eia_calendar_dry_run_returns_plan(
    store: SQLiteEngineStore,
) -> None:
    with store._connection(commit=False) as conn:
        summary = fetch_eia_calendar(
            conn, _FakeEIAClient(response={}),
            dry_run=True,
        )
    assert summary.dry_run is True
    assert set(summary.indicators_planned) == set(INDICATOR_REGISTRY.keys())


def test_fetch_eia_calendar_unknown_indicator_warns(
    store: SQLiteEngineStore,
) -> None:
    with store._connection(commit=True) as conn:
        summary = fetch_eia_calendar(
            conn, _FakeEIAClient(response={}),
            indicators=["CRUDE_OIL_STOCKS", "NOT_REAL"],
            dry_run=False,
        )
    assert "NOT_REAL" in summary.indicators_unknown


# ── scheduler + agency wiring ───────────────────────────────────


def test_eia_listed_in_scheduler_rosters() -> None:
    from ingestion.calendar.scheduler import (
        ALL_VALUE_SIDE_CONNECTORS, ALL_CONNECTORS, _VALUE_SIDE_DUE_ROW_FILTERS,
    )
    assert "eia" in ALL_CONNECTORS
    assert "eia" in ALL_VALUE_SIDE_CONNECTORS
    assert "eia" in _VALUE_SIDE_DUE_ROW_FILTERS


def test_eia_agency_registered_with_empty_indicator_set() -> None:
    from ingestion.calendar.agency_registry import provider_to_agency
    eia = provider_to_agency("eia")
    assert eia is not None and eia.agency_id == "EIA"
    # No parity whitelist yet — TE renders these with magnitude
    # markers that need an alignment spec.
    assert eia.indicators == frozenset()

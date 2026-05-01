from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ingestion.series_config import BIS_SERIES
from ingestion.timeseries.clients import _sdmx_clients
from storage.sqlite import SQLiteEngineStore


@dataclass(frozen=True)
class _FakeBISObservation:
    series_id: str
    dataflow: str
    date: str = "2025-01-01"
    value: float = 1.0


class _FakeBISClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def get_data(
        self,
        dataflow: str,
        key: str,
        *,
        series_id: str,
        limit: int,
        **kwargs: object,
    ) -> list[_FakeBISObservation]:
        self.calls.append({
            "dataflow": dataflow,
            "key": key,
            "series_id": series_id,
            "limit": limit,
            **kwargs,
        })
        return [_FakeBISObservation(series_id=series_id, dataflow=dataflow)]


def test_workbook_bis_total_credit_series_config() -> None:
    expected = {
        "tc_gov_us": ("Q.US.G.A.N.770.A", "BIS_TC_GOV_US"),
        "tc_hh_us": ("Q.US.H.A.M.770.A", "BIS_TC_HH_US"),
        "tc_nfc_us": ("Q.US.N.A.M.770.A", "BIS_TC_NFC_US"),
    }

    for key, (bis_key, series_id) in expected.items():
        assert BIS_SERIES[key] == {
            "dataflow": "WS_TC",
            "version": "2.0",
            "key": bis_key,
            "series_id": series_id,
            "category": "credit",
        }


def test_workbook_bis_total_credit_seed_families_concepts_and_schedules(
    tmp_path: Path,
) -> None:
    store = SQLiteEngineStore(tmp_path / "engine.db")
    store.seed_obs_sources_and_families()
    store.seed_concept_map()
    store.seed_release_schedules()

    expected = {
        "GOV_LEVERAGE_US": (
            "BIS_TC_GOV_US", "us.credit.gov_leverage",
        ),
        "HOUSEHOLD_LEVERAGE_US": (
            "BIS_TC_HH_US", "us.credit.household_leverage",
        ),
        "NFC_LEVERAGE_US": (
            "BIS_TC_NFC_US", "us.credit.nfc_leverage",
        ),
    }

    for concept_id, (series_id, family_id) in expected.items():
        family = store.get_obs_family(family_id)
        assert family is not None
        assert family.source_id == "bis"
        assert family.provider_series_id == series_id
        assert family.unit == "percent"
        assert family.frequency == "quarterly"

        mappings = store.get_concept_series(concept_id)
        assert len(mappings) == 1
        assert mappings[0].source_id == "bis"
        assert mappings[0].provider_series_id == series_id
        assert mappings[0].obs_family_id == family_id

        schedule = store.get_release_schedule(concept_id)
        assert schedule is not None
        assert schedule.rule_type == "approximate_window"
        assert schedule.frequency == "quarterly"


def test_bis_direct_refresh_passes_configured_total_credit_version(
    tmp_path: Path,
    monkeypatch,
) -> None:
    store = SQLiteEngineStore(tmp_path / "engine.db")
    fake_client = _FakeBISClient()
    client = _sdmx_clients.BISIngestionClient()
    client.client = fake_client
    monkeypatch.setattr(_sdmx_clients.time, "sleep", lambda _: None)

    stats = client.refresh(store)

    assert stats.count == len(BIS_SERIES)
    calls_by_series = {
        str(call["series_id"]): call
        for call in fake_client.calls
    }
    for series_id in {"BIS_TC_GOV_US", "BIS_TC_HH_US", "BIS_TC_NFC_US"}:
        assert calls_by_series[series_id]["version"] == "2.0"
    assert "version" not in calls_by_series["BIS_CREDIT_GAP_US"]

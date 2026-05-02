from __future__ import annotations

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ingestion.fetchers._sdmx import SDMXFetcher
from ingestion.series_config import BUNDESBANK_SERIES
from ingestion.source_capabilities import SourceCapabilityManager
from ingestion.sources import IngestionOrchestrator
from ingestion.sdmx._types import SDMXObservation
from ingestion.sdmx.providers.bundesbank import BundesbankClient
from storage.sqlite import SQLiteEngineStore
from storage.subjects import sync_from_yaml


def _sdmx_json() -> dict:
    return {
        "structure": {
            "dimensions": {
                "observation": [
                    {
                        "id": "TIME_PERIOD",
                        "values": [
                            {"id": "2026-04-29"},
                            {"id": "2026-04-30"},
                        ],
                    }
                ]
            }
        },
        "dataSets": [
            {
                "series": {
                    "0:0": {
                        "observations": {
                            "0": [3.08],
                            "1": [3.11],
                        }
                    }
                }
            }
        ],
    }


class _FakeResponse:
    status_code = 200
    content = b"{present}"
    headers: dict[str, str] = {}
    text = ""
    url = "https://api.statistiken.bundesbank.de/rest/data/BBSSY/key"

    def __init__(self, payload: dict) -> None:
        self.payload = payload

    def json(self) -> dict:
        return self.payload

    def raise_for_status(self) -> None:
        return None


class _FakeBundesbankClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str, int]] = []

    def get_data_with_raw(
        self,
        dataflow: str,
        key: str,
        *,
        series_id: str,
        limit: int,
    ) -> tuple[list[SDMXObservation], dict, dict[str, str]]:
        self.calls.append((dataflow, key, series_id, limit))
        return (
            [
                SDMXObservation(
                    series_id=series_id,
                    date="2026-04-30",
                    value=3.11,
                    dataflow=dataflow,
                )
            ],
            {"series_id": series_id, "dataflow": dataflow},
            {"dataflow_id": dataflow, "key": key, "lastNObservations": str(limit)},
        )


class _FakeBundesbankIngestionClient:
    def __init__(self) -> None:
        self.client = _FakeBundesbankClient()


def test_bundesbank_series_config_contains_official_yield_keys() -> None:
    expected = {
        "de_govt_2y": ("D.REN.EUR.A610.000000WT0202.A", "BUNDESBANK_DE_GOVT_2Y"),
        "de_govt_5y": ("D.REN.EUR.A620.000000WT0505.A", "BUNDESBANK_DE_GOVT_5Y"),
        "de_govt_7y": ("D.REN.EUR.A607.000000WT7070.A", "BUNDESBANK_DE_GOVT_7Y"),
        "de_govt_10y": ("D.REN.EUR.A630.000000WT1010.A", "BUNDESBANK_DE_GOVT_10Y"),
        "de_govt_15y": ("D.REN.EUR.A615.000000WT1515.A", "BUNDESBANK_DE_GOVT_15Y"),
        "de_govt_30y": ("D.REN.EUR.A640.000000WT3030.A", "BUNDESBANK_DE_GOVT_30Y"),
    }

    for key, (series_key, series_id) in expected.items():
        cfg = BUNDESBANK_SERIES[key]
        assert cfg["dataflow"] == "BBSSY"
        assert cfg["key"] == series_key
        assert cfg["series_id"] == series_id
        assert cfg["category"] == "rates"


def test_bundesbank_client_uses_sdmx_json_accept_without_format_param(monkeypatch) -> None:
    client = BundesbankClient()
    calls: list[tuple[str, dict[str, str]]] = []

    def fake_get(url: str, params: dict[str, str]) -> _FakeResponse:
        calls.append((url, dict(params)))
        return _FakeResponse(_sdmx_json())

    monkeypatch.setattr(client, "_get", fake_get)

    observations, payload, params = client.get_data_with_raw(
        "BBSSY",
        "D.REN.EUR.A630.000000WT1010.A",
        series_id="BUNDESBANK_DE_GOVT_10Y",
        limit=2,
    )

    assert client.session.headers["Accept"] == (
        "application/vnd.sdmx.data+json;version=1.0.0"
    )
    assert calls == [
        (
            "https://api.statistiken.bundesbank.de/rest/data/BBSSY/"
            "D.REN.EUR.A630.000000WT1010.A",
            {"lastNObservations": "2"},
        )
    ]
    assert params == {
        "dataflow_id": "BBSSY",
        "key": "D.REN.EUR.A630.000000WT1010.A",
        "lastNObservations": "2",
    }
    assert "format" not in params
    assert payload == _sdmx_json()
    assert observations == [
        SDMXObservation(
            series_id="BUNDESBANK_DE_GOVT_10Y",
            date="2026-04-30",
            value=3.11,
            dataflow="BBSSY",
        ),
        SDMXObservation(
            series_id="BUNDESBANK_DE_GOVT_10Y",
            date="2026-04-29",
            value=3.08,
            dataflow="BBSSY",
        ),
    ]


def test_bundesbank_fetcher_normalizes_to_raw_series(monkeypatch) -> None:
    fake_client = _FakeBundesbankClient()
    monkeypatch.setattr("ingestion.fetchers._sdmx.time.sleep", lambda _: None)

    fetcher = SDMXFetcher(
        fake_client,
        "bundesbank",
        {"de_govt_10y": BUNDESBANK_SERIES["de_govt_10y"]},
    )
    rows = fetcher.fetch()

    assert len(rows) == 1
    row = rows[0]
    assert row.source == "bundesbank"
    assert row.series_id == "BUNDESBANK_DE_GOVT_10Y"
    assert row.observations[0].date == "2026-04-30"
    assert row.observations[0].value == 3.11
    assert row.observations[0].provider_metadata == {"dataflow": "BBSSY"}
    assert row.series_metadata == {"category": "rates"}
    assert row.content_hash is not None
    assert json.loads(row.request_params_json or "{}") == {
        "dataflow_id": "BBSSY",
        "key": "D.REN.EUR.A630.000000WT1010.A",
        "lastNObservations": "30",
    }


def test_bundesbank_seed_families_concepts_schedules_subjects_and_discovery(
    tmp_path: Path,
) -> None:
    store = SQLiteEngineStore(tmp_path / "engine.db")
    store.seed_obs_sources_and_families()
    store.seed_concept_map()
    store.seed_release_schedules()
    sync_from_yaml(store)

    source = store.get_obs_source("bundesbank")
    assert source is not None
    assert source.source_name == "Deutsche Bundesbank"
    assert source.country_code == "DE"

    expected = {
        "DE_GOVT_2Y": ("BUNDESBANK_DE_GOVT_2Y", "de.rates.govt_2y"),
        "DE_GOVT_5Y": ("BUNDESBANK_DE_GOVT_5Y", "de.rates.govt_5y"),
        "DE_GOVT_7Y": ("BUNDESBANK_DE_GOVT_7Y", "de.rates.govt_7y"),
        "DE_GOVT_10Y": ("BUNDESBANK_DE_GOVT_10Y", "de.rates.govt_10y"),
        "DE_GOVT_15Y": ("BUNDESBANK_DE_GOVT_15Y", "de.rates.govt_15y"),
        "DE_GOVT_30Y": ("BUNDESBANK_DE_GOVT_30Y", "de.rates.govt_30y"),
    }

    for concept_id, (series_id, family_id) in expected.items():
        family = store.get_obs_family(family_id)
        assert family is not None
        assert family.source_id == "bundesbank"
        assert family.provider_series_id == series_id
        assert family.unit == "percent"
        assert family.frequency == "daily"
        assert family.country_code == "DE"

        mappings = store.get_concept_series(concept_id)
        assert len(mappings) == 1
        assert mappings[0].source_id == "bundesbank"
        assert mappings[0].provider_series_id == series_id
        assert mappings[0].obs_family_id == family_id

        schedule = store.get_release_schedule(concept_id)
        assert schedule is not None
        assert schedule.rule_type == "daily"
        assert schedule.frequency == "daily"

    assert store.resolve_subjects_for_concept("DE_GOVT_10Y") == ["rate.de.govt"]

    manager = SourceCapabilityManager(store)
    entities = manager.list_entities("bundesbank", query="10Y", limit=5)["entities"]
    assert [entity["entity_id"] for entity in entities] == [
        "BUNDESBANK_DE_GOVT_10Y"
    ]
    assert entities[0]["metadata"]["key"] == "D.REN.EUR.A630.000000WT1010.A"


def test_bundesbank_orchestrator_source_stores_indicator_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    store = SQLiteEngineStore(tmp_path / "engine.db")
    bundesbank = _FakeBundesbankIngestionClient()
    monkeypatch.setattr("ingestion.fetchers._sdmx.time.sleep", lambda _: None)
    orchestrator = IngestionOrchestrator(store, bundesbank=bundesbank)

    report = orchestrator.run_source("bundesbank")

    assert report.error == ""
    assert report.fetched == len(BUNDESBANK_SERIES)
    assert report.stored == len(BUNDESBANK_SERIES)
    assert len(bundesbank.client.calls) == len(BUNDESBANK_SERIES)

    with store._connection(commit=False) as connection:
        row = connection.execute(
            """
            SELECT series_id, source, date, value, obs_family_id
            FROM indicators
            WHERE series_id = 'BUNDESBANK_DE_GOVT_10Y'
            """
        ).fetchone()
        raw = connection.execute(
            """
            SELECT source, series_id, request_params_json
            FROM obs_raw
            WHERE series_id = 'BUNDESBANK_DE_GOVT_10Y'
            """
        ).fetchone()

    assert row is not None
    assert raw is not None
    assert dict(row) == {
        "series_id": "BUNDESBANK_DE_GOVT_10Y",
        "source": "bundesbank",
        "date": "2026-04-30",
        "value": 3.11,
        "obs_family_id": "de.rates.govt_10y",
    }
    assert raw["source"] == "bundesbank"
    assert raw["series_id"] == "BUNDESBANK_DE_GOVT_10Y"
    assert json.loads(raw["request_params_json"]) == {
        "dataflow_id": "BBSSY",
        "key": "D.REN.EUR.A630.000000WT1010.A",
        "lastNObservations": "30",
    }

    sync_from_yaml(store)
    subject_rows = store.list_subject_indicators("rate.de.govt", limit=10)
    assert any(
        item["series_id"] == "BUNDESBANK_DE_GOVT_10Y"
        and item["source"] == "bundesbank"
        and item["concept_id"] == "DE_GOVT_10Y"
        for item in subject_rows
    )

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ingestion.fetchers._mof_jgb import MOFJGBFetcher
from ingestion.release_schedule import next_expected_release
from ingestion.series_config import MOF_JGB_SERIES
from ingestion.source_capabilities import SourceCapabilityManager
from ingestion.sources import IngestionOrchestrator
from ingestion.validation._dimensions import check_dimensions
from ingestion.scrapers.mof_jgb import (
    MOFJGBClient,
    MOFJGBObservation,
    MOF_JGB_INTEREST_RATE_URL,
    parse_jgb_interest_rate_csv,
)
from storage.sqlite import SQLiteEngineStore
from storage.subjects import sync_from_yaml


_MATURITIES = (
    "1Y", "2Y", "3Y", "4Y", "5Y", "6Y", "7Y", "8Y", "9Y",
    "10Y", "15Y", "20Y", "25Y", "30Y", "40Y",
)


def _csv_text() -> str:
    return (
        "Interest Rate,,,,,,,,,,,,,,,(Unit : %)\n"
        "Date,1Y,2Y,3Y,4Y,5Y,6Y,7Y,8Y,9Y,10Y,15Y,20Y,25Y,30Y,40Y\n"
        "2026/4/28,0.45,0.50,0.55,0.60,0.65,0.70,0.75,0.80,0.85,0.90,1.20,1.50,1.70,1.90,2.10\n"
        "2026/4/29,0.46,0.51,0.56,0.61,0.66,0.71,0.76,0.81,0.86,0.91,1.21,1.51,1.71,1.91,2.11\n"
        "2026/4/30,0.47,0.52,0.57,0.62,0.67,0.72,0.77,0.82,0.87,0.92,1.22,1.52,1.72,1.92,2.12\n"
    )


class _FakeResponse:
    content = _csv_text().encode("utf-8")

    def raise_for_status(self) -> None:
        return None


class _FakeMOFJGBClient:
    def __init__(self) -> None:
        self.calls: list[tuple[tuple[str, ...], int]] = []

    def get_all_series_with_raw(
        self,
        maturities: list[str] | tuple[str, ...],
        *,
        limit: int,
    ) -> dict[str, tuple[list[MOFJGBObservation], dict, dict[str, str]]]:
        self.calls.append((tuple(maturities), limit))
        result: dict[str, tuple[list[MOFJGBObservation], dict, dict[str, str]]] = {}
        for maturity in maturities:
            obs = [
                MOFJGBObservation(
                    date="2026-04-30",
                    maturity=maturity,
                    value=1.23,
                )
            ]
            result[maturity] = (
                obs,
                {
                    "source_url": MOF_JGB_INTEREST_RATE_URL,
                    "maturity": maturity,
                    "observations": [{"date": "2026-04-30", "value": 1.23}],
                },
                {
                    "url": MOF_JGB_INTEREST_RATE_URL,
                    "maturity": maturity,
                    "lastNObservations": str(limit),
                },
            )
        return result

    def get_series_with_raw(
        self,
        maturity: str,
        *,
        limit: int,
    ) -> tuple[list[MOFJGBObservation], dict, dict[str, str]]:
        return self.get_all_series_with_raw((maturity,), limit=limit)[maturity]


def test_mof_jgb_series_config_covers_official_curve() -> None:
    assert tuple(cfg["maturity"] for cfg in MOF_JGB_SERIES.values()) == _MATURITIES
    for maturity in _MATURITIES:
        cfg = MOF_JGB_SERIES[f"jp_govt_{maturity.lower()}"]
        assert cfg["series_id"] == f"MOF_JP_GOVT_{maturity}"
        assert cfg["category"] == "rates"


def test_mof_jgb_csv_parser_normalizes_dates_and_values() -> None:
    parsed = parse_jgb_interest_rate_csv(_csv_text())

    assert parsed["10Y"][:2] == [
        MOFJGBObservation(date="2026-04-30", maturity="10Y", value=0.92),
        MOFJGBObservation(date="2026-04-29", maturity="10Y", value=0.91),
    ]
    assert parsed["1Y"][0].date == "2026-04-30"
    assert parsed["40Y"][0].value == 2.12


def test_mof_jgb_client_fetches_single_csv_and_returns_raw_payload(monkeypatch) -> None:
    client = MOFJGBClient()
    calls: list[tuple[str, int]] = []

    def fake_get(url: str, timeout: int) -> _FakeResponse:
        calls.append((url, timeout))
        return _FakeResponse()

    monkeypatch.setattr(client.session, "get", fake_get)

    result = client.get_all_series_with_raw(("1Y", "10Y"), limit=2)

    assert calls == [(MOF_JGB_INTEREST_RATE_URL, 30)]
    observations, payload, params = result["10Y"]
    assert [obs.date for obs in observations] == ["2026-04-30", "2026-04-29"]
    assert payload["maturity"] == "10Y"
    assert payload["observations"][0] == {"date": "2026-04-30", "value": 0.92}
    assert params == {
        "url": MOF_JGB_INTEREST_RATE_URL,
        "maturity": "10Y",
        "lastNObservations": "2",
    }


def test_mof_jgb_fetcher_normalizes_to_raw_series() -> None:
    fake_client = _FakeMOFJGBClient()
    fetcher = MOFJGBFetcher(
        client=fake_client,
        series_config={"jp_govt_10y": MOF_JGB_SERIES["jp_govt_10y"]},
    )

    rows = fetcher.fetch()

    assert len(rows) == 1
    row = rows[0]
    assert row.source == "mof_jp"
    assert row.series_id == "MOF_JP_GOVT_10Y"
    assert row.observations[0].date == "2026-04-30"
    assert row.observations[0].value == 1.23
    assert row.observations[0].provider_metadata == {"maturity": "10Y"}
    assert row.series_metadata == {
        "category": "rates",
        "maturity": "10Y",
        "name": "Japan 10Y Government Bond Yield",
    }
    assert row.content_hash is not None
    assert json.loads(row.request_params_json or "{}") == {
        "url": MOF_JGB_INTEREST_RATE_URL,
        "maturity": "10Y",
        "lastNObservations": "30",
    }


def test_mof_jgb_seed_families_concepts_schedules_subjects_and_discovery(
    tmp_path: Path,
) -> None:
    store = SQLiteEngineStore(tmp_path / "engine.db")
    store.seed_obs_sources_and_families()
    store.seed_concept_map()
    store.seed_release_schedules()
    sync_from_yaml(store)

    source = store.get_obs_source("mof_jp")
    assert source is not None
    assert source.source_name == "Japan Ministry of Finance"
    assert source.country_code == "JP"

    for maturity in _MATURITIES:
        concept_id = f"JP_GOVT_{maturity}"
        series_id = f"MOF_JP_GOVT_{maturity}"
        family_id = f"jp.rates.govt_{maturity.lower()}"

        family = store.get_obs_family(family_id)
        assert family is not None
        assert family.source_id == "mof_jp"
        assert family.provider_series_id == series_id
        assert family.unit == "percent"
        assert family.frequency == "daily"
        assert family.country_code == "JP"

        mappings = store.get_concept_series(concept_id)
        assert len(mappings) == 1
        assert mappings[0].source_id == "mof_jp"
        assert mappings[0].provider_series_id == series_id
        assert mappings[0].obs_family_id == family_id

        schedule = store.get_release_schedule(concept_id)
        assert schedule is not None
        assert schedule.rule_type == "daily"
        assert schedule.rule_json == {
            "calendar": "japan",
            "time": "09:30",
            "timezone": "Asia/Tokyo",
        }
        assert schedule.frequency == "daily"

    families = [
        family
        for family in store.list_obs_families(active_only=False)
        if family.source_id == "mof_jp"
    ]
    assert len(families) == len(_MATURITIES)
    assert all(result.passed for result in check_dimensions("mof_jp", families))

    expected = next_expected_release(
        "daily",
        {"calendar": "japan", "time": "09:30", "timezone": "Asia/Tokyo"},
        reference=datetime(2026, 5, 1, 0, 31, tzinfo=timezone.utc),
    )
    assert expected is not None
    assert expected.isoformat() == "2026-05-07T00:30:00+00:00"

    assert store.resolve_subjects_for_concept("JP_GOVT_10Y") == ["rate.jp.govt"]
    assert "JP_GOVT_10Y" in store.list_concepts(country_code="JP")

    manager = SourceCapabilityManager(store)
    entities = manager.list_entities("mof_jp", query="10Y", limit=5)["entities"]
    assert [entity["entity_id"] for entity in entities] == ["MOF_JP_GOVT_10Y"]
    assert entities[0]["metadata"]["maturity"] == "10Y"


def test_mof_jgb_orchestrator_source_stores_indicator_rows(tmp_path: Path) -> None:
    store = SQLiteEngineStore(tmp_path / "engine.db")
    mof_jp = _FakeMOFJGBClient()
    orchestrator = IngestionOrchestrator(store, mof_jp=mof_jp)

    report = orchestrator.run_source("mof_jp")

    assert report.error == ""
    assert report.fetched == len(MOF_JGB_SERIES)
    assert report.stored == len(MOF_JGB_SERIES)
    assert len(mof_jp.calls) == 1

    with store._connection(commit=False) as connection:
        row = connection.execute(
            """
            SELECT series_id, source, date, value, obs_family_id
            FROM indicators
            WHERE series_id = 'MOF_JP_GOVT_10Y'
            """
        ).fetchone()
        raw = connection.execute(
            """
            SELECT source, series_id, request_params_json
            FROM obs_raw
            WHERE series_id = 'MOF_JP_GOVT_10Y'
            """
        ).fetchone()

    assert row is not None
    assert raw is not None
    assert dict(row) == {
        "series_id": "MOF_JP_GOVT_10Y",
        "source": "mof_jp",
        "date": "2026-04-30",
        "value": 1.23,
        "obs_family_id": "jp.rates.govt_10y",
    }
    assert raw["source"] == "mof_jp"
    assert raw["series_id"] == "MOF_JP_GOVT_10Y"
    assert json.loads(raw["request_params_json"]) == {
        "url": MOF_JGB_INTEREST_RATE_URL,
        "maturity": "10Y",
        "lastNObservations": "30",
    }

    sync_from_yaml(store)
    subject_rows = store.list_subject_indicators("rate.jp.govt", limit=20)
    assert any(
        item["series_id"] == "MOF_JP_GOVT_10Y"
        and item["source"] == "mof_jp"
        and item["concept_id"] == "JP_GOVT_10Y"
        for item in subject_rows
    )

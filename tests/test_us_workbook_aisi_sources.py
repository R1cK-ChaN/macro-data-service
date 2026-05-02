from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ingestion.fetchers._aisi import AISIFetcher
from ingestion.release_schedule import next_expected_release
from ingestion.scrapers.aisi import (
    AISIClient,
    AISIObservation,
    AISI_INDUSTRY_DATA_URL,
    parse_weekly_raw_steel_page,
)
from ingestion.series_config import AISI_WEEKLY_STEEL_SERIES
from ingestion.source_capabilities import SourceCapabilityManager
from ingestion.sources import IngestionOrchestrator
from ingestion.validation._dimensions import check_dimensions
from storage.sqlite import SQLiteEngineStore
from storage.subjects import sync_from_yaml


_METRICS = (
    "production_net_tons",
    "wow_percent",
    "yoy_percent",
)


def _aisi_html() -> str:
    return """
    <html><body>
    <h2>Weekly Raw Steel Production</h2>
    <p>In the week ending on April 25, 2026 , domestic raw steel production was
    1,830,000 net tons while the capability utilization rate was 79.3 percent.
    Production was 1,684,000 net tons in the week ending April 25, 2025, while
    the capability utilization then was 75.0 percent. The current week
    production represents an 8.7 percent increase from the same period in the
    previous year.</p>
    <p>Production for the week ending April 25, 2026 is down 1.0 percent from
    the previous week ending April 18, 2026 when production was 1,848,000 net
    tons and the rate of capability utilization was 80.0 percent.</p>
    </body></html>
    """


class _FakeResponse:
    text = _aisi_html()

    def raise_for_status(self) -> None:
        return None


class _FakeAISIClient:
    def __init__(self) -> None:
        self.calls: list[tuple[tuple[str, ...]]] = []

    def get_all_series_with_raw(
        self,
        metrics: list[str] | tuple[str, ...],
    ) -> dict[str, tuple[list[AISIObservation], dict, dict[str, str]]]:
        self.calls.append((tuple(metrics),))
        values = {
            "production_net_tons": 1_830_000.0,
            "wow_percent": -1.0,
            "yoy_percent": 8.7,
        }
        result: dict[str, tuple[list[AISIObservation], dict, dict[str, str]]] = {}
        for metric in metrics:
            obs = [
                AISIObservation(
                    date="2026-04-25",
                    metric=metric,
                    value=values[metric],
                )
            ]
            result[metric] = (
                obs,
                {
                    "source_url": AISI_INDUSTRY_DATA_URL,
                    "metric": metric,
                    "week_ending": "2026-04-25",
                    "observations": [{"date": "2026-04-25", "value": values[metric]}],
                },
                {
                    "url": AISI_INDUSTRY_DATA_URL,
                    "metric": metric,
                    "lastNObservations": "1",
                },
            )
        return result

    def get_series_with_raw(
        self,
        metric: str,
    ) -> tuple[list[AISIObservation], dict, dict[str, str]]:
        return self.get_all_series_with_raw((metric,))[metric]


def test_aisi_series_config_covers_workbook_weekly_steel_fields() -> None:
    assert tuple(cfg["metric"] for cfg in AISI_WEEKLY_STEEL_SERIES.values()) == _METRICS
    assert AISI_WEEKLY_STEEL_SERIES["raw_steel_production"]["series_id"] == (
        "AISI_RAW_STEEL_PRODUCTION_US"
    )
    assert AISI_WEEKLY_STEEL_SERIES["raw_steel_wow"]["series_id"] == (
        "AISI_RAW_STEEL_WOW_US"
    )
    assert AISI_WEEKLY_STEEL_SERIES["raw_steel_yoy"]["series_id"] == (
        "AISI_RAW_STEEL_YOY_US"
    )


def test_aisi_weekly_raw_steel_parser_extracts_value_wow_and_yoy() -> None:
    report = parse_weekly_raw_steel_page(_aisi_html())

    assert report.week_ending == "2026-04-25"
    assert report.production_net_tons == 1_830_000.0
    assert report.capability_utilization_rate == 79.3
    assert report.previous_week_ending == "2026-04-18"
    assert report.previous_week_production_net_tons == 1_848_000.0
    assert report.wow_percent == -1.0
    assert report.prior_year_week_ending == "2025-04-25"
    assert report.prior_year_production_net_tons == 1_684_000.0
    assert report.yoy_percent == 8.7


def test_aisi_client_fetches_official_page_and_returns_raw_payload(monkeypatch) -> None:
    client = AISIClient()
    calls: list[tuple[str, int]] = []

    def fake_get(url: str, timeout: int) -> _FakeResponse:
        calls.append((url, timeout))
        return _FakeResponse()

    monkeypatch.setattr(client.session, "get", fake_get)

    result = client.get_all_series_with_raw(("production_net_tons", "wow_percent"))

    assert calls == [(AISI_INDUSTRY_DATA_URL, 30)]
    observations, payload, params = result["wow_percent"]
    assert observations == [
        AISIObservation(date="2026-04-25", metric="wow_percent", value=-1.0)
    ]
    assert payload["metric"] == "wow_percent"
    assert payload["week_ending"] == "2026-04-25"
    assert payload["report"]["production_net_tons"] == 1_830_000.0
    assert params == {
        "url": AISI_INDUSTRY_DATA_URL,
        "metric": "wow_percent",
        "lastNObservations": "1",
    }


def test_aisi_fetcher_normalizes_to_raw_series() -> None:
    fake_client = _FakeAISIClient()
    fetcher = AISIFetcher(
        client=fake_client,
        series_config={
            "raw_steel_production": AISI_WEEKLY_STEEL_SERIES["raw_steel_production"]
        },
    )

    rows = fetcher.fetch()

    assert len(rows) == 1
    row = rows[0]
    assert row.source == "aisi"
    assert row.series_id == "AISI_RAW_STEEL_PRODUCTION_US"
    assert row.observations[0].date == "2026-04-25"
    assert row.observations[0].value == 1_830_000.0
    assert row.observations[0].provider_metadata == {"metric": "production_net_tons"}
    assert row.series_metadata == {
        "category": "industry",
        "metric": "production_net_tons",
        "name": "US Weekly Raw Steel Production",
        "unit": "net_tons",
    }
    assert row.content_hash is not None
    assert json.loads(row.request_params_json or "{}") == {
        "url": AISI_INDUSTRY_DATA_URL,
        "metric": "production_net_tons",
        "lastNObservations": "1",
    }


def test_aisi_seed_families_concepts_schedules_subjects_and_discovery(
    tmp_path: Path,
) -> None:
    store = SQLiteEngineStore(tmp_path / "engine.db")
    store.seed_obs_sources_and_families()
    store.seed_concept_map()
    store.seed_release_schedules()
    sync_from_yaml(store)

    source = store.get_obs_source("aisi")
    assert source is not None
    assert source.source_name == "American Iron and Steel Institute"
    assert source.country_code == "US"

    expected = {
        "RAW_STEEL_PRODUCTION_US": (
            "AISI_RAW_STEEL_PRODUCTION_US",
            "us.industry.raw_steel_production",
            "net_tons",
        ),
        "RAW_STEEL_PRODUCTION_WOW_US": (
            "AISI_RAW_STEEL_WOW_US",
            "us.industry.raw_steel_production_wow",
            "percent",
        ),
        "RAW_STEEL_PRODUCTION_YOY_US": (
            "AISI_RAW_STEEL_YOY_US",
            "us.industry.raw_steel_production_yoy",
            "percent",
        ),
    }

    for concept_id, (series_id, family_id, unit) in expected.items():
        family = store.get_obs_family(family_id)
        assert family is not None
        assert family.source_id == "aisi"
        assert family.provider_series_id == series_id
        assert family.unit == unit
        assert family.frequency == "weekly"
        assert family.country_code == "US"

        mappings = store.get_concept_series(concept_id)
        assert len(mappings) == 1
        assert mappings[0].source_id == "aisi"
        assert mappings[0].provider_series_id == series_id
        assert mappings[0].obs_family_id == family_id

        schedule = store.get_release_schedule(concept_id)
        assert schedule is not None
        assert schedule.rule_type == "weekly"
        assert schedule.rule_json == {
            "calendar": "us_federal",
            "time": "14:00",
            "timezone": "America/New_York",
            "weekday": 0,
        }

    families = [
        family
        for family in store.list_obs_families(active_only=False)
        if family.source_id == "aisi"
    ]
    assert len(families) == 3
    assert all(result.passed for result in check_dimensions("aisi", families))

    expected_release = next_expected_release(
        "weekly",
        {
            "calendar": "us_federal",
            "weekday": 0,
            "time": "14:00",
            "timezone": "America/New_York",
        },
        reference=datetime(2026, 5, 22, 20, 0, tzinfo=timezone.utc),
    )
    assert expected_release is not None
    assert expected_release.isoformat() == "2026-05-26T18:00:00+00:00"

    assert store.resolve_subjects_for_concept("RAW_STEEL_PRODUCTION_US") == [
        "industry.us.steel"
    ]
    assert "RAW_STEEL_PRODUCTION_US" in store.list_concepts(country_code="US")

    manager = SourceCapabilityManager(store)
    entities = manager.list_entities("aisi", query="raw steel", limit=5)["entities"]
    assert [entity["entity_id"] for entity in entities] == [
        "AISI_RAW_STEEL_PRODUCTION_US",
        "AISI_RAW_STEEL_WOW_US",
        "AISI_RAW_STEEL_YOY_US",
    ]


def test_aisi_orchestrator_source_stores_indicator_rows(tmp_path: Path) -> None:
    store = SQLiteEngineStore(tmp_path / "engine.db")
    aisi = _FakeAISIClient()
    orchestrator = IngestionOrchestrator(store, aisi=aisi)

    report = orchestrator.run_source("aisi")

    assert report.error == ""
    assert report.fetched == len(AISI_WEEKLY_STEEL_SERIES)
    assert report.stored == len(AISI_WEEKLY_STEEL_SERIES)
    assert len(aisi.calls) == 1

    with store._connection(commit=False) as connection:
        row = connection.execute(
            """
            SELECT series_id, source, date, value, obs_family_id
            FROM indicators
            WHERE series_id = 'AISI_RAW_STEEL_PRODUCTION_US'
            """
        ).fetchone()
        raw = connection.execute(
            """
            SELECT source, series_id, request_params_json
            FROM obs_raw
            WHERE series_id = 'AISI_RAW_STEEL_PRODUCTION_US'
            """
        ).fetchone()

    assert row is not None
    assert raw is not None
    assert dict(row) == {
        "series_id": "AISI_RAW_STEEL_PRODUCTION_US",
        "source": "aisi",
        "date": "2026-04-25",
        "value": 1_830_000.0,
        "obs_family_id": "us.industry.raw_steel_production",
    }
    assert raw["source"] == "aisi"
    assert raw["series_id"] == "AISI_RAW_STEEL_PRODUCTION_US"
    assert json.loads(raw["request_params_json"]) == {
        "url": AISI_INDUSTRY_DATA_URL,
        "metric": "production_net_tons",
        "lastNObservations": "1",
    }

    sync_from_yaml(store)
    subject_rows = store.list_subject_indicators("industry.us.steel", limit=20)
    assert any(
        item["series_id"] == "AISI_RAW_STEEL_PRODUCTION_US"
        and item["source"] == "aisi"
        and item["concept_id"] == "RAW_STEEL_PRODUCTION_US"
        for item in subject_rows
    )

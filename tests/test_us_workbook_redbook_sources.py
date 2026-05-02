from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ingestion.fetchers._redbook import RedbookFetcher
from ingestion.release_schedule import next_expected_release
from ingestion.scrapers.redbook import (
    REDBOOK_RESEARCH_URL,
    TE_API_BASE_URL,
    TE_REDBOOK_HISTORICAL_PATH,
    RedbookAPIError,
    RedbookClient,
    RedbookObservation,
    parse_redbook_historical_rows,
)
from ingestion.series_config import REDBOOK_SERIES
from ingestion.source_capabilities import SourceCapabilityManager
from ingestion.sources import IngestionOrchestrator
from ingestion.validation._dimensions import check_dimensions
from storage.sqlite import SQLiteEngineStore
from storage.subjects import sync_from_yaml


_REDBOOK_ROWS: list[dict[str, Any]] = [
    {
        "Country": "United States",
        "Category": "Redbook Index",
        "DateTime": "2026-04-18T00:00:00",
        "Value": 6.7,
        "Frequency": "Weekly",
        "HistoricalDataSymbol": "UNITEDSTAREDIND",
        "LastUpdate": "2026-04-21T12:55:00",
    },
    {
        "Country": "United States",
        "Category": "Redbook Index",
        "DateTime": "2026-04-25T00:00:00",
        "Value": 7.7,
        "Frequency": "Weekly",
        "HistoricalDataSymbol": "UNITEDSTAREDIND",
        "LastUpdate": "2026-04-28T12:55:00",
    },
]


def _expected_d1(lookback_days: int) -> str:
    return (datetime.now(timezone.utc).date() - timedelta(days=lookback_days)).isoformat()


class _FakeResponse:
    def json(self) -> list[dict[str, Any]]:
        return _REDBOOK_ROWS

    def raise_for_status(self) -> None:
        return None


class _FakeRedbookClient:
    def __init__(self) -> None:
        self.calls: list[tuple[tuple[str, ...], int]] = []

    def get_all_series_with_raw(
        self,
        series_config: dict[str, dict[str, Any]],
        *,
        lookback_days: int = 365,
    ) -> dict[str, tuple[list[RedbookObservation], dict, dict[str, str]]]:
        self.calls.append((tuple(series_config), lookback_days))
        rows = parse_redbook_historical_rows(_REDBOOK_ROWS)
        result: dict[str, tuple[list[RedbookObservation], dict, dict[str, str]]] = {}
        for cfg in series_config.values():
            observations = [
                RedbookObservation(date=row.date, value=row.value)
                for row in rows
            ]
            payload = {
                "source_url": REDBOOK_RESEARCH_URL,
                "source_name": "Redbook Research Inc.",
                "provider": "tradingeconomics",
                "api_path": TE_REDBOOK_HISTORICAL_PATH,
                "country": cfg["country"],
                "indicator": cfg["indicator"],
                "source_symbol": cfg["source_symbol"],
                "series_id": cfg["series_id"],
                "observations": [
                    {"date": obs.date, "value": obs.value}
                    for obs in observations
                ],
            }
            params = {
                "url": f"{TE_API_BASE_URL}{TE_REDBOOK_HISTORICAL_PATH}",
                "country": str(cfg["country"]),
                "indicator": str(cfg["indicator"]),
                "source_symbol": str(cfg["source_symbol"]),
                "lookback_days": str(lookback_days),
                "d1": _expected_d1(lookback_days),
            }
            result[str(cfg["series_id"])] = (observations, payload, params)
        return result

    def get_series_with_raw(
        self,
        cfg: dict[str, Any],
        *,
        lookback_days: int = 365,
    ) -> tuple[list[RedbookObservation], dict, dict[str, str]]:
        return self.get_all_series_with_raw(
            {"series": cfg},
            lookback_days=lookback_days,
        )[str(cfg["series_id"])]


def test_redbook_series_config_covers_workbook_weekly_retail_yoy() -> None:
    cfg = REDBOOK_SERIES["redbook_retail_sales_yoy"]

    assert cfg["series_id"] == "REDBOOK_RETAIL_SALES_YOY_US"
    assert cfg["source_symbol"] == "UNITEDSTAREDIND"
    assert cfg["unit"] == "percent"
    assert cfg["category"] == "consumer"


def test_redbook_parser_extracts_sorted_weekly_values() -> None:
    rows = parse_redbook_historical_rows(list(reversed(_REDBOOK_ROWS)))

    assert [row.date for row in rows] == ["2026-04-18", "2026-04-25"]
    assert [row.value for row in rows] == [6.7, 7.7]
    assert rows[0].frequency == "Weekly"
    assert rows[0].source_symbol == "UNITEDSTAREDIND"


def test_redbook_client_fetches_authorized_feed_and_returns_raw_payload(
    monkeypatch,
) -> None:
    client = RedbookClient(api_key="unit:test")
    calls: list[tuple[str, dict[str, str], float]] = []

    def fake_get(url: str, params: dict[str, str], timeout: float) -> _FakeResponse:
        calls.append((url, dict(params), timeout))
        return _FakeResponse()

    monkeypatch.setattr(client.session, "get", fake_get)

    observations, payload, params = client.get_series_with_raw(
        REDBOOK_SERIES["redbook_retail_sales_yoy"],
        lookback_days=30,
    )

    assert calls[0][0] == f"{TE_API_BASE_URL}{TE_REDBOOK_HISTORICAL_PATH}"
    assert calls[0][1]["c"] == "unit:test"
    assert calls[0][1]["f"] == "json"
    assert calls[0][1]["d1"] == _expected_d1(30)
    assert observations == [
        RedbookObservation(date="2026-04-18", value=6.7),
        RedbookObservation(date="2026-04-25", value=7.7),
    ]
    assert payload["source_name"] == "Redbook Research Inc."
    assert payload["source_url"] == REDBOOK_RESEARCH_URL
    assert payload["source_symbol"] == "UNITEDSTAREDIND"
    assert payload["observations"][-1] == {"date": "2026-04-25", "value": 7.7}
    assert params == {
        "url": f"{TE_API_BASE_URL}{TE_REDBOOK_HISTORICAL_PATH}",
        "country": "united states",
        "indicator": "redbook index",
        "source_symbol": "UNITEDSTAREDIND",
        "lookback_days": "30",
        "d1": _expected_d1(30),
    }


def test_redbook_client_sanitizes_request_errors(monkeypatch) -> None:
    client = RedbookClient(api_key="secret-token")

    def fake_get(url: str, params: dict[str, str], timeout: float) -> _FakeResponse:
        raise requests.ConnectionError("secret-token")

    monkeypatch.setattr(client.session, "get", fake_get)

    with pytest.raises(RedbookAPIError) as excinfo:
        client.fetch_historical_rows(lookback_days=30)

    assert str(excinfo.value) == "Redbook historical API request failed"


def test_redbook_fetcher_normalizes_to_raw_series() -> None:
    fake_client = _FakeRedbookClient()
    fetcher = RedbookFetcher(
        client=fake_client,
        series_config={
            "redbook_retail_sales_yoy": REDBOOK_SERIES["redbook_retail_sales_yoy"]
        },
    )

    rows = fetcher.fetch(lookback_days=30)

    assert len(rows) == 1
    row = rows[0]
    assert row.source == "redbook"
    assert row.series_id == "REDBOOK_RETAIL_SALES_YOY_US"
    assert [obs.date for obs in row.observations] == ["2026-04-18", "2026-04-25"]
    assert row.observations[-1].value == 7.7
    assert row.observations[-1].provider_metadata == {
        "country": "united states",
        "indicator": "redbook index",
        "source_symbol": "UNITEDSTAREDIND",
    }
    assert row.series_metadata == {
        "category": "consumer",
        "country": "united states",
        "indicator": "redbook index",
        "name": "US Redbook Retail Sales YoY",
        "source_symbol": "UNITEDSTAREDIND",
        "unit": "percent",
    }
    assert row.content_hash is not None
    assert json.loads(row.request_params_json or "{}") == {
        "url": f"{TE_API_BASE_URL}{TE_REDBOOK_HISTORICAL_PATH}",
        "country": "united states",
        "indicator": "redbook index",
        "source_symbol": "UNITEDSTAREDIND",
        "lookback_days": "30",
        "d1": _expected_d1(30),
    }


def test_redbook_seed_families_concepts_schedules_subjects_and_discovery(
    tmp_path: Path,
) -> None:
    store = SQLiteEngineStore(tmp_path / "engine.db")
    store.seed_obs_sources_and_families()
    store.seed_concept_map()
    store.seed_release_schedules()
    sync_from_yaml(store)

    source = store.get_obs_source("redbook")
    assert source is not None
    assert source.source_name == "Redbook Research Inc."
    assert source.country_code == "US"

    family = store.get_obs_family("us.consumer.redbook_retail_sales_yoy")
    assert family is not None
    assert family.source_id == "redbook"
    assert family.provider_series_id == "REDBOOK_RETAIL_SALES_YOY_US"
    assert family.unit == "percent"
    assert family.frequency == "weekly"
    assert family.seasonal_adjustment == "nsa"
    assert family.country_code == "US"

    mappings = store.get_concept_series("REDBOOK_RETAIL_SALES_YOY_US")
    assert len(mappings) == 1
    assert mappings[0].source_id == "redbook"
    assert mappings[0].provider_series_id == "REDBOOK_RETAIL_SALES_YOY_US"
    assert mappings[0].obs_family_id == "us.consumer.redbook_retail_sales_yoy"

    schedule = store.get_release_schedule("REDBOOK_RETAIL_SALES_YOY_US")
    assert schedule is not None
    assert schedule.rule_type == "weekly"
    assert schedule.rule_json == {
        "calendar": "us_federal",
        "time": "08:55",
        "timezone": "America/New_York",
        "weekday": 1,
    }

    expected_release = next_expected_release(
        "weekly",
        schedule.rule_json,
        reference=datetime(2026, 5, 4, 12, 0, tzinfo=timezone.utc),
    )
    assert expected_release is not None
    assert expected_release.isoformat() == "2026-05-05T12:55:00+00:00"

    families = [
        item
        for item in store.list_obs_families(active_only=False)
        if item.source_id == "redbook"
    ]
    assert len(families) == 1
    assert all(result.passed for result in check_dimensions("redbook", families))

    stats = store.get_source_storage_stats("redbook")
    assert stats["table"] == "indicators"
    assert stats["count"] == 0

    assert store.resolve_subjects_for_concept("REDBOOK_RETAIL_SALES_YOY_US") == [
        "econ.us.redbook"
    ]
    assert "REDBOOK_RETAIL_SALES_YOY_US" in store.list_concepts(country_code="US")

    manager = SourceCapabilityManager(store)
    entities = manager.list_entities("redbook", query="redbook", limit=5)["entities"]
    assert [entity["entity_id"] for entity in entities] == [
        "REDBOOK_RETAIL_SALES_YOY_US",
    ]


def test_redbook_orchestrator_source_stores_indicator_rows(
    tmp_path: Path,
) -> None:
    store = SQLiteEngineStore(tmp_path / "engine.db")
    redbook = _FakeRedbookClient()
    orchestrator = IngestionOrchestrator(store, redbook=redbook)

    report = orchestrator.run_source("redbook")

    assert report.error == ""
    assert report.fetched == 1
    assert report.stored == 2
    assert redbook.calls == [(("redbook_retail_sales_yoy",), 365)]

    with store._connection(commit=False) as connection:
        row = connection.execute(
            """
            SELECT series_id, source, date, value, obs_family_id
            FROM indicators
            WHERE series_id = 'REDBOOK_RETAIL_SALES_YOY_US'
              AND date = '2026-04-25'
            """
        ).fetchone()
        raw = connection.execute(
            """
            SELECT source, series_id, request_params_json
            FROM obs_raw
            WHERE series_id = 'REDBOOK_RETAIL_SALES_YOY_US'
            """
        ).fetchone()

    assert row is not None
    assert raw is not None
    assert dict(row) == {
        "series_id": "REDBOOK_RETAIL_SALES_YOY_US",
        "source": "redbook",
        "date": "2026-04-25",
        "value": 7.7,
        "obs_family_id": "us.consumer.redbook_retail_sales_yoy",
    }
    assert raw["source"] == "redbook"
    assert raw["series_id"] == "REDBOOK_RETAIL_SALES_YOY_US"
    assert json.loads(raw["request_params_json"]) == {
        "url": f"{TE_API_BASE_URL}{TE_REDBOOK_HISTORICAL_PATH}",
        "country": "united states",
        "indicator": "redbook index",
        "source_symbol": "UNITEDSTAREDIND",
        "lookback_days": "365",
        "d1": _expected_d1(365),
    }

    stats = store.get_source_storage_stats("redbook")
    assert stats["table"] == "indicators"
    assert stats["count"] == 2

    health = SourceCapabilityManager(store).get_customer_health()
    redbook_health = next(
        item for item in health["sources"] if item["source_id"] == "redbook"
    )
    assert redbook_health["status"] == "healthy"
    assert redbook_health["record_count"] == 2

    sync_from_yaml(store)
    subject_rows = store.list_subject_indicators("econ.us.redbook", limit=20)
    assert any(
        item["series_id"] == "REDBOOK_RETAIL_SALES_YOY_US"
        and item["source"] == "redbook"
        and item["concept_id"] == "REDBOOK_RETAIL_SALES_YOY_US"
        for item in subject_rows
    )

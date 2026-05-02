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

from ingestion.fetchers._sentix import SentixFetcher
from ingestion.release_schedule import next_expected_release
from ingestion.scrapers.sentix import (
    SENTIX_API_BASE_URL,
    SENTIX_HOMEPAGE_URL,
    SENTIX_TIMESERIES_PATH,
    SentixAPIError,
    SentixClient,
    SentixObservation,
    parse_sentix_timeseries,
)
from ingestion.series_config import SENTIX_SERIES
from ingestion.source_capabilities import SourceCapabilityManager
from ingestion.sources import IngestionOrchestrator
from ingestion.validation._dimensions import check_dimensions
from storage.sqlite import SQLiteEngineStore
from storage.subjects import sync_from_yaml


_CURRENT_ROWS = [
    {"date": "2026-01-09", "value": 21.3, "ticker": "SNTEUSH0"},
    {"date": "2026-02-06", "value": 17.1, "ticker": "SNTEUSH0"},
]

_EXPECTATIONS_ROWS = [
    {"date": "2026-01-09", "value": 5.5, "ticker": "SNTEUSH6"},
    {"date": "2026-02-06", "value": -1.5, "ticker": "SNTEUSH6"},
]


def _expected_start(lookback_days: int) -> str:
    return (datetime.now(timezone.utc).date() - timedelta(days=lookback_days)).isoformat()


class _FakeResponse:
    def __init__(self, payload: Any) -> None:
        self._payload = payload

    def json(self) -> Any:
        return self._payload

    def raise_for_status(self) -> None:
        return None


class _FakeSentixClient:
    def __init__(self) -> None:
        self.calls: list[tuple[tuple[str, ...], int]] = []

    def get_all_series_with_raw(
        self,
        series_config: dict[str, dict[str, Any]],
        *,
        lookback_days: int = 365 * 3,
    ) -> dict[str, tuple[list[SentixObservation], dict, dict[str, str]]]:
        self.calls.append((tuple(series_config), lookback_days))
        current = {row["date"]: float(row["value"]) for row in _CURRENT_ROWS}
        expectations = {row["date"]: float(row["value"]) for row in _EXPECTATIONS_ROWS}
        result: dict[str, tuple[list[SentixObservation], dict, dict[str, str]]] = {}
        for cfg in series_config.values():
            series_id = str(cfg["series_id"])
            source_ticker = str(cfg.get("source_ticker", ""))
            component_tickers = tuple(str(item) for item in cfg.get("component_tickers", ()))
            if source_ticker == "SNTEUSH0":
                observations = [
                    SentixObservation(date=row["date"], value=float(row["value"]), ticker="SNTEUSH0")
                    for row in _CURRENT_ROWS
                ]
            elif source_ticker == "SNTEUSH6":
                observations = [
                    SentixObservation(date=row["date"], value=float(row["value"]), ticker="SNTEUSH6")
                    for row in _EXPECTATIONS_ROWS
                ]
            else:
                observations = [
                    SentixObservation(
                        date=date_value,
                        value=round((current[date_value] + expectations[date_value]) / 2.0, 10),
                        ticker="+".join(component_tickers),
                    )
                    for date_value in sorted(set(current) & set(expectations))
                ]
            payload = {
                "source_url": SENTIX_HOMEPAGE_URL,
                "source_name": "sentix GmbH",
                "api_path": SENTIX_TIMESERIES_PATH,
                "series_id": series_id,
                "name": cfg["name"],
                "country": cfg["country"],
                "family": cfg["family"],
                "source_ticker": source_ticker,
                "component_tickers": list(component_tickers),
                "formula": cfg.get("formula", ""),
                "observations": [
                    {"date": obs.date, "value": obs.value}
                    for obs in observations
                ],
                "ticker_payloads": {
                    "SNTEUSH0": {"data": _CURRENT_ROWS},
                    "SNTEUSH6": {"data": _EXPECTATIONS_ROWS},
                },
            }
            params = {
                "url": f"{SENTIX_API_BASE_URL}{SENTIX_TIMESERIES_PATH}",
                "series_id": series_id,
                "tickers": ",".join(component_tickers or ((source_ticker,) if source_ticker else ())),
                "lookback_days": str(lookback_days),
                "format": "json",
                "start_date": _expected_start(lookback_days),
            }
            result[series_id] = (observations, payload, params)
        return result

    def get_series_with_raw(
        self,
        cfg: dict[str, Any],
        *,
        lookback_days: int = 365 * 3,
    ) -> tuple[list[SentixObservation], dict, dict[str, str]]:
        return self.get_all_series_with_raw(
            {"series": cfg},
            lookback_days=lookback_days,
        )[str(cfg["series_id"])]


def test_sentix_series_config_covers_workbook_headline_current_expectations() -> None:
    assert SENTIX_SERIES["sentix_us_headline"]["series_id"] == "SENTIX_US_HEADLINE"
    assert SENTIX_SERIES["sentix_us_headline"]["component_tickers"] == (
        "SNTEUSH0",
        "SNTEUSH6",
    )
    assert SENTIX_SERIES["sentix_us_current"]["source_ticker"] == "SNTEUSH0"
    assert SENTIX_SERIES["sentix_us_expectations"]["source_ticker"] == "SNTEUSH6"


def test_sentix_parser_accepts_common_json_shapes() -> None:
    rows = parse_sentix_timeseries(
        {"data": list(reversed(_CURRENT_ROWS))},
        ticker="SNTEUSH0",
    )

    assert [row.date for row in rows] == ["2026-01-09", "2026-02-06"]
    assert [row.value for row in rows] == [21.3, 17.1]
    assert rows[0].ticker == "SNTEUSH0"

    array_rows = parse_sentix_timeseries(
        [["2026-01-09T00:00:00", "5,5"]],
        ticker="SNTEUSH6",
    )
    assert array_rows[0].date == "2026-01-09"
    assert array_rows[0].value == 5.5


def test_sentix_client_fetches_official_tickers_and_derives_headline(monkeypatch) -> None:
    client = SentixClient(access_token="unit-token")
    calls: list[tuple[str, str, dict[str, str], float]] = []

    def fake_get(
        url: str,
        headers: dict[str, str],
        params: dict[str, str],
        timeout: float,
    ) -> _FakeResponse:
        calls.append((url, headers["Authorization"], dict(params), timeout))
        ticker = params["ticker"]
        if ticker == "SNTEUSH0":
            return _FakeResponse({"data": _CURRENT_ROWS})
        if ticker == "SNTEUSH6":
            return _FakeResponse({"data": _EXPECTATIONS_ROWS})
        raise AssertionError(ticker)

    monkeypatch.setattr(client.session, "get", fake_get)

    observations, payload, params = client.get_series_with_raw(
        SENTIX_SERIES["sentix_us_headline"],
        lookback_days=90,
    )

    assert [call[2]["ticker"] for call in calls] == ["SNTEUSH0", "SNTEUSH6"]
    assert all(call[1] == "Bearer unit-token" for call in calls)
    assert all(call[2]["start_date"] == _expected_start(90) for call in calls)
    assert observations == [
        SentixObservation(date="2026-01-09", value=13.4, ticker="SNTEUSH0+SNTEUSH6"),
        SentixObservation(date="2026-02-06", value=7.8, ticker="SNTEUSH0+SNTEUSH6"),
    ]
    assert payload["source_name"] == "sentix GmbH"
    assert payload["component_tickers"] == ["SNTEUSH0", "SNTEUSH6"]
    assert payload["formula"] == "average"
    assert payload["observations"][-1] == {"date": "2026-02-06", "value": 7.8}
    assert params == {
        "url": f"{SENTIX_API_BASE_URL}{SENTIX_TIMESERIES_PATH}",
        "series_id": "SENTIX_US_HEADLINE",
        "tickers": "SNTEUSH0,SNTEUSH6",
        "lookback_days": "90",
        "format": "json",
        "start_date": _expected_start(90),
    }


def test_sentix_client_sanitizes_request_errors(monkeypatch) -> None:
    client = SentixClient(access_token="secret-token")

    def fake_get(
        url: str,
        headers: dict[str, str],
        params: dict[str, str],
        timeout: float,
    ) -> _FakeResponse:
        raise requests.ConnectionError("secret-token")

    monkeypatch.setattr(client.session, "get", fake_get)

    with pytest.raises(SentixAPIError) as excinfo:
        client.fetch_ticker_rows("SNTEUSH0", lookback_days=30)

    assert str(excinfo.value) == "sentix timeseries API request failed"


def test_sentix_fetcher_normalizes_to_raw_series() -> None:
    fake_client = _FakeSentixClient()
    fetcher = SentixFetcher(
        client=fake_client,
        series_config={
            "sentix_us_current": SENTIX_SERIES["sentix_us_current"],
        },
    )

    rows = fetcher.fetch(lookback_days=90)

    assert len(rows) == 1
    row = rows[0]
    assert row.source == "sentix"
    assert row.series_id == "SENTIX_US_CURRENT"
    assert [obs.date for obs in row.observations] == ["2026-01-09", "2026-02-06"]
    assert row.observations[-1].value == 17.1
    assert row.observations[-1].provider_metadata == {
        "ticker": "SNTEUSH0",
        "source_ticker": "SNTEUSH0",
        "component_tickers": "",
    }
    assert row.series_metadata == {
        "category": "sentiment",
        "country": "USA",
        "family": "SNTE",
        "formula": "",
        "name": "US Sentix Economic Index Headline Current Situation",
        "source_ticker": "SNTEUSH0",
        "component_tickers": "",
        "unit": "index",
    }
    assert row.content_hash is not None
    assert json.loads(row.request_params_json or "{}") == {
        "url": f"{SENTIX_API_BASE_URL}{SENTIX_TIMESERIES_PATH}",
        "series_id": "SENTIX_US_CURRENT",
        "tickers": "SNTEUSH0",
        "lookback_days": "90",
        "format": "json",
        "start_date": _expected_start(90),
    }


def test_sentix_seed_families_concepts_schedules_subjects_and_discovery(
    tmp_path: Path,
) -> None:
    store = SQLiteEngineStore(tmp_path / "engine.db")
    store.seed_obs_sources_and_families()
    store.seed_concept_map()
    store.seed_release_schedules()
    sync_from_yaml(store)

    source = store.get_obs_source("sentix")
    assert source is not None
    assert source.source_name == "sentix GmbH"
    assert source.country_code == "US"

    expected = {
        "SENTIX_US_HEADLINE": "us.sentiment.sentix_headline",
        "SENTIX_US_CURRENT": "us.sentiment.sentix_current",
        "SENTIX_US_EXPECTATIONS": "us.sentiment.sentix_expectations",
    }
    for concept_id, family_id in expected.items():
        family = store.get_obs_family(family_id)
        assert family is not None
        assert family.source_id == "sentix"
        assert family.provider_series_id == concept_id
        assert family.unit == "index"
        assert family.frequency == "monthly"
        assert family.country_code == "US"

        mappings = store.get_concept_series(concept_id)
        assert len(mappings) == 1
        assert mappings[0].source_id == "sentix"
        assert mappings[0].provider_series_id == concept_id
        assert mappings[0].obs_family_id == family_id

        schedule = store.get_release_schedule(concept_id)
        assert schedule is not None
        assert schedule.rule_type == "weekday_of_month"
        assert schedule.rule_json == {
            "ordinal": 1,
            "time": "10:00",
            "timezone": "Europe/Berlin",
            "weekday": 4,
        }

    expected_release = next_expected_release(
        "weekday_of_month",
        {
            "ordinal": 1,
            "weekday": 4,
            "time": "10:00",
            "timezone": "Europe/Berlin",
        },
        reference=datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc),
    )
    assert expected_release is not None
    assert expected_release.isoformat() == "2026-06-05T00:00:00+00:00"

    families = [
        item
        for item in store.list_obs_families(active_only=False)
        if item.source_id == "sentix"
    ]
    assert len(families) == 3
    assert all(result.passed for result in check_dimensions("sentix", families))

    stats = store.get_source_storage_stats("sentix")
    assert stats["table"] == "indicators"
    assert stats["count"] == 0

    assert store.resolve_subjects_for_concept("SENTIX_US_HEADLINE") == [
        "econ.us.sentix"
    ]
    assert "SENTIX_US_HEADLINE" in store.list_concepts(country_code="US")

    manager = SourceCapabilityManager(store)
    entities = manager.list_entities("sentix", query="sentix", limit=10)["entities"]
    assert [entity["entity_id"] for entity in entities] == [
        "SENTIX_US_HEADLINE",
        "SENTIX_US_CURRENT",
        "SENTIX_US_EXPECTATIONS",
    ]


def test_sentix_orchestrator_source_stores_indicator_rows(
    tmp_path: Path,
) -> None:
    store = SQLiteEngineStore(tmp_path / "engine.db")
    sentix = _FakeSentixClient()
    orchestrator = IngestionOrchestrator(store, sentix=sentix)

    report = orchestrator.run_source("sentix")

    assert report.error == ""
    assert report.fetched == 3
    assert report.stored == 6
    assert sentix.calls == [(("sentix_us_headline", "sentix_us_current", "sentix_us_expectations"), 365)]

    with store._connection(commit=False) as connection:
        row = connection.execute(
            """
            SELECT series_id, source, date, value, obs_family_id
            FROM indicators
            WHERE series_id = 'SENTIX_US_HEADLINE'
              AND date = '2026-02-01'
            """
        ).fetchone()
        raw = connection.execute(
            """
            SELECT source, series_id, request_params_json
            FROM obs_raw
            WHERE series_id = 'SENTIX_US_HEADLINE'
            """
        ).fetchone()

    assert row is not None
    assert raw is not None
    assert dict(row) == {
        "series_id": "SENTIX_US_HEADLINE",
        "source": "sentix",
        "date": "2026-02-01",
        "value": 7.8,
        "obs_family_id": "us.sentiment.sentix_headline",
    }
    assert raw["source"] == "sentix"
    assert raw["series_id"] == "SENTIX_US_HEADLINE"
    assert json.loads(raw["request_params_json"]) == {
        "url": f"{SENTIX_API_BASE_URL}{SENTIX_TIMESERIES_PATH}",
        "series_id": "SENTIX_US_HEADLINE",
        "tickers": "SNTEUSH0,SNTEUSH6",
        "lookback_days": "365",
        "format": "json",
        "start_date": _expected_start(365),
    }

    stats = store.get_source_storage_stats("sentix")
    assert stats["table"] == "indicators"
    assert stats["count"] == 6

    health = SourceCapabilityManager(store).get_customer_health()
    sentix_health = next(
        item for item in health["sources"] if item["source_id"] == "sentix"
    )
    assert sentix_health["status"] == "healthy"
    assert sentix_health["record_count"] == 6

    sync_from_yaml(store)
    subject_rows = store.list_subject_indicators("econ.us.sentix", limit=20)
    assert any(
        item["series_id"] == "SENTIX_US_HEADLINE"
        and item["source"] == "sentix"
        and item["concept_id"] == "SENTIX_US_HEADLINE"
        for item in subject_rows
    )

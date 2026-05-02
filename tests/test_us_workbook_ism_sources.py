from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ingestion.fetchers._ism import ISMFetcher
from ingestion.release_schedule import next_expected_release
from ingestion.scrapers.ism import (
    ISMClient,
    ISMObservation,
    ISM_REPORTS_URL,
    discover_current_report_urls,
    parse_ism_report_page,
)
from ingestion.series_config import ISM_REPORT_SERIES
from ingestion.source_capabilities import SourceCapabilityManager
from ingestion.sources import IngestionOrchestrator
from ingestion.validation._dimensions import check_dimensions
from storage.sqlite import SQLiteEngineStore
from storage.subjects import sync_from_yaml


_MFG_URL = (
    "https://www.ismworld.org/supply-management-news-and-reports/"
    "reports/ism-pmi-reports/pmi/april/"
)
_SERVICES_URL = (
    "https://www.ismworld.org/supply-management-news-and-reports/"
    "reports/ism-pmi-reports/services/march/"
)


def _landing_html() -> str:
    return f"""
    <html><body>
      <h3>Manufacturing PMI</h3>
      <p><a href="{_MFG_URL}">View Report</a></p>
      <p><a href="/supply-management-news-and-reports/reports/ism-pmi-reports/pmi/march/">View Report</a></p>
      <h3>Services PMI</h3>
      <p><a href="{_SERVICES_URL}">View Report</a></p>
      <p><a href="/supply-management-news-and-reports/reports/ism-pmi-reports/services/february/">View Report</a></p>
    </body></html>
    """


def _manufacturing_html() -> str:
    return """
    <html><body>
      <h1>Manufacturing PMI<sup>®</sup> at 52.7%</h1>
      <h1>April 2026 ISM<sup>®</sup> Manufacturing PMI<sup>®</sup> Report</h1>
      <table>
        <thead>
          <tr>
            <th>Index</th><th>Series Index Apr</th><th>Series Index Mar</th>
            <th>Percentage Point Change</th><th>Direction</th>
          </tr>
        </thead>
        <tbody>
          <tr><th>Manufacturing PMI<sup>®</sup></th><td>52.7</td><td>52.7</td><td>0.0</td><td>Growing</td></tr>
          <tr><th>New Orders</th><td>54.1</td><td>53.5</td><td>+0.6</td><td>Growing</td></tr>
          <tr><th>Production</th><td>53.4</td><td>55.1</td><td>-1.7</td><td>Growing</td></tr>
          <tr><th>Employment</th><td>46.4</td><td>48.7</td><td>-2.3</td><td>Contracting</td></tr>
          <tr><th>Supplier Deliveries</th><td>60.6</td><td>58.9</td><td>+1.7</td><td>Slowing</td></tr>
          <tr><th>Inventories</th><td>49.0</td><td>47.1</td><td>+1.9</td><td>Contracting</td></tr>
          <tr><th>Customers’ Inventories</th><td>39.1</td><td>40.1</td><td>-1.0</td><td>Too Low</td></tr>
          <tr><th>Prices</th><td>84.6</td><td>78.3</td><td>+6.3</td><td>Increasing</td></tr>
          <tr><th>Backlog of Orders</th><td>51.4</td><td>54.4</td><td>-3.0</td><td>Growing</td></tr>
          <tr><th>New Export Orders</th><td>47.9</td><td>49.9</td><td>-2.0</td><td>Contracting</td></tr>
          <tr><th>Imports</th><td>50.3</td><td>52.6</td><td>-2.3</td><td>Growing</td></tr>
        </tbody>
      </table>
    </body></html>
    """


def _services_html() -> str:
    return """
    <html><body>
      <h1>Services PMI<sup>®</sup> at 54%</h1>
      <h1>March 2026 ISM<sup>®</sup> Services PMI<sup>®</sup> Report</h1>
      <table>
        <thead>
          <tr>
            <th>Index</th><th>Series Index Mar</th><th>Series Index Feb</th>
            <th>Percent Point Change</th><th>Direction</th>
          </tr>
        </thead>
        <tbody>
          <tr><th>Services PMI<sup>®</sup></th><td>54.0</td><td>56.1</td><td>-2.1</td><td>Growing</td></tr>
          <tr><th>Business Activity/<br>Production</th><td>53.9</td><td>59.9</td><td>-6.0</td><td>Growing</td></tr>
          <tr><th>New Orders</th><td>60.6</td><td>58.6</td><td>+2.0</td><td>Growing</td></tr>
          <tr><th>Employment</th><td>45.2</td><td>51.8</td><td>-6.6</td><td>Contracting</td></tr>
          <tr><th>Supplier Deliveries</th><td>56.2</td><td>53.9</td><td>+2.3</td><td>Slowing</td></tr>
          <tr><th>Inventories</th><td>54.8</td><td>56.4</td><td>-1.6</td><td>Growing</td></tr>
          <tr><th>Prices</th><td>70.7</td><td>63.0</td><td>+7.7</td><td>Increasing</td></tr>
          <tr><th>Backlog of Orders</th><td>53.6</td><td>55.9</td><td>-2.3</td><td>Growing</td></tr>
          <tr><th>New Export Orders</th><td>50.7</td><td>57.2</td><td>-6.5</td><td>Growing</td></tr>
          <tr><th>Imports</th><td>55.2</td><td>51.8</td><td>+3.4</td><td>Growing</td></tr>
          <tr><th>Inventory Sentiment</th><td>54.3</td><td>55.3</td><td>-1.0</td><td>Too High</td></tr>
        </tbody>
      </table>
    </body></html>
    """


class _FakeResponse:
    def __init__(self, text: str) -> None:
        self.text = text

    def raise_for_status(self) -> None:
        return None


class _FakeISMClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []

    def get_all_series_with_raw(
        self,
        series_config: dict[str, dict[str, Any]],
    ) -> dict[str, tuple[list[ISMObservation], dict, dict[str, str]]]:
        self.calls.append(tuple(series_config))
        result: dict[str, tuple[list[ISMObservation], dict, dict[str, str]]] = {}
        for cfg in series_config.values():
            series_id = cfg["series_id"]
            survey = cfg["survey"]
            metric = cfg["metric"]
            measure = cfg["measure"]
            date = "2026-04-01" if survey == "manufacturing" else "2026-03-01"
            value = 54.1 if metric == "new_orders" and measure == "index" else 0.6
            if survey == "services" and metric == "pmi" and measure == "index":
                value = 54.0
            obs = [
                ISMObservation(
                    date=date,
                    survey=survey,
                    metric=metric,
                    measure=measure,
                    value=value,
                )
            ]
            result[series_id] = (
                obs,
                {
                    "source_url": _MFG_URL if survey == "manufacturing" else _SERVICES_URL,
                    "survey": survey,
                    "metric": metric,
                    "measure": measure,
                    "reference_date": date,
                    "observations": [{"date": date, "value": value}],
                },
                {
                    "url": _MFG_URL if survey == "manufacturing" else _SERVICES_URL,
                    "survey": survey,
                    "metric": metric,
                    "measure": measure,
                    "lastNObservations": "1",
                },
            )
        return result

    def get_series_with_raw(
        self,
        cfg: dict[str, Any],
    ) -> tuple[list[ISMObservation], dict, dict[str, str]]:
        return self.get_all_series_with_raw({"series": cfg})[cfg["series_id"]]


def test_ism_series_config_covers_workbook_fields() -> None:
    assert len(ISM_REPORT_SERIES) == 44
    assert ISM_REPORT_SERIES["manufacturing_pmi"]["series_id"] == "ISM_MFG_PMI_US"
    assert ISM_REPORT_SERIES["manufacturing_pmi_mom"]["series_id"] == (
        "ISM_MFG_PMI_MOM_US"
    )
    assert ISM_REPORT_SERIES["services_business_activity"]["series_id"] == (
        "ISM_SERVICES_BUSINESS_ACTIVITY_US"
    )
    assert ISM_REPORT_SERIES["services_inventory_sentiment_mom"]["unit"] == (
        "percentage_points"
    )


def test_ism_report_parser_extracts_manufacturing_metrics() -> None:
    report = parse_ism_report_page(_manufacturing_html(), source_url=_MFG_URL)

    assert report.survey == "manufacturing"
    assert report.reference_date == "2026-04-01"
    assert report.metrics["pmi"].index_value == 52.7
    assert report.metrics["new_orders"].change_value == 0.6
    assert report.metrics["customers_inventories"].index_value == 39.1
    assert report.metrics["imports"].change_value == -2.3


def test_ism_report_parser_extracts_services_metrics() -> None:
    report = parse_ism_report_page(_services_html(), source_url=_SERVICES_URL)

    assert report.survey == "services"
    assert report.reference_date == "2026-03-01"
    assert report.metrics["pmi"].index_value == 54.0
    assert report.metrics["business_activity"].change_value == -6.0
    assert report.metrics["inventory_sentiment"].index_value == 54.3


def test_ism_client_discovers_reports_and_returns_raw_payload(monkeypatch) -> None:
    client = ISMClient()
    calls: list[str] = []
    pages = {
        ISM_REPORTS_URL: _landing_html(),
        _MFG_URL: _manufacturing_html(),
        _SERVICES_URL: _services_html(),
    }

    def fake_get(url: str, timeout: float) -> _FakeResponse:
        calls.append(url)
        return _FakeResponse(pages[url])

    monkeypatch.setattr(client.session, "get", fake_get)

    urls = discover_current_report_urls(
        _landing_html(),
        surveys={"manufacturing", "services"},
    )
    result = client.get_all_series_with_raw(
        {
            "mfg_new_orders": ISM_REPORT_SERIES["manufacturing_new_orders"],
            "services_pmi_mom": ISM_REPORT_SERIES["services_pmi_mom"],
        }
    )

    assert urls == {"manufacturing": _MFG_URL, "services": _SERVICES_URL}
    assert calls == [ISM_REPORTS_URL, _MFG_URL, _SERVICES_URL]
    observations, payload, params = result["ISM_MFG_NEW_ORDERS_US"]
    assert observations == [
        ISMObservation(
            date="2026-04-01",
            survey="manufacturing",
            metric="new_orders",
            measure="index",
            value=54.1,
        )
    ]
    assert payload["report"]["metrics"]["new_orders"]["change_value"] == 0.6
    assert params == {
        "url": _MFG_URL,
        "survey": "manufacturing",
        "metric": "new_orders",
        "measure": "index",
        "lastNObservations": "1",
    }
    assert result["ISM_SERVICES_PMI_MOM_US"][0][0].value == -2.1


def test_ism_fetcher_normalizes_to_raw_series() -> None:
    fake_client = _FakeISMClient()
    fetcher = ISMFetcher(
        client=fake_client,
        series_config={
            "manufacturing_new_orders": ISM_REPORT_SERIES["manufacturing_new_orders"]
        },
    )

    rows = fetcher.fetch()

    assert len(rows) == 1
    row = rows[0]
    assert row.source == "ism"
    assert row.series_id == "ISM_MFG_NEW_ORDERS_US"
    assert row.observations[0].date == "2026-04-01"
    assert row.observations[0].value == 54.1
    assert row.observations[0].provider_metadata == {
        "survey": "manufacturing",
        "metric": "new_orders",
        "measure": "index",
    }
    assert row.series_metadata == {
        "category": "growth",
        "survey": "manufacturing",
        "metric": "new_orders",
        "measure": "index",
        "name": "US ISM Manufacturing New Orders",
        "unit": "index",
    }
    assert row.content_hash is not None
    assert json.loads(row.request_params_json or "{}") == {
        "url": _MFG_URL,
        "survey": "manufacturing",
        "metric": "new_orders",
        "measure": "index",
        "lastNObservations": "1",
    }


def test_ism_seed_families_concepts_schedules_subjects_and_discovery(
    tmp_path: Path,
) -> None:
    store = SQLiteEngineStore(tmp_path / "engine.db")
    store.seed_obs_sources_and_families()
    store.seed_concept_map()
    store.seed_release_schedules()
    sync_from_yaml(store)

    source = store.get_obs_source("ism")
    assert source is not None
    assert source.source_name == "Institute for Supply Management"
    assert source.country_code == "US"

    expected = {
        "ISM_MFG_PMI_US": (
            "ISM_MFG_PMI_US",
            "us.growth.ism_mfg_pmi",
            "index",
            "sa",
        ),
        "ISM_MFG_PMI_MOM_US": (
            "ISM_MFG_PMI_MOM_US",
            "us.growth.ism_mfg_pmi_mom",
            "percentage_points",
            "sa",
        ),
        "ISM_MFG_SUPPLIER_DELIVERIES_US": (
            "ISM_MFG_SUPPLIER_DELIVERIES_US",
            "us.growth.ism_mfg_supplier_deliveries",
            "index",
            "sa",
        ),
        "ISM_SERVICES_BUSINESS_ACTIVITY_US": (
            "ISM_SERVICES_BUSINESS_ACTIVITY_US",
            "us.growth.ism_services_business_activity",
            "index",
            "sa",
        ),
        "ISM_SERVICES_SUPPLIER_DELIVERIES_US": (
            "ISM_SERVICES_SUPPLIER_DELIVERIES_US",
            "us.growth.ism_services_supplier_deliveries",
            "index",
            "sa",
        ),
        "ISM_SERVICES_PRICES_US": (
            "ISM_SERVICES_PRICES_US",
            "us.growth.ism_services_prices",
            "index",
            "nsa",
        ),
    }

    for concept_id, (series_id, family_id, unit, seasonal_adjustment) in expected.items():
        family = store.get_obs_family(family_id)
        assert family is not None
        assert family.source_id == "ism"
        assert family.provider_series_id == series_id
        assert family.unit == unit
        assert family.frequency == "monthly"
        assert family.seasonal_adjustment == seasonal_adjustment
        assert family.country_code == "US"

        mappings = store.get_concept_series(concept_id)
        assert len(mappings) == 1
        assert mappings[0].source_id == "ism"
        assert mappings[0].provider_series_id == series_id
        assert mappings[0].obs_family_id == family_id

    mfg_schedule = store.get_release_schedule("ISM_MFG_PMI_US")
    services_schedule = store.get_release_schedule("ISM_SERVICES_PMI_US")
    assert mfg_schedule is not None
    assert services_schedule is not None
    assert mfg_schedule.rule_json == {
        "calendar": "us_federal",
        "ordinal": 1,
        "time": "10:00",
        "timezone": "America/New_York",
    }
    assert services_schedule.rule_json == {
        "calendar": "us_federal",
        "ordinal": 3,
        "time": "10:00",
        "timezone": "America/New_York",
    }

    expected_release = next_expected_release(
        "business_day_of_month",
        mfg_schedule.rule_json,
        reference=datetime(2026, 3, 15, tzinfo=timezone.utc),
    )
    assert expected_release is not None
    assert expected_release.isoformat() == "2026-04-01T14:00:00+00:00"

    families = [
        family
        for family in store.list_obs_families(active_only=False)
        if family.source_id == "ism"
    ]
    assert len(families) == len(ISM_REPORT_SERIES)
    assert all(result.passed for result in check_dimensions("ism", families))

    assert store.resolve_subjects_for_concept("ISM_MFG_PMI_US") == [
        "econ.us.ism_pmi"
    ]
    assert "ISM_SERVICES_PMI_US" in store.list_concepts(country_code="US")

    manager = SourceCapabilityManager(store)
    entities = manager.list_entities("ism", query="services prices", limit=5)["entities"]
    assert [entity["entity_id"] for entity in entities] == [
        "ISM_SERVICES_PRICES_US",
        "ISM_SERVICES_PRICES_MOM_US",
    ]


def test_ism_orchestrator_source_stores_indicator_rows(tmp_path: Path) -> None:
    store = SQLiteEngineStore(tmp_path / "engine.db")
    ism = _FakeISMClient()
    orchestrator = IngestionOrchestrator(store, ism=ism)

    report = orchestrator.run_source("ism")

    assert report.error == ""
    assert report.fetched == len(ISM_REPORT_SERIES)
    assert report.stored == len(ISM_REPORT_SERIES)
    assert len(ism.calls) == 1

    with store._connection(commit=False) as connection:
        row = connection.execute(
            """
            SELECT series_id, source, date, value, obs_family_id
            FROM indicators
            WHERE series_id = 'ISM_MFG_NEW_ORDERS_US'
            """
        ).fetchone()
        raw = connection.execute(
            """
            SELECT source, series_id, request_params_json
            FROM obs_raw
            WHERE series_id = 'ISM_MFG_NEW_ORDERS_US'
            """
        ).fetchone()

    assert row is not None
    assert raw is not None
    assert dict(row) == {
        "series_id": "ISM_MFG_NEW_ORDERS_US",
        "source": "ism",
        "date": "2026-04-01",
        "value": 54.1,
        "obs_family_id": "us.growth.ism_mfg_new_orders",
    }
    assert raw["source"] == "ism"
    assert raw["series_id"] == "ISM_MFG_NEW_ORDERS_US"
    assert json.loads(raw["request_params_json"]) == {
        "url": _MFG_URL,
        "survey": "manufacturing",
        "metric": "new_orders",
        "measure": "index",
        "lastNObservations": "1",
    }

    sync_from_yaml(store)
    subject_rows = store.list_subject_indicators("econ.us.ism_pmi", limit=100)
    assert any(
        item["series_id"] == "ISM_MFG_NEW_ORDERS_US"
        and item["source"] == "ism"
        and item["concept_id"] == "ISM_MFG_NEW_ORDERS_US"
        for item in subject_rows
    )

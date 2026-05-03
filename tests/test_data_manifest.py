from __future__ import annotations

import json
import sys
import threading
from datetime import datetime, timezone
from http.client import HTTPConnection
from http.server import ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ingestion.validation import ValidationStore
from macro_data.server import MacroDataRequestHandler
from macro_data.service import LocalMacroDataService
from storage.sqlite import (
    DocSourceRecord,
    DocumentRecord,
    IndicatorVintageRecord,
    NewsArticleRecord,
    SQLiteEngineStore,
)


class _FakeMarketStore:
    def __init__(self, stats: dict[str, Any]) -> None:
        self._stats = stats

    def get_manifest_stats(self) -> dict[str, Any]:
        return dict(self._stats)


class _FakeQueryResult:
    def __init__(self, column_names: list[str], result_rows: list[tuple[Any, ...]]) -> None:
        self.column_names = column_names
        self.result_rows = result_rows


class _RecordingClickHouseClient:
    def __init__(self, result: _FakeQueryResult) -> None:
        self.result = result
        self.sql = ""
        self.parameters: dict[str, Any] = {}

    def query(
        self,
        sql: str,
        *,
        parameters: dict[str, Any] | None = None,
    ) -> _FakeQueryResult:
        self.sql = sql
        self.parameters = dict(parameters or {})
        return self.result


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _seed_available_surfaces(store: SQLiteEngineStore) -> None:
    now = "2026-05-02T10:00:00+00:00"
    store.upsert_indicator_vintage(
        IndicatorVintageRecord(
            series_id="CPIAUCSL",
            source="fred",
            observation_date="2026-03-31",
            vintage_date="2026-04-10",
            value=310.12,
            vintage_quality="native_pit",
        )
    )
    with store._connection(commit=True) as connection:
        connection.execute(
            """
            INSERT INTO cal_econ_event (
                provider, provider_event_id, event_time_utc,
                event_time_precision, reference_date, reference_label,
                country_code, indicator_id, category, title, importance,
                currency, unit, actual, previous, revised, forecast,
                consensus_forecast, ticker, source, source_url, content_hash,
                last_update_epoch_ms, observed_at_epoch_ms, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "bls",
                "cpi-2026-03",
                "2026-04-10T12:30:00+00:00",
                "datetime",
                "2026-03-31",
                "Mar 2026",
                "US",
                None,
                "inflation",
                "CPI",
                "high",
                "USD",
                "index",
                "310.12",
                None,
                None,
                None,
                None,
                "",
                "bls",
                "https://example.test/cpi",
                "1" * 64,
                1_775_822_400_000,
                1_775_822_400_000,
                now,
                now,
            ),
        )

    store.upsert_doc_source(
        DocSourceRecord(
            source_id="us.bls",
            source_code="BLS",
            source_name="Bureau of Labor Statistics",
            source_type="government_agency",
            country_code="US",
            default_language_code="en",
            homepage_url="https://www.bls.gov",
            is_active=True,
            created_at=now,
            updated_at=now,
        )
    )
    store.upsert_document(
        DocumentRecord(
            document_id="doc-cpi",
            release_family_id="",
            source_id="us.bls",
            canonical_url="https://example.test/cpi",
            title="CPI report",
            subtitle="",
            document_type="report",
            mime_type="text/html",
            language_code="en",
            country_code="US",
            topic_code="inflation",
            published_date="2026-04-10",
            published_at="2026-04-10T12:30:00+00:00",
            status="published",
            version_no=1,
            parent_document_id="",
            hash_sha256="2" * 64,
            created_at=now,
            updated_at=now,
            published_precision="exact",
        )
    )
    store.upsert_news_article(
        NewsArticleRecord(
            url_hash="news-1",
            source_feed="reuters",
            feed_category="macro",
            title="Macro news",
            url="https://example.test/news",
            timestamp=1_775_830_000,
            description="News item",
            content_markdown="Body",
            content_fetched=True,
        )
    )


def _save_quality_report(
    store: SQLiteEngineStore,
    *,
    source: str,
    passed: bool,
    timestamp: str = "2026-05-02T11:00:00+00:00",
) -> None:
    validation = ValidationStore(str(store.db_path))
    try:
        validation.save_report({
            "source": source,
            "run_id": f"{source}-{timestamp}",
            "timestamp": timestamp,
            "passed": passed,
            "error_count": 0 if passed else 1,
            "warning_count": 0,
            "total_checks": 1,
            "duration_ms": 10,
        })
    finally:
        validation.close()


def _by_dataset(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {row["dataset"]: row for row in payload["datasets"]}


def test_manifest_reports_available_and_empty_launch_surfaces(
    store: SQLiteEngineStore,
) -> None:
    _seed_available_surfaces(store)
    _save_quality_report(store, source="macro_timeseries", passed=True)
    service = LocalMacroDataService(store=store)

    payload = service.invoke("get_data_manifest", {})
    rows = _by_dataset(payload)

    assert payload["version"] == "v1"
    assert set(rows) == {
        "macro_timeseries",
        "calendar",
        "documents",
        "news",
        "market_bars",
        "fundamentals",
        "corp_calendar",
    }
    assert rows["macro_timeseries"]["status"] == "available"
    assert rows["macro_timeseries"]["row_count"] == 1
    assert rows["macro_timeseries"]["latest_timestamp"] == "2026-03-31"
    assert rows["macro_timeseries"]["quality_status"] == "pass"
    assert rows["macro_timeseries"]["last_quality_run"] == "2026-05-02T11:00:00+00:00"

    assert rows["calendar"]["status"] == "available"
    assert rows["documents"]["status"] == "available"
    assert rows["news"]["status"] == "available"
    assert rows["market_bars"]["status"] == "empty"
    assert rows["fundamentals"]["status"] == "empty"
    assert rows["corp_calendar"]["status"] == "empty"
    assert payload["summary"]["available"] == 4
    assert payload["summary"]["empty"] == 3


def test_manifest_marks_failed_quality_report_as_degraded(
    store: SQLiteEngineStore,
) -> None:
    _seed_available_surfaces(store)
    _save_quality_report(store, source="calendar", passed=False)
    service = LocalMacroDataService(store=store)

    rows = _by_dataset(service.invoke("get_data_manifest", {}))

    assert rows["calendar"]["row_count"] == 1
    assert rows["calendar"]["status"] == "degraded"
    assert rows["calendar"]["quality_status"] == "fail"


def test_manifest_uses_market_store_stats(store: SQLiteEngineStore) -> None:
    service = LocalMacroDataService(
        store=store,
        market_store=_FakeMarketStore({
            "row_count": 12,
            "latest_timestamp": "2026-05-01T00:00:00Z",
            "latest_ingested_at": "2026-05-01T00:10:00Z",
        }),
    )

    rows = _by_dataset(service.invoke("get_data_manifest", {}))

    assert rows["market_bars"]["status"] == "available"
    assert rows["market_bars"]["row_count"] == 12
    assert rows["market_bars"]["latest_timestamp"] == "2026-05-01T00:00:00Z"


def test_clickhouse_manifest_stats_use_part_metadata() -> None:
    from storage.clickhouse.store import ClickHouseMarketStore

    client = _RecordingClickHouseClient(
        _FakeQueryResult(
            ["row_count", "latest_timestamp", "latest_ingested_at"],
            [(
                180_000_000,
                datetime(2026, 5, 1, 20, 0, tzinfo=timezone.utc),
                datetime(2026, 5, 2, 2, 10, tzinfo=timezone.utc),
            )],
        )
    )

    stats = ClickHouseMarketStore(client, database="market").get_manifest_stats()

    assert "FROM system.parts" in client.sql
    assert client.sql.count("FINAL") == 0
    assert client.parameters == {"database": "market", "table": "bars_1d"}
    assert stats == {
        "row_count": 180_000_000,
        "latest_timestamp": "2026-05-01T20:00:00Z",
        "latest_ingested_at": "2026-05-02T02:10:00Z",
    }


@pytest.fixture()
def live_server(store: SQLiteEngineStore):
    service = LocalMacroDataService(store=store)
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), MacroDataRequestHandler)
    httpd.service = service  # type: ignore[attr-defined]
    httpd.api_token = ""  # type: ignore[attr-defined]
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    host, port = httpd.server_address[0], httpd.server_address[1]
    try:
        yield host, port
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=5)


def _http_get(host: str, port: int, path: str) -> tuple[int, dict[str, Any]]:
    conn = HTTPConnection(host, port, timeout=5)
    try:
        conn.request("GET", path)
        response = conn.getresponse()
        raw = response.read().decode("utf-8")
    finally:
        conn.close()
    payload = json.loads(raw) if raw else {}
    return response.status, payload


def test_http_manifest_route_returns_manifest(
    store: SQLiteEngineStore,
    live_server,
) -> None:
    _seed_available_surfaces(store)
    host, port = live_server

    status, payload = _http_get(host, port, "/v1/manifest")

    assert status == 200
    rows = _by_dataset(payload)
    assert rows["macro_timeseries"]["status"] == "available"
    assert rows["fundamentals"]["status"] == "empty"

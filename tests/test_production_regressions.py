from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ingestion.clients._gov_report import GovReportIngestionClient
from ingestion.clients._news import NewsIngestionClient
from ingestion.clients._sdmx_clients import IMFIngestionClient
from ingestion.scrapers.gov_report import GovReportItem
from ingestion.sdmx.providers.imf import IMFVintageObservation


def test_news_fetch_entries_raises_when_all_feeds_fail(monkeypatch) -> None:
    client = NewsIngestionClient()
    monkeypatch.setattr(
        "ingestion.clients._news.get_feeds",
        lambda category=None: [SimpleNamespace(name="feed-a", url="https://example.com/rss", category="markets")],
    )
    client._session = Mock()
    client._session.get.side_effect = RuntimeError("boom")

    with pytest.raises(RuntimeError, match="news feed fetch failed for all 1 feeds"):
        client.fetch_entries()


def test_news_fetch_entries_builds_records_when_feed_succeeds(monkeypatch) -> None:
    client = NewsIngestionClient()
    monkeypatch.setattr(
        "ingestion.clients._news.get_feeds",
        lambda category=None: [SimpleNamespace(name="feed-a", url="https://example.com/rss", category="markets")],
    )
    monkeypatch.setattr(
        "ingestion.clients._news.feedparser.parse",
        lambda text: SimpleNamespace(entries=[{"title": "Headline", "link": "https://example.com/a", "summary": "Desc"}]),
    )
    client._session = Mock()
    response = Mock()
    response.text = "<rss />"
    response.raise_for_status.return_value = None
    client._session.get.return_value = response

    entries = client.fetch_entries()

    assert len(entries) == 1
    assert entries[0].source_feed == "feed-a"
    assert entries[0].title == "Headline"


def test_gov_report_store_items_raises_when_every_item_fails(monkeypatch) -> None:
    client = GovReportIngestionClient()
    monkeypatch.setattr(client, "_ensure_seed", lambda store: None)

    store = Mock()
    item = GovReportItem(
        source="gov_bls",
        source_id="us_bls_cpi",
        title="CPI release",
        url="https://www.bls.gov/news.release/cpi.htm",
        published_at="2026-03-19T08:30:00Z",
        institution="BLS",
        country="US",
        language="en",
        data_category="inflation",
    )
    monkeypatch.setattr(
        "ingestion.clients._gov_report.canonicalize_url",
        Mock(side_effect=RuntimeError("url failure")),
    )

    with pytest.raises(RuntimeError, match="gov report storage failed for all 1 items"):
        client.store_items(store, [item])


def test_imf_refresh_vintages_uses_supported_signature() -> None:
    store = Mock()

    class StubIMFClient:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def get_vintages(self, dataflow_id, key, *, series_id, version="", as_of_dates=(), limit=1):
            self.calls.append({
                "dataflow_id": dataflow_id,
                "key": key,
                "series_id": series_id,
                "version": version,
                "as_of_dates": list(as_of_dates),
                "limit": limit,
            })
            return [
                IMFVintageObservation(
                    series_id=series_id,
                    date="2026-01-01",
                    vintage_date="2026-02-01",
                    value=123.4,
                    dataflow=dataflow_id,
                )
            ]

    client = IMFIngestionClient()
    stub = StubIMFClient()
    client.client = stub

    stats = client.refresh_vintages(store, family_lookup={})

    assert stats.count > 0
    assert stub.calls
    assert "start_period" not in stub.calls[0]
    store.upsert_indicator_vintage.assert_called()

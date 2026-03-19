from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ingestion.clients._eia import EIAIngestionClient
from ingestion.clients._gov_report import GovReportIngestionClient
from ingestion.clients._news import NewsIngestionClient
from ingestion.clients._sdmx_clients import IMFIngestionClient
from ingestion.news_feeds import get_feeds
from ingestion.scrapers.gov_report import GovReportItem
from ingestion.sdmx.providers.imf import IMFVintageObservation


def test_news_fetch_entries_raises_when_all_feeds_fail(monkeypatch) -> None:
    client = NewsIngestionClient()
    monkeypatch.setattr(
        "ingestion.news.client.get_feeds",
        lambda category=None: [SimpleNamespace(name="feed-a", url="https://example.com/rss", category="markets")],
    )
    client._session = Mock()
    client._session.get.side_effect = RuntimeError("boom")

    with pytest.raises(RuntimeError, match="news feed fetch failed for all 1 feeds"):
        client.fetch_entries()


def test_news_fetch_entries_builds_records_when_feed_succeeds(monkeypatch) -> None:
    client = NewsIngestionClient()
    monkeypatch.setattr(
        "ingestion.news.client.get_feeds",
        lambda category=None: [SimpleNamespace(name="feed-a", url="https://example.com/rss", category="markets")],
    )
    monkeypatch.setattr(
        "ingestion.news.client.feedparser.parse",
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


def test_news_feed_registry_hides_disabled_feeds_by_default() -> None:
    feeds = get_feeds()

    names = {feed.name for feed in feeds}

    assert "IMF News" not in names
    assert "AEI" not in names


def test_news_store_articles_drops_short_content() -> None:
    client = NewsIngestionClient()
    store = Mock()
    client._article_fetcher = Mock()
    client._article_fetcher.fetch_article.return_value = SimpleNamespace(
        content="too short",
        fetched=False,
        content_length=9,
    )
    entry = SimpleNamespace(
        url_hash="u",
        title_hash="t",
        canonical_url="https://example.com/a",
        raw_url="https://example.com/a",
        raw_title="Headline",
        description="Desc",
        source_feed="feed-a",
        feed_category="markets",
        timestamp=1_700_000_000,
    )

    count = client.store_articles(store, [entry])

    assert count == 0
    store.upsert_news_article.assert_not_called()


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


def test_gov_report_store_items_rejects_short_content(monkeypatch) -> None:
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
        content_markdown="short",
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


def test_eia_refresh_subset_uses_recent_cache() -> None:
    client = EIAIngestionClient()
    client.client = Mock()
    client.client.get_series.side_effect = RuntimeError("upstream down")
    client._fred = Mock()
    store = Mock()
    store.get_indicator_history.return_value = [SimpleNamespace(date="2099-01-01")]

    stats = client.refresh_subset(
        store,
        series_configs={
            "petroleum_brent": {
                "route": "petroleum/pri/spt/data",
                "params": {"frequency": "daily"},
                "series_id": "EIA_BRENT",
                "category": "energy",
            }
        },
        family_lookup={},
    )

    assert stats.count == 1
    store.upsert_indicator_observation.assert_not_called()


def test_eia_refresh_subset_uses_fred_fallback() -> None:
    client = EIAIngestionClient()
    client.client = Mock()
    client.client.get_series.side_effect = RuntimeError("upstream down")
    client._fred = Mock()
    client._fred.get_series.return_value = [
        SimpleNamespace(series_id="DCOILWTICO", date="2026-03-19", value=68.5),
    ]
    store = Mock()
    store.get_indicator_history.return_value = []

    stats = client.refresh_subset(
        store,
        series_configs={
            "petroleum_wti": {
                "route": "petroleum/pri/spt/data",
                "params": {"frequency": "daily"},
                "series_id": "EIA_WTI",
                "category": "energy",
            }
        },
        family_lookup={},
    )

    assert stats.count == 1
    store.upsert_indicator_observation.assert_called()

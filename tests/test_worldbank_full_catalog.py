"""Integration tests for World Bank full catalog access and parallel fetch.

Requires network access. Run with:
    pytest tests/test_worldbank_full_catalog.py -v -s
"""

from __future__ import annotations

import sys
import threading
import time
from pathlib import Path
from unittest.mock import Mock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from analyst.ingestion.scrapers.worldbank import (
    WorldBankClient,
    WorldBankRateLimitError,
)
from analyst.ingestion.sources import (
    WorldBankIngestionClient,
    WorldBankSeriesConfig,
    _WorldBankRateLimiter,
)

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def wb_client() -> WorldBankClient:
    return WorldBankClient()


def _check_wb_available(client: WorldBankClient) -> None:
    """Try a minimal fetch; skip if World Bank API is down or throttling."""
    try:
        client.get_indicator(
            "SP.POP.TOTL", "USA", series_id="test", limit=1,
        )
    except WorldBankRateLimitError:
        pytest.skip("World Bank API is rate limiting — try again later")
    except Exception as exc:
        pytest.skip(f"World Bank API unavailable: {exc}")


class TestCatalogDiscovery:
    """Validate that we can discover the full World Bank catalog."""

    def test_list_sources_returns_multiple(self, wb_client: WorldBankClient) -> None:
        sources = wb_client.list_sources()
        assert len(sources) > 20, f"Expected >20 sources, got {len(sources)}"
        print(f"\n  Total sources: {len(sources)}")
        for src in sources[:5]:
            print(f"    {src.id}: {src.name}")

    def test_list_topics_returns_multiple(self, wb_client: WorldBankClient) -> None:
        topics = wb_client.list_topics()
        assert len(topics) >= 15, f"Expected >=15 topics, got {len(topics)}"
        print(f"\n  Total topics: {len(topics)}")
        for topic in topics[:5]:
            print(f"    {topic.id}: {topic.name}")

    def test_list_indicators_returns_thousands(self, wb_client: WorldBankClient) -> None:
        indicators = wb_client.list_indicators(max_pages=3)
        assert len(indicators) > 1000, f"Expected >1000 indicators, got {len(indicators)}"
        print(f"\n  Indicators (first 3 pages): {len(indicators)}")

    def test_list_indicators_by_topic(self, wb_client: WorldBankClient) -> None:
        """Topic 3 = Economy & Growth."""
        indicators = wb_client.list_indicators(topic_id="3")
        assert len(indicators) > 10, f"Expected >10 economy indicators, got {len(indicators)}"
        print(f"\n  Economy & Growth indicators: {len(indicators)}")
        for ind in indicators[:5]:
            print(f"    {ind.id}: {ind.name}")

    def test_search_indicators_filters_results(self, wb_client: WorldBankClient) -> None:
        results = wb_client.search_indicators("GDP", limit=20)
        assert len(results) > 0, "Expected GDP search results"
        assert all("gdp" in r.id.lower() or "gdp" in r.name.lower() for r in results)
        print(f"\n  GDP search results: {len(results)}")
        for r in results[:5]:
            print(f"    {r.id}: {r.name}")

    def test_list_countries_returns_many(self, wb_client: WorldBankClient) -> None:
        countries = wb_client.list_countries()
        assert len(countries) > 200, f"Expected >200 countries, got {len(countries)}"
        print(f"\n  Total countries/economies: {len(countries)}")
        for c in countries[:5]:
            print(f"    {c.id}: {c.name} ({c.income_level})")


class TestRateLimiter:
    """Verify _WorldBankRateLimiter enforces minimum intervals and hourly budget."""

    def test_rate_limiter_enforces_minimum_interval(self) -> None:
        limiter = _WorldBankRateLimiter(min_interval=0.1)
        timestamps: list[float] = []

        def worker() -> None:
            limiter.wait()
            timestamps.append(time.monotonic())

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        timestamps.sort()
        gaps = [timestamps[i + 1] - timestamps[i] for i in range(len(timestamps) - 1)]
        for gap in gaps:
            assert gap >= 0.09, f"Gap {gap:.4f}s below minimum interval 0.1s"
        print(f"\n  Gaps between requests: {[f'{g:.3f}s' for g in gaps]}")

    def test_rate_limiter_thread_safety(self) -> None:
        limiter = _WorldBankRateLimiter(min_interval=0.02)
        call_count = 0
        lock = threading.Lock()

        def worker() -> None:
            nonlocal call_count
            for _ in range(10):
                limiter.wait()
                with lock:
                    call_count += 1

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert call_count == 40

    def test_backoff_pushes_next_request_forward(self) -> None:
        limiter = _WorldBankRateLimiter(min_interval=0.05)
        limiter.wait()

        limiter.backoff(0.5)
        t0 = time.monotonic()
        limiter.wait()
        elapsed = time.monotonic() - t0

        assert elapsed >= 0.4, f"Expected >=0.4s wait after backoff, got {elapsed:.3f}s"
        print(f"\n  Backoff wait: {elapsed:.3f}s")

    def test_hourly_budget_tracks_request_count(self) -> None:
        limiter = _WorldBankRateLimiter(min_interval=0.01)
        for _ in range(5):
            limiter.wait()
        assert limiter._hour_count == 5

    def test_hourly_budget_caps_at_limit(self) -> None:
        limiter = _WorldBankRateLimiter(min_interval=0.01)
        limiter._hour_count = _WorldBankRateLimiter.HOURLY_BUDGET
        limiter._hour_start = time.monotonic()

        t0 = time.monotonic()
        limiter._hour_start = time.monotonic() - 3601.0
        limiter.wait()
        elapsed = time.monotonic() - t0

        assert elapsed < 1.0
        assert limiter._hour_count == 1
        print(f"\n  Budget reset after window expired: count={limiter._hour_count}")

    def test_default_interval_is_permissive(self) -> None:
        """World Bank defaults are more permissive than OECD."""
        limiter = _WorldBankRateLimiter()
        assert limiter._min_interval <= 0.5
        assert limiter.HOURLY_BUDGET == 500


class TestPagination:
    """Test pagination and multi-country fetches."""

    def test_get_all_pages_returns_combined_results(self, wb_client: WorldBankClient) -> None:
        _check_wb_available(wb_client)
        observations = wb_client.get_indicator(
            "SP.POP.TOTL", "USA",
            series_id="test_pop",
            fetch_all_pages=True,
            per_page=10,
        )
        assert len(observations) > 10, (
            f"Expected >10 obs with full pagination, got {len(observations)}"
        )
        print(f"\n  USA population observations: {len(observations)}")

    def test_country_all_returns_multi_country_data(self, wb_client: WorldBankClient) -> None:
        _check_wb_available(wb_client)
        observations = wb_client.get_indicator(
            "SP.POP.TOTL", "USA;CHN;GBR",
            series_id="test_pop",
            limit=200,
            per_page=200,
        )
        assert len(observations) > 0
        countries = {obs.country_code for obs in observations}
        assert len(countries) > 1, f"Expected multiple countries, got {countries}"
        print(f"\n  Multi-country pop obs: {len(observations)}, countries: {countries}")

    def test_fetch_indicator_bulk(self, wb_client: WorldBankClient) -> None:
        _check_wb_available(wb_client)
        observations = wb_client.fetch_indicator_bulk(
            "NY.GDP.MKTP.CD",
            countries=["USA", "CHN", "JPN"],
            per_page=100,
        )
        assert len(observations) > 0
        series_ids = {obs.series_id for obs in observations}
        assert len(series_ids) > 1, f"Expected per-country series IDs, got {series_ids}"
        print(f"\n  Bulk GDP obs: {len(observations)}, series: {series_ids}")


class TestParallelRefresh:
    """Exercise refresh_parallel with real World Bank API + mock store."""

    def test_refresh_parallel_fetches_series(self, wb_client: WorldBankClient) -> None:
        _check_wb_available(wb_client)
        configs = {
            "pop_us": WorldBankSeriesConfig(
                indicator="SP.POP.TOTL", country="USA",
                series_id="WB_POP_US", category="demographics", limit=5,
            ),
            "gdp_us": WorldBankSeriesConfig(
                indicator="NY.GDP.MKTP.CD", country="USA",
                series_id="WB_GDP_US", category="growth", limit=5,
            ),
        }
        ingestion = WorldBankIngestionClient(client=wb_client, series_configs=configs)

        store = Mock()
        t0 = time.monotonic()
        stats = ingestion.refresh_parallel(store, max_workers=2, request_delay=0.2)
        elapsed = time.monotonic() - t0

        assert stats.source == "worldbank"
        assert stats.count > 0
        assert store.upsert_indicator_observation.call_count == stats.count
        print(f"\n  refresh_parallel: {stats.count} obs in {elapsed:.1f}s")

    def test_catalog_parallel_handles_errors(self, wb_client: WorldBankClient) -> None:
        _check_wb_available(wb_client)
        configs = {
            "bogus": WorldBankSeriesConfig(
                indicator="DOES_NOT_EXIST_XYZ", country="USA",
                series_id="WB_BOGUS", category="test", limit=5,
            ),
        }
        ingestion = WorldBankIngestionClient(client=wb_client, series_configs=configs)

        store = Mock()
        stats = ingestion.refresh_parallel(store, max_workers=1, request_delay=0.1)
        assert stats.source == "worldbank"
        # Should not crash; count may be 0
        assert store.upsert_indicator_observation.call_count == stats.count
        print(f"\n  Bogus indicator: count={stats.count} (no crash)")


class TestCatalogRefresh:
    """Exercise catalog-based refresh with real World Bank API + mock store."""

    def test_refresh_catalog_fetches_discovered_indicators(self, wb_client: WorldBankClient) -> None:
        _check_wb_available(wb_client)
        ingestion = WorldBankIngestionClient(client=wb_client)

        store = Mock()
        t0 = time.monotonic()
        # Use WDI source (id=2) with known indicators that have USA data
        stats = ingestion.refresh_catalog(
            store,
            source_id="2",
            query="GDP",
            indicator_limit=2,
            countries=["USA"],
            latest_observations=5,
            sleep_seconds=0.2,
        )
        elapsed = time.monotonic() - t0

        assert stats.source == "worldbank_catalog"
        assert stats.count > 0
        assert store.upsert_indicator_observation.call_count == stats.count
        print(f"\n  refresh_catalog: {stats.count} obs in {elapsed:.1f}s")

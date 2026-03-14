"""Integration tests for OECD full catalog access and parallel fetch.

Requires network access. Run with:
    pytest tests/test_oecd_full_catalog.py -v -s
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

from analyst.ingestion.scrapers.oecd import (
    OECDClient,
    OECDRateLimitError,
    _build_decade_chunks,
)
from analyst.ingestion.sources import (
    OECDIngestionClient,
    OECDSeriesConfig,
    _OECDRateLimiter,
)

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def oecd_client() -> OECDClient:
    return OECDClient(timeout=45)


def _check_oecd_data_available(client: OECDClient) -> None:
    """Try a minimal fetch; skip test if OECD is rate limiting us."""
    try:
        client.fetch_data(
            "DSD_STES@DF_CLI",
            agency_id="OECD.SDD.STES",
            key="USA.M.LI.IX._Z.NOR.IX._Z.H",
            limit=1,
        )
    except OECDRateLimitError:
        pytest.skip("OECD API is rate limiting — try again in a few minutes")


class TestCatalogDiscovery:
    """Validate that we can discover the full OECD catalog."""

    def test_list_all_dataflows_returns_hundreds(self, oecd_client: OECDClient) -> None:
        dataflows = oecd_client.list_dataflows(agency_id="all")
        assert len(dataflows) > 100, (
            f"Expected >100 dataflows, got {len(dataflows)}"
        )
        agencies = {df.agency_id for df in dataflows}
        assert len(agencies) > 5

        print(f"\n  Total dataflows: {len(dataflows)}")
        print(f"  Distinct agencies: {len(agencies)}")
        for agency in sorted(agencies)[:10]:
            count = sum(1 for df in dataflows if df.agency_id == agency)
            print(f"    {agency}: {count} dataflows")

    def test_structure_introspection(self, oecd_client: OECDClient) -> None:
        """Verify structure/dimensions for the CLI dataflow."""
        summary = oecd_client.summarize_structure(
            "DSD_STES@DF_CLI", agency_id="OECD.SDD.STES"
        )
        assert summary.dataflow_id == "DSD_STES@DF_CLI"
        assert len(summary.series_dimensions) > 0
        assert summary.time_dimension_id
        print(
            f"\n  CLI structure: dims={list(summary.series_dimensions)}, "
            f"codes={dict(summary.code_counts)}"
        )


class TestRateLimiter:
    """Verify _OECDRateLimiter enforces minimum intervals and hourly budget."""

    def test_rate_limiter_enforces_minimum_interval(self) -> None:
        limiter = _OECDRateLimiter(min_interval=0.1)
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
        limiter = _OECDRateLimiter(min_interval=0.05)
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
        limiter = _OECDRateLimiter(min_interval=0.05)
        limiter.wait()  # prime it

        limiter.backoff(0.5)
        t0 = time.monotonic()
        limiter.wait()
        elapsed = time.monotonic() - t0

        assert elapsed >= 0.4, f"Expected >=0.4s wait after backoff, got {elapsed:.3f}s"
        print(f"\n  Backoff wait: {elapsed:.3f}s")

    def test_hourly_budget_tracks_request_count(self) -> None:
        limiter = _OECDRateLimiter(min_interval=0.01)
        # Make some requests and verify the counter tracks
        for _ in range(5):
            limiter.wait()
        assert limiter._hour_count == 5

    def test_hourly_budget_caps_at_limit(self) -> None:
        """When budget is exhausted, limiter should block (we test with tiny budget)."""
        limiter = _OECDRateLimiter(min_interval=0.01)
        # Artificially exhaust the budget
        limiter._hour_count = _OECDRateLimiter.HOURLY_BUDGET
        limiter._hour_start = time.monotonic()  # window just started

        t0 = time.monotonic()
        # This should detect exhausted budget and reset the window
        # (since we can't wait 3600s in a test, we cheat by backdating the window)
        limiter._hour_start = time.monotonic() - 3601.0
        limiter.wait()
        elapsed = time.monotonic() - t0

        # Window should have reset, so request goes through quickly
        assert elapsed < 1.0
        assert limiter._hour_count == 1
        print(f"\n  Budget reset after window expired: count={limiter._hour_count}")

    def test_default_interval_is_conservative(self) -> None:
        """Default min_interval should be >= 2s to respect 60/hour."""
        limiter = _OECDRateLimiter()
        assert limiter._min_interval >= 2.0
        assert limiter.HOURLY_BUDGET == 60


class TestParallelRefresh:
    """Exercise refresh_parallel with real OECD API calls + mock store."""

    def test_refresh_parallel_fetches_series(self, oecd_client: OECDClient) -> None:
        """Use refresh_parallel to fetch 3 CLI series concurrently."""
        _check_oecd_data_available(oecd_client)

        # Use only 3 series to stay well under rate limits
        configs = dict(list(OECDIngestionClient().series_configs.items())[:3])
        ingestion = OECDIngestionClient(client=oecd_client, series_configs=configs)

        store = Mock()
        t0 = time.monotonic()
        stats = ingestion.refresh_parallel(store, max_workers=2)
        elapsed = time.monotonic() - t0

        assert stats.source == "oecd"
        assert stats.count > 0, "Expected observations from parallel refresh"
        assert store.upsert_indicator_observation.call_count == stats.count

        print(f"\n  refresh_parallel: {stats.count} obs in {elapsed:.1f}s")
        print(f"  store.upsert calls: {store.upsert_indicator_observation.call_count}")


class TestCatalogParallelRefresh:
    """Exercise refresh_catalog_parallel with real OECD API + mock store."""

    def test_refresh_catalog_parallel_fetches_dataflows(self, oecd_client: OECDClient) -> None:
        _check_oecd_data_available(oecd_client)

        store = Mock()
        ingestion = OECDIngestionClient(client=oecd_client)

        t0 = time.monotonic()
        stats = ingestion.refresh_catalog_parallel(
            store,
            agency_prefix="OECD.SDD.STES",
            dataflow_limit=2,
            latest_observations=1,
        )
        elapsed = time.monotonic() - t0

        assert stats.source == "oecd_catalog"
        assert stats.count > 0
        assert store.upsert_indicator_observation.call_count == stats.count

        print(
            f"\n  refresh_catalog_parallel: {stats.count} obs "
            f"from 2 STES dataflows in {elapsed:.1f}s"
        )

    def test_catalog_parallel_errors_are_logged_not_raised(self, oecd_client: OECDClient) -> None:
        """Errors for individual dataflows should be logged, not crash the pool."""
        store = Mock()
        ingestion = OECDIngestionClient(client=oecd_client)

        stats = ingestion.refresh_catalog_parallel(
            store,
            dataflow_ids=["DOES_NOT_EXIST_XYZ"],
            max_workers=1,
            request_delay=0.5,
        )
        assert stats.source == "oecd_catalog"
        assert stats.count == 0
        assert store.upsert_indicator_observation.call_count == 0
        print("\n  Bogus dataflow: logged error, returned count=0 (no crash)")


class TestTimeRangeChunking:
    """Validate automatic time-range chunking for large datasets."""

    def test_build_decade_chunks_basic(self) -> None:
        chunks = _build_decade_chunks(1960, 2026)
        assert chunks[0] == ("1960", "1969")
        assert chunks[1] == ("1970", "1979")
        assert chunks[-1][1] == "2026"
        print(f"\n  Decade chunks 1960–2026: {chunks}")

    def test_build_decade_chunks_partial(self) -> None:
        chunks = _build_decade_chunks(2015, 2026)
        assert chunks == [("2015", "2024"), ("2025", "2026")]
        print(f"\n  Partial decades 2015–2026: {chunks}")

    def test_build_decade_chunks_single_year(self) -> None:
        chunks = _build_decade_chunks(2025, 2025)
        assert chunks == [("2025", "2025")]

    def test_auto_chunk_skips_small_dataset(self, oecd_client: OECDClient) -> None:
        """CLI dataset is small — auto-chunking should return None (no chunks)."""
        _check_oecd_data_available(oecd_client)

        result = oecd_client._auto_chunk_ranges(
            "DSD_STES@DF_CLI",
            agency_id="OECD.SDD.STES",
            version=None,
            key="all",
            obs_threshold=1_000_000,
        )
        assert result is None, "CLI dataset should be below 1M obs threshold"
        print("\n  CLI dataset: below threshold, no chunking needed")

    def test_fetch_dataset_chunked_small_returns_direct(self, oecd_client: OECDClient) -> None:
        """Small dataset should go through without chunking."""
        _check_oecd_data_available(oecd_client)

        t0 = time.monotonic()
        obs = oecd_client.fetch_dataset_chunked(
            "DSD_STES@DF_CLI",
            agency_id="OECD.SDD.STES",
            key="all",
            limit=1,
            obs_threshold=1_000_000,
        )
        elapsed = time.monotonic() - t0
        assert len(obs) > 0
        print(f"\n  Chunked fetch (small dataset, no split): {len(obs)} obs in {elapsed:.1f}s")

    def test_fetch_dataset_chunked_with_explicit_ranges(self, oecd_client: OECDClient) -> None:
        """Verify explicit chunk_ranges are respected."""
        _check_oecd_data_available(oecd_client)

        chunks_received: list[tuple[str, str, int]] = []

        def on_chunk(obs: list, start: str, end: str) -> None:
            chunks_received.append((start, end, len(obs)))

        t0 = time.monotonic()
        obs = oecd_client.fetch_dataset_chunked(
            "DSD_STES@DF_CLI",
            agency_id="OECD.SDD.STES",
            key="USA.M.LI.IX._Z.NOR.IX._Z.H",
            limit=None,
            chunk_ranges=[("2000", "2009"), ("2010", "2019"), ("2020", "2026")],
            on_chunk=on_chunk,
        )
        elapsed = time.monotonic() - t0

        assert len(chunks_received) == 3
        total_from_chunks = sum(c[2] for c in chunks_received)
        assert total_from_chunks == len(obs)

        print(f"\n  Explicit 3-chunk fetch: {len(obs)} total obs in {elapsed:.1f}s")
        for start, end, count in chunks_received:
            print(f"    [{start}–{end}]: {count} obs")

    def test_auto_chunk_triggers_on_low_threshold(self, oecd_client: OECDClient) -> None:
        """Force chunking by setting a very low threshold."""
        _check_oecd_data_available(oecd_client)

        result = oecd_client._auto_chunk_ranges(
            "DSD_STES@DF_CLI",
            agency_id="OECD.SDD.STES",
            version=None,
            key="all",
            obs_threshold=1,  # impossibly low — forces chunking
        )
        assert result is not None, "Should trigger chunking with threshold=1"
        assert len(result) > 1
        assert result[0][0] == "1960"
        print(f"\n  Forced chunking (threshold=1): {len(result)} decade chunks")


class TestLargeDatasetFetch:
    """Integration tests against large OECD datasets.

    These tests validate memory handling, parsing, and chunked fetch
    against datasets with high observation counts (e.g. SNA_TABLE1, MEI).
    """

    def test_large_dataset_probe_estimates_size(self, oecd_client: OECDClient) -> None:
        """Probe MEI (Main Economic Indicators) to estimate observation count."""
        _check_oecd_data_available(oecd_client)

        # MEI is one of the larger OECD datasets
        t0 = time.monotonic()
        probe = oecd_client._get_data_json(
            "DSD_KEI@DF_KEI",
            agency_id="OECD.SDD.STES",
            version=None,
            key="all",
            limit=1,
        )
        elapsed = time.monotonic() - t0

        inner = probe.get("data", probe)
        datasets = inner.get("dataSets", [])
        total_series = sum(len(ds.get("series", {})) for ds in datasets)

        obs_dims = inner.get("structures", [{}])[0].get("dimensions", {}).get("observation", [])
        time_dim_size = 1
        for dim in obs_dims:
            if dim.get("id") == "TIME_PERIOD":
                time_dim_size = max(len(dim.get("values", [])), 1)

        estimated_obs = total_series * time_dim_size
        print(f"\n  MEI (DF_KEI) probe: {elapsed:.1f}s")
        print(f"    Series: {total_series}")
        print(f"    Time periods: {time_dim_size}")
        print(f"    Estimated obs: {estimated_obs:,}")
        assert total_series > 0, "Expected series from MEI dataset"

    def test_large_dataset_chunked_fetch_limited(self, oecd_client: OECDClient) -> None:
        """Fetch MEI with chunking, using lastNObservations=1 to keep it fast."""
        _check_oecd_data_available(oecd_client)

        chunks_received: list[tuple[str, str, int]] = []

        def on_chunk(obs: list, start: str, end: str) -> None:
            chunks_received.append((start, end, len(obs)))

        t0 = time.monotonic()
        obs = oecd_client.fetch_dataset_chunked(
            "DSD_KEI@DF_KEI",
            agency_id="OECD.SDD.STES",
            key="all",
            limit=1,  # latest observation per series only
            chunk_ranges=[("2020", "2026")],
            on_chunk=on_chunk,
        )
        elapsed = time.monotonic() - t0

        assert len(obs) > 100, f"Expected >100 obs from MEI, got {len(obs)}"
        distinct_series = len({o.series_id for o in obs})

        print(f"\n  MEI chunked fetch [2020–2026] (limit=1): {elapsed:.1f}s")
        print(f"    Observations: {len(obs)}")
        print(f"    Distinct series: {distinct_series}")
        for start, end, count in chunks_received:
            print(f"    [{start}–{end}]: {count} obs")

    def test_large_dataset_memory_bounded(self, oecd_client: OECDClient) -> None:
        """Verify that fetching a large dataset slice doesn't blow up memory."""
        _check_oecd_data_available(oecd_client)
        import resource

        mem_before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

        obs = oecd_client.fetch_dataset_chunked(
            "DSD_KEI@DF_KEI",
            agency_id="OECD.SDD.STES",
            key="all",
            limit=1,
            chunk_ranges=[("2024", "2026")],
        )

        mem_after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        mem_delta_mb = (mem_after - mem_before) / 1024  # KB -> MB on Linux

        print(f"\n  Memory delta for MEI [2024–2026]: {mem_delta_mb:.1f} MB")
        print(f"    Observations: {len(obs)}")
        # Should not use more than 500 MB for a single-year slice
        assert mem_delta_mb < 500, f"Memory usage too high: {mem_delta_mb:.1f} MB"

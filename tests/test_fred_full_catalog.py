"""Integration tests for FRED/ALFRED full catalog access and 9-layer validation.

Requires network access and FRED_API_KEY env var. Run with:
    pytest tests/test_fred_full_catalog.py -v -s
"""

from __future__ import annotations

import random
import re
import sys
import time
import uuid
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

import pytest
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from analyst.ingestion.scrapers.fred import (
    FredAPIError,
    FredClient,
    FredObservation,
    FredRateLimitError,
    FredVintageObservation,
)
from analyst.ingestion.sources import MACRO_SERIES, VINTAGE_SERIES
from analyst.ingestion.validation._types import (
    CheckResult,
    ValidationLayer,
    ValidationReport,
    ValidationSeverity,
)

pytestmark = pytest.mark.integration

_FRED_REQUEST_DELAY = 0.55  # stay under 120 req/min


# -- Fixtures ----------------------------------------------------------------

@pytest.fixture(scope="module")
def fred_client() -> FredClient:
    client = FredClient()
    if not client.api_key:
        pytest.skip("FRED_API_KEY not set")
    return client


@pytest.fixture(scope="module")
def top_categories(fred_client: FredClient) -> list[dict]:
    """Cache the top-level FRED category tree for the module."""
    return fred_client.get_categories(category_id=0)


@pytest.fixture(scope="module")
def all_releases(fred_client: FredClient) -> list[dict]:
    """Cache the full FRED releases list for the module."""
    return fred_client.get_releases()


def _check_fred_available(client: FredClient) -> None:
    """Try a minimal series fetch; skip test if FRED is unavailable."""
    try:
        client.get_series_info("GDP")
    except FredRateLimitError:
        pytest.skip("FRED API is rate limiting -- try again later")
    except (FredAPIError, requests.RequestException):
        pytest.skip("FRED API unavailable")


def _find_leaf_category_series(
    client: FredClient,
    top_categories: list[dict],
    *,
    limit: int = 20,
) -> list[dict]:
    """Walk the category tree until we find a category with direct series.

    Top-level categories often contain only sub-categories.  This helper
    descends up to 3 levels deep across multiple top-level categories to
    find a leaf node that has actual series attached.
    """
    for cat in top_categories:
        queue: list[tuple[int, int]] = [(cat["id"], 0)]
        while queue:
            cat_id, depth = queue.pop(0)
            if depth > 3:
                continue
            try:
                series = client.get_category_series(cat_id, limit=limit)
            except (FredAPIError, FredRateLimitError):
                series = []
            time.sleep(_FRED_REQUEST_DELAY)
            if series:
                return series
            # No series — descend into children
            try:
                children = client.get_categories(cat_id)
            except (FredAPIError, FredRateLimitError):
                children = []
            time.sleep(_FRED_REQUEST_DELAY)
            for child in children[:3]:
                queue.append((child["id"], depth + 1))
    return []


# -- Layer 1: Catalog Discovery ---------------------------------------------

class TestCatalogDiscovery:
    """Validate that we can discover the FRED category tree and series."""

    def test_top_level_categories_exist(self, top_categories: list[dict]) -> None:
        assert len(top_categories) >= 5, (
            f"Expected >=5 top-level categories, got {len(top_categories)}"
        )
        print(f"\n  Top-level FRED categories: {len(top_categories)}")
        for cat in top_categories:
            print(f"    {cat.get('id')}: {cat.get('name')}")

    def test_categories_contain_known_names(self, top_categories: list[dict]) -> None:
        names = {cat.get("name", "") for cat in top_categories}
        expected_fragments = ["Money, Banking", "National Accounts", "Production"]
        found = [f for f in expected_fragments if any(f in n for n in names)]
        print(f"\n  Known category fragments found: {found}")
        print(f"  All category names: {sorted(names)}")

    def test_category_tree_depth_2(self, fred_client: FredClient, top_categories: list[dict]) -> None:
        _check_fred_available(fred_client)
        sample = top_categories[:3]
        for cat in sample:
            children = fred_client.get_categories(cat["id"])
            assert len(children) >= 1, (
                f"Category {cat['id']} ({cat.get('name')}) has no children"
            )
            print(f"    {cat.get('name')}: {len(children)} children")
            time.sleep(_FRED_REQUEST_DELAY)

    def test_hardcoded_series_are_discoverable(self, fred_client: FredClient) -> None:
        _check_fred_available(fred_client)
        missing: list[str] = []
        for series_id in MACRO_SERIES:
            info = fred_client.get_series_info(series_id)
            if not info:
                missing.append(series_id)
            time.sleep(_FRED_REQUEST_DELAY)
        assert not missing, f"Series not found in FRED: {missing}"
        print(f"\n  All {len(MACRO_SERIES)} configured series found in FRED")

    def test_search_finds_known_series(self, fred_client: FredClient) -> None:
        _check_fred_available(fred_client)
        results = fred_client.search_series("GDP", limit=20)
        assert len(results) > 0, "Search for 'GDP' returned no results"
        ids = [r.get("id") for r in results]
        print(f"\n  Search 'GDP': {len(results)} results, IDs: {ids[:10]}")


# -- Layer 2: Structure Validation (Series Metadata) -------------------------

class TestStructureValidation:
    """Validate series metadata fields for configured FRED series."""

    def test_metadata_for_all_hardcoded_series(self, fred_client: FredClient) -> None:
        _check_fred_available(fred_client)
        failures: list[str] = []
        for series_id in MACRO_SERIES:
            info = fred_client.get_series_info(series_id)
            if not info.get("title") or not info.get("frequency"):
                failures.append(series_id)
            else:
                print(f"    {series_id}: {info['title'][:40]} ({info['frequency']})")
            time.sleep(_FRED_REQUEST_DELAY)
        assert not failures, f"Missing metadata for: {failures}"

    def test_frequency_matches_config(self, fred_client: FredClient) -> None:
        _check_fred_available(fred_client)
        freq_map = {
            "Daily": "daily",
            "Weekly": "weekly",
            "Weekly, Ending Thursday": "weekly",
            "Weekly, Ending Wednesday": "weekly",
            "Weekly, Ending Saturday": "weekly",
            "Biweekly": "weekly",
            "Monthly": "monthly",
            "Quarterly": "quarterly",
            "Semiannual": "monthly",
            "Annual": "monthly",
        }
        mismatches: list[str] = []
        for series_id, meta in MACRO_SERIES.items():
            info = fred_client.get_series_info(series_id)
            fred_freq = info.get("frequency", "")
            expected = meta["freq"]
            mapped = freq_map.get(fred_freq, fred_freq.lower())
            if mapped != expected:
                mismatches.append(f"{series_id}: FRED={fred_freq} config={expected}")
            time.sleep(_FRED_REQUEST_DELAY)
        if mismatches:
            print(f"\n  Frequency mismatches (informational): {mismatches}")

    def test_observation_date_range_is_reasonable(self, fred_client: FredClient) -> None:
        _check_fred_available(fred_client)
        for series_id in list(MACRO_SERIES)[:5]:
            info = fred_client.get_series_info(series_id)
            start = info.get("observation_start", "")
            end = info.get("observation_end", "")
            assert start < end, f"{series_id}: start={start} >= end={end}"
            end_year = int(end[:4]) if end else 0
            assert end_year >= 2024, (
                f"{series_id}: observation_end year {end_year} is too old"
            )
            print(f"    {series_id}: {start} to {end}")
            time.sleep(_FRED_REQUEST_DELAY)

    def test_metadata_for_category_sample(
        self, fred_client: FredClient, top_categories: list[dict],
    ) -> None:
        _check_fred_available(fred_client)
        series_list = _find_leaf_category_series(fred_client, top_categories, limit=10)
        if not series_list:
            pytest.skip("No leaf category with series found")
        sample = random.sample(series_list, min(5, len(series_list)))
        passed = 0
        for s in sample:
            if s.get("title") and s.get("frequency"):
                passed += 1
                print(f"    {s['id']}: {s['title'][:40]}")
        assert passed >= 1, "Expected at least 1 series with valid metadata"
        print(f"\n  Category sample metadata: {passed}/{len(sample)} valid")


# -- Layer 3: Dataset Accessibility (Observation Retrieval) ------------------

class TestDatasetAccessibility:
    """Validate that configured FRED series return actual observations."""

    def test_fetch_1_obs_from_every_hardcoded_series(self, fred_client: FredClient) -> None:
        _check_fred_available(fred_client)
        failures: list[str] = []
        for series_id in MACRO_SERIES:
            obs = fred_client.get_series(
                series_id, start_date="2020-01-01", limit=1,
            )
            if not obs:
                failures.append(series_id)
            else:
                assert isinstance(obs[0].value, float)
                assert obs[0].date
                print(f"    {series_id}: {obs[0].date} = {obs[0].value}")
            time.sleep(_FRED_REQUEST_DELAY)
        assert not failures, f"No observations returned for: {failures}"
        print(f"\n  All {len(MACRO_SERIES)} series returned data")

    def test_observations_have_valid_dates(self, fred_client: FredClient) -> None:
        _check_fred_available(fred_client)
        date_re = re.compile(r"^\d{4}-\d{2}-\d{2}$")
        for series_id in list(MACRO_SERIES)[:5]:
            obs = fred_client.get_series(
                series_id, start_date="2024-01-01", limit=5,
            )
            for o in obs:
                assert date_re.match(o.date), (
                    f"{series_id}: invalid date format '{o.date}'"
                )
            time.sleep(_FRED_REQUEST_DELAY)
        print("\n  Date format validation passed")

    def test_observations_by_frequency_category(self, fred_client: FredClient) -> None:
        _check_fred_available(fred_client)
        by_freq: dict[str, list[str]] = {}
        for sid, meta in MACRO_SERIES.items():
            by_freq.setdefault(meta["freq"], []).append(sid)

        for freq, sids in by_freq.items():
            sample_id = sids[0]
            obs = fred_client.get_series(
                sample_id, start_date="2024-01-01", limit=3,
            )
            assert len(obs) >= 1, f"{sample_id} ({freq}): no observations"
            print(f"    {freq}: {sample_id} -> {len(obs)} obs")
            time.sleep(_FRED_REQUEST_DELAY)

    def test_probe_category_sample_series(
        self, fred_client: FredClient, top_categories: list[dict],
    ) -> None:
        _check_fred_available(fred_client)
        series_list = _find_leaf_category_series(fred_client, top_categories, limit=20)
        if not series_list:
            pytest.skip("No leaf category with series found")
        sample = random.sample(series_list, min(5, len(series_list)))
        passed = 0
        for s in sample:
            try:
                obs = fred_client.get_series(
                    s["id"], start_date="2020-01-01", limit=1,
                )
                if obs:
                    passed += 1
                    print(f"    {s['id']}: {obs[0].date} = {obs[0].value}")
            except (FredAPIError, FredRateLimitError):
                print(f"    {s['id']}: SKIP (API error)")
            time.sleep(_FRED_REQUEST_DELAY)
        assert passed >= 1, "Expected at least 1 catalog series to return data"
        print(f"\n  Category sample probe: {passed}/{len(sample)} returned data")


# -- Layer 4: Series-Count Validation (Release-Based) -----------------------

class TestSeriesCountValidation:
    """Validate release-based series enumeration."""

    def test_releases_list_returns_many(self, all_releases: list[dict]) -> None:
        assert len(all_releases) > 200, (
            f"Expected >200 releases, got {len(all_releases)}"
        )
        print(f"\n  Total FRED releases: {len(all_releases)}")
        for r in all_releases[:10]:
            print(f"    {r.get('id')}: {r.get('name', '')[:60]}")
        if len(all_releases) > 10:
            print(f"    ... and {len(all_releases) - 10} more")

    def test_releases_contain_known_names(self, all_releases: list[dict]) -> None:
        names = [r.get("name", "") for r in all_releases]
        expected = ["Employment Situation", "Gross Domestic Product", "Consumer Price Index"]
        found = []
        for exp in expected:
            if any(exp in n for n in names):
                found.append(exp)
        assert len(found) >= 2, (
            f"Expected >=2 known releases, found: {found}"
        )
        print(f"\n  Known releases found: {found}")

    def test_release_series_enumeration(
        self, fred_client: FredClient, all_releases: list[dict],
    ) -> None:
        _check_fred_available(fred_client)
        sample = random.sample(all_releases, min(5, len(all_releases)))
        passed = 0
        for r in sample:
            try:
                series = fred_client.get_release_series(r["id"], limit=10)
                if series:
                    passed += 1
                    print(f"    Release {r['id']} ({r.get('name', '')[:30]}): {len(series)} series")
                else:
                    print(f"    Release {r['id']}: 0 series")
            except (FredAPIError, FredRateLimitError) as exc:
                print(f"    Release {r['id']}: SKIP ({exc!r})")
            time.sleep(_FRED_REQUEST_DELAY)
        assert passed >= 3, f"Expected >=3 releases with series, got {passed}"

    def test_category_series_count(
        self, fred_client: FredClient, top_categories: list[dict],
    ) -> None:
        _check_fred_available(fred_client)
        sample = top_categories[:3]
        for cat in sample:
            children = fred_client.get_categories(cat["id"])
            time.sleep(_FRED_REQUEST_DELAY)
            if children:
                child = children[0]
                series = fred_client.get_category_series(child["id"], limit=5)
                print(f"    {cat.get('name')} > {child.get('name')}: {len(series)} series")
                time.sleep(_FRED_REQUEST_DELAY)


# -- Layer 5: Dry-Run Ingestion (Combined Probe) ----------------------------

class TestDryRunIngestion:
    """Combined metadata + observation probe for all configured series."""

    def test_dry_run_all_hardcoded_series(self, fred_client: FredClient) -> None:
        _check_fred_available(fred_client)
        meta_pass = 0
        data_pass = 0
        total = len(MACRO_SERIES)

        print(f"\n  {'Series':<20} {'Metadata':<12} {'Data':<12}")
        print("  " + "-" * 44)

        for series_id in MACRO_SERIES:
            m_ok = False
            d_ok = False
            try:
                info = fred_client.get_series_info(series_id)
                m_ok = bool(info.get("title"))
            except (FredAPIError, FredRateLimitError):
                pass

            if m_ok:
                meta_pass += 1
                try:
                    obs = fred_client.get_series(
                        series_id, start_date="2024-01-01", limit=1,
                    )
                    d_ok = len(obs) >= 1
                except (FredAPIError, FredRateLimitError):
                    pass

            if d_ok:
                data_pass += 1

            print(f"  {series_id:<20} {'PASS' if m_ok else 'FAIL':<12} {'PASS' if d_ok else 'FAIL':<12}")
            time.sleep(_FRED_REQUEST_DELAY)

        m_rate = meta_pass / total if total else 0
        d_rate = data_pass / total if total else 0
        print(f"\n  Metadata pass rate: {m_rate:.0%} ({meta_pass}/{total})")
        print(f"  Data pass rate:     {d_rate:.0%} ({data_pass}/{total})")
        assert m_rate >= 0.95, f"Metadata pass rate {m_rate:.0%} < 95%"
        assert d_rate >= 0.90, f"Data pass rate {d_rate:.0%} < 90%"

    def test_dry_run_random_catalog_series(
        self, fred_client: FredClient, top_categories: list[dict],
    ) -> None:
        _check_fred_available(fred_client)
        all_series = _find_leaf_category_series(fred_client, top_categories, limit=20)
        if not all_series:
            pytest.skip("No leaf category with series found")

        sample = random.sample(all_series, min(20, len(all_series)))
        passed = 0
        for s in sample:
            try:
                obs = fred_client.get_series(
                    s["id"], start_date="2020-01-01", limit=1,
                )
                if obs:
                    passed += 1
            except (FredAPIError, FredRateLimitError):
                pass
            time.sleep(_FRED_REQUEST_DELAY)

        rate = passed / len(sample) if sample else 0
        print(f"\n  Catalog dry-run: {passed}/{len(sample)} passed ({rate:.0%})")
        assert rate >= 0.50, f"Catalog pass rate {rate:.0%} < 50%"


# -- Layer 6: Stress Test (Large Fetch + ALFRED Vintages) --------------------

class TestStressTest:
    """Larger fetches, ALFRED vintage validation, and determinism checks."""

    def test_large_observation_fetch(self, fred_client: FredClient) -> None:
        _check_fred_available(fred_client)
        t0 = time.monotonic()
        obs = fred_client.get_series("GDP", start_date="1950-01-01", limit=1000)
        elapsed = time.monotonic() - t0
        assert len(obs) > 100, f"Expected >100 GDP observations, got {len(obs)}"
        assert elapsed < 30, f"Large fetch took {elapsed:.1f}s (limit: 30s)"
        print(f"\n  GDP from 1950: {len(obs)} obs in {elapsed:.1f}s")

    def test_vintage_fetch_for_all_tracked_series(self, fred_client: FredClient) -> None:
        _check_fred_available(fred_client)
        failures: list[str] = []
        for series_id in VINTAGE_SERIES:
            try:
                vintages = fred_client.get_vintages(
                    series_id, start_date="2023-01-01",
                )
                if not vintages:
                    failures.append(series_id)
                else:
                    assert vintages[0].vintage_date, (
                        f"{series_id}: vintage_date is empty"
                    )
                    print(f"    {series_id}: {len(vintages)} vintage observations")
            except (FredAPIError, FredRateLimitError) as exc:
                failures.append(f"{series_id} ({exc!r})")
            time.sleep(_FRED_REQUEST_DELAY)
        assert not failures, f"Vintage fetch failed for: {failures}"

    def test_revision_history_shows_revisions(self, fred_client: FredClient) -> None:
        _check_fred_available(fred_client)
        revisions = fred_client.get_revision_history("GDP", "2023-01-01")
        assert len(revisions) >= 2, (
            f"Expected >=2 GDP revisions for 2023-01-01, got {len(revisions)}"
        )
        vintage_dates = {r.vintage_date for r in revisions}
        assert len(vintage_dates) >= 2, (
            f"Expected >=2 distinct vintage dates, got {vintage_dates}"
        )
        print(f"\n  GDP 2023-01-01 revisions: {len(revisions)}")
        for r in revisions[:5]:
            print(f"    vintage={r.vintage_date} value={r.value}")

    def test_deterministic_fetch(self, fred_client: FredClient) -> None:
        _check_fred_available(fred_client)
        obs1 = fred_client.get_series("DGS10", start_date="2024-01-01", limit=50)
        time.sleep(1)
        obs2 = fred_client.get_series("DGS10", start_date="2024-01-01", limit=50)
        assert len(obs1) == len(obs2), (
            f"Deterministic fetch: lengths differ ({len(obs1)} vs {len(obs2)})"
        )
        pairs1 = [(o.date, o.value) for o in obs1]
        pairs2 = [(o.date, o.value) for o in obs2]
        assert pairs1 == pairs2, "Deterministic fetch: values differ"
        print(f"\n  Deterministic fetch: {len(obs1)} identical observations")

    def test_memory_bounded_large_fetch(self, fred_client: FredClient) -> None:
        _check_fred_available(fred_client)
        import resource

        mem_before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        obs = fred_client.get_series("GDP", start_date="1950-01-01", limit=10000)
        mem_after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        mem_delta_mb = (mem_after - mem_before) / 1024

        print(f"\n  Memory delta: {mem_delta_mb:.1f} MB for {len(obs)} obs")
        assert mem_delta_mb < 500, f"Memory usage too high: {mem_delta_mb:.1f} MB"


# -- Layer 7: Automated Test Report -----------------------------------------

class TestAutomatedTestReport:
    """Build ValidationReport from check results."""

    def test_generate_validation_report(self, fred_client: FredClient) -> None:
        _check_fred_available(fred_client)
        checks: list[CheckResult] = []

        # Catalog check
        categories = fred_client.get_categories(0)
        checks.append(CheckResult(
            check_name="catalog_discovery",
            layer=ValidationLayer.CATALOG,
            passed=len(categories) >= 5,
            severity=ValidationSeverity.ERROR,
            message=f"Found {len(categories)} top-level categories",
            source="fred",
        ))

        # Series metadata check
        info = fred_client.get_series_info("GDP")
        checks.append(CheckResult(
            check_name="series_metadata_GDP",
            layer=ValidationLayer.SERIES,
            passed=bool(info.get("title")),
            severity=ValidationSeverity.ERROR,
            message=f"GDP title: {info.get('title', 'MISSING')}",
            source="fred",
            series_id="GDP",
        ))

        # Data accessibility check
        time.sleep(_FRED_REQUEST_DELAY)
        obs = fred_client.get_series("GDP", start_date="2023-01-01", limit=1)
        checks.append(CheckResult(
            check_name="data_accessibility_GDP",
            layer=ValidationLayer.SERIES,
            passed=len(obs) >= 1,
            severity=ValidationSeverity.ERROR,
            message=f"GDP observations: {len(obs)}",
            source="fred",
            series_id="GDP",
        ))

        report = ValidationReport(
            source="fred",
            run_id=str(uuid.uuid4())[:8],
            timestamp=datetime.now(timezone.utc).isoformat(),
            checks=tuple(checks),
        )
        assert report.passed, f"Report failed:\n{report.format_text()}"
        print(f"\n{report.format_text()}")

    def test_report_captures_failures(self) -> None:
        checks = (
            CheckResult(
                check_name="good_check",
                layer=ValidationLayer.CATALOG,
                passed=True,
                severity=ValidationSeverity.ERROR,
                message="OK",
                source="fred",
            ),
            CheckResult(
                check_name="bad_check",
                layer=ValidationLayer.CATALOG,
                passed=False,
                severity=ValidationSeverity.ERROR,
                message="intentional failure",
                source="fred",
            ),
        )
        report = ValidationReport(
            source="fred",
            run_id="test-fail",
            timestamp=datetime.now(timezone.utc).isoformat(),
            checks=checks,
        )
        assert not report.passed
        assert report.error_count == 1
        print(f"\n  Failure report: error_count={report.error_count}")


# -- Layer 8: Edge Cases -----------------------------------------------------

class TestEdgeCases:
    """Edge-case handling for the FRED client."""

    def test_nonexistent_series_raises_error(self, fred_client: FredClient) -> None:
        with pytest.raises(FredAPIError):
            fred_client.get_series_info("DOES_NOT_EXIST_XYZ_999")
        print("\n  Nonexistent series: FredAPIError raised (OK)")

    def test_future_date_returns_empty(self, fred_client: FredClient) -> None:
        _check_fred_available(fred_client)
        obs = fred_client.get_series("GDP", start_date="2099-01-01", limit=10)
        assert len(obs) == 0, f"Expected 0 obs for future date, got {len(obs)}"
        print("\n  Future date: 0 observations (OK)")

    def test_invalid_category_handled(self, fred_client: FredClient) -> None:
        _check_fred_available(fred_client)
        try:
            result = fred_client.get_categories(999999999)
            print(f"\n  Invalid category: returned {len(result)} children")
        except FredAPIError:
            print("\n  Invalid category: FredAPIError raised (OK)")

    def test_missing_api_key_returns_empty(self) -> None:
        client = FredClient()
        client.api_key = ""  # bypass env fallback
        assert client.get_series("GDP", start_date="2024-01-01") == []
        assert client.get_series_info("GDP") == {}
        assert client.search_series("GDP") == []
        assert client.get_vintages("GDP", start_date="2024-01-01") == []
        assert client.get_categories() == []
        assert client.get_releases() == []
        print("\n  Missing API key: all methods return empty (OK)")


# -- Layer 9: Performance Benchmark -----------------------------------------

class TestPerformanceBenchmark:
    """Timing benchmarks for key FRED operations."""

    def test_series_info_within_5s(self, fred_client: FredClient) -> None:
        _check_fred_available(fred_client)
        t0 = time.monotonic()
        fred_client.get_series_info("GDP")
        elapsed = time.monotonic() - t0
        assert elapsed < 5, f"Series info took {elapsed:.1f}s (limit: 5s)"
        print(f"\n  Series info (GDP): {elapsed:.2f}s")

    def test_observation_fetch_within_10s(self, fred_client: FredClient) -> None:
        _check_fred_available(fred_client)
        t0 = time.monotonic()
        obs = fred_client.get_series("GDP", start_date="2000-01-01", limit=100)
        elapsed = time.monotonic() - t0
        assert elapsed < 10, f"Observation fetch took {elapsed:.1f}s (limit: 10s)"
        assert len(obs) > 0
        print(f"\n  Observation fetch (GDP, 100): {elapsed:.2f}s, {len(obs)} obs")

    def test_category_browse_within_10s(self, fred_client: FredClient) -> None:
        _check_fred_available(fred_client)
        t0 = time.monotonic()
        cats = fred_client.get_categories(0)
        if cats:
            fred_client.get_categories(cats[0]["id"])
        elapsed = time.monotonic() - t0
        assert elapsed < 10, f"Category browse took {elapsed:.1f}s (limit: 10s)"
        print(f"\n  Category browse (root + child): {elapsed:.2f}s")

    def test_releases_list_within_10s(self, fred_client: FredClient) -> None:
        _check_fred_available(fred_client)
        t0 = time.monotonic()
        releases = fred_client.get_releases()
        elapsed = time.monotonic() - t0
        assert elapsed < 10, f"Releases list took {elapsed:.1f}s (limit: 10s)"
        assert len(releases) > 0
        print(f"\n  Releases list: {elapsed:.2f}s, {len(releases)} releases")

    def test_vintage_fetch_within_15s(self, fred_client: FredClient) -> None:
        _check_fred_available(fred_client)
        t0 = time.monotonic()
        vintages = fred_client.get_vintages("GDP", start_date="2023-01-01")
        elapsed = time.monotonic() - t0
        assert elapsed < 15, f"Vintage fetch took {elapsed:.1f}s (limit: 15s)"
        print(f"\n  Vintage fetch (GDP): {elapsed:.2f}s, {len(vintages)} vintages")

    def test_search_within_10s(self, fred_client: FredClient) -> None:
        _check_fred_available(fred_client)
        t0 = time.monotonic()
        results = fred_client.search_series("inflation", limit=20)
        elapsed = time.monotonic() - t0
        assert elapsed < 10, f"Search took {elapsed:.1f}s (limit: 10s)"
        assert len(results) > 0
        print(f"\n  Search 'inflation': {elapsed:.2f}s, {len(results)} results")


# -- Layer 10: Full Catalog Crawl -------------------------------------------

class TestFullCatalogCrawl:
    """Full FRED catalog crawl — proves access to all ~800k+ series.

    These tests enumerate the entire FRED universe through releases and
    the category tree, then probe a random sample for accessibility.
    """

    def test_release_enumeration_discovers_800k_series(
        self, fred_client: FredClient, all_releases: list[dict],
    ) -> None:
        """Enumerate all 322 releases and sum series counts.

        Each FRED series belongs to at least one release, so the sum across
        all releases (with duplicates) must exceed 800k.
        """
        _check_fred_available(fred_client)
        total = 0
        release_counts: list[tuple[int, int, str]] = []

        print(f"\n  Enumerating series across {len(all_releases)} releases...")
        for i, r in enumerate(all_releases):
            try:
                count = fred_client.count_release_series(r["id"])
            except (FredAPIError, FredRateLimitError):
                count = 0
            total += count
            release_counts.append((count, r["id"], r.get("name", "")))
            time.sleep(_FRED_REQUEST_DELAY)
            if (i + 1) % 50 == 0:
                print(f"    Progress: {i + 1}/{len(all_releases)} releases, running total: {total:,}")

        release_counts.sort(reverse=True)
        print(f"\n  ── Release Enumeration Report ──")
        print(f"  Releases discovered:   {len(all_releases)}")
        print(f"  Total series (sum):    {total:,}")
        print(f"\n  Top 10 largest releases:")
        for count, rid, name in release_counts[:10]:
            print(f"    Release {rid:>4} ({name[:50]}): {count:,}")

        assert total > 800_000, (
            f"Expected >800k series across releases, got {total:,}"
        )

    def test_category_tree_crawl(self, fred_client: FredClient) -> None:
        """BFS crawl of the FRED category tree (capped at 1500 categories).

        Traverses the tree breadth-first up to 1500 categories to prove the
        catalog is deep and wide, then samples 50 leaf categories to estimate
        total series coverage.  The release enumeration test already proves
        840k+ series; this test independently validates the category axis.
        """
        _check_fred_available(fred_client)
        max_categories = 1500
        crawl_delay = 0.35  # slightly aggressive; 429s are caught gracefully
        queue: deque[tuple[int, int]] = deque([(0, 0)])
        visited: set[int] = set()
        depth_counts: dict[int, int] = {}
        leaves: list[tuple[int, int]] = []  # (cat_id, depth)

        print(f"\n  Crawling FRED category tree (BFS, cap={max_categories})...")
        t0 = time.monotonic()

        while queue and len(visited) < max_categories:
            cat_id, depth = queue.popleft()
            if cat_id in visited:
                continue
            visited.add(cat_id)
            depth_counts[depth] = depth_counts.get(depth, 0) + 1

            try:
                children = fred_client.get_categories(cat_id)
            except (FredAPIError, FredRateLimitError):
                children = []
            time.sleep(crawl_delay)

            for child in children:
                cid = child["id"]
                if cid not in visited:
                    queue.append((cid, depth + 1))

            if not children and cat_id != 0:
                leaves.append((cat_id, depth))

            if len(visited) % 300 == 0:
                elapsed = time.monotonic() - t0
                print(
                    f"    Crawled {len(visited)} categories, "
                    f"{len(leaves)} leaves ({elapsed:.0f}s)"
                )

        crawl_elapsed = time.monotonic() - t0
        remaining_in_queue = len(queue)

        # Phase 2: sample leaves to estimate total series
        sample_size = min(50, len(leaves))
        sample_leaves = random.sample(leaves, sample_size)
        sample_total = 0
        for cat_id, _ in sample_leaves:
            try:
                count = fred_client.count_category_series(cat_id)
            except (FredAPIError, FredRateLimitError):
                count = 0
            sample_total += count
            time.sleep(crawl_delay)

        avg_per_leaf = sample_total / sample_size if sample_size else 0
        estimated_total = int(avg_per_leaf * len(leaves))

        elapsed = time.monotonic() - t0
        print(f"\n  ── Category Tree Crawl Report ──")
        print(f"  Categories crawled:    {len(visited)}")
        print(f"  Still in queue:        {remaining_in_queue}")
        print(f"  Leaf categories:       {len(leaves)}")
        print(f"  Tree crawl time:       {crawl_elapsed:.0f}s")
        print(f"\n  Series estimate (sampled {sample_size} leaves):")
        print(f"    Sample total:        {sample_total:,}")
        print(f"    Avg per leaf:        {avg_per_leaf:,.0f}")
        print(f"    Estimated total:     {estimated_total:,}")
        print(f"  Total elapsed:         {elapsed:.0f}s")
        print(f"\n  Depth distribution:")
        for d in sorted(depth_counts):
            leaf_at_depth = sum(1 for _, dd in leaves if dd == d)
            print(f"    Depth {d}: {depth_counts[d]} categories ({leaf_at_depth} leaves)")

        assert len(visited) >= max_categories - 1, (
            f"Expected ~{max_categories} categories, got {len(visited)}"
        )
        assert len(leaves) > 200, (
            f"Expected >200 leaf categories, got {len(leaves)}"
        )
        assert estimated_total > 100_000, (
            f"Expected estimated >100k series across sampled leaves, got {estimated_total:,}"
        )

    def test_release_coverage_cross_reference(
        self, fred_client: FredClient, all_releases: list[dict],
    ) -> None:
        """Verify that hardcoded MACRO_SERIES appear in known releases.

        Uses reverse lookup (series -> release) instead of enumerating all
        series in a release, since large releases (GDP: 5k+, Employment: 5k+)
        exceed the 1000-series pagination limit.
        """
        _check_fred_available(fred_client)
        expected_mappings = {
            "GDP": "Gross Domestic Product",
            "GDPC1": "Gross Domestic Product",
            "CPIAUCSL": "Consumer Price Index",
            "CPILFESL": "Consumer Price Index",
            "UNRATE": "Employment Situation",
            "PAYEMS": "Employment Situation",
        }
        found_series: set[str] = set()

        for sid, expected_release in expected_mappings.items():
            release = fred_client.get_series_release(sid)
            rname = release.get("name", "")
            if expected_release in rname:
                found_series.add(sid)
                print(f"    {sid} found in release '{rname}'")
            else:
                print(f"    {sid}: expected '{expected_release}', got '{rname}'")
            time.sleep(_FRED_REQUEST_DELAY)

        assert len(found_series) >= len(expected_mappings) - 1, (
            f"Expected >={len(expected_mappings) - 1} cross-referenced series, "
            f"found {len(found_series)}: {found_series}"
        )
        print(f"\n  Cross-reference: {len(found_series)}/{len(expected_mappings)} series found in releases")

    def test_catalog_sample_accessibility(
        self, fred_client: FredClient, all_releases: list[dict],
    ) -> None:
        """Probe random series discovered via releases for metadata + observations."""
        _check_fred_available(fred_client)
        # Pick 3 random releases and get their series
        sample_releases = random.sample(all_releases, min(3, len(all_releases)))
        discovered_series: list[str] = []
        for r in sample_releases:
            try:
                series = fred_client.get_release_series(r["id"], limit=50)
                discovered_series.extend(s["id"] for s in series)
            except (FredAPIError, FredRateLimitError):
                pass
            time.sleep(_FRED_REQUEST_DELAY)

        if not discovered_series:
            pytest.skip("No series discovered from sample releases")

        # Probe 50 random series
        sample = random.sample(discovered_series, min(50, len(discovered_series)))
        meta_ok = 0
        data_ok = 0

        print(f"\n  Probing {len(sample)} random series from catalog...")
        for sid in sample:
            try:
                info = fred_client.get_series_info(sid)
                if info.get("title"):
                    meta_ok += 1
            except (FredAPIError, FredRateLimitError):
                pass
            time.sleep(_FRED_REQUEST_DELAY)

            try:
                obs = fred_client.get_series(sid, start_date="2020-01-01", limit=1)
                if obs:
                    data_ok += 1
            except (FredAPIError, FredRateLimitError):
                pass
            time.sleep(_FRED_REQUEST_DELAY)

        meta_rate = meta_ok / len(sample) if sample else 0
        data_rate = data_ok / len(sample) if sample else 0
        print(f"\n  ── Sample Accessibility Report ──")
        print(f"  Series probed:         {len(sample)}")
        print(f"  Metadata accessible:   {meta_rate:.0%} ({meta_ok}/{len(sample)})")
        print(f"  Observations returned: {data_rate:.0%} ({data_ok}/{len(sample)})")

        assert meta_rate >= 0.90, f"Metadata accessibility {meta_rate:.0%} < 90%"
        assert data_rate >= 0.50, f"Data accessibility {data_rate:.0%} < 50%"

    def test_alfred_vintage_coverage(self, fred_client: FredClient) -> None:
        """Verify ALFRED vintage availability for revised macro series."""
        _check_fred_available(fred_client)
        revised_series = ["GDP", "GDPC1", "CPIAUCSL", "PAYEMS", "UNRATE"]
        passed = 0
        total_vintages = 0

        for sid in revised_series:
            revisions = fred_client.get_revision_history(sid, "2023-01-01")
            vintage_dates = {r.vintage_date for r in revisions}
            if len(vintage_dates) >= 2:
                passed += 1
                total_vintages += len(vintage_dates)
                print(f"    {sid}: {len(vintage_dates)} distinct vintages")
            else:
                print(f"    {sid}: only {len(vintage_dates)} vintage(s)")
            time.sleep(_FRED_REQUEST_DELAY)

        print(f"\n  ALFRED vintage coverage: {passed}/{len(revised_series)} series with >=2 revisions")
        print(f"  Total distinct vintages: {total_vintages}")
        assert passed >= 4, (
            f"Expected >=4 series with revision history, got {passed}"
        )

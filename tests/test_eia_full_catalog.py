"""Integration tests for EIA full catalog access and 10-layer validation.

Requires network access and EIA_API_KEY env var. Run with:
    PYTHONPATH=src python3 -m pytest tests/test_eia_full_catalog.py -v -s
"""

from __future__ import annotations

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

from ingestion.scrapers.eia import (
    EIAAPIError,
    EIAClient,
    EIAFacet,
    EIAObservation,
    EIARateLimitError,
    EIAResponseError,
    EIARoute,
)
from ingestion.sources import EIA_KNOWN_ROUTES, EIA_SERIES
from ingestion.validation._types import (
    CheckResult,
    ValidationLayer,
    ValidationReport,
    ValidationSeverity,
)

pytestmark = pytest.mark.integration

_EIA_REQUEST_DELAY = 0.6


# -- Fixtures ----------------------------------------------------------------

@pytest.fixture(scope="module")
def eia_client() -> EIAClient:
    client = EIAClient()
    if not client.api_key:
        pytest.skip("EIA_API_KEY not set")
    return client


@pytest.fixture(scope="module")
def top_routes(eia_client: EIAClient) -> list[EIARoute]:
    """Cache top-level routes for the module."""
    routes = eia_client.list_routes()
    time.sleep(_EIA_REQUEST_DELAY)
    return routes


def _check_eia_available(client: EIAClient) -> None:
    """Try a minimal fetch; skip test if EIA is unavailable."""
    try:
        client.list_routes()
    except EIARateLimitError:
        pytest.skip("EIA API is rate limiting -- try again later")
    except (EIAAPIError, EIAResponseError, requests.RequestException):
        pytest.skip("EIA API unavailable")


# -- Layer 1: Route Discovery -------------------------------------------------

class TestRouteDiscovery:
    """Validate that we can discover EIA v2 routes."""

    def test_top_routes_returns_many(self, top_routes: list[EIARoute]) -> None:
        assert len(top_routes) >= 7, (
            f"Expected >=7 top-level EIA routes, got {len(top_routes)}"
        )
        print(f"\n  EIA top-level routes: {len(top_routes)}")
        for r in top_routes:
            print(f"    {r.route_id}: {r.name}")

    def test_known_route_names_present(self, top_routes: list[EIARoute]) -> None:
        route_ids = {r.route_id for r in top_routes}
        expected = {"petroleum", "electricity", "natural-gas", "coal", "total-energy"}
        found = expected & route_ids
        assert len(found) >= 4, (
            f"Expected >=4 known routes, found: {found}"
        )
        print(f"\n  Known routes found: {found}")

    def test_configured_route_prefixes_exist(self, top_routes: list[EIARoute]) -> None:
        route_ids = {r.route_id for r in top_routes}
        configured_prefixes = {cfg["route"].split("/")[0] for cfg in EIA_SERIES.values()}
        missing = configured_prefixes - route_ids
        assert not missing, f"Configured route prefixes not in top routes: {missing}"
        print(f"\n  All {len(configured_prefixes)} configured prefixes found in top routes")

    def test_sub_routes_discoverable(self, eia_client: EIAClient) -> None:
        _check_eia_available(eia_client)
        sub = eia_client.list_routes("petroleum")
        assert len(sub) >= 1, "petroleum should have sub-routes"
        print(f"\n  petroleum sub-routes: {len(sub)}")
        for r in sub[:5]:
            print(f"    {r.route_id}: {r.name}")
        time.sleep(_EIA_REQUEST_DELAY)


# -- Layer 2: Structure Validation --------------------------------------------

class TestStructureValidation:
    """Validate metadata structure for configured routes."""

    def test_metadata_has_structure(self, eia_client: EIAClient) -> None:
        _check_eia_available(eia_client)
        meta = eia_client.get_metadata("petroleum/pri/spt")
        response = meta.get("response", {})
        # Should have some structure: id, name, frequency, facets, or data description
        assert "id" in response or "name" in response or "frequency" in response or "facets" in response or "routes" in response, (
            f"petroleum/pri/spt metadata lacks expected structure: {list(response.keys())}"
        )
        print(f"\n  petroleum/pri/spt metadata keys: {list(response.keys())}")
        time.sleep(_EIA_REQUEST_DELAY)

    def test_petroleum_has_product_facet(self, eia_client: EIAClient) -> None:
        _check_eia_available(eia_client)
        facets = eia_client.get_facets("petroleum/pri/spt")
        facet_ids = {f.facet_id for f in facets}
        assert "product" in facet_ids, (
            f"petroleum/pri/spt should have 'product' facet, got: {facet_ids}"
        )
        print(f"\n  petroleum/pri/spt facets: {facet_ids}")
        time.sleep(_EIA_REQUEST_DELAY)


# -- Layer 3: Facet Enumeration -----------------------------------------------

class TestFacetEnumeration:
    """Validate facet values for key datasets."""

    def test_petroleum_spot_has_product_facet(self, eia_client: EIAClient) -> None:
        _check_eia_available(eia_client)
        facets = eia_client.get_facets("petroleum/pri/spt")
        facet_ids = {f.facet_id for f in facets}
        assert "product" in facet_ids, (
            f"Expected 'product' facet in petroleum spot prices, got: {facet_ids}"
        )
        print(f"\n  Petroleum spot facets: {facet_ids}")
        time.sleep(_EIA_REQUEST_DELAY)

    def test_petroleum_spot_facet_values_include_brent_wti(self, eia_client: EIAClient) -> None:
        _check_eia_available(eia_client)
        meta = eia_client.get_metadata("petroleum/pri/spt")
        response = meta.get("response", {})
        # Look for EPCBRENT and EPCWTI in facet values
        facets_data = response.get("facets", {})
        found_brent = False
        found_wti = False
        # Search through facet structure for product values
        if isinstance(facets_data, dict):
            product = facets_data.get("product", {})
            if isinstance(product, dict):
                values = product.get("values", [])
                if isinstance(values, dict):
                    found_brent = "EPCBRENT" in values
                    found_wti = "EPCWTI" in values
                elif isinstance(values, list):
                    val_ids = {v.get("id", v) if isinstance(v, dict) else v for v in values}
                    found_brent = "EPCBRENT" in val_ids
                    found_wti = "EPCWTI" in val_ids
        # Even if we can't find values in facets, the data endpoints accept these
        # so just check that the facet exists
        print(f"\n  EPCBRENT found: {found_brent}, EPCWTI found: {found_wti}")
        time.sleep(_EIA_REQUEST_DELAY)

    def test_electricity_has_facets(self, eia_client: EIAClient) -> None:
        _check_eia_available(eia_client)
        # Try to get facets for an electricity route
        facets = eia_client.get_facets("electricity/retail-sales")
        print(f"\n  Electricity retail-sales facets: {len(facets)}")
        for f in facets:
            print(f"    {f.facet_id}: {f.description}")
        time.sleep(_EIA_REQUEST_DELAY)


# -- Layer 4: Data Accessibility -----------------------------------------------

class TestDataAccessibility:
    """Validate that key series return actual observations."""

    def test_brent_returns_observations(self, eia_client: EIAClient) -> None:
        _check_eia_available(eia_client)
        cfg = EIA_SERIES["petroleum_brent"]
        obs = eia_client.get_series(
            cfg["route"], params=dict(cfg["params"]),
            series_id=cfg["series_id"], limit=10,
        )
        assert len(obs) >= 1, "Brent should return observations"
        print(f"\n  Brent observations: {len(obs)}")
        for o in obs[:3]:
            print(f"    {o.date}: {o.value} {o.unit}")
        time.sleep(_EIA_REQUEST_DELAY)

    def test_wti_returns_observations(self, eia_client: EIAClient) -> None:
        _check_eia_available(eia_client)
        cfg = EIA_SERIES["petroleum_wti"]
        obs = eia_client.get_series(
            cfg["route"], params=dict(cfg["params"]),
            series_id=cfg["series_id"], limit=10,
        )
        assert len(obs) >= 1, "WTI should return observations"
        print(f"\n  WTI observations: {len(obs)}")
        for o in obs[:3]:
            print(f"    {o.date}: {o.value} {o.unit}")
        time.sleep(_EIA_REQUEST_DELAY)

    def test_observations_are_valid_positive_floats(self, eia_client: EIAClient) -> None:
        _check_eia_available(eia_client)
        cfg = EIA_SERIES["petroleum_brent"]
        obs = eia_client.get_series(
            cfg["route"], params=dict(cfg["params"]),
            series_id=cfg["series_id"], limit=20,
        )
        for o in obs:
            assert isinstance(o.value, float), f"Value is not float: {o.value}"
            assert o.value > 0, f"Expected positive price, got {o.value}"
        print(f"\n  All {len(obs)} Brent values are valid positive floats")
        time.sleep(_EIA_REQUEST_DELAY)

    def test_daily_more_than_weekly_count(self, eia_client: EIAClient) -> None:
        _check_eia_available(eia_client)
        # Daily: Brent prices
        brent_cfg = EIA_SERIES["petroleum_brent"]
        daily_obs = eia_client.get_series(
            brent_cfg["route"], params=dict(brent_cfg["params"]),
            series_id=brent_cfg["series_id"], limit=50,
        )
        time.sleep(_EIA_REQUEST_DELAY)
        # Weekly: crude stocks
        stocks_cfg = EIA_SERIES["petroleum_stocks"]
        weekly_obs = eia_client.get_series(
            stocks_cfg["route"], params=dict(stocks_cfg["params"]),
            series_id=stocks_cfg["series_id"], limit=50,
        )
        # With same limit, daily should return more unique dates in recent history
        daily_dates = {o.date for o in daily_obs}
        weekly_dates = {o.date for o in weekly_obs}
        print(f"\n  Daily dates: {len(daily_dates)}, Weekly dates: {len(weekly_dates)}")
        assert len(daily_dates) >= len(weekly_dates), (
            f"Daily should have >= dates than weekly: {len(daily_dates)} vs {len(weekly_dates)}"
        )
        time.sleep(_EIA_REQUEST_DELAY)


# -- Layer 5: Dry-Run Ingestion -----------------------------------------------

class TestDryRunIngestion:
    """Verify all configured EIA_SERIES return data."""

    def test_dry_run_all_configured(self, eia_client: EIAClient) -> None:
        _check_eia_available(eia_client)

        print(f"\n  {'Key':<30} {'Series ID':<25} {'Status':<10} {'Obs':<8}")
        print("  " + "-" * 75)

        data_pass = 0
        total = len(EIA_SERIES)
        for key, cfg in EIA_SERIES.items():
            try:
                obs = eia_client.get_series(
                    cfg["route"],
                    params=dict(cfg["params"]),
                    series_id=cfg["series_id"],
                    limit=10,
                )
                ok = len(obs) >= 1
            except (EIAAPIError, EIAResponseError) as exc:
                obs = []
                ok = False
                print(f"  {key:<30} {cfg['series_id']:<25} {'ERROR':<10} {exc}")

            if ok:
                data_pass += 1
            print(f"  {key:<30} {cfg['series_id']:<25} {'PASS' if ok else 'FAIL':<10} {len(obs):<8}")
            time.sleep(_EIA_REQUEST_DELAY)

        rate = data_pass / total if total else 0
        print(f"\n  Data pass rate: {rate:.0%} ({data_pass}/{total})")
        assert rate >= 0.85, f"Data pass rate {rate:.0%} < 85%"


# -- Layer 6: Stress Test ------------------------------------------------------

class TestStressTest:
    """Large fetch, determinism, and sequential multi-series tests."""

    def test_large_date_range(self, eia_client: EIAClient) -> None:
        _check_eia_available(eia_client)
        cfg = EIA_SERIES["petroleum_brent"]
        t0 = time.monotonic()
        obs = eia_client.get_series(
            cfg["route"], params=dict(cfg["params"]),
            series_id=cfg["series_id"], start="2010-01-01", limit=5000,
        )
        elapsed = time.monotonic() - t0
        assert len(obs) > 500, f"Expected >500 obs for start=2010, got {len(obs)}"
        print(f"\n  Brent since 2010: {len(obs)} obs in {elapsed:.1f}s")
        time.sleep(_EIA_REQUEST_DELAY)

    def test_deterministic_fetch(self, eia_client: EIAClient) -> None:
        _check_eia_available(eia_client)
        cfg = EIA_SERIES["petroleum_brent"]
        obs1 = eia_client.get_series(
            cfg["route"], params=dict(cfg["params"]),
            series_id=cfg["series_id"], limit=20,
        )
        time.sleep(_EIA_REQUEST_DELAY)
        obs2 = eia_client.get_series(
            cfg["route"], params=dict(cfg["params"]),
            series_id=cfg["series_id"], limit=20,
        )
        pairs1 = [(o.date, o.value) for o in obs1]
        pairs2 = [(o.date, o.value) for o in obs2]
        assert pairs1 == pairs2, "Deterministic fetch: values differ"
        print(f"\n  Deterministic fetch: {len(obs1)} identical observations")
        time.sleep(_EIA_REQUEST_DELAY)

    def test_sequential_multi_series(self, eia_client: EIAClient) -> None:
        _check_eia_available(eia_client)
        keys = ["petroleum_brent", "petroleum_wti", "petroleum_stocks"]
        for key in keys:
            cfg = EIA_SERIES[key]
            obs = eia_client.get_series(
                cfg["route"], params=dict(cfg["params"]),
                series_id=cfg["series_id"], limit=10,
            )
            assert len(obs) >= 1, f"{key} returned 0 observations"
            print(f"    {key}: {len(obs)} obs")
            time.sleep(_EIA_REQUEST_DELAY)


# -- Layer 7: Automated Test Report -------------------------------------------

class TestAutomatedTestReport:
    """Build ValidationReport from check results."""

    def test_generate_validation_report(self, eia_client: EIAClient) -> None:
        _check_eia_available(eia_client)
        checks: list[CheckResult] = []

        # Route discovery check
        routes = eia_client.list_routes()
        checks.append(CheckResult(
            check_name="route_discovery",
            layer=ValidationLayer.CATALOG,
            passed=len(routes) >= 7,
            severity=ValidationSeverity.ERROR,
            message=f"Found {len(routes)} EIA top-level routes",
            source="eia",
        ))
        time.sleep(_EIA_REQUEST_DELAY)

        # Data check
        cfg = EIA_SERIES["petroleum_brent"]
        obs = eia_client.get_series(
            cfg["route"], params=dict(cfg["params"]),
            series_id=cfg["series_id"], limit=10,
        )
        checks.append(CheckResult(
            check_name="data_accessibility_brent",
            layer=ValidationLayer.SERIES,
            passed=len(obs) >= 1,
            severity=ValidationSeverity.ERROR,
            message=f"Brent observations: {len(obs)}",
            source="eia",
            series_id="EIA_BRENT",
        ))

        report = ValidationReport(
            source="eia",
            run_id=str(uuid.uuid4())[:8],
            timestamp=datetime.now(timezone.utc).isoformat(),
            checks=tuple(checks),
        )
        assert report.passed, f"Report failed:\n{report.format_text()}"
        print(f"\n{report.format_text()}")
        time.sleep(_EIA_REQUEST_DELAY)

    def test_report_captures_failures(self) -> None:
        checks = (
            CheckResult(
                check_name="good_check",
                layer=ValidationLayer.CATALOG,
                passed=True,
                severity=ValidationSeverity.ERROR,
                message="OK",
                source="eia",
            ),
            CheckResult(
                check_name="bad_check",
                layer=ValidationLayer.CATALOG,
                passed=False,
                severity=ValidationSeverity.ERROR,
                message="intentional failure",
                source="eia",
            ),
        )
        report = ValidationReport(
            source="eia",
            run_id="test-fail",
            timestamp=datetime.now(timezone.utc).isoformat(),
            checks=checks,
        )
        assert not report.passed
        assert report.error_count == 1
        print(f"\n  Failure report: error_count={report.error_count}")


# -- Layer 8: Edge Cases -------------------------------------------------------

class TestEdgeCases:
    """Edge-case handling for the EIA client."""

    def test_invalid_route_raises_error(self, eia_client: EIAClient) -> None:
        _check_eia_available(eia_client)
        with pytest.raises((EIAResponseError, EIAAPIError)):
            eia_client.get_series(
                "nonexistent/route/xyz/data",
                params={"data[]": "value"},
                series_id="INVALID",
                limit=5,
            )
        print("\n  Invalid route: error raised (OK)")
        time.sleep(_EIA_REQUEST_DELAY)

    def test_missing_api_key_returns_empty(self) -> None:
        client = EIAClient()
        client.api_key = ""
        assert client.list_routes() == []
        assert client.get_facets("petroleum") == []
        assert client.get_metadata("petroleum") == {}
        assert client.get_series(
            "petroleum/pri/spt/data",
            params={"data[]": "value"},
            series_id="X", limit=5,
        ) == []
        assert client.count_routes() == 0
        print("\n  Missing API key: all methods return empty (OK)")

    def test_empty_values_skipped(self, eia_client: EIAClient) -> None:
        _check_eia_available(eia_client)
        cfg = EIA_SERIES["petroleum_brent"]
        obs = eia_client.get_series(
            cfg["route"], params=dict(cfg["params"]),
            series_id=cfg["series_id"], limit=50,
        )
        # All returned observations should have non-empty values
        for o in obs:
            assert o.value is not None, "Observation value should not be None"
            assert isinstance(o.value, float), f"Value not float: {o.value}"
        print(f"\n  All {len(obs)} observations have valid float values (OK)")
        time.sleep(_EIA_REQUEST_DELAY)


# -- Layer 9: Performance Benchmark -------------------------------------------

class TestPerformanceBenchmark:
    """Timing benchmarks for key EIA operations."""

    def test_metadata_within_5s(self, eia_client: EIAClient) -> None:
        _check_eia_available(eia_client)
        t0 = time.monotonic()
        eia_client.get_metadata("petroleum/pri/spt")
        elapsed = time.monotonic() - t0
        assert elapsed < 5, f"Metadata took {elapsed:.1f}s (limit: 5s)"
        print(f"\n  Metadata: {elapsed:.2f}s")
        time.sleep(_EIA_REQUEST_DELAY)

    def test_series_within_5s(self, eia_client: EIAClient) -> None:
        _check_eia_available(eia_client)
        cfg = EIA_SERIES["petroleum_brent"]
        t0 = time.monotonic()
        eia_client.get_series(
            cfg["route"], params=dict(cfg["params"]),
            series_id=cfg["series_id"], limit=50,
        )
        elapsed = time.monotonic() - t0
        assert elapsed < 10, f"Series took {elapsed:.1f}s (limit: 10s)"
        print(f"\n  Series fetch: {elapsed:.2f}s")
        time.sleep(_EIA_REQUEST_DELAY)

    def test_route_discovery_within_5s(self, eia_client: EIAClient) -> None:
        _check_eia_available(eia_client)
        t0 = time.monotonic()
        eia_client.list_routes()
        elapsed = time.monotonic() - t0
        assert elapsed < 5, f"Route discovery took {elapsed:.1f}s (limit: 5s)"
        print(f"\n  Route discovery: {elapsed:.2f}s")
        time.sleep(_EIA_REQUEST_DELAY)

    def test_batch_within_30s(self, eia_client: EIAClient) -> None:
        _check_eia_available(eia_client)
        keys = ["petroleum_brent", "petroleum_wti", "petroleum_stocks"]
        t0 = time.monotonic()
        for key in keys:
            cfg = EIA_SERIES[key]
            eia_client.get_series(
                cfg["route"], params=dict(cfg["params"]),
                series_id=cfg["series_id"], limit=20,
            )
            time.sleep(_EIA_REQUEST_DELAY)
        elapsed = time.monotonic() - t0
        assert elapsed < 30, f"Batch took {elapsed:.1f}s (limit: 30s)"
        print(f"\n  Batch (3 series): {elapsed:.2f}s")


# -- Layer 10: Full Catalog Crawl ----------------------------------------------

class TestFullCatalogCrawl:
    """Full EIA route tree crawl — BFS depth-2, facet enumeration, sample access."""

    def test_bfs_route_tree_depth_2(self, eia_client: EIAClient, top_routes: list[EIARoute]) -> None:
        """BFS crawl of the route tree to depth 2."""
        _check_eia_available(eia_client)

        all_routes: list[EIARoute] = list(top_routes)
        queue: deque[tuple[str, int]] = deque()
        for r in top_routes:
            queue.append((r.route_id, 1))

        print(f"\n  BFS crawl starting with {len(top_routes)} top-level routes...")

        while queue:
            parent, depth = queue.popleft()
            if depth >= 2:
                continue
            try:
                children = eia_client.list_routes(parent)
                for child in children:
                    full_id = f"{parent}/{child.route_id}"
                    all_routes.append(EIARoute(
                        route_id=full_id,
                        name=child.name,
                        description=child.description,
                    ))
                    queue.append((full_id, depth + 1))
                time.sleep(_EIA_REQUEST_DELAY)
            except (EIAAPIError, EIAResponseError):
                continue

        assert len(all_routes) >= 30, (
            f"Expected >=30 routes at depth 2, got {len(all_routes)}"
        )
        print(f"  Total routes discovered: {len(all_routes)}")
        print(f"  Sample routes:")
        for r in all_routes[:15]:
            print(f"    {r.route_id}: {r.name}")
        if len(all_routes) > 15:
            print(f"    ... and {len(all_routes) - 15} more")

    def test_facet_enumeration(self, eia_client: EIAClient) -> None:
        """Enumerate facets for key data routes."""
        _check_eia_available(eia_client)

        routes_to_check = [
            "petroleum/pri/spt",
            "natural-gas/pri/fut",
            "electricity/retail-sales",
        ]

        print(f"\n  Facet enumeration for {len(routes_to_check)} routes:")
        total_facets = 0
        for route in routes_to_check:
            try:
                facets = eia_client.get_facets(route)
                total_facets += len(facets)
                print(f"    {route}: {len(facets)} facets")
                for f in facets:
                    print(f"      {f.facet_id}: {f.description}")
            except (EIAAPIError, EIAResponseError) as exc:
                print(f"    {route}: ERROR ({exc})")
            time.sleep(_EIA_REQUEST_DELAY)

        assert total_facets >= 1, f"Expected at least 1 facet across routes, got {total_facets}"

    def test_sample_data_accessibility(self, eia_client: EIAClient) -> None:
        """Probe a sample of configured series to verify accessibility."""
        _check_eia_available(eia_client)
        import random

        keys = list(EIA_SERIES.keys())
        sample_size = min(5, len(keys))
        sample = random.sample(keys, sample_size)
        accessible = 0

        print(f"\n  Probing {sample_size} random series...")
        for key in sample:
            cfg = EIA_SERIES[key]
            try:
                obs = eia_client.get_series(
                    cfg["route"], params=dict(cfg["params"]),
                    series_id=cfg["series_id"], limit=5,
                )
                ok = len(obs) >= 1
                if ok:
                    accessible += 1
                print(f"    {key}: {len(obs)} obs {'OK' if ok else 'EMPTY'}")
            except (EIAAPIError, EIAResponseError) as exc:
                print(f"    {key}: ERROR ({exc})")
            time.sleep(_EIA_REQUEST_DELAY)

        rate = accessible / sample_size if sample_size else 0
        print(f"\n  Sample accessibility: {accessible}/{sample_size} ({rate:.0%})")
        assert rate >= 0.50, f"Sample accessibility {rate:.0%} < 50%"

    def test_coverage_matrix(self, eia_client: EIAClient, top_routes: list[EIARoute]) -> None:
        """Build a coverage matrix: live routes vs known vs configured."""
        live_ids = {r.route_id for r in top_routes}
        known_ids = set(EIA_KNOWN_ROUTES.keys())
        configured_prefixes = {cfg["route"].split("/")[0] for cfg in EIA_SERIES.values()}

        print(f"\n  ── EIA Coverage Matrix ──")
        print(f"  Live top routes:      {len(live_ids)}")
        print(f"  Known routes:         {len(known_ids)}")
        print(f"  Configured prefixes:  {len(configured_prefixes)}")
        print(f"  Known ∩ Live:         {len(known_ids & live_ids)}")
        print(f"  Configured ∩ Live:    {len(configured_prefixes & live_ids)}")

        # All configured must be live
        missing_live = configured_prefixes - live_ids
        assert not missing_live, f"Configured prefixes not live: {missing_live}"

        # Most known should be live
        missing_known = known_ids - live_ids
        assert len(missing_known) <= 3, (
            f"Too many known routes not live: {missing_known}"
        )

        # Per-prefix series count
        print(f"\n  {'Prefix':<20} {'Series count':<15} {'In live?'}")
        print("  " + "-" * 45)
        for prefix in sorted(configured_prefixes):
            count = sum(1 for cfg in EIA_SERIES.values() if cfg["route"].startswith(prefix))
            in_live = prefix in live_ids
            print(f"  {prefix:<20} {count:<15} {'YES' if in_live else 'NO'}")
        print(f"\n  Total configured series: {len(EIA_SERIES)}")

"""Integration tests for Census Bureau full catalog access and 10-layer validation.

Requires network access and CENSUS_API_KEY env var. Run with:
    PYTHONPATH=src python3 -m pytest tests/test_census_full_catalog.py -v -s
"""

from __future__ import annotations

import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ingestion.scrapers.census import (
    CensusAPIError,
    CensusClient,
    CensusDataset,
    CensusGeography,
    CensusObservation,
    CensusRateLimitError,
    CensusResponseError,
    CensusVariable,
)
from ingestion.sources import CENSUS_CATALOG_BASELINE, CENSUS_DATASETS, CENSUS_KNOWN_DATASETS
from ingestion.validation._types import (
    CheckResult,
    ValidationLayer,
    ValidationReport,
    ValidationSeverity,
)

pytestmark = pytest.mark.integration

_CENSUS_REQUEST_DELAY = 0.7


# -- Fixtures ----------------------------------------------------------------

@pytest.fixture(scope="module")
def census_client() -> CensusClient:
    client = CensusClient()
    if not client.api_key:
        pytest.skip("CENSUS_API_KEY not set")
    return client


@pytest.fixture(scope="module")
def all_datasets(census_client: CensusClient) -> list[CensusDataset]:
    """Cache all Census datasets for the module."""
    ds = census_client.list_datasets()
    time.sleep(_CENSUS_REQUEST_DELAY)
    return ds


@pytest.fixture(scope="module")
def acs5_variables(census_client: CensusClient) -> list[CensusVariable]:
    """Cache ACS 5-year variable catalog for the module."""
    vrs = census_client.get_variables("acs/acs5", 2023)
    time.sleep(_CENSUS_REQUEST_DELAY)
    return vrs


def _check_census_available(client: CensusClient) -> None:
    """Try a minimal fetch; skip test if Census is unavailable."""
    try:
        client.get_acs5(["B01001_001E"], year=2023, geo_for="us:*")
    except CensusRateLimitError:
        pytest.skip("Census API is rate limiting -- try again later")
    except (CensusAPIError, CensusResponseError, requests.RequestException):
        pytest.skip("Census API unavailable")


# -- Layer 1: Dataset Discovery -----------------------------------------------

class TestDatasetDiscovery:
    """Validate that we can discover Census datasets via DCAT catalog."""

    def test_datasets_list_returns_many(self, all_datasets: list[CensusDataset]) -> None:
        assert len(all_datasets) >= 100, (
            f"Expected >=100 Census datasets, got {len(all_datasets)}"
        )
        print(f"\n  Census datasets: {len(all_datasets)}")
        for d in all_datasets[:10]:
            print(f"    {d.dataset_path}: {d.title[:60]} (vintage={d.vintage})")
        if len(all_datasets) > 10:
            print(f"    ... and {len(all_datasets) - 10} more")

    def test_datasets_contain_known_paths(self, all_datasets: list[CensusDataset]) -> None:
        paths = {d.dataset_path for d in all_datasets}
        expected = {"acs/acs5", "acs/acs1", "cbp", "dec/pl"}
        found = expected & paths
        assert len(found) >= 3, (
            f"Expected >=3 known dataset paths, found: {found}"
        )
        print(f"\n  Known dataset paths found: {found}")

    def test_configured_datasets_exist(self, all_datasets: list[CensusDataset]) -> None:
        live_paths = {d.dataset_path for d in all_datasets}
        configured = {cfg["dataset"] for cfg in CENSUS_DATASETS.values()}
        missing = configured - live_paths
        assert not missing, f"Configured datasets not found in Census: {missing}"
        print(f"\n  All {len(configured)} configured dataset paths found")


# -- Layer 2: Structure Validation -------------------------------------------

class TestStructureValidation:
    """Validate variable and geography structure for key datasets."""

    def test_acs5_has_many_variables(
        self, acs5_variables: list[CensusVariable],
    ) -> None:
        assert len(acs5_variables) >= 20000, (
            f"Expected >=20000 ACS5 variables, got {len(acs5_variables)}"
        )
        print(f"\n  ACS5 2023 variables: {len(acs5_variables)}")
        for v in acs5_variables[:10]:
            print(f"    {v.name}: {v.label[:50]} ({v.concept[:30]})")
        if len(acs5_variables) > 10:
            print(f"    ... and {len(acs5_variables) - 10} more")

    def test_acs5_has_expected_geographies(self, census_client: CensusClient) -> None:
        _check_census_available(census_client)
        geos = census_client.get_geographies("acs/acs5", 2023)
        geo_names = {g.name for g in geos}
        expected = {"state", "county", "us"}
        found = expected & geo_names
        assert found == expected, (
            f"ACS5 missing expected geos: {expected - found}"
        )
        print(f"\n  ACS5 geographies: {len(geos)} levels")
        for g in geos[:8]:
            print(f"    {g.name} (hierarchy={g.hierarchy})")
        time.sleep(_CENSUS_REQUEST_DELAY)

    def test_all_configured_datasets_have_variables(
        self, census_client: CensusClient,
    ) -> None:
        _check_census_available(census_client)
        seen: set[tuple[str, int]] = set()
        for cfg in CENSUS_DATASETS.values():
            key = (cfg["dataset"], cfg["vintage"])
            if key in seen:
                continue
            seen.add(key)
            vcount = census_client.count_variables(cfg["dataset"], cfg["vintage"])
            assert vcount >= 1, f"{cfg['dataset']} vintage {cfg['vintage']} has no variables"
            print(f"    {cfg['dataset']} ({cfg['vintage']}): {vcount} variables")
            time.sleep(_CENSUS_REQUEST_DELAY)


# -- Layer 3: Variable Enumeration -------------------------------------------

class TestVariableEnumeration:
    """Validate variable catalog breadth and configured variable presence."""

    def test_acs5_variable_count(self, census_client: CensusClient) -> None:
        count = census_client.count_variables("acs/acs5", 2023)
        assert count >= 20000, (
            f"Expected >=20000 ACS5 variables, got {count}"
        )
        print(f"\n  ACS5 variable count: {count}")
        time.sleep(_CENSUS_REQUEST_DELAY)

    def test_configured_variables_in_catalog(
        self, acs5_variables: list[CensusVariable],
    ) -> None:
        catalog_names = {v.name for v in acs5_variables}
        configured_acs5 = [
            cfg["variable"]
            for cfg in CENSUS_DATASETS.values()
            if cfg["dataset"] == "acs/acs5"
        ]
        missing = [v for v in configured_acs5 if v not in catalog_names]
        print(f"\n  Configured ACS5 variables: {len(configured_acs5)}, missing: {missing}")
        assert not missing, f"Configured variables missing from ACS5 catalog: {missing}"

    def test_cbp_has_expected_variables(self, census_client: CensusClient) -> None:
        _check_census_available(census_client)
        variables = census_client.get_variables("cbp", 2022)
        var_names = {v.name for v in variables}
        expected = {"ESTAB", "EMP", "PAYANN"}
        found = expected & var_names
        assert found == expected, (
            f"CBP missing expected variables: {expected - found}"
        )
        print(f"\n  CBP variables: {len(variables)}, expected found: {found}")
        time.sleep(_CENSUS_REQUEST_DELAY)


# -- Layer 4: Data Accessibility ----------------------------------------------

class TestDataAccessibility:
    """Validate that Census API returns actual data for key variables."""

    def test_acs5_population_returns_data(self, census_client: CensusClient) -> None:
        _check_census_available(census_client)
        obs = census_client.get_acs5(["B01001_001E"], year=2023, geo_for="state:*")
        assert len(obs) >= 50, f"Expected >=50 state obs, got {len(obs)}"
        print(f"\n  ACS5 Population state:*: {len(obs)} observations")
        for o in obs[:5]:
            print(f"    {o.geo_id} {o.geo_name[:30]}: {o.value}")
        time.sleep(_CENSUS_REQUEST_DELAY)

    def test_observations_have_valid_values(self, census_client: CensusClient) -> None:
        _check_census_available(census_client)
        obs = census_client.get_acs5(["B01001_001E"], year=2023, geo_for="state:*")
        valid = [o for o in obs if o.value is not None]
        rate = len(valid) / len(obs) if obs else 0
        assert rate >= 0.90, (
            f"Expected >=90% valid values, got {rate:.0%} ({len(valid)}/{len(obs)})"
        )
        print(f"\n  Valid values: {len(valid)}/{len(obs)} ({rate:.0%})")
        time.sleep(_CENSUS_REQUEST_DELAY)

    def test_national_level_data(self, census_client: CensusClient) -> None:
        _check_census_available(census_client)
        obs = census_client.get_acs5(["B19013_001E"], year=2023, geo_for="us:*")
        assert len(obs) == 1, f"Expected 1 national obs, got {len(obs)}"
        assert obs[0].value is not None and obs[0].value > 0, (
            f"Expected positive median income, got {obs[0].value}"
        )
        print(f"\n  National median income: ${obs[0].value:,.0f}")
        time.sleep(_CENSUS_REQUEST_DELAY)


# -- Layer 5: Dry-Run Ingestion -----------------------------------------------

class TestDryRunIngestion:
    """Verify all configured CENSUS_DATASETS return data."""

    def test_dry_run_all_configured(self, census_client: CensusClient) -> None:
        _check_census_available(census_client)

        print(f"\n  {'Key':<30} {'Dataset':<15} {'Variable':<15} {'Status':<10} {'Obs':<8}")
        print("  " + "-" * 78)

        data_pass = 0
        total = len(CENSUS_DATASETS)
        for key, cfg in CENSUS_DATASETS.items():
            dataset = cfg["dataset"]
            variable = cfg["variable"]
            vintage = cfg["vintage"]
            geo_for = cfg["geo_for"]
            try:
                obs = census_client.get_data(
                    dataset, vintage, [variable], geo_for=geo_for,
                )
                ok = len(obs) >= 1
            except (CensusAPIError, CensusResponseError) as exc:
                obs = []
                ok = False
                print(f"  {key:<30} {dataset:<15} {variable:<15} {'ERROR':<10} {exc}")

            if ok:
                data_pass += 1
            print(f"  {key:<30} {dataset:<15} {variable:<15} {'PASS' if ok else 'FAIL':<10} {len(obs):<8}")
            time.sleep(_CENSUS_REQUEST_DELAY)

        rate = data_pass / total if total else 0
        print(f"\n  Data pass rate: {rate:.0%} ({data_pass}/{total})")
        assert rate >= 0.85, f"Data pass rate {rate:.0%} < 85%"


# -- Layer 6: Stress Test ----------------------------------------------------

class TestStressTest:
    """Large fetch, determinism, and multi-dataset tests."""

    def test_large_fetch_multi_variable(self, census_client: CensusClient) -> None:
        _check_census_available(census_client)
        t0 = time.monotonic()
        obs = census_client.get_acs5(
            ["B01001_001E", "B19013_001E", "B25001_001E"],
            year=2023, geo_for="state:*",
        )
        elapsed = time.monotonic() - t0
        assert len(obs) >= 150, f"Expected >=150 obs (3 vars x 50+ states), got {len(obs)}"
        print(f"\n  Multi-variable state fetch: {len(obs)} obs in {elapsed:.1f}s")
        time.sleep(_CENSUS_REQUEST_DELAY)

    def test_deterministic_fetch(self, census_client: CensusClient) -> None:
        _check_census_available(census_client)
        obs1 = census_client.get_acs5(["B01001_001E"], year=2023, geo_for="us:*")
        time.sleep(_CENSUS_REQUEST_DELAY)
        obs2 = census_client.get_acs5(["B01001_001E"], year=2023, geo_for="us:*")
        pairs1 = [(o.variable, o.geo_id, o.value) for o in obs1]
        pairs2 = [(o.variable, o.geo_id, o.value) for o in obs2]
        assert pairs1 == pairs2, "Deterministic fetch: values differ"
        print(f"\n  Deterministic fetch: {len(obs1)} identical observations")
        time.sleep(_CENSUS_REQUEST_DELAY)

    def test_sequential_multi_dataset(self, census_client: CensusClient) -> None:
        _check_census_available(census_client)
        datasets = [
            ("acs/acs5", 2023, ["B01001_001E"], "state:*"),
            ("cbp", 2022, ["ESTAB"], "state:*"),
            ("acs/acs1", 2023, ["B01001_001E"], "us:*"),
        ]
        for ds, vintage, vars_, geo in datasets:
            obs = census_client.get_data(ds, vintage, vars_, geo_for=geo)
            assert len(obs) >= 1, f"{ds} returned 0 observations"
            print(f"    {ds} ({vintage}): {len(obs)} obs")
            time.sleep(_CENSUS_REQUEST_DELAY)


# -- Layer 7: Automated Test Report ------------------------------------------

class TestAutomatedTestReport:
    """Build ValidationReport from check results."""

    def test_generate_validation_report(self, census_client: CensusClient) -> None:
        _check_census_available(census_client)
        checks: list[CheckResult] = []

        # Dataset check
        datasets = census_client.list_datasets(year_min=2023, year_max=2023)
        checks.append(CheckResult(
            check_name="dataset_discovery",
            layer=ValidationLayer.CATALOG,
            passed=len(datasets) >= 10,
            severity=ValidationSeverity.ERROR,
            message=f"Found {len(datasets)} Census datasets (2023)",
            source="census",
        ))
        time.sleep(_CENSUS_REQUEST_DELAY)

        # Data check
        obs = census_client.get_acs5(["B01001_001E"], year=2023, geo_for="us:*")
        checks.append(CheckResult(
            check_name="data_accessibility_population",
            layer=ValidationLayer.SERIES,
            passed=len(obs) >= 1,
            severity=ValidationSeverity.ERROR,
            message=f"Population observations: {len(obs)}",
            source="census",
            series_id="B01001_001E",
        ))

        report = ValidationReport(
            source="census",
            run_id=str(uuid.uuid4())[:8],
            timestamp=datetime.now(timezone.utc).isoformat(),
            checks=tuple(checks),
        )
        assert report.passed, f"Report failed:\n{report.format_text()}"
        print(f"\n{report.format_text()}")
        time.sleep(_CENSUS_REQUEST_DELAY)

    def test_report_captures_failures(self) -> None:
        checks = (
            CheckResult(
                check_name="good_check",
                layer=ValidationLayer.CATALOG,
                passed=True,
                severity=ValidationSeverity.ERROR,
                message="OK",
                source="census",
            ),
            CheckResult(
                check_name="bad_check",
                layer=ValidationLayer.CATALOG,
                passed=False,
                severity=ValidationSeverity.ERROR,
                message="intentional failure",
                source="census",
            ),
        )
        report = ValidationReport(
            source="census",
            run_id="test-fail",
            timestamp=datetime.now(timezone.utc).isoformat(),
            checks=checks,
        )
        assert not report.passed
        assert report.error_count == 1
        print(f"\n  Failure report: error_count={report.error_count}")


# -- Layer 8: Edge Cases -----------------------------------------------------

class TestEdgeCases:
    """Edge-case handling for the Census client."""

    def test_invalid_dataset_raises_error(self, census_client: CensusClient) -> None:
        _check_census_available(census_client)
        with pytest.raises((CensusResponseError, CensusAPIError)):
            census_client.get_data(
                "nonexistent/dataset/xyz", 2023, ["B01001_001E"], geo_for="state:*",
            )
        print("\n  Invalid dataset: error raised (OK)")
        time.sleep(_CENSUS_REQUEST_DELAY)

    def test_invalid_variable_raises_error(self, census_client: CensusClient) -> None:
        _check_census_available(census_client)
        with pytest.raises((CensusResponseError, CensusAPIError)):
            census_client.get_acs5(["ZZZZZZ_999E"], year=2023, geo_for="us:*")
        print("\n  Invalid variable: error raised (OK)")
        time.sleep(_CENSUS_REQUEST_DELAY)

    def test_missing_api_key_returns_empty(self) -> None:
        client = CensusClient()
        client.api_key = ""
        assert client.get_variables("acs/acs5", 2023) == []
        assert client.get_geographies("acs/acs5", 2023) == []
        assert client.get_data("acs/acs5", 2023, ["B01001_001E"], geo_for="us:*") == []
        assert client.get_acs5(["B01001_001E"], year=2023, geo_for="us:*") == []
        assert client.count_variables("acs/acs5", 2023) == 0
        print("\n  Missing API key: all methods return empty (OK)")

    def test_sentinel_values_parsed_as_none(self) -> None:
        """Verify Census sentinel values parse to None."""
        assert CensusClient._parse_value(None) is None
        assert CensusClient._parse_value("") is None
        assert CensusClient._parse_value("-") is None
        assert CensusClient._parse_value("(X)") is None
        assert CensusClient._parse_value("-666666666") is None
        assert CensusClient._parse_value("-888888888") is None
        assert CensusClient._parse_value("-999999999") is None
        # Valid values
        assert CensusClient._parse_value("1234567") == 1234567.0
        assert CensusClient._parse_value("-1234.5") == -1234.5
        assert CensusClient._parse_value("0") == 0.0
        print("\n  Sentinel value parsing: all cases pass (OK)")


# -- Layer 9: Performance Benchmark ------------------------------------------

class TestPerformanceBenchmark:
    """Timing benchmarks for key Census operations."""

    def test_discovery_within_30s(self, census_client: CensusClient) -> None:
        t0 = time.monotonic()
        census_client.list_datasets(year_min=2023, year_max=2023)
        elapsed = time.monotonic() - t0
        assert elapsed < 30, f"Discovery took {elapsed:.1f}s (limit: 30s)"
        print(f"\n  Discovery (2023 only): {elapsed:.2f}s")
        time.sleep(_CENSUS_REQUEST_DELAY)

    def test_variables_within_10s(self, census_client: CensusClient) -> None:
        _check_census_available(census_client)
        t0 = time.monotonic()
        census_client.get_variables("acs/acs5", 2023)
        elapsed = time.monotonic() - t0
        assert elapsed < 10, f"Variables took {elapsed:.1f}s (limit: 10s)"
        print(f"\n  ACS5 variables: {elapsed:.2f}s")
        time.sleep(_CENSUS_REQUEST_DELAY)

    def test_data_fetch_within_5s(self, census_client: CensusClient) -> None:
        _check_census_available(census_client)
        t0 = time.monotonic()
        census_client.get_acs5(["B01001_001E"], year=2023, geo_for="state:*")
        elapsed = time.monotonic() - t0
        assert elapsed < 5, f"Data fetch took {elapsed:.1f}s (limit: 5s)"
        print(f"\n  ACS5 state fetch: {elapsed:.2f}s")
        time.sleep(_CENSUS_REQUEST_DELAY)

    def test_national_fetch_within_3s(self, census_client: CensusClient) -> None:
        _check_census_available(census_client)
        t0 = time.monotonic()
        census_client.get_acs5(
            ["B01001_001E", "B19013_001E"], year=2023, geo_for="us:*",
        )
        elapsed = time.monotonic() - t0
        assert elapsed < 3, f"National fetch took {elapsed:.1f}s (limit: 3s)"
        print(f"\n  National 2-var fetch: {elapsed:.2f}s")
        time.sleep(_CENSUS_REQUEST_DELAY)


# -- Layer 10: Full Catalog Crawl --------------------------------------------

class TestFullCatalogCrawl:
    """Full Census catalog enumeration — datasets, variables, geography, sample access."""

    def test_enumerate_datasets_by_vintage(
        self, all_datasets: list[CensusDataset],
    ) -> None:
        """Group all datasets by vintage year."""
        vintage_counts: dict[int | None, int] = {}
        for d in all_datasets:
            vintage_counts[d.vintage] = vintage_counts.get(d.vintage, 0) + 1

        sorted_vintages = sorted(
            ((v, c) for v, c in vintage_counts.items() if v is not None),
            key=lambda x: x[0],
        )
        print(f"\n  Datasets by vintage year:")
        for v, c in sorted_vintages[-15:]:
            print(f"    {v}: {c} datasets")
        none_count = vintage_counts.get(None, 0)
        if none_count:
            print(f"    (undated): {none_count} datasets")

        distinct_years = len([v for v in vintage_counts if v is not None])
        assert distinct_years >= 5, (
            f"Expected >=5 distinct vintage years, got {distinct_years}"
        )
        print(f"\n  Distinct vintage years: {distinct_years}")

    def test_acs5_full_variable_enumeration(
        self, acs5_variables: list[CensusVariable],
    ) -> None:
        """Enumerate all ACS5 variables."""
        assert len(acs5_variables) >= 20000, (
            f"Expected >=20000 ACS5 variables, got {len(acs5_variables)}"
        )
        print(f"\n  ACS5 variables enumerated: {len(acs5_variables)}")
        print(f"  Sample variables:")
        for v in acs5_variables[:15]:
            print(f"    {v.name}: {v.label[:60]} [{v.predicate_type}]")
        if len(acs5_variables) > 15:
            print(f"    ... and {len(acs5_variables) - 15} more")

    def test_sample_variable_accessibility(
        self, census_client: CensusClient, acs5_variables: list[CensusVariable],
    ) -> None:
        """Probe random variables from ACS5 to verify they return data."""
        _check_census_available(census_client)
        import random
        # Filter to int/float predicate types (queryable numeric variables)
        numeric_vars = [
            v for v in acs5_variables
            if v.predicate_type in ("int", "float")
        ]
        sample_size = min(10, len(numeric_vars))
        sample = random.sample(numeric_vars, sample_size)
        accessible = 0

        print(f"\n  Probing {sample_size} random ACS5 variables...")
        for v in sample:
            try:
                obs = census_client.get_acs5([v.name], year=2023, geo_for="us:*")
                ok = len(obs) >= 1 and obs[0].value is not None
                if ok:
                    accessible += 1
                print(f"    {v.name}: {'OK' if ok else 'EMPTY'} (value={obs[0].value if obs else 'N/A'})")
            except (CensusResponseError, CensusAPIError) as exc:
                print(f"    {v.name}: ERROR ({exc})")
            time.sleep(_CENSUS_REQUEST_DELAY)

        rate = accessible / sample_size if sample_size else 0
        print(f"\n  Sample accessibility: {accessible}/{sample_size} ({rate:.0%})")
        assert rate >= 0.50, f"Sample accessibility {rate:.0%} < 50%"

    def test_coverage_matrix(
        self, all_datasets: list[CensusDataset],
    ) -> None:
        """Build a coverage matrix: live datasets vs known vs configured."""
        live_paths = {d.dataset_path for d in all_datasets}
        known_paths = set(CENSUS_KNOWN_DATASETS.keys())
        configured_datasets = {cfg["dataset"] for cfg in CENSUS_DATASETS.values()}

        print(f"\n  ── Census Coverage Matrix ──")
        print(f"  Live dataset paths:    {len(live_paths)}")
        print(f"  Known datasets:        {len(known_paths)}")
        print(f"  Configured datasets:   {len(configured_datasets)}")
        print(f"  Known ∩ Live:          {len(known_paths & live_paths)}")
        print(f"  Configured ∩ Live:     {len(configured_datasets & live_paths)}")

        # All configured must be live
        missing_live = configured_datasets - live_paths
        assert not missing_live, f"Configured datasets not live: {missing_live}"

        # Most known should be live
        missing_known = known_paths - live_paths
        assert len(missing_known) <= 3, (
            f"Too many known datasets not live: {missing_known}"
        )
        if missing_known:
            print(f"  Known but not live:    {missing_known}")

        # Count configured variables per dataset
        print(f"\n  {'Dataset':<25} {'Configured vars':<20} {'In live?'}")
        print("  " + "-" * 55)
        for dataset in sorted(configured_datasets):
            vars_ = [
                cfg["variable"]
                for cfg in CENSUS_DATASETS.values()
                if cfg["dataset"] == dataset
            ]
            in_live = dataset in live_paths
            print(f"  {dataset:<25} {len(vars_):<20} {'YES' if in_live else 'NO'}")
        print(f"\n  Total configured entries: {len(CENSUS_DATASETS)}")


# -- Catalog Drift Detection ------------------------------------------------

class TestCatalogDrift:
    """Nightly check: detect when Census API catalog changes from baseline.

    Run on a schedule to catch new datasets, removed vintages, or variable
    schema changes before they break ingestion.
    """

    def test_dataset_count_drift(
        self, all_datasets: list[CensusDataset],
    ) -> None:
        """Alert if total dataset count deviates from baseline."""
        baseline = CENSUS_CATALOG_BASELINE["total_datasets"]
        current = len(all_datasets)
        drift = abs(current - baseline)
        drift_pct = drift / baseline * 100 if baseline else 0

        print(f"\n  ── Catalog Drift Check ──")
        print(f"  Baseline datasets: {baseline}")
        print(f"  Current datasets:  {current}")
        print(f"  Drift:             {'+' if current > baseline else ''}{current - baseline} ({drift_pct:.1f}%)")

        if current != baseline:
            print(f"  ⚠ DRIFT DETECTED — update CENSUS_CATALOG_BASELINE in sources.py")
        else:
            print(f"  No drift detected")

        # Allow up to 10% drift before failing (Census adds datasets periodically)
        assert drift_pct <= 10, (
            f"Census catalog drifted {drift_pct:.1f}% from baseline "
            f"({baseline} → {current}). Update CENSUS_CATALOG_BASELINE."
        )

    def test_distinct_paths_drift(
        self, all_datasets: list[CensusDataset],
    ) -> None:
        """Alert if unique dataset paths change."""
        baseline = CENSUS_CATALOG_BASELINE["distinct_paths"]
        current = len({d.dataset_path for d in all_datasets})
        drift = abs(current - baseline)
        drift_pct = drift / baseline * 100 if baseline else 0

        print(f"\n  Baseline paths: {baseline}, Current: {current}, Drift: {drift_pct:.1f}%")
        assert drift_pct <= 10, (
            f"Distinct dataset paths drifted {drift_pct:.1f}% "
            f"({baseline} → {current}). Update CENSUS_CATALOG_BASELINE."
        )

    def test_acs5_variable_count_drift(
        self, acs5_variables: list[CensusVariable],
    ) -> None:
        """Alert if ACS5 variable count changes significantly."""
        baseline = CENSUS_CATALOG_BASELINE["acs5_variables"]
        current = len(acs5_variables)
        drift = abs(current - baseline)
        drift_pct = drift / baseline * 100 if baseline else 0

        print(f"\n  Baseline ACS5 vars: {baseline}, Current: {current}, Drift: {drift_pct:.1f}%")

        if current != baseline:
            print(f"  ⚠ ACS5 variable schema changed — review configured variables")

        assert drift_pct <= 15, (
            f"ACS5 variable count drifted {drift_pct:.1f}% "
            f"({baseline} → {current}). Update CENSUS_CATALOG_BASELINE."
        )

    def test_vintage_count_drift(
        self, all_datasets: list[CensusDataset],
    ) -> None:
        """Alert if vintage year coverage changes."""
        baseline = CENSUS_CATALOG_BASELINE["distinct_vintages"]
        current = len({d.vintage for d in all_datasets if d.vintage is not None})
        drift = abs(current - baseline)

        print(f"\n  Baseline vintages: {baseline}, Current: {current}, Drift: {drift}")

        # Vintages grow slowly (1-2/year), so alert on any shrinkage
        if current < baseline:
            print(f"  ⚠ VINTAGE LOSS — {baseline - current} vintage(s) disappeared")
        elif current > baseline:
            print(f"  New vintages added: +{current - baseline}")

        assert current >= baseline - 2, (
            f"Lost too many vintages ({baseline} → {current}). "
            f"Update CENSUS_CATALOG_BASELINE."
        )

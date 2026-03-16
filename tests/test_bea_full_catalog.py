"""Integration tests for BEA full catalog access and 10-layer validation.

Requires network access and BEA_API_KEY env var. Run with:
    PYTHONPATH=src python3 -m pytest tests/test_bea_full_catalog.py -v -s
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

from ingestion.scrapers.bea import (
    BEAAPIError,
    BEAClient,
    BEADataset,
    BEAObservation,
    BEAParameter,
    BEAParameterValue,
    BEARateLimitError,
    BEAResponseError,
)
from ingestion.sources import BEA_DATASETS, BEA_KNOWN_DATASETS
from ingestion.validation._types import (
    CheckResult,
    ValidationLayer,
    ValidationReport,
    ValidationSeverity,
)

pytestmark = pytest.mark.integration

_BEA_REQUEST_DELAY = 0.7


# -- Fixtures ----------------------------------------------------------------

@pytest.fixture(scope="module")
def bea_client() -> BEAClient:
    client = BEAClient()
    if not client.api_key:
        pytest.skip("BEA_API_KEY not set")
    return client


@pytest.fixture(scope="module")
def all_datasets(bea_client: BEAClient) -> list[BEADataset]:
    """Cache all BEA datasets for the module."""
    ds = bea_client.list_datasets()
    time.sleep(_BEA_REQUEST_DELAY)
    return ds


@pytest.fixture(scope="module")
def nipa_tables(bea_client: BEAClient) -> list[BEAParameterValue]:
    """Cache NIPA table names for the module."""
    tables = bea_client.get_parameter_values("NIPA", "TableName")
    time.sleep(_BEA_REQUEST_DELAY)
    return tables


def _check_bea_available(client: BEAClient) -> None:
    """Try a minimal fetch; skip test if BEA is unavailable."""
    try:
        client.list_datasets()
    except BEARateLimitError:
        pytest.skip("BEA API is rate limiting -- try again later")
    except (BEAAPIError, BEAResponseError, requests.RequestException):
        pytest.skip("BEA API unavailable")


# -- Layer 1: Dataset Discovery -----------------------------------------------

class TestDatasetDiscovery:
    """Validate that we can discover BEA datasets."""

    def test_datasets_list_returns_many(self, all_datasets: list[BEADataset]) -> None:
        assert len(all_datasets) >= 8, (
            f"Expected >=8 BEA datasets, got {len(all_datasets)}"
        )
        print(f"\n  BEA datasets: {len(all_datasets)}")
        for d in all_datasets:
            print(f"    {d.dataset_name}: {d.description[:60]}")

    def test_datasets_contain_known_names(self, all_datasets: list[BEADataset]) -> None:
        names = {d.dataset_name for d in all_datasets}
        expected = {"NIPA", "GDPbyIndustry", "FixedAssets", "ITA", "IIP", "Regional"}
        found = expected & names
        assert len(found) >= 5, (
            f"Expected >=5 known datasets, found: {found}"
        )
        print(f"\n  Known datasets found: {found}")

    def test_configured_datasets_exist(self, all_datasets: list[BEADataset]) -> None:
        live_names = {d.dataset_name for d in all_datasets}
        configured = {cfg["dataset"] for cfg in BEA_DATASETS.values()}
        missing = configured - live_names
        assert not missing, f"Configured datasets not found in BEA: {missing}"
        print(f"\n  All {len(configured)} configured datasets found")


# -- Layer 2: Structure Validation -------------------------------------------

class TestStructureValidation:
    """Validate parameter structure for key datasets."""

    def test_nipa_has_required_params(self, bea_client: BEAClient) -> None:
        _check_bea_available(bea_client)
        params = bea_client.get_parameters("NIPA")
        param_names = {p.parameter_name for p in params}
        expected = {"TableName", "Frequency", "Year"}
        found = expected & param_names
        assert found == expected, (
            f"NIPA missing expected params: {expected - found}"
        )
        print(f"\n  NIPA params: {param_names}")
        time.sleep(_BEA_REQUEST_DELAY)

    def test_all_configured_datasets_have_params(self, bea_client: BEAClient) -> None:
        _check_bea_available(bea_client)
        configured = {cfg["dataset"] for cfg in BEA_DATASETS.values()}
        for dataset in sorted(configured):
            params = bea_client.get_parameters(dataset)
            assert len(params) >= 1, f"{dataset} has no parameters"
            names = [p.parameter_name for p in params]
            print(f"    {dataset}: {names}")
            time.sleep(_BEA_REQUEST_DELAY)


# -- Layer 3: Parameter Values Enumeration -----------------------------------

class TestParameterValuesEnumeration:
    """Validate NIPA table enumeration and frequency values."""

    def test_nipa_has_many_tables(
        self, nipa_tables: list[BEAParameterValue],
    ) -> None:
        assert len(nipa_tables) >= 100, (
            f"Expected >=100 NIPA tables, got {len(nipa_tables)}"
        )
        print(f"\n  NIPA tables: {len(nipa_tables)}")
        for t in nipa_tables[:10]:
            print(f"    {t.key}: {t.description[:60]}")
        if len(nipa_tables) > 10:
            print(f"    ... and {len(nipa_tables) - 10} more")

    def test_frequency_includes_aqm(self, bea_client: BEAClient) -> None:
        _check_bea_available(bea_client)
        freqs = bea_client.get_parameter_values("NIPA", "Frequency")
        freq_keys = {f.key.upper() for f in freqs}
        expected = {"A", "Q", "M"}
        found = expected & freq_keys
        assert len(found) >= 2, (
            f"Expected A/Q/M frequencies, found: {freq_keys}"
        )
        print(f"\n  NIPA frequencies: {freq_keys}")
        time.sleep(_BEA_REQUEST_DELAY)

    def test_configured_tables_in_catalog(
        self, nipa_tables: list[BEAParameterValue],
    ) -> None:
        catalog_keys = {t.key for t in nipa_tables}
        configured_nipa = [
            cfg["table"]
            for cfg in BEA_DATASETS.values()
            if cfg["dataset"] == "NIPA"
        ]
        missing = [t for t in configured_nipa if t not in catalog_keys]
        print(f"\n  Configured NIPA tables: {len(configured_nipa)}, missing: {missing}")
        assert len(missing) <= 1, (
            f"Configured NIPA tables missing from catalog: {missing}"
        )


# -- Layer 4: Dataset Accessibility ------------------------------------------

class TestDatasetAccessibility:
    """Validate that NIPA GDP table returns actual data."""

    def test_nipa_gdp_returns_data(self, bea_client: BEAClient) -> None:
        _check_bea_available(bea_client)
        obs = bea_client.get_nipa_table("T10101", frequency="Q", year="2020,2021,2022,2023,2024,2025")
        assert len(obs) >= 10, f"Expected >=10 GDP obs, got {len(obs)}"
        print(f"\n  GDP (T10101): {len(obs)} observations")
        for o in obs[:5]:
            print(f"    {o.date} line {o.line_number}: {o.value}")
        time.sleep(_BEA_REQUEST_DELAY)

    def test_observations_have_valid_values(self, bea_client: BEAClient) -> None:
        _check_bea_available(bea_client)
        obs = bea_client.get_nipa_table("T10101", frequency="Q", year="2020,2021,2022,2023,2024,2025")
        valid = [o for o in obs if o.value is not None]
        assert len(valid) >= len(obs) * 0.5, (
            f"Expected >=50% valid values, got {len(valid)}/{len(obs)}"
        )
        print(f"\n  Valid values: {len(valid)}/{len(obs)}")
        time.sleep(_BEA_REQUEST_DELAY)

    def test_frequency_correct_obs_counts(self, bea_client: BEAClient) -> None:
        _check_bea_available(bea_client)
        # Annual should have fewer unique dates than quarterly
        annual = bea_client.get_nipa_table("T10101", frequency="A", year="2020,2021,2022,2023,2024,2025")
        quarterly = bea_client.get_nipa_table("T10101", frequency="Q", year="2020,2021,2022,2023,2024,2025")
        annual_dates = {o.date for o in annual}
        quarterly_dates = {o.date for o in quarterly}
        assert len(quarterly_dates) >= len(annual_dates), (
            f"Quarterly should have more dates than annual: "
            f"{len(quarterly_dates)} vs {len(annual_dates)}"
        )
        print(f"\n  Annual dates: {len(annual_dates)}, Quarterly dates: {len(quarterly_dates)}")
        time.sleep(_BEA_REQUEST_DELAY)


# -- Layer 5: Dry-Run Ingestion ----------------------------------------------

class TestDryRunIngestion:
    """Verify all configured BEA_DATASETS return data."""

    def test_dry_run_all_configured(self, bea_client: BEAClient) -> None:
        _check_bea_available(bea_client)

        print(f"\n  {'Key':<25} {'Dataset':<20} {'Table':<12} {'Status':<10} {'Obs':<8}")
        print("  " + "-" * 75)

        data_pass = 0
        total = len(BEA_DATASETS)
        for key, cfg in BEA_DATASETS.items():
            dataset = cfg["dataset"]
            table = cfg["table"]
            try:
                if dataset == "NIPA":
                    obs = bea_client.get_nipa_table(
                        table, frequency=cfg.get("freq", "Q"), year="2020,2021,2022,2023,2024,2025",
                    )
                elif dataset == "GDPbyIndustry":
                    obs = bea_client.get_data(
                        dataset, TableID=table, Frequency="Q",
                        Year="2022,2023,2024", Industry="ALL",
                    )
                elif dataset == "Regional":
                    obs = bea_client.get_data(
                        dataset, TableName=table, LineCode="1",
                        GeoFips="STATE", Year="2022,2023",
                    )
                elif dataset == "FixedAssets":
                    obs = bea_client.get_data(
                        dataset, TableName=table, Year="2022,2023",
                    )
                elif dataset == "ITA":
                    obs = bea_client.get_data(
                        dataset, Indicator=table, AreaOrCountry="AllCountries",
                        Frequency="QSA", Year="2022,2023",
                    )
                elif dataset == "IIP":
                    obs = bea_client.get_data(
                        dataset, TypeOfInvestment="ALL",
                        Component="ALL", Frequency="QNSA",
                        Year="2023",
                    )
                else:
                    obs = bea_client.get_data(dataset, TableName=table, Year="2022,2023")
                ok = len(obs) >= 1
            except (BEAAPIError, BEAResponseError) as exc:
                obs = []
                ok = False
                print(f"  {key:<25} {dataset:<20} {table:<12} {'ERROR':<10} {exc}")

            if ok:
                data_pass += 1
            print(f"  {key:<25} {dataset:<20} {table:<12} {'PASS' if ok else 'FAIL':<10} {len(obs):<8}")
            time.sleep(_BEA_REQUEST_DELAY)

        rate = data_pass / total if total else 0
        print(f"\n  Data pass rate: {rate:.0%} ({data_pass}/{total})")
        assert rate >= 0.85, f"Data pass rate {rate:.0%} < 85%"


# -- Layer 6: Stress Test ----------------------------------------------------

class TestStressTest:
    """Large fetch, determinism, and sequential multi-table tests."""

    def test_large_fetch_year_all(self, bea_client: BEAClient) -> None:
        _check_bea_available(bea_client)
        t0 = time.monotonic()
        obs = bea_client.get_nipa_table("T10101", frequency="Q", year="ALL")
        elapsed = time.monotonic() - t0
        assert len(obs) > 100, f"Expected >100 obs for Year=ALL, got {len(obs)}"
        print(f"\n  GDP Year=ALL: {len(obs)} obs in {elapsed:.1f}s")
        time.sleep(_BEA_REQUEST_DELAY)

    def test_deterministic_fetch(self, bea_client: BEAClient) -> None:
        _check_bea_available(bea_client)
        obs1 = bea_client.get_nipa_table("T10101", frequency="Q", year="2023")
        time.sleep(_BEA_REQUEST_DELAY)
        obs2 = bea_client.get_nipa_table("T10101", frequency="Q", year="2023")
        pairs1 = [(o.date, o.line_number, o.value) for o in obs1]
        pairs2 = [(o.date, o.line_number, o.value) for o in obs2]
        assert pairs1 == pairs2, "Deterministic fetch: values differ"
        print(f"\n  Deterministic fetch: {len(obs1)} identical observations")
        time.sleep(_BEA_REQUEST_DELAY)

    def test_sequential_multi_table(self, bea_client: BEAClient) -> None:
        _check_bea_available(bea_client)
        tables = ["T10101", "T20100", "T20301"]
        for table in tables:
            obs = bea_client.get_nipa_table(table, frequency="Q", year="2023")
            assert len(obs) >= 1, f"Table {table} returned 0 observations"
            print(f"    {table}: {len(obs)} obs")
            time.sleep(_BEA_REQUEST_DELAY)


# -- Layer 7: Automated Test Report ------------------------------------------

class TestAutomatedTestReport:
    """Build ValidationReport from check results."""

    def test_generate_validation_report(self, bea_client: BEAClient) -> None:
        _check_bea_available(bea_client)
        checks: list[CheckResult] = []

        # Dataset check
        datasets = bea_client.list_datasets()
        checks.append(CheckResult(
            check_name="dataset_discovery",
            layer=ValidationLayer.CATALOG,
            passed=len(datasets) >= 8,
            severity=ValidationSeverity.ERROR,
            message=f"Found {len(datasets)} BEA datasets",
            source="bea",
        ))
        time.sleep(_BEA_REQUEST_DELAY)

        # Data check
        obs = bea_client.get_nipa_table("T10101", frequency="Q", year="2023")
        checks.append(CheckResult(
            check_name="data_accessibility_GDP",
            layer=ValidationLayer.SERIES,
            passed=len(obs) >= 1,
            severity=ValidationSeverity.ERROR,
            message=f"GDP observations: {len(obs)}",
            source="bea",
            series_id="NIPA_T10101",
        ))

        report = ValidationReport(
            source="bea",
            run_id=str(uuid.uuid4())[:8],
            timestamp=datetime.now(timezone.utc).isoformat(),
            checks=tuple(checks),
        )
        assert report.passed, f"Report failed:\n{report.format_text()}"
        print(f"\n{report.format_text()}")
        time.sleep(_BEA_REQUEST_DELAY)

    def test_report_captures_failures(self) -> None:
        checks = (
            CheckResult(
                check_name="good_check",
                layer=ValidationLayer.CATALOG,
                passed=True,
                severity=ValidationSeverity.ERROR,
                message="OK",
                source="bea",
            ),
            CheckResult(
                check_name="bad_check",
                layer=ValidationLayer.CATALOG,
                passed=False,
                severity=ValidationSeverity.ERROR,
                message="intentional failure",
                source="bea",
            ),
        )
        report = ValidationReport(
            source="bea",
            run_id="test-fail",
            timestamp=datetime.now(timezone.utc).isoformat(),
            checks=checks,
        )
        assert not report.passed
        assert report.error_count == 1
        print(f"\n  Failure report: error_count={report.error_count}")


# -- Layer 8: Edge Cases -----------------------------------------------------

class TestEdgeCases:
    """Edge-case handling for the BEA client."""

    def test_invalid_dataset_raises_error(self, bea_client: BEAClient) -> None:
        _check_bea_available(bea_client)
        with pytest.raises((BEAResponseError, BEAAPIError)):
            bea_client.get_data("NONEXISTENT_DATASET_XYZ", TableName="T10101")
        print("\n  Invalid dataset: error raised (OK)")
        time.sleep(_BEA_REQUEST_DELAY)

    def test_invalid_table_returns_empty_or_error(self, bea_client: BEAClient) -> None:
        _check_bea_available(bea_client)
        try:
            obs = bea_client.get_nipa_table("TZZZZZ", frequency="Q", year="2023")
            # Some invalid tables return empty, some raise errors — both acceptable
            print(f"\n  Invalid table: returned {len(obs)} obs (OK)")
        except (BEAResponseError, BEAAPIError) as exc:
            print(f"\n  Invalid table: {type(exc).__name__} raised (OK)")
        time.sleep(_BEA_REQUEST_DELAY)

    def test_missing_api_key_returns_empty(self) -> None:
        client = BEAClient()
        client.api_key = ""
        assert client.list_datasets() == []
        assert client.get_parameters("NIPA") == []
        assert client.get_parameter_values("NIPA", "TableName") == []
        assert client.get_data("NIPA", TableName="T10101") == []
        assert client.get_nipa_table("T10101") == []
        assert client.count_nipa_tables() == 0
        print("\n  Missing API key: all methods return empty (OK)")

    def test_na_values_filtered(self, bea_client: BEAClient) -> None:
        """Verify (NA) and similar values parse to None."""
        assert BEAClient._parse_data_value("(NA)") is None
        assert BEAClient._parse_data_value("(D)") is None
        assert BEAClient._parse_data_value("---") is None
        assert BEAClient._parse_data_value("N/A") is None
        assert BEAClient._parse_data_value("") is None
        # Valid values
        assert BEAClient._parse_data_value("21,542.5") == 21542.5
        assert BEAClient._parse_data_value("-1,234.5") == -1234.5
        assert BEAClient._parse_data_value("0.0") == 0.0
        print("\n  NA value filtering: all cases pass (OK)")


# -- Layer 9: Performance Benchmark ------------------------------------------

class TestPerformanceBenchmark:
    """Timing benchmarks for key BEA operations."""

    def test_dataset_list_within_5s(self, bea_client: BEAClient) -> None:
        _check_bea_available(bea_client)
        t0 = time.monotonic()
        bea_client.list_datasets()
        elapsed = time.monotonic() - t0
        assert elapsed < 5, f"Dataset list took {elapsed:.1f}s (limit: 5s)"
        print(f"\n  Dataset list: {elapsed:.2f}s")
        time.sleep(_BEA_REQUEST_DELAY)

    def test_params_within_5s(self, bea_client: BEAClient) -> None:
        _check_bea_available(bea_client)
        t0 = time.monotonic()
        bea_client.get_parameters("NIPA")
        elapsed = time.monotonic() - t0
        assert elapsed < 5, f"Params took {elapsed:.1f}s (limit: 5s)"
        print(f"\n  NIPA params: {elapsed:.2f}s")
        time.sleep(_BEA_REQUEST_DELAY)

    def test_data_fetch_within_10s(self, bea_client: BEAClient) -> None:
        _check_bea_available(bea_client)
        t0 = time.monotonic()
        bea_client.get_nipa_table("T10101", frequency="Q", year="2020,2021,2022,2023,2024,2025")
        elapsed = time.monotonic() - t0
        assert elapsed < 10, f"Data fetch took {elapsed:.1f}s (limit: 10s)"
        print(f"\n  GDP data fetch: {elapsed:.2f}s")
        time.sleep(_BEA_REQUEST_DELAY)


# -- Layer 10: Full Catalog Crawl --------------------------------------------

class TestFullCatalogCrawl:
    """Full BEA catalog enumeration — datasets, params, tables, sample access."""

    def test_enumerate_all_datasets_and_params(
        self, bea_client: BEAClient, all_datasets: list[BEADataset],
    ) -> None:
        """Enumerate every dataset and its parameters."""
        _check_bea_available(bea_client)
        print(f"\n  {'Dataset':<25} {'Params':<8} {'Parameter names'}")
        print("  " + "-" * 70)

        total_params = 0
        for ds in all_datasets:
            try:
                params = bea_client.get_parameters(ds.dataset_name)
                names = [p.parameter_name for p in params]
                total_params += len(params)
                print(f"  {ds.dataset_name:<25} {len(params):<8} {names}")
            except (BEAResponseError, BEAAPIError) as exc:
                print(f"  {ds.dataset_name:<25} ERROR    {exc}")
            time.sleep(_BEA_REQUEST_DELAY)

        print(f"\n  Total datasets: {len(all_datasets)}, total params: {total_params}")
        assert total_params >= 20, f"Expected >=20 total params, got {total_params}"

    def test_nipa_full_table_enumeration(
        self, bea_client: BEAClient, nipa_tables: list[BEAParameterValue],
    ) -> None:
        """Enumerate all NIPA table names."""
        assert len(nipa_tables) >= 100, (
            f"Expected >=100 NIPA tables, got {len(nipa_tables)}"
        )
        print(f"\n  NIPA tables enumerated: {len(nipa_tables)}")
        print(f"  Sample tables:")
        for t in nipa_tables[:15]:
            print(f"    {t.key}: {t.description[:70]}")
        if len(nipa_tables) > 15:
            print(f"    ... and {len(nipa_tables) - 15} more")

    def test_sample_table_accessibility(
        self, bea_client: BEAClient, nipa_tables: list[BEAParameterValue],
    ) -> None:
        """Probe a sample of NIPA tables to verify they return data."""
        _check_bea_available(bea_client)
        import random
        sample_size = min(10, len(nipa_tables))
        sample = random.sample(nipa_tables, sample_size)
        accessible = 0

        print(f"\n  Probing {sample_size} random NIPA tables...")
        for t in sample:
            try:
                obs = bea_client.get_nipa_table(
                    t.key, frequency="Q", year="2023",
                )
                ok = len(obs) >= 1
                if ok:
                    accessible += 1
                print(f"    {t.key}: {len(obs)} obs {'OK' if ok else 'EMPTY'}")
            except (BEAResponseError, BEAAPIError) as exc:
                print(f"    {t.key}: ERROR ({exc})")
            time.sleep(_BEA_REQUEST_DELAY)

        rate = accessible / sample_size if sample_size else 0
        print(f"\n  Sample accessibility: {accessible}/{sample_size} ({rate:.0%})")
        assert rate >= 0.50, f"Sample accessibility {rate:.0%} < 50%"

    def test_coverage_matrix(
        self, bea_client: BEAClient, all_datasets: list[BEADataset],
    ) -> None:
        """Build a coverage matrix: datasets × configured entries."""
        live_names = {d.dataset_name for d in all_datasets}
        known_names = set(BEA_KNOWN_DATASETS.keys())
        configured_datasets = {cfg["dataset"] for cfg in BEA_DATASETS.values()}

        print(f"\n  ── BEA Coverage Matrix ──")
        print(f"  Live datasets:       {len(live_names)}")
        print(f"  Known datasets:      {len(known_names)}")
        print(f"  Configured datasets: {len(configured_datasets)}")
        print(f"  Known ∩ Live:        {len(known_names & live_names)}")
        print(f"  Configured ∩ Live:   {len(configured_datasets & live_names)}")

        # All configured must be live
        missing_live = configured_datasets - live_names
        assert not missing_live, f"Configured datasets not live: {missing_live}"

        # Most known should be live
        missing_known = known_names - live_names
        assert len(missing_known) <= 2, (
            f"Too many known datasets not live: {missing_known}"
        )

        # Count configured tables per dataset
        print(f"\n  {'Dataset':<25} {'Configured tables':<20} {'In live?'}")
        print("  " + "-" * 55)
        for dataset in sorted(configured_datasets):
            tables = [
                cfg["table"]
                for cfg in BEA_DATASETS.values()
                if cfg["dataset"] == dataset
            ]
            in_live = dataset in live_names
            print(f"  {dataset:<25} {len(tables):<20} {'YES' if in_live else 'NO'}")
        print(f"\n  Total configured entries: {len(BEA_DATASETS)}")

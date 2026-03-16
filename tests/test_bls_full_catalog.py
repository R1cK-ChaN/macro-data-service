"""Integration tests for BLS full catalog access and 10-layer validation.

Requires network access and BLS_API_KEY env var. Run with:
    pytest tests/test_bls_full_catalog.py -v -s
"""

from __future__ import annotations

import re
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ingestion.scrapers.bls import (
    BLSAPIError,
    BLSClient,
    BLSObservation,
    BLSRateLimitError,
    BLSResponseError,
    BLSSeriesInfo,
    BLSSurvey,
)
from ingestion.sources import BLS_SERIES, BLS_SURVEY_PREFIXES
from ingestion.validation._types import (
    CheckResult,
    ValidationLayer,
    ValidationReport,
    ValidationSeverity,
)

pytestmark = pytest.mark.integration

_BLS_REQUEST_DELAY = 1.0  # conservative; BLS allows 500 queries/day


# -- Fixtures ----------------------------------------------------------------

@pytest.fixture(scope="module")
def bls_client() -> BLSClient:
    client = BLSClient()
    if not client.api_key:
        pytest.skip("BLS_API_KEY not set")
    return client


@pytest.fixture(scope="module")
def all_surveys(bls_client: BLSClient) -> list[BLSSurvey]:
    """Cache all BLS surveys for the module."""
    return bls_client.list_surveys()


@pytest.fixture(scope="module")
def configured_series_ids() -> list[str]:
    """Return all series IDs from BLS_SERIES config."""
    return [meta["series_id"] for meta in BLS_SERIES.values()]


def _check_bls_available(client: BLSClient) -> None:
    """Try a minimal series fetch; skip test if BLS is unavailable."""
    try:
        client.get_series_single("CUUR0000SA0", start_year=2024, end_year=2024)
    except BLSRateLimitError:
        pytest.skip("BLS API is rate limiting -- try again later")
    except (BLSAPIError, BLSResponseError, requests.RequestException):
        pytest.skip("BLS API unavailable")


# -- Layer 1: Survey Discovery -----------------------------------------------

class TestSurveyDiscovery:
    """Validate that we can discover BLS survey programs."""

    def test_surveys_list_returns_many(self, all_surveys: list[BLSSurvey]) -> None:
        assert len(all_surveys) >= 15, (
            f"Expected >=15 BLS surveys, got {len(all_surveys)}"
        )
        print(f"\n  BLS surveys: {len(all_surveys)}")
        for s in all_surveys[:10]:
            print(f"    {s.survey_abbreviation}: {s.survey_name}")
        if len(all_surveys) > 10:
            print(f"    ... and {len(all_surveys) - 10} more")

    def test_surveys_contain_known_programs(self, all_surveys: list[BLSSurvey]) -> None:
        abbreviations = {s.survey_abbreviation for s in all_surveys}
        expected = {"CU", "CE", "LN", "WP", "JT", "AP"}
        found = expected & abbreviations
        assert len(found) >= 5, (
            f"Expected >=5 known surveys, found: {found}"
        )
        print(f"\n  Known survey programs found: {found}")

    def test_configured_surveys_are_known(
        self, all_surveys: list[BLSSurvey],
    ) -> None:
        abbreviations = {s.survey_abbreviation for s in all_surveys}
        configured_surveys = {meta["survey"] for meta in BLS_SERIES.values()}
        missing = configured_surveys - abbreviations
        assert not missing, f"Configured surveys not found in BLS: {missing}"
        print(f"\n  All {len(configured_surveys)} configured surveys found")

    def test_configured_series_probe(
        self, bls_client: BLSClient, configured_series_ids: list[str],
    ) -> None:
        """Batch-probe all configured series IDs exist."""
        _check_bls_available(bls_client)
        results = bls_client.get_series(
            configured_series_ids[:50], start_year=2024, end_year=2024,
        )
        found = {sid for sid, obs in results.items() if obs}
        missing = set(configured_series_ids[:50]) - found
        print(f"\n  Configured series probe: {len(found)}/{len(configured_series_ids)} found")
        if missing:
            print(f"  Missing: {missing}")
        assert len(missing) <= 2, f"Too many series not returning data: {missing}"
        time.sleep(_BLS_REQUEST_DELAY)


# -- Layer 2: Structure Validation -------------------------------------------

class TestStructureValidation:
    """Validate series metadata for configured BLS series."""

    def test_catalog_metadata_for_sample(self, bls_client: BLSClient) -> None:
        _check_bls_available(bls_client)
        sample_ids = [meta["series_id"] for meta in list(BLS_SERIES.values())[:5]]
        failures: list[str] = []
        for sid in sample_ids:
            info = bls_client.get_series_info(sid)
            if not info.series_title or not info.survey_abbreviation:
                failures.append(sid)
            else:
                print(f"    {sid}: {info.series_title[:50]} ({info.survey_abbreviation})")
            time.sleep(_BLS_REQUEST_DELAY)
        assert not failures, f"Missing metadata for: {failures}"

    def test_date_range_is_reasonable(self, bls_client: BLSClient) -> None:
        """Verify CPI has long historical data by fetching an early window."""
        _check_bls_available(bls_client)
        # CPI data goes back to 1913; fetch a sample from the 1940s
        obs = bls_client.get_series_single(
            "CUUR0000SA0", start_year=1940, end_year=1950,
        )
        assert len(obs) > 0, "Expected CPI data from 1940-1950"
        earliest = min(o.date for o in obs)
        print(f"\n  CPI earliest available: {earliest} ({len(obs)} obs in 1940-1950)")
        time.sleep(_BLS_REQUEST_DELAY)

    def test_observation_period_format(self, bls_client: BLSClient) -> None:
        _check_bls_available(bls_client)
        obs = bls_client.get_series_single(
            "CUUR0000SA0", start_year=2024, end_year=2024,
        )
        date_re = re.compile(r"^\d{4}-\d{2}-\d{2}$")
        for o in obs:
            assert date_re.match(o.date), f"Invalid date format: {o.date}"
            assert o.period.startswith("M"), f"Expected monthly period, got: {o.period}"
        print(f"\n  Period format validation passed ({len(obs)} observations)")
        time.sleep(_BLS_REQUEST_DELAY)


# -- Layer 3: Dataset Accessibility ------------------------------------------

class TestDatasetAccessibility:
    """Validate that configured BLS series return actual observations."""

    def test_batch_fetch_all_configured(
        self, bls_client: BLSClient, configured_series_ids: list[str],
    ) -> None:
        _check_bls_available(bls_client)
        results = bls_client.get_series(
            configured_series_ids, start_year=2023, end_year=2024,
        )
        failures = [sid for sid in configured_series_ids if not results.get(sid)]
        for sid in configured_series_ids:
            obs = results.get(sid, [])
            status = f"{len(obs)} obs" if obs else "MISSING"
            print(f"    {sid}: {status}")
        assert len(failures) <= 2, f"No observations for: {failures}"
        print(f"\n  All configured: {len(configured_series_ids) - len(failures)}/{len(configured_series_ids)} returning data")
        time.sleep(_BLS_REQUEST_DELAY)

    def test_observations_have_valid_values(self, bls_client: BLSClient) -> None:
        _check_bls_available(bls_client)
        results = bls_client.get_series(
            ["CUUR0000SA0", "CES0000000001", "LNS14000000"],
            start_year=2024, end_year=2024,
        )
        for sid, obs in results.items():
            for o in obs:
                assert isinstance(o.value, float), f"{sid}: value is not float"
                assert o.value > 0, f"{sid}: unexpected non-positive value {o.value}"
            print(f"    {sid}: {len(obs)} obs, all valid floats")
        time.sleep(_BLS_REQUEST_DELAY)

    def test_observations_by_frequency(self, bls_client: BLSClient) -> None:
        _check_bls_available(bls_client)
        # Monthly: ~12 obs per year
        monthly = bls_client.get_series_single(
            "CUUR0000SA0", start_year=2023, end_year=2023,
        )
        assert len(monthly) >= 10, f"Expected ~12 monthly obs, got {len(monthly)}"
        print(f"\n  Monthly (CPI): {len(monthly)} obs")
        time.sleep(_BLS_REQUEST_DELAY)
        # Quarterly: ~4 obs per year
        quarterly = bls_client.get_series_single(
            "CIU1010000000000A", start_year=2023, end_year=2023,
        )
        assert len(quarterly) >= 3, f"Expected ~4 quarterly obs, got {len(quarterly)}"
        print(f"  Quarterly (ECI): {len(quarterly)} obs")
        time.sleep(_BLS_REQUEST_DELAY)


# -- Layer 4: Series Count Validation ----------------------------------------

class TestSeriesCountValidation:
    """Validate survey-based series coverage.

    BLS has no catalog browsing endpoint, so we probe series ID patterns
    per survey to estimate coverage.
    """

    def test_survey_list_covers_known_prefixes(
        self, all_surveys: list[BLSSurvey],
    ) -> None:
        live_abbrevs = {s.survey_abbreviation for s in all_surveys}
        configured = set(BLS_SURVEY_PREFIXES.keys())
        covered = configured & live_abbrevs
        assert len(covered) >= len(configured) - 2, (
            f"Expected >={len(configured) - 2} known prefixes in live surveys, "
            f"found {len(covered)}: {covered}"
        )
        print(f"\n  Known prefixes covered: {len(covered)}/{len(configured)}")

    def test_cpi_series_pattern_probe(self, bls_client: BLSClient) -> None:
        """Probe CPI series patterns to verify breadth."""
        _check_bls_available(bls_client)
        probe_ids = [
            "CUUR0000SA0",      # National, All items
            "CUUR0100SA0",      # Northeast
            "CUUR0200SA0",      # Midwest
            "CUUR0300SA0",      # South
            "CUUR0400SA0",      # West
            "CUUR0000SAF1",     # Food
            "CUUR0000SAH1",     # Shelter
            "CUUR0000SA0E",     # Energy
            "CUUR0000SAM1",     # Medical care
            "CUUR0000SAT1",     # Transportation
        ]
        results = bls_client.get_series(probe_ids, start_year=2024, end_year=2024)
        valid = sum(1 for obs in results.values() if obs)
        assert valid >= 8, f"Expected >=8 valid CPI series, got {valid}"
        print(f"\n  CPI probe: {valid}/{len(probe_ids)} valid")
        time.sleep(_BLS_REQUEST_DELAY)

    def test_employment_series_pattern_probe(self, bls_client: BLSClient) -> None:
        """Probe CES series patterns to verify breadth."""
        _check_bls_available(bls_client)
        probe_ids = [
            "CES0000000001",    # Total Nonfarm
            "CES0500000001",    # Total Private
            "CES1000000001",    # Mining and Logging
            "CES2000000001",    # Construction
            "CES3000000001",    # Manufacturing
            "CES4000000001",    # Trade, Transportation
            "CES5000000001",    # Information
            "CES6000000001",    # Financial Activities
            "CES7000000001",    # Professional/Business
            "CES8000000001",    # Education/Health
        ]
        results = bls_client.get_series(probe_ids, start_year=2024, end_year=2024)
        valid = sum(1 for obs in results.values() if obs)
        assert valid >= 8, f"Expected >=8 valid CES series, got {valid}"
        print(f"\n  CES probe: {valid}/{len(probe_ids)} valid")
        time.sleep(_BLS_REQUEST_DELAY)

    def test_multi_survey_probe(self, bls_client: BLSClient) -> None:
        """Probe one representative series per major survey."""
        _check_bls_available(bls_client)
        representative = {
            "CU": "CUUR0000SA0",
            "CE": "CES0000000001",
            "LN": "LNS14000000",
            "WP": "WPSFD4",
            "JT": "JTS000000000000000JOL",
            "CI": "CIU1010000000000A",
            "PR": "PRS85006092",
            "AP": "APU0000SA0",
        }
        results = bls_client.get_series(
            list(representative.values()), start_year=2024, end_year=2024,
        )
        valid = {
            prefix: sid
            for prefix, sid in representative.items()
            if results.get(sid)
        }
        for prefix, sid in representative.items():
            obs = results.get(sid, [])
            print(f"    {prefix} ({sid}): {len(obs)} obs")
        assert len(valid) >= 6, f"Expected >=6 surveys returning data, got {len(valid)}"
        print(f"\n  Multi-survey probe: {len(valid)}/{len(representative)} surveys active")
        time.sleep(_BLS_REQUEST_DELAY)


# -- Layer 5: Dry-Run Ingestion ----------------------------------------------

class TestDryRunIngestion:
    """Combined metadata + observation probe for all configured series."""

    def test_dry_run_all_configured(self, bls_client: BLSClient) -> None:
        _check_bls_available(bls_client)
        all_ids = [meta["series_id"] for meta in BLS_SERIES.values()]
        results = bls_client.get_series(all_ids, start_year=2023, end_year=2024)

        print(f"\n  {'Series':<30} {'Status':<12} {'Obs':<8}")
        print("  " + "-" * 50)

        data_pass = 0
        for name, meta in BLS_SERIES.items():
            sid = meta["series_id"]
            obs = results.get(sid, [])
            ok = len(obs) >= 1
            if ok:
                data_pass += 1
            print(f"  {sid:<30} {'PASS' if ok else 'FAIL':<12} {len(obs):<8}")

        rate = data_pass / len(all_ids) if all_ids else 0
        print(f"\n  Data pass rate: {rate:.0%} ({data_pass}/{len(all_ids)})")
        assert rate >= 0.90, f"Data pass rate {rate:.0%} < 90%"
        time.sleep(_BLS_REQUEST_DELAY)


# -- Layer 6: Stress Test ----------------------------------------------------

class TestStressTest:
    """Large fetch, batch validation, and determinism checks."""

    def test_large_historical_fetch(self, bls_client: BLSClient) -> None:
        _check_bls_available(bls_client)
        t0 = time.monotonic()
        obs = bls_client.get_series_single(
            "CUUR0000SA0", start_year=2005, end_year=2024,
        )
        elapsed = time.monotonic() - t0
        assert len(obs) > 200, f"Expected >200 CPI observations, got {len(obs)}"
        assert elapsed < 30, f"Large fetch took {elapsed:.1f}s (limit: 30s)"
        print(f"\n  CPI 2005-2024: {len(obs)} obs in {elapsed:.1f}s")
        time.sleep(_BLS_REQUEST_DELAY)

    def test_max_batch_series(self, bls_client: BLSClient) -> None:
        _check_bls_available(bls_client)
        # Build a batch of up to 30 series (configured + regional CPI)
        all_ids = [meta["series_id"] for meta in BLS_SERIES.values()]
        extra = [f"CUUR0{i}00SA0" for i in range(1, 10)]
        batch = list(dict.fromkeys(all_ids + extra))[:30]
        results = bls_client.get_series(batch, start_year=2024, end_year=2024)
        valid = sum(1 for obs in results.values() if obs)
        print(f"\n  Batch {len(batch)} series: {valid} returned data")
        assert valid >= len(batch) * 0.7, (
            f"Expected >=70% valid, got {valid}/{len(batch)}"
        )
        time.sleep(_BLS_REQUEST_DELAY)

    def test_deterministic_fetch(self, bls_client: BLSClient) -> None:
        _check_bls_available(bls_client)
        obs1 = bls_client.get_series_single(
            "CUUR0000SA0", start_year=2024, end_year=2024,
        )
        time.sleep(1)
        obs2 = bls_client.get_series_single(
            "CUUR0000SA0", start_year=2024, end_year=2024,
        )
        assert len(obs1) == len(obs2), (
            f"Deterministic fetch: lengths differ ({len(obs1)} vs {len(obs2)})"
        )
        pairs1 = [(o.date, o.value) for o in obs1]
        pairs2 = [(o.date, o.value) for o in obs2]
        assert pairs1 == pairs2, "Deterministic fetch: values differ"
        print(f"\n  Deterministic fetch: {len(obs1)} identical observations")
        time.sleep(_BLS_REQUEST_DELAY)

    def test_year_chunking(self, bls_client: BLSClient) -> None:
        """Verify >20 year spans are handled via auto-chunking."""
        _check_bls_available(bls_client)
        obs = bls_client.get_series_single(
            "CUUR0000SA0", start_year=2000, end_year=2024,
        )
        assert len(obs) > 250, f"Expected >250 obs for 25 years, got {len(obs)}"
        print(f"\n  25-year fetch (auto-chunked): {len(obs)} obs")
        time.sleep(_BLS_REQUEST_DELAY)


# -- Layer 7: Automated Test Report ------------------------------------------

class TestAutomatedTestReport:
    """Build ValidationReport from check results."""

    def test_generate_validation_report(self, bls_client: BLSClient) -> None:
        _check_bls_available(bls_client)
        checks: list[CheckResult] = []

        # Survey check
        surveys = bls_client.list_surveys()
        checks.append(CheckResult(
            check_name="survey_discovery",
            layer=ValidationLayer.CATALOG,
            passed=len(surveys) >= 15,
            severity=ValidationSeverity.ERROR,
            message=f"Found {len(surveys)} BLS surveys",
            source="bls",
        ))
        time.sleep(_BLS_REQUEST_DELAY)

        # Series data check
        obs = bls_client.get_series_single(
            "CUUR0000SA0", start_year=2024, end_year=2024,
        )
        checks.append(CheckResult(
            check_name="data_accessibility_CPI",
            layer=ValidationLayer.SERIES,
            passed=len(obs) >= 1,
            severity=ValidationSeverity.ERROR,
            message=f"CPI observations: {len(obs)}",
            source="bls",
            series_id="CUUR0000SA0",
        ))

        report = ValidationReport(
            source="bls",
            run_id=str(uuid.uuid4())[:8],
            timestamp=datetime.now(timezone.utc).isoformat(),
            checks=tuple(checks),
        )
        assert report.passed, f"Report failed:\n{report.format_text()}"
        print(f"\n{report.format_text()}")
        time.sleep(_BLS_REQUEST_DELAY)

    def test_report_captures_failures(self) -> None:
        checks = (
            CheckResult(
                check_name="good_check",
                layer=ValidationLayer.CATALOG,
                passed=True,
                severity=ValidationSeverity.ERROR,
                message="OK",
                source="bls",
            ),
            CheckResult(
                check_name="bad_check",
                layer=ValidationLayer.CATALOG,
                passed=False,
                severity=ValidationSeverity.ERROR,
                message="intentional failure",
                source="bls",
            ),
        )
        report = ValidationReport(
            source="bls",
            run_id="test-fail",
            timestamp=datetime.now(timezone.utc).isoformat(),
            checks=checks,
        )
        assert not report.passed
        assert report.error_count == 1
        print(f"\n  Failure report: error_count={report.error_count}")


# -- Layer 8: Edge Cases -----------------------------------------------------

class TestEdgeCases:
    """Edge-case handling for the BLS client."""

    def test_nonexistent_series_returns_empty(self, bls_client: BLSClient) -> None:
        _check_bls_available(bls_client)
        results = bls_client.get_series(
            ["ZZZZZZ9999999"], start_year=2024, end_year=2024,
        )
        obs = results.get("ZZZZZZ9999999", [])
        assert len(obs) == 0, f"Expected 0 obs for fake series, got {len(obs)}"
        print("\n  Nonexistent series: 0 observations (OK)")
        time.sleep(_BLS_REQUEST_DELAY)

    def test_future_year_raises_error(self, bls_client: BLSClient) -> None:
        _check_bls_available(bls_client)
        with pytest.raises(BLSResponseError):
            bls_client.get_series_single(
                "CUUR0000SA0", start_year=2099, end_year=2099,
            )
        print("\n  Future year: BLSResponseError raised (OK)")
        time.sleep(_BLS_REQUEST_DELAY)

    def test_missing_api_key_returns_empty(self) -> None:
        client = BLSClient()
        client.api_key = ""
        assert client.get_series("CUUR0000SA0", start_year=2024, end_year=2024) == {}
        assert client.get_series_single("CUUR0000SA0", start_year=2024, end_year=2024) == []
        assert client.list_surveys() == []
        print("\n  Missing API key: all methods return empty (OK)")

    def test_single_string_series_id(self, bls_client: BLSClient) -> None:
        """Verify passing a single string (not list) works."""
        _check_bls_available(bls_client)
        results = bls_client.get_series(
            "CUUR0000SA0", start_year=2024, end_year=2024,
        )
        assert "CUUR0000SA0" in results
        assert len(results["CUUR0000SA0"]) > 0
        print("\n  Single string series ID: works (OK)")
        time.sleep(_BLS_REQUEST_DELAY)


# -- Layer 9: Performance Benchmark ------------------------------------------

class TestPerformanceBenchmark:
    """Timing benchmarks for key BLS operations."""

    def test_single_series_within_5s(self, bls_client: BLSClient) -> None:
        _check_bls_available(bls_client)
        t0 = time.monotonic()
        bls_client.get_series_single("CUUR0000SA0", start_year=2024, end_year=2024)
        elapsed = time.monotonic() - t0
        assert elapsed < 5, f"Single series took {elapsed:.1f}s (limit: 5s)"
        print(f"\n  Single series fetch: {elapsed:.2f}s")
        time.sleep(_BLS_REQUEST_DELAY)

    def test_batch_20_within_10s(self, bls_client: BLSClient) -> None:
        _check_bls_available(bls_client)
        ids = [meta["series_id"] for meta in list(BLS_SERIES.values())[:20]]
        t0 = time.monotonic()
        bls_client.get_series(ids, start_year=2024, end_year=2024)
        elapsed = time.monotonic() - t0
        assert elapsed < 10, f"Batch 20 took {elapsed:.1f}s (limit: 10s)"
        print(f"\n  Batch 20 series: {elapsed:.2f}s")
        time.sleep(_BLS_REQUEST_DELAY)

    def test_survey_list_within_5s(self, bls_client: BLSClient) -> None:
        _check_bls_available(bls_client)
        t0 = time.monotonic()
        bls_client.list_surveys()
        elapsed = time.monotonic() - t0
        assert elapsed < 5, f"Survey list took {elapsed:.1f}s (limit: 5s)"
        print(f"\n  Survey list: {elapsed:.2f}s")
        time.sleep(_BLS_REQUEST_DELAY)

    def test_catalog_metadata_within_10s(self, bls_client: BLSClient) -> None:
        _check_bls_available(bls_client)
        t0 = time.monotonic()
        bls_client.get_series_info("CUUR0000SA0")
        elapsed = time.monotonic() - t0
        assert elapsed < 10, f"Catalog metadata took {elapsed:.1f}s (limit: 10s)"
        print(f"\n  Catalog metadata: {elapsed:.2f}s")
        time.sleep(_BLS_REQUEST_DELAY)


# -- Layer 10: Full Catalog Crawl --------------------------------------------

class TestFullCatalogCrawl:
    """Full BLS catalog probe — estimates coverage across all surveys.

    BLS has no catalog-enumeration API. We probe series ID patterns
    per survey to verify breadth, then cross-reference with FRED.
    """

    def test_survey_enumeration_and_probe(
        self, bls_client: BLSClient, all_surveys: list[BLSSurvey],
    ) -> None:
        """Probe representative series from each known survey."""
        _check_bls_available(bls_client)
        # Representative series per survey prefix
        survey_probes: dict[str, list[str]] = {
            "CU": ["CUUR0000SA0", "CUUR0000SA0L1E", "CUUR0100SA0"],
            "CE": ["CES0000000001", "CES0500000001", "CES3000000001"],
            "LN": ["LNS14000000", "LNS11300000", "LNS12000000"],
            "WP": ["WPSFD4", "WPSFD49116"],
            "JT": ["JTS000000000000000JOL", "JTS000000000000000HIL"],
            "CI": ["CIU1010000000000A"],
            "PR": ["PRS85006092", "PRS85006112"],
            "AP": ["APU0000SA0", "APU0000706111"],
        }

        total_probed = 0
        total_valid = 0
        print(f"\n  {'Survey':<8} {'Name':<35} {'Probed':<8} {'Valid':<8}")
        print("  " + "-" * 59)

        for abbrev, probe_ids in survey_probes.items():
            results = bls_client.get_series(
                probe_ids, start_year=2024, end_year=2024,
            )
            valid = sum(1 for obs in results.values() if obs)
            total_probed += len(probe_ids)
            total_valid += valid
            name = BLS_SURVEY_PREFIXES.get(abbrev, "Unknown")
            print(f"  {abbrev:<8} {name:<35} {len(probe_ids):<8} {valid:<8}")
            time.sleep(_BLS_REQUEST_DELAY)

        print(f"\n  Total: {total_valid}/{total_probed} probed series valid")
        assert total_valid >= 15, (
            f"Expected >=15 valid series across surveys, got {total_valid}"
        )

    def test_cross_reference_fred_bls_overlap(self, bls_client: BLSClient) -> None:
        """Verify BLS series that overlap with FRED MACRO_SERIES.

        FRED sources CPI and Employment from BLS. Verify the BLS originals
        return data for the same time period.
        """
        _check_bls_available(bls_client)
        overlap = {
            "CPI (FRED:CPIAUCSL ↔ BLS:CUUR0000SA0)": "CUUR0000SA0",
            "NFP (FRED:PAYEMS ↔ BLS:CES0000000001)": "CES0000000001",
            "UNRATE (FRED:UNRATE ↔ BLS:LNS14000000)": "LNS14000000",
        }
        results = bls_client.get_series(
            list(overlap.values()), start_year=2024, end_year=2024,
        )
        for label, sid in overlap.items():
            obs = results.get(sid, [])
            assert len(obs) >= 6, f"{label}: expected >=6 obs, got {len(obs)}"
            print(f"    {label}: {len(obs)} obs")
        print(f"\n  FRED-BLS overlap: all {len(overlap)} verified")
        time.sleep(_BLS_REQUEST_DELAY)

    def test_configured_series_full_probe(self, bls_client: BLSClient) -> None:
        """Verify all BLS_SERIES entries are accessible."""
        _check_bls_available(bls_client)
        all_ids = [meta["series_id"] for meta in BLS_SERIES.values()]
        results = bls_client.get_series(all_ids, start_year=2020, end_year=2024)
        accessible = {sid for sid, obs in results.items() if obs}
        coverage = len(accessible) / len(all_ids) if all_ids else 0

        print(f"\n  ── BLS Catalog Probe Report ──")
        print(f"  Configured series:  {len(all_ids)}")
        print(f"  Accessible:         {len(accessible)} ({coverage:.0%})")
        for name, meta in BLS_SERIES.items():
            sid = meta["series_id"]
            obs = results.get(sid, [])
            status = "OK" if sid in accessible else "MISSING"
            print(f"    {sid:<30} {status} ({len(obs)} obs)")

        assert coverage >= 0.90, f"Coverage {coverage:.0%} < 90%"
        time.sleep(_BLS_REQUEST_DELAY)

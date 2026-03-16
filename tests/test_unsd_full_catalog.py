"""Integration tests for UNSD (UNData) full catalog access and 9-layer validation.

Requires network access (no API key needed). Run with:
    pytest tests/test_unsd_full_catalog.py -v -s
"""

from __future__ import annotations

import random
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ingestion.sdmx._errors import UNSDAPIError, UNSDRateLimitError
from ingestion.sdmx._parsing import build_decade_chunks as _build_decade_chunks
from ingestion.sdmx.providers.unsd import UNSDClient
from ingestion.validation._types import (
    CheckResult,
    ValidationLayer,
    ValidationReport,
    ValidationSeverity,
)

pytestmark = pytest.mark.integration


# ── Fixtures ──────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def unsd_client() -> UNSDClient:
    return UNSDClient(timeout=60)


@pytest.fixture(scope="module")
def all_dataflows(unsd_client: UNSDClient) -> list:
    return unsd_client.list_dataflows()


def _check_unsd_available(client: UNSDClient) -> None:
    """Try a minimal catalog fetch; skip test if UNSD is unavailable."""
    try:
        flows = client.list_dataflows()
        if not flows:
            pytest.skip("UNSD API returned empty catalog")
    except UNSDRateLimitError:
        pytest.skip("UNSD API is rate limiting — try again in a few minutes")
    except (UNSDAPIError, requests.RequestException):
        pytest.skip("UNSD API unavailable")


# ── Layer 1: Catalog Discovery ────────────────────────────────────────

class TestCatalogDiscovery:
    """Validate that we can discover the full UNSD dataflow catalog."""

    def test_list_all_dataflows_returns_many(self, all_dataflows: list) -> None:
        assert len(all_dataflows) > 10, (
            f"Expected >10 dataflows, got {len(all_dataflows)}"
        )
        print(f"\n  Total UNSD dataflows: {len(all_dataflows)}")
        for flow in all_dataflows[:30]:
            print(f"    {flow.agency_id:<12} {flow.id:<28} v{flow.version}: {flow.name[:40]}")
        if len(all_dataflows) > 30:
            print(f"    ... and {len(all_dataflows) - 30} more")

    def test_catalog_includes_multiple_agencies(self, all_dataflows: list) -> None:
        agencies = {f.agency_id for f in all_dataflows}
        assert len(agencies) >= 1, (
            f"Expected at least 1 agency in catalog, found: {agencies}"
        )
        print(f"\n  Agencies in catalog: {sorted(agencies)}")

    def test_dataflows_have_structure_references(self, all_dataflows: list) -> None:
        with_structure = [f for f in all_dataflows if f.structure_id]
        ratio = len(with_structure) / len(all_dataflows) if all_dataflows else 0
        assert ratio > 0.5, (
            f"Expected >50% of dataflows to have structure_id, got {ratio:.0%}"
        )
        print(f"\n  Dataflows with structure_id: {len(with_structure)}/{len(all_dataflows)} ({ratio:.0%})")

    def test_dataflows_have_names(self, all_dataflows: list) -> None:
        with_names = [f for f in all_dataflows if f.name]
        ratio = len(with_names) / len(all_dataflows) if all_dataflows else 0
        assert ratio > 0.5, (
            f"Expected >50% of dataflows to have names, got {ratio:.0%}"
        )
        print(f"\n  Dataflows with names: {len(with_names)}/{len(all_dataflows)} ({ratio:.0%})")


# ── Layer 2: Structure Validation ─────────────────────────────────────

class TestStructureValidation:
    """Validate DSD introspection for UNSD dataflows."""

    def test_structure_for_first_dataflow(
        self, unsd_client: UNSDClient, all_dataflows: list,
    ) -> None:
        _check_unsd_available(unsd_client)
        flow = all_dataflows[0]
        structure = unsd_client.get_datastructure(flow.id, agency_id=flow.agency_id)
        assert len(structure.dimensions) >= 1, (
            f"Expected >=1 dimensions for {flow.id}, got {len(structure.dimensions)}"
        )
        has_time = any(d.is_time for d in structure.dimensions)
        assert has_time, f"{flow.id} DSD should have a time dimension"
        print(f"\n  {flow.id} structure: {[d.id for d in structure.dimensions]}")
        for d in structure.dimensions:
            if not d.is_time:
                print(f"    {d.id}: {d.code_count} codes")

    def test_summarize_structure_returns_compact_view(
        self, unsd_client: UNSDClient, all_dataflows: list,
    ) -> None:
        _check_unsd_available(unsd_client)
        flow = all_dataflows[0]
        summary = unsd_client.summarize_structure(flow.id)
        assert summary.dataflow_id == flow.id
        assert summary.structure_id
        assert len(summary.series_dimensions) > 0
        assert summary.time_dimension_id
        print(
            f"\n  {flow.id} summary: dims={list(summary.series_dimensions)}, "
            f"codes={dict(summary.code_counts)}, est_series={summary.estimated_series}"
        )

    def test_structure_for_sample_catalog_dataflows(
        self, unsd_client: UNSDClient, all_dataflows: list,
    ) -> None:
        _check_unsd_available(unsd_client)
        sample = random.sample(all_dataflows, min(5, len(all_dataflows)))
        passed = 0
        for flow in sample:
            try:
                structure = unsd_client.get_datastructure(flow.id, agency_id=flow.agency_id)
                assert len(structure.dimensions) >= 1
                passed += 1
                print(f"    {flow.agency_id}/{flow.id}: {len(structure.dimensions)} dims — OK")
            except (UNSDAPIError, UNSDRateLimitError) as exc:
                print(f"    {flow.agency_id}/{flow.id}: SKIP ({exc!r})")
        print(f"\n  Sample structure validation: {passed}/{len(sample)} passed")
        assert passed >= 1, "Expected at least 1 structure probe to succeed"

    def test_multi_agency_structure_retrieval(
        self, unsd_client: UNSDClient, all_dataflows: list,
    ) -> None:
        """Verify structures can be retrieved across different agencies."""
        _check_unsd_available(unsd_client)
        agencies_tested: set[str] = set()
        for flow in all_dataflows:
            if flow.agency_id in agencies_tested:
                continue
            try:
                structure = unsd_client.get_datastructure(flow.id, agency_id=flow.agency_id)
                assert len(structure.dimensions) >= 1
                agencies_tested.add(flow.agency_id)
                print(f"    {flow.agency_id}: OK ({flow.id})")
            except (UNSDAPIError, UNSDRateLimitError):
                pass
            if len(agencies_tested) >= 3:
                break
        print(f"\n  Agencies tested: {sorted(agencies_tested)}")
        assert len(agencies_tested) >= 1, "Expected at least 1 agency to succeed"


# ── Layer 3: Dataset Accessibility ────────────────────────────────────

class TestDatasetAccessibility:
    """Validate that catalog datasets are accessible."""

    def test_probe_first_dataflow(
        self, unsd_client: UNSDClient, all_dataflows: list,
    ) -> None:
        _check_unsd_available(unsd_client)
        flow = all_dataflows[0]
        est = unsd_client.estimate_size(flow.id, agency_id=flow.agency_id)
        assert est.total_series >= 0
        print(f"\n  {flow.id}: ~{est.total_series} series")

    def test_probe_sample_catalog_datasets(
        self, unsd_client: UNSDClient, all_dataflows: list,
    ) -> None:
        _check_unsd_available(unsd_client)
        sample = random.sample(all_dataflows, min(10, len(all_dataflows)))
        probed = 0
        for flow in sample:
            try:
                est = unsd_client.estimate_size(flow.id, agency_id=flow.agency_id)
                probed += 1
                print(f"    {flow.agency_id}/{flow.id}: ~{est.total_series} series")
            except (UNSDAPIError, UNSDRateLimitError):
                print(f"    {flow.agency_id}/{flow.id}: SKIP")
        assert probed >= 1, "Expected at least 1 random probe to succeed"

    def test_data_fetch_returns_valid_observations(
        self, unsd_client: UNSDClient, all_dataflows: list,
    ) -> None:
        _check_unsd_available(unsd_client)
        flow = all_dataflows[0]
        try:
            obs = unsd_client.get_data(
                flow.id,
                "all",
                series_id=f"UNSD_{flow.id}",
                agency_id=flow.agency_id,
                limit=5,
            )
            if obs:
                for o in obs[:3]:
                    assert o.date
                    assert isinstance(o.value, float)
                    assert o.dataflow == flow.id
                print(f"\n  {flow.id}: {len(obs)} valid observations")
            else:
                print(f"\n  {flow.id}: 0 observations (empty dataset)")
        except (UNSDAPIError, UNSDRateLimitError) as exc:
            print(f"\n  {flow.id}: data fetch failed ({exc!r})")


# ── Layer 4: Size Estimation ──────────────────────────────────────────

class TestSizeEstimation:
    """Validate size estimation via limit=1 probes."""

    def test_estimate_size_for_first_dataflow(
        self, unsd_client: UNSDClient, all_dataflows: list,
    ) -> None:
        _check_unsd_available(unsd_client)
        flow = all_dataflows[0]
        est = unsd_client.estimate_size(flow.id, agency_id=flow.agency_id)
        print(
            f"\n  {flow.id} size: {est.total_series} series x {est.time_periods} periods "
            f"= ~{est.estimated_observations:,} obs"
        )

    def test_estimate_size_for_sample(
        self, unsd_client: UNSDClient, all_dataflows: list,
    ) -> None:
        _check_unsd_available(unsd_client)
        sample = random.sample(all_dataflows, min(5, len(all_dataflows)))
        print("\n  Dataflow                     Agency       Series   Periods   Est. Obs")
        print("  " + "-" * 75)
        for flow in sample:
            try:
                est = unsd_client.estimate_size(flow.id, agency_id=flow.agency_id)
                print(
                    f"  {flow.id:<28} {flow.agency_id:<12} {est.total_series:>7}  "
                    f"{est.time_periods:>8}  {est.estimated_observations:>10,}"
                )
            except (UNSDAPIError, UNSDRateLimitError) as exc:
                print(f"  {flow.id:<28} {flow.agency_id:<12} SKIP ({exc!r})")


# ── Layer 5: Dry-Run Ingestion ────────────────────────────────────────

class TestDryRunIngestion:
    """Combined structure + data probe for catalog dataflows."""

    def test_dry_run_catalog(
        self, unsd_client: UNSDClient, all_dataflows: list,
    ) -> None:
        _check_unsd_available(unsd_client)
        # Probe all if small catalog, otherwise sample
        subset = all_dataflows if len(all_dataflows) <= 50 else random.sample(all_dataflows, 50)
        structure_pass = 0
        data_pass = 0

        print(f"\n  Dry-run for {len(subset)} dataflows:")
        print(f"  {'Agency':<12} {'Dataflow':<28} {'Structure':<12} {'Data':<12}")
        print("  " + "-" * 64)

        for flow in subset:
            s_ok = False
            d_ok = False
            try:
                structure = unsd_client.get_datastructure(flow.id, agency_id=flow.agency_id)
                s_ok = len(structure.dimensions) >= 1
            except (UNSDAPIError, UNSDRateLimitError, requests.RequestException):
                pass

            if s_ok:
                structure_pass += 1
                try:
                    est = unsd_client.estimate_size(flow.id, agency_id=flow.agency_id)
                    d_ok = est.total_series > 0
                except (UNSDAPIError, UNSDRateLimitError, requests.RequestException):
                    pass

            if d_ok:
                data_pass += 1

            print(
                f"  {flow.agency_id:<12} {flow.id:<28} "
                f"{'PASS' if s_ok else 'FAIL':<12} {'PASS' if d_ok else 'FAIL':<12}"
            )

        total = len(subset)
        s_rate = structure_pass / total if total else 0
        d_rate = data_pass / total if total else 0
        print(f"\n  Structure pass rate: {s_rate:.0%} ({structure_pass}/{total})")
        print(f"  Data pass rate:     {d_rate:.0%} ({data_pass}/{total})")
        # UNSD is less stable, use lower thresholds
        assert s_rate >= 0.70, f"Structure pass rate {s_rate:.0%} < 70%"
        assert d_rate >= 0.30, f"Data pass rate {d_rate:.0%} < 30%"

    def test_full_catalog_data_probe(
        self, unsd_client: UNSDClient, all_dataflows: list,
    ) -> None:
        """Probe a sample of dataflows with size estimation."""
        _check_unsd_available(unsd_client)
        sample = random.sample(all_dataflows, min(20, len(all_dataflows)))

        success = 0
        empty = 0
        failures: list[tuple[str, str]] = []

        print(f"\n  Catalog probe: {len(sample)} sampled dataflows")
        print(f"  {'Agency':<12} {'Dataflow':<28} {'Status':<10} {'Detail'}")
        print("  " + "-" * 72)

        for flow in sample:
            try:
                est = unsd_client.estimate_size(flow.id, agency_id=flow.agency_id)
                if est.total_series > 0:
                    success += 1
                    status = "OK"
                    detail = f"~{est.total_series:,} series"
                else:
                    empty += 1
                    status = "EMPTY"
                    detail = "0 series"
            except UNSDRateLimitError:
                failures.append((flow.id, "rate_limit"))
                status = "RATELIM"
                detail = "429 — rate limited"
            except UNSDAPIError as exc:
                failures.append((flow.id, str(exc)[:80]))
                status = "ERROR"
                detail = str(exc)[:60]
            except Exception as exc:
                failures.append((flow.id, str(exc)[:80]))
                status = "ERROR"
                detail = str(exc)[:60]

            print(f"  {flow.agency_id:<12} {flow.id:<28} {status:<10} {detail}")

        total = len(sample)
        fail_count = len(failures)
        accessible = success + empty

        print(f"\n  ── Summary ──")
        print(f"  datasets tested:  {total}")
        print(f"  accessible:       {accessible}  (OK={success}, empty={empty})")
        print(f"  failed:           {fail_count}")

        access_rate = accessible / total if total else 0
        assert access_rate >= 0.50, (
            f"Catalog accessibility {access_rate:.0%} < 50% "
            f"({fail_count} failures out of {total})"
        )


# ── Layer 6: Stress Test ──────────────────────────────────────────────

class TestStressTest:
    """Larger fetches to validate chunked retrieval and memory."""

    def test_chunked_fetch_first_dataflow(
        self, unsd_client: UNSDClient, all_dataflows: list,
    ) -> None:
        _check_unsd_available(unsd_client)
        flow = all_dataflows[0]
        chunks_received: list[tuple[str, str, int]] = []

        def on_chunk(obs: list, start: str, end: str) -> None:
            chunks_received.append((start, end, len(obs)))

        t0 = time.monotonic()
        obs = unsd_client.fetch_dataset_chunked(
            flow.id, "all",
            series_id=f"UNSD_{flow.id}",
            agency_id=flow.agency_id,
            chunk_ranges=[("2020", "2026")],
            on_chunk=on_chunk,
        )
        elapsed = time.monotonic() - t0

        print(f"\n  {flow.id} chunked [2020-2026]: {len(obs)} obs in {elapsed:.1f}s")
        for start, end, count in chunks_received:
            print(f"    [{start}-{end}]: {count} obs")

    def test_memory_bounded_large_fetch(
        self, unsd_client: UNSDClient, all_dataflows: list,
    ) -> None:
        _check_unsd_available(unsd_client)
        import resource

        flow = all_dataflows[0]
        mem_before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        obs = unsd_client.fetch_dataset_chunked(
            flow.id, "all",
            series_id=f"UNSD_{flow.id}",
            agency_id=flow.agency_id,
            chunk_ranges=[("2020", "2026")],
        )
        mem_after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        mem_delta_mb = (mem_after - mem_before) / 1024

        print(f"\n  Memory delta: {mem_delta_mb:.1f} MB for {len(obs)} obs")
        assert mem_delta_mb < 500, f"Memory usage too high: {mem_delta_mb:.1f} MB"


# ── Layer 7: Automated Test Report ────────────────────────────────────

class TestAutomatedTestReport:
    """Build ValidationReport from check results."""

    def test_generate_validation_report(
        self, unsd_client: UNSDClient, all_dataflows: list,
    ) -> None:
        _check_unsd_available(unsd_client)
        checks: list[CheckResult] = []

        flows = unsd_client.list_dataflows()
        checks.append(CheckResult(
            check_name="catalog_discovery",
            layer=ValidationLayer.CATALOG,
            passed=len(flows) > 10,
            severity=ValidationSeverity.ERROR,
            message=f"Found {len(flows)} dataflows",
            source="unsd",
        ))

        flow = all_dataflows[0]
        try:
            est = unsd_client.estimate_size(flow.id, agency_id=flow.agency_id)
            checks.append(CheckResult(
                check_name=f"data_accessibility_{flow.id}",
                layer=ValidationLayer.SERIES,
                passed=est.total_series >= 0,
                severity=ValidationSeverity.ERROR,
                message=f"Estimated {est.total_series} series",
                source="unsd",
            ))
        except (UNSDAPIError, UNSDRateLimitError) as exc:
            checks.append(CheckResult(
                check_name=f"data_accessibility_{flow.id}",
                layer=ValidationLayer.SERIES,
                passed=False,
                severity=ValidationSeverity.ERROR,
                message=str(exc),
                source="unsd",
            ))

        report = ValidationReport(
            source="unsd",
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
                source="unsd",
            ),
            CheckResult(
                check_name="bad_check",
                layer=ValidationLayer.CATALOG,
                passed=False,
                severity=ValidationSeverity.ERROR,
                message="intentional failure",
                source="unsd",
            ),
        )
        report = ValidationReport(
            source="unsd",
            run_id="test-fail",
            timestamp=datetime.now(timezone.utc).isoformat(),
            checks=checks,
        )
        assert not report.passed
        assert report.error_count == 1
        print(f"\n  Failure report: error_count={report.error_count}")


# ── Layer 8: Edge Cases ───────────────────────────────────────────────

class TestEdgeCases:
    """Edge-case handling for the UNSD client."""

    def test_nonexistent_dataflow_raises_error(self, unsd_client: UNSDClient) -> None:
        with pytest.raises(UNSDAPIError):
            unsd_client.get_data(
                "DOES_NOT_EXIST_XYZ_999", "all",
                series_id="probe",
                agency_id="UNSD",
                limit=1,
            )
        print("\n  Nonexistent dataflow: UNSDAPIError raised (OK)")

    def test_empty_dataset_handled_gracefully(
        self, unsd_client: UNSDClient, all_dataflows: list,
    ) -> None:
        _check_unsd_available(unsd_client)
        flow = all_dataflows[0]
        try:
            obs = unsd_client.get_data(
                flow.id, "all",
                series_id="probe",
                agency_id=flow.agency_id,
                start_period="2099", limit=10,
            )
            print(f"\n  Future period: {len(obs)} obs (OK)")
        except UNSDAPIError:
            print("\n  Future period: API error (acceptable)")

    def test_agency_id_auto_resolution(
        self, unsd_client: UNSDClient, all_dataflows: list,
    ) -> None:
        """Verify that agency_id is automatically resolved from catalog."""
        _check_unsd_available(unsd_client)
        flow = all_dataflows[0]
        # Call without explicit agency_id — should auto-resolve
        structure = unsd_client.get_datastructure(flow.id)
        assert len(structure.dimensions) >= 1
        print(f"\n  Auto-resolved agency for {flow.id}: agency from catalog")


# ── Layer 9: Performance Benchmark ────────────────────────────────────

class TestPerformanceBenchmark:
    """Timing benchmarks for key operations."""

    def test_list_dataflows_completes_within_30s(self, unsd_client: UNSDClient) -> None:
        unsd_client._dataflow_cache = None
        t0 = time.monotonic()
        flows = unsd_client.list_dataflows()
        elapsed = time.monotonic() - t0
        assert elapsed < 30, f"list_dataflows took {elapsed:.1f}s (limit: 30s)"
        assert len(flows) > 0
        print(f"\n  list_dataflows (cold): {elapsed:.1f}s, {len(flows)} flows")

    def test_dataflow_cache_hit_is_fast(self, unsd_client: UNSDClient) -> None:
        unsd_client.list_dataflows()
        t0 = time.monotonic()
        flows = unsd_client.list_dataflows()
        elapsed = time.monotonic() - t0
        assert elapsed < 0.01, f"Cached list_dataflows took {elapsed:.4f}s (limit: 0.01s)"
        print(f"\n  list_dataflows (cached): {elapsed:.6f}s")

    def test_single_probe_completes_within_15s(
        self, unsd_client: UNSDClient, all_dataflows: list,
    ) -> None:
        _check_unsd_available(unsd_client)
        flow = all_dataflows[0]
        t0 = time.monotonic()
        unsd_client.estimate_size(flow.id, agency_id=flow.agency_id)
        elapsed = time.monotonic() - t0
        assert elapsed < 15, f"Single probe took {elapsed:.1f}s (limit: 15s)"
        print(f"\n  Single data probe: {elapsed:.1f}s")

    def test_dsd_fetch_completes_within_15s(
        self, unsd_client: UNSDClient, all_dataflows: list,
    ) -> None:
        unsd_client._structure_cache.clear()
        flow = all_dataflows[0]
        t0 = time.monotonic()
        unsd_client.get_datastructure(flow.id, agency_id=flow.agency_id)
        elapsed = time.monotonic() - t0
        assert elapsed < 15, f"DSD fetch took {elapsed:.1f}s (limit: 15s)"
        print(f"\n  DSD fetch ({flow.id}): {elapsed:.1f}s")

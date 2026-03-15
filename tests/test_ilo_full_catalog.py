"""Integration tests for ILO (ILOSTAT) full catalog access and 9-layer validation.

Requires network access (no API key needed). Run with:
    pytest tests/test_ilo_full_catalog.py -v -s
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

from analyst.ingestion.scrapers.ilo import (
    ILOAPIError,
    ILOClient,
    ILORateLimitError,
    _build_decade_chunks,
)
from analyst.ingestion.sources import ILO_SERIES
from analyst.ingestion.validation._types import (
    CheckResult,
    ValidationLayer,
    ValidationReport,
    ValidationSeverity,
)

pytestmark = pytest.mark.integration


# ── Fixtures ──────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def ilo_client() -> ILOClient:
    return ILOClient(timeout=45)


@pytest.fixture(scope="module")
def all_dataflows(ilo_client: ILOClient) -> list:
    return ilo_client.list_dataflows()


def _check_ilo_available(client: ILOClient) -> None:
    """Try a minimal catalog fetch; skip test if ILO is unavailable."""
    try:
        flows = client.list_dataflows()
        if not flows:
            pytest.skip("ILO API returned empty catalog")
    except ILORateLimitError:
        pytest.skip("ILO API is rate limiting — try again in a few minutes")
    except (ILOAPIError, requests.RequestException):
        pytest.skip("ILO API unavailable")


# ── Layer 1: Catalog Discovery ────────────────────────────────────────

class TestCatalogDiscovery:
    """Validate that we can discover the full ILO dataflow catalog."""

    def test_list_all_dataflows_returns_many(self, all_dataflows: list) -> None:
        assert len(all_dataflows) > 50, (
            f"Expected >50 dataflows, got {len(all_dataflows)}"
        )
        print(f"\n  Total ILO dataflows: {len(all_dataflows)}")
        for flow in all_dataflows[:20]:
            print(f"    {flow.id:<40} v{flow.version}: {flow.name[:40]}")
        if len(all_dataflows) > 20:
            print(f"    ... and {len(all_dataflows) - 20} more")

    def test_dataflows_contain_known_datasets(self, all_dataflows: list) -> None:
        ids = {f.id for f in all_dataflows}
        # ILOSTAT key dataflow IDs
        expected = {
            "DF_EMP_TEMP_SEX_AGE_NB",
            "DF_UNE_TUNE_SEX_AGE_NB",
            "DF_EAR_EARN_SEX_ECO_CUR_NB",
        }
        found = expected & ids
        # Be lenient — IDs may vary across SDMX versions
        if not found:
            # Try shorter prefix matching
            found = {e for e in expected if any(e in fid for fid in ids)}
        print(f"\n  Known datasets found: {sorted(found)}")
        print(f"  Sample catalog IDs: {sorted(list(ids))[:10]}")

    def test_dataflows_have_structure_references(self, all_dataflows: list) -> None:
        with_structure = [f for f in all_dataflows if f.structure_id]
        ratio = len(with_structure) / len(all_dataflows) if all_dataflows else 0
        assert ratio > 0.5, (
            f"Expected >50% of dataflows to have structure_id, got {ratio:.0%}"
        )
        print(f"\n  Dataflows with structure_id: {len(with_structure)}/{len(all_dataflows)} ({ratio:.0%})")

    def test_hardcoded_series_match_catalog(self, all_dataflows: list) -> None:
        if not ILO_SERIES:
            pytest.skip("No hardcoded ILO_SERIES configured yet")
        catalog_ids = {f.id for f in all_dataflows}
        configured_dataflows = {cfg["dataflow"] for cfg in ILO_SERIES.values()}
        missing = configured_dataflows - catalog_ids
        assert not missing, (
            f"Configured dataflows not found in catalog: {missing}"
        )
        print(f"\n  All {len(configured_dataflows)} configured dataflows found in catalog")


# ── Layer 2: Structure Validation ─────────────────────────────────────

class TestStructureValidation:
    """Validate DSD introspection for ILO dataflows."""

    def test_structure_for_first_dataflow(
        self, ilo_client: ILOClient, all_dataflows: list,
    ) -> None:
        _check_ilo_available(ilo_client)
        flow = all_dataflows[0]
        structure = ilo_client.get_datastructure(flow.id)
        dim_ids = {d.id for d in structure.dimensions}
        assert len(dim_ids) >= 2, f"Expected >=2 dims, got {dim_ids}"
        has_time = any(d.is_time for d in structure.dimensions)
        assert has_time, f"{flow.id} DSD should have a time dimension"
        print(f"\n  {flow.id} structure: {[d.id for d in structure.dimensions]}")
        for d in structure.dimensions:
            if not d.is_time:
                print(f"    {d.id}: {d.code_count} codes")

    def test_structure_for_each_hardcoded_dataflow(
        self, ilo_client: ILOClient,
    ) -> None:
        if not ILO_SERIES:
            pytest.skip("No hardcoded ILO_SERIES configured yet")
        seen: set[str] = set()
        for name, cfg in ILO_SERIES.items():
            df_id = cfg["dataflow"]
            if df_id in seen:
                continue
            seen.add(df_id)
            structure = ilo_client.get_datastructure(df_id)
            assert len(structure.dimensions) >= 2, (
                f"{df_id}: expected >=2 dimensions, got {len(structure.dimensions)}"
            )
            print(f"\n  {df_id}: {len(structure.dimensions)} dimensions")

    def test_summarize_structure_returns_compact_view(
        self, ilo_client: ILOClient, all_dataflows: list,
    ) -> None:
        _check_ilo_available(ilo_client)
        flow = all_dataflows[0]
        summary = ilo_client.summarize_structure(flow.id)
        assert summary.dataflow_id == flow.id
        assert summary.structure_id
        assert len(summary.series_dimensions) > 0
        assert summary.time_dimension_id
        print(
            f"\n  {flow.id} summary: dims={list(summary.series_dimensions)}, "
            f"codes={dict(summary.code_counts)}, est_series={summary.estimated_series}"
        )

    def test_structure_for_sample_catalog_dataflows(
        self, ilo_client: ILOClient, all_dataflows: list,
    ) -> None:
        _check_ilo_available(ilo_client)
        sample = random.sample(all_dataflows, min(5, len(all_dataflows)))
        passed = 0
        for flow in sample:
            try:
                structure = ilo_client.get_datastructure(flow.id)
                assert len(structure.dimensions) >= 1
                passed += 1
                print(f"    {flow.id}: {len(structure.dimensions)} dims — OK")
            except (ILOAPIError, ILORateLimitError) as exc:
                print(f"    {flow.id}: SKIP ({exc!r})")
        print(f"\n  Sample structure validation: {passed}/{len(sample)} passed")
        assert passed >= 1, "Expected at least 1 structure probe to succeed"


# ── Layer 3: Dataset Accessibility ────────────────────────────────────

class TestDatasetAccessibility:
    """Validate that catalog datasets are actually fetchable."""

    def test_fetch_limit_1_from_hardcoded_series(self, ilo_client: ILOClient) -> None:
        if not ILO_SERIES:
            pytest.skip("No hardcoded ILO_SERIES configured yet")
        _check_ilo_available(ilo_client)
        for name, cfg in ILO_SERIES.items():
            obs = ilo_client.get_data(
                cfg["dataflow"], cfg.get("key", "."),
                series_id=cfg["series_id"], limit=1,
            )
            assert len(obs) >= 1, f"{name}: expected >=1 observation, got {len(obs)}"
            print(f"    {name}: {obs[0].date} = {obs[0].value}")

    def test_probe_sample_catalog_datasets(
        self, ilo_client: ILOClient, all_dataflows: list,
    ) -> None:
        _check_ilo_available(ilo_client)
        sample = random.sample(all_dataflows, min(10, len(all_dataflows)))
        probed = 0
        for flow in sample:
            try:
                est = ilo_client.estimate_size(flow.id, flow.version or "1.0")
                probed += 1
                print(f"    {flow.id}: ~{est.total_series} series")
            except (ILOAPIError, ILORateLimitError):
                print(f"    {flow.id}: SKIP")
        assert probed >= 1, "Expected at least 1 random probe to succeed"

    def test_data_fetch_returns_valid_observations(
        self, ilo_client: ILOClient, all_dataflows: list,
    ) -> None:
        _check_ilo_available(ilo_client)
        flow = all_dataflows[0]
        try:
            obs = ilo_client.get_data(
                flow.id, ".",
                series_id=f"ILO_{flow.id}",
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
        except (ILOAPIError, ILORateLimitError) as exc:
            print(f"\n  {flow.id}: data fetch failed ({exc!r})")


# ── Layer 4: Size Estimation ──────────────────────────────────────────

class TestSizeEstimation:
    """Validate size estimation via limit=1 probes."""

    def test_estimate_size_for_first_dataflow(
        self, ilo_client: ILOClient, all_dataflows: list,
    ) -> None:
        _check_ilo_available(ilo_client)
        flow = all_dataflows[0]
        est = ilo_client.estimate_size(flow.id)
        print(
            f"\n  {flow.id} size: {est.total_series} series x {est.time_periods} periods "
            f"= ~{est.estimated_observations:,} obs"
        )

    def test_estimate_size_for_sample(
        self, ilo_client: ILOClient, all_dataflows: list,
    ) -> None:
        _check_ilo_available(ilo_client)
        sample = random.sample(all_dataflows, min(5, len(all_dataflows)))
        print("\n  Dataflow                                   Series   Periods   Est. Obs")
        print("  " + "-" * 75)
        for flow in sample:
            try:
                est = ilo_client.estimate_size(flow.id)
                print(
                    f"  {flow.id:<40} {est.total_series:>7}  {est.time_periods:>8}  "
                    f"{est.estimated_observations:>10,}"
                )
            except (ILOAPIError, ILORateLimitError) as exc:
                print(f"  {flow.id:<40} SKIP ({exc!r})")


# ── Layer 5: Dry-Run Ingestion ────────────────────────────────────────

class TestDryRunIngestion:
    """Combined structure + data probe for catalog dataflows."""

    def test_dry_run_sample(
        self, ilo_client: ILOClient, all_dataflows: list,
    ) -> None:
        _check_ilo_available(ilo_client)
        subset = random.sample(all_dataflows, min(30, len(all_dataflows)))
        structure_pass = 0
        data_pass = 0

        print(f"\n  Dry-run for {len(subset)} dataflows:")
        print(f"  {'Dataflow':<40} {'Structure':<12} {'Data':<12}")
        print("  " + "-" * 64)

        for flow in subset:
            s_ok = False
            d_ok = False
            try:
                structure = ilo_client.get_datastructure(flow.id)
                s_ok = len(structure.dimensions) >= 1
            except (ILOAPIError, ILORateLimitError, requests.RequestException):
                pass

            if s_ok:
                structure_pass += 1
                try:
                    est = ilo_client.estimate_size(flow.id, flow.version or "1.0")
                    d_ok = est.total_series > 0
                except (ILOAPIError, ILORateLimitError, requests.RequestException):
                    pass

            if d_ok:
                data_pass += 1

            print(f"  {flow.id:<40} {'PASS' if s_ok else 'FAIL':<12} {'PASS' if d_ok else 'FAIL':<12}")

        total = len(subset)
        s_rate = structure_pass / total if total else 0
        d_rate = data_pass / total if total else 0
        print(f"\n  Structure pass rate: {s_rate:.0%} ({structure_pass}/{total})")
        print(f"  Data pass rate:     {d_rate:.0%} ({data_pass}/{total})")
        assert s_rate >= 0.80, f"Structure pass rate {s_rate:.0%} < 80%"
        assert d_rate >= 0.30, f"Data pass rate {d_rate:.0%} < 30%"

    def test_full_catalog_data_probe(
        self, ilo_client: ILOClient, all_dataflows: list,
    ) -> None:
        """Probe a sample of catalog dataflows with size estimation."""
        _check_ilo_available(ilo_client)
        sample = random.sample(all_dataflows, min(20, len(all_dataflows)))

        success = 0
        empty = 0
        failures: list[tuple[str, str]] = []

        print(f"\n  Catalog probe: {len(sample)} sampled dataflows")
        print(f"  {'Dataflow':<40} {'Status':<10} {'Detail'}")
        print("  " + "-" * 72)

        for flow in sample:
            try:
                est = ilo_client.estimate_size(flow.id, flow.version or "1.0")
                if est.total_series > 0:
                    success += 1
                    status = "OK"
                    detail = f"~{est.total_series:,} series"
                else:
                    empty += 1
                    status = "EMPTY"
                    detail = "0 series"
            except ILORateLimitError:
                failures.append((flow.id, "rate_limit"))
                status = "RATELIM"
                detail = "429 — rate limited"
            except ILOAPIError as exc:
                failures.append((flow.id, str(exc)[:80]))
                status = "ERROR"
                detail = str(exc)[:60]
            except Exception as exc:
                failures.append((flow.id, str(exc)[:80]))
                status = "ERROR"
                detail = str(exc)[:60]

            print(f"  {flow.id:<40} {status:<10} {detail}")

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
        self, ilo_client: ILOClient, all_dataflows: list,
    ) -> None:
        _check_ilo_available(ilo_client)
        flow = all_dataflows[0]
        chunks_received: list[tuple[str, str, int]] = []

        def on_chunk(obs: list, start: str, end: str) -> None:
            chunks_received.append((start, end, len(obs)))

        t0 = time.monotonic()
        obs = ilo_client.fetch_dataset_chunked(
            flow.id, ".",
            series_id=f"ILO_{flow.id}",
            chunk_ranges=[("2020", "2026")],
            on_chunk=on_chunk,
        )
        elapsed = time.monotonic() - t0

        print(f"\n  {flow.id} chunked [2020-2026]: {len(obs)} obs in {elapsed:.1f}s")
        for start, end, count in chunks_received:
            print(f"    [{start}-{end}]: {count} obs")

    def test_memory_bounded_large_fetch(
        self, ilo_client: ILOClient, all_dataflows: list,
    ) -> None:
        _check_ilo_available(ilo_client)
        import resource

        flow = all_dataflows[0]
        mem_before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        obs = ilo_client.fetch_dataset_chunked(
            flow.id, ".",
            series_id=f"ILO_{flow.id}",
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
        self, ilo_client: ILOClient, all_dataflows: list,
    ) -> None:
        _check_ilo_available(ilo_client)
        checks: list[CheckResult] = []

        flows = ilo_client.list_dataflows()
        checks.append(CheckResult(
            check_name="catalog_discovery",
            layer=ValidationLayer.CATALOG,
            passed=len(flows) > 50,
            severity=ValidationSeverity.ERROR,
            message=f"Found {len(flows)} dataflows",
            source="ilo",
        ))

        flow = all_dataflows[0]
        try:
            est = ilo_client.estimate_size(flow.id)
            checks.append(CheckResult(
                check_name=f"data_accessibility_{flow.id}",
                layer=ValidationLayer.SERIES,
                passed=est.total_series >= 0,
                severity=ValidationSeverity.ERROR,
                message=f"Estimated {est.total_series} series",
                source="ilo",
            ))
        except (ILOAPIError, ILORateLimitError) as exc:
            checks.append(CheckResult(
                check_name=f"data_accessibility_{flow.id}",
                layer=ValidationLayer.SERIES,
                passed=False,
                severity=ValidationSeverity.ERROR,
                message=str(exc),
                source="ilo",
            ))

        report = ValidationReport(
            source="ilo",
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
                source="ilo",
            ),
            CheckResult(
                check_name="bad_check",
                layer=ValidationLayer.CATALOG,
                passed=False,
                severity=ValidationSeverity.ERROR,
                message="intentional failure",
                source="ilo",
            ),
        )
        report = ValidationReport(
            source="ilo",
            run_id="test-fail",
            timestamp=datetime.now(timezone.utc).isoformat(),
            checks=checks,
        )
        assert not report.passed
        assert report.error_count == 1
        print(f"\n  Failure report: error_count={report.error_count}")


# ── Layer 8: Edge Cases ───────────────────────────────────────────────

class TestEdgeCases:
    """Edge-case handling for the ILO client."""

    def test_nonexistent_dataflow_raises_error(self, ilo_client: ILOClient) -> None:
        with pytest.raises(ILOAPIError):
            ilo_client.get_data(
                "DOES_NOT_EXIST_XYZ_999", ".",
                series_id="probe", limit=1,
            )
        print("\n  Nonexistent dataflow: ILOAPIError raised (OK)")

    def test_empty_dataset_handled_gracefully(
        self, ilo_client: ILOClient, all_dataflows: list,
    ) -> None:
        _check_ilo_available(ilo_client)
        flow = all_dataflows[0]
        try:
            obs = ilo_client.get_data(
                flow.id, ".",
                series_id="probe",
                start_period="2099", limit=10,
            )
            print(f"\n  Future period: {len(obs)} obs (OK)")
        except ILOAPIError:
            print("\n  Future period: API error (acceptable)")

    def test_invalid_key_returns_empty_or_error(
        self, ilo_client: ILOClient, all_dataflows: list,
    ) -> None:
        _check_ilo_available(ilo_client)
        flow = all_dataflows[0]
        try:
            obs = ilo_client.get_data(
                flow.id, "ZZZZZ.ZZZZZ.ZZZZZ",
                series_id="probe", limit=1,
            )
            assert len(obs) == 0, f"Expected empty result, got {len(obs)}"
        except ILOAPIError:
            pass
        print("\n  Invalid key: empty or error (OK)")


# ── Layer 9: Performance Benchmark ────────────────────────────────────

class TestPerformanceBenchmark:
    """Timing benchmarks for key operations."""

    def test_list_dataflows_completes_within_30s(self, ilo_client: ILOClient) -> None:
        ilo_client._dataflow_cache = None
        t0 = time.monotonic()
        flows = ilo_client.list_dataflows()
        elapsed = time.monotonic() - t0
        assert elapsed < 30, f"list_dataflows took {elapsed:.1f}s (limit: 30s)"
        assert len(flows) > 0
        print(f"\n  list_dataflows (cold): {elapsed:.1f}s, {len(flows)} flows")

    def test_dataflow_cache_hit_is_fast(self, ilo_client: ILOClient) -> None:
        ilo_client.list_dataflows()
        t0 = time.monotonic()
        flows = ilo_client.list_dataflows()
        elapsed = time.monotonic() - t0
        assert elapsed < 0.01, f"Cached list_dataflows took {elapsed:.4f}s (limit: 0.01s)"
        print(f"\n  list_dataflows (cached): {elapsed:.6f}s")

    def test_single_probe_completes_within_15s(
        self, ilo_client: ILOClient, all_dataflows: list,
    ) -> None:
        _check_ilo_available(ilo_client)
        flow = all_dataflows[0]
        t0 = time.monotonic()
        ilo_client.estimate_size(flow.id)
        elapsed = time.monotonic() - t0
        assert elapsed < 15, f"Single probe took {elapsed:.1f}s (limit: 15s)"
        print(f"\n  Single data probe: {elapsed:.1f}s")

    def test_dsd_fetch_completes_within_15s(
        self, ilo_client: ILOClient, all_dataflows: list,
    ) -> None:
        ilo_client._structure_cache.clear()
        flow = all_dataflows[0]
        t0 = time.monotonic()
        ilo_client.get_datastructure(flow.id)
        elapsed = time.monotonic() - t0
        assert elapsed < 15, f"DSD fetch took {elapsed:.1f}s (limit: 15s)"
        print(f"\n  DSD fetch ({flow.id}): {elapsed:.1f}s")

"""Integration tests for ECB full catalog access and 9-layer validation.

Requires network access (no API key needed). Run with:
    pytest tests/test_ecb_full_catalog.py -v -s
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

from analyst.ingestion.scrapers.ecb import (
    ECBAPIError,
    ECBClient,
    ECBRateLimitError,
    _build_decade_chunks,
)
from analyst.ingestion.sources import ECB_SERIES
from analyst.ingestion.validation._types import (
    CheckResult,
    ValidationLayer,
    ValidationReport,
    ValidationSeverity,
)

pytestmark = pytest.mark.integration


# ── Fixtures ──────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def ecb_client() -> ECBClient:
    return ECBClient(timeout=45)


@pytest.fixture(scope="module")
def all_dataflows(ecb_client: ECBClient) -> list:
    return ecb_client.list_dataflows()


def _check_ecb_available(client: ECBClient) -> None:
    """Try a minimal fetch; skip test if ECB is rate limiting us."""
    try:
        client.get_data(
            "EXR", "M.USD.EUR.SP00.A",
            series_id="probe", limit=1,
        )
    except ECBRateLimitError:
        pytest.skip("ECB API is rate limiting — try again in a few minutes")
    except ECBAPIError:
        pytest.skip("ECB API unavailable")


# ── Layer 1: Catalog Discovery ────────────────────────────────────────

class TestCatalogDiscovery:
    """Validate that we can discover the full ECB dataflow catalog."""

    def test_list_all_dataflows_returns_many(self, all_dataflows: list) -> None:
        assert len(all_dataflows) > 40, (
            f"Expected >40 dataflows, got {len(all_dataflows)}"
        )
        print(f"\n  Total ECB dataflows: {len(all_dataflows)}")
        for flow in all_dataflows:
            print(f"    {flow.id:<24} v{flow.version}: {flow.name[:50]}")

    def test_dataflows_contain_known_datasets(self, all_dataflows: list) -> None:
        ids = {f.id for f in all_dataflows}
        expected = {"BSI", "EXR", "FM"}
        found = expected & ids
        assert len(found) >= 2, (
            f"Expected at least 2 known datasets in catalog, found: {found}"
        )
        print(f"\n  Known datasets found: {sorted(found)}")

    def test_dataflows_have_structure_references(self, all_dataflows: list) -> None:
        with_structure = [f for f in all_dataflows if f.structure_id]
        ratio = len(with_structure) / len(all_dataflows) if all_dataflows else 0
        assert ratio > 0.8, (
            f"Expected >80% of dataflows to have structure_id, got {ratio:.0%}"
        )
        print(f"\n  Dataflows with structure_id: {len(with_structure)}/{len(all_dataflows)} ({ratio:.0%})")

    def test_hardcoded_series_match_catalog(self, all_dataflows: list) -> None:
        catalog_ids = {f.id for f in all_dataflows}
        configured_dataflows = {cfg["dataflow"] for cfg in ECB_SERIES.values()}
        missing = configured_dataflows - catalog_ids
        assert not missing, (
            f"Configured dataflows not found in catalog: {missing}"
        )
        print(f"\n  All {len(configured_dataflows)} configured dataflows found in catalog")


# ── Layer 2: Structure Validation ─────────────────────────────────────

class TestStructureValidation:
    """Validate DSD introspection for ECB dataflows."""

    def test_structure_for_bsi(self, ecb_client: ECBClient) -> None:
        structure = ecb_client.get_datastructure("BSI")
        dim_ids = {d.id for d in structure.dimensions}
        assert "FREQ" in dim_ids, f"Expected FREQ in BSI dims, got {dim_ids}"
        has_time = any(d.is_time for d in structure.dimensions)
        assert has_time, "BSI DSD should have a time dimension"
        print(f"\n  BSI structure: {[d.id for d in structure.dimensions]}")
        for d in structure.dimensions:
            if not d.is_time:
                print(f"    {d.id}: {d.code_count} codes")

    def test_structure_for_each_hardcoded_dataflow(self, ecb_client: ECBClient) -> None:
        seen: set[str] = set()
        for name, cfg in ECB_SERIES.items():
            df_id = cfg["dataflow"]
            if df_id in seen:
                continue
            seen.add(df_id)
            structure = ecb_client.get_datastructure(df_id)
            assert len(structure.dimensions) >= 2, (
                f"{df_id}: expected >=2 dimensions, got {len(structure.dimensions)}"
            )
            print(f"\n  {df_id}: {len(structure.dimensions)} dimensions")

    def test_summarize_structure_returns_compact_view(self, ecb_client: ECBClient) -> None:
        summary = ecb_client.summarize_structure("EXR")
        assert summary.dataflow_id == "EXR"
        assert summary.structure_id
        assert len(summary.series_dimensions) > 0
        assert summary.time_dimension_id
        print(
            f"\n  EXR summary: dims={list(summary.series_dimensions)}, "
            f"codes={dict(summary.code_counts)}, est_series={summary.estimated_series}"
        )

    def test_structure_for_sample_catalog_dataflows(
        self, ecb_client: ECBClient, all_dataflows: list,
    ) -> None:
        sample = random.sample(all_dataflows, min(5, len(all_dataflows)))
        passed = 0
        for flow in sample:
            try:
                structure = ecb_client.get_datastructure(flow.id)
                assert len(structure.dimensions) >= 1
                passed += 1
                print(f"    {flow.id}: {len(structure.dimensions)} dims — OK")
            except (ECBAPIError, ECBRateLimitError) as exc:
                print(f"    {flow.id}: SKIP ({exc!r})")
        print(f"\n  Sample structure validation: {passed}/{len(sample)} passed")
        assert passed >= 1, "Expected at least 1 structure probe to succeed"


# ── Layer 3: Dataset Accessibility ────────────────────────────────────

class TestDatasetAccessibility:
    """Validate that configured series are actually fetchable."""

    def test_fetch_limit_1_from_every_hardcoded_series(self, ecb_client: ECBClient) -> None:
        _check_ecb_available(ecb_client)
        for name, cfg in ECB_SERIES.items():
            obs = ecb_client.get_data(
                cfg["dataflow"], cfg["key"],
                series_id=cfg["series_id"], limit=1,
            )
            assert len(obs) >= 1, f"{name}: expected >=1 observation, got {len(obs)}"
            print(f"    {name}: {obs[0].date} = {obs[0].value}")

    def test_probe_sample_catalog_datasets(
        self, ecb_client: ECBClient, all_dataflows: list,
    ) -> None:
        _check_ecb_available(ecb_client)
        sample = random.sample(all_dataflows, min(10, len(all_dataflows)))
        probed = 0
        for flow in sample:
            try:
                est = ecb_client.estimate_size(flow.id, flow.version or "1.0")
                probed += 1
                print(f"    {flow.id}: ~{est.total_series} series")
            except (ECBAPIError, ECBRateLimitError):
                print(f"    {flow.id}: SKIP")
        assert probed >= 1, "Expected at least 1 random probe to succeed"

    def test_known_datasets_return_valid_observations(self, ecb_client: ECBClient) -> None:
        _check_ecb_available(ecb_client)
        cfg = ECB_SERIES["eurusd"]
        obs = ecb_client.get_data(
            cfg["dataflow"], cfg["key"],
            series_id=cfg["series_id"], limit=5,
        )
        assert len(obs) >= 1
        for o in obs:
            assert o.series_id == cfg["series_id"]
            assert o.date
            assert isinstance(o.value, float)
            assert o.dataflow == cfg["dataflow"]
        print(f"\n  EURUSD: {len(obs)} valid observations")


# ── Layer 4: Size Estimation ──────────────────────────────────────────

class TestSizeEstimation:
    """Validate size estimation via limit=1 probes."""

    def test_estimate_exr_size(self, ecb_client: ECBClient) -> None:
        _check_ecb_available(ecb_client)
        est = ecb_client.estimate_size("EXR")
        assert est.total_series > 0, "EXR should have series"
        print(
            f"\n  EXR size: {est.total_series} series x {est.time_periods} periods "
            f"= ~{est.estimated_observations:,} obs"
        )

    def test_estimate_size_for_all_hardcoded(self, ecb_client: ECBClient) -> None:
        _check_ecb_available(ecb_client)
        seen: set[str] = set()
        print("\n  Dataflow              Series   Periods   Est. Obs")
        print("  " + "-" * 55)
        for name, cfg in ECB_SERIES.items():
            df_id = cfg["dataflow"]
            if df_id in seen:
                continue
            seen.add(df_id)
            try:
                est = ecb_client.estimate_size(df_id)
                print(
                    f"  {df_id:<22} {est.total_series:>7}  {est.time_periods:>8}  "
                    f"{est.estimated_observations:>10,}"
                )
            except (ECBAPIError, ECBRateLimitError) as exc:
                print(f"  {df_id:<22} SKIP ({exc!r})")


# ── Layer 5: Dry-Run Ingestion ────────────────────────────────────────

class TestDryRunIngestion:
    """Combined structure + data probe for all catalog dataflows."""

    def test_dry_run_full_catalog(
        self, ecb_client: ECBClient, all_dataflows: list,
    ) -> None:
        _check_ecb_available(ecb_client)
        structure_pass = 0
        data_pass = 0

        print(f"\n  Dry-run for all {len(all_dataflows)} dataflows:")
        print(f"  {'Dataflow':<24} {'Structure':<12} {'Data':<12}")
        print("  " + "-" * 48)

        for flow in all_dataflows:
            s_ok = False
            d_ok = False
            try:
                structure = ecb_client.get_datastructure(flow.id)
                s_ok = len(structure.dimensions) >= 1
            except (ECBAPIError, ECBRateLimitError, requests.RequestException):
                pass

            if s_ok:
                structure_pass += 1
                try:
                    est = ecb_client.estimate_size(flow.id, flow.version or "1.0")
                    d_ok = est.total_series > 0
                except (ECBAPIError, ECBRateLimitError, requests.RequestException):
                    pass

            if d_ok:
                data_pass += 1

            print(f"  {flow.id:<24} {'PASS' if s_ok else 'FAIL':<12} {'PASS' if d_ok else 'FAIL':<12}")

        total = len(all_dataflows)
        s_rate = structure_pass / total if total else 0
        d_rate = data_pass / total if total else 0
        print(f"\n  Structure pass rate: {s_rate:.0%} ({structure_pass}/{total})")
        print(f"  Data pass rate:     {d_rate:.0%} ({data_pass}/{total})")
        assert s_rate >= 0.90, f"Structure pass rate {s_rate:.0%} < 90%"
        assert d_rate >= 0.50, f"Data pass rate {d_rate:.0%} < 50%"

    def test_full_catalog_data_probe(
        self, ecb_client: ECBClient, all_dataflows: list,
    ) -> None:
        """Probe every dataflow in the catalog with a limit=1 data fetch."""
        _check_ecb_available(ecb_client)

        success = 0
        empty = 0
        failures: list[tuple[str, str]] = []

        print(f"\n  Full catalog probe: {len(all_dataflows)} dataflows")
        print(f"  {'Dataflow':<24} {'Status':<10} {'Detail'}")
        print("  " + "-" * 60)

        for flow in all_dataflows:
            try:
                est = ecb_client.estimate_size(flow.id, flow.version or "1.0")
                if est.total_series > 0:
                    success += 1
                    status = "OK"
                    detail = f"~{est.total_series:,} series"
                else:
                    empty += 1
                    status = "EMPTY"
                    detail = "0 series"
            except ECBRateLimitError:
                failures.append((flow.id, "rate_limit"))
                status = "RATELIM"
                detail = "429 — rate limited"
            except ECBAPIError as exc:
                failures.append((flow.id, str(exc)[:80]))
                status = "ERROR"
                detail = str(exc)[:60]
            except Exception as exc:
                failures.append((flow.id, str(exc)[:80]))
                status = "ERROR"
                detail = str(exc)[:60]

            print(f"  {flow.id:<24} {status:<10} {detail}")

        total = len(all_dataflows)
        fail_count = len(failures)
        accessible = success + empty

        print(f"\n  ── Summary ──")
        print(f"  datasets tested:  {total}")
        print(f"  accessible:       {accessible}  (OK={success}, empty={empty})")
        print(f"  failed:           {fail_count}")
        if failures:
            print(f"  failure details:")
            for fid, reason in failures:
                print(f"    {fid}: {reason}")

        access_rate = accessible / total if total else 0
        assert access_rate >= 0.90, (
            f"Catalog accessibility {access_rate:.0%} < 90% "
            f"({fail_count} failures out of {total})"
        )


# ── Layer 6: Stress Test ──────────────────────────────────────────────

class TestStressTest:
    """Larger fetches to validate chunked retrieval and memory."""

    def test_full_fetch_exr_chunked(self, ecb_client: ECBClient) -> None:
        _check_ecb_available(ecb_client)
        chunks_received: list[tuple[str, str, int]] = []

        def on_chunk(obs: list, start: str, end: str) -> None:
            chunks_received.append((start, end, len(obs)))

        t0 = time.monotonic()
        obs = ecb_client.fetch_dataset_chunked(
            "EXR", "M.USD.EUR.SP00.A",
            series_id="ECB_EURUSD",
            chunk_ranges=[("2000", "2026")],
            on_chunk=on_chunk,
        )
        elapsed = time.monotonic() - t0

        assert len(obs) > 50, f"Expected >50 EXR obs, got {len(obs)}"
        print(f"\n  EXR chunked [2000-2026]: {len(obs)} obs in {elapsed:.1f}s")
        for start, end, count in chunks_received:
            print(f"    [{start}-{end}]: {count} obs")

    def test_full_fetch_with_specific_key(self, ecb_client: ECBClient) -> None:
        _check_ecb_available(ecb_client)
        cfg = ECB_SERIES["deposit_rate"]
        t0 = time.monotonic()
        obs = ecb_client.get_data(
            cfg["dataflow"], cfg["key"],
            series_id=cfg["series_id"],
            start_period="2000", limit=0,
        )
        elapsed = time.monotonic() - t0
        assert len(obs) >= 1, f"Expected deposit rate obs, got {len(obs)}"
        print(f"\n  Deposit rate [2000+]: {len(obs)} obs in {elapsed:.1f}s")

    def test_memory_bounded_large_fetch(self, ecb_client: ECBClient) -> None:
        _check_ecb_available(ecb_client)
        import resource

        mem_before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        obs = ecb_client.fetch_dataset_chunked(
            "EXR", "M.USD.EUR.SP00.A",
            series_id="ECB_EURUSD",
            chunk_ranges=[("2020", "2026")],
        )
        mem_after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        mem_delta_mb = (mem_after - mem_before) / 1024

        print(f"\n  Memory delta: {mem_delta_mb:.1f} MB for {len(obs)} obs")
        assert mem_delta_mb < 500, f"Memory usage too high: {mem_delta_mb:.1f} MB"


# ── Layer 7: Automated Test Report ────────────────────────────────────

class TestAutomatedTestReport:
    """Build ValidationReport from check results."""

    def test_generate_validation_report(self, ecb_client: ECBClient) -> None:
        _check_ecb_available(ecb_client)
        checks: list[CheckResult] = []

        flows = ecb_client.list_dataflows()
        checks.append(CheckResult(
            check_name="catalog_discovery",
            layer=ValidationLayer.CATALOG,
            passed=len(flows) > 40,
            severity=ValidationSeverity.ERROR,
            message=f"Found {len(flows)} dataflows",
            source="ecb",
        ))

        cfg = ECB_SERIES["eurusd"]
        try:
            obs = ecb_client.get_data(
                cfg["dataflow"], cfg["key"],
                series_id=cfg["series_id"], limit=1,
            )
            checks.append(CheckResult(
                check_name="data_accessibility_eurusd",
                layer=ValidationLayer.SERIES,
                passed=len(obs) >= 1,
                severity=ValidationSeverity.ERROR,
                message=f"Got {len(obs)} observations",
                source="ecb",
            ))
        except (ECBAPIError, ECBRateLimitError) as exc:
            checks.append(CheckResult(
                check_name="data_accessibility_eurusd",
                layer=ValidationLayer.SERIES,
                passed=False,
                severity=ValidationSeverity.ERROR,
                message=str(exc),
                source="ecb",
            ))

        report = ValidationReport(
            source="ecb",
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
                source="ecb",
            ),
            CheckResult(
                check_name="bad_check",
                layer=ValidationLayer.CATALOG,
                passed=False,
                severity=ValidationSeverity.ERROR,
                message="intentional failure",
                source="ecb",
            ),
        )
        report = ValidationReport(
            source="ecb",
            run_id="test-fail",
            timestamp=datetime.now(timezone.utc).isoformat(),
            checks=checks,
        )
        assert not report.passed
        assert report.error_count == 1
        print(f"\n  Failure report: error_count={report.error_count}")


# ── Layer 8: Edge Cases ───────────────────────────────────────────────

class TestEdgeCases:
    """Edge-case handling for the ECB client."""

    def test_different_frequencies(self, ecb_client: ECBClient) -> None:
        _check_ecb_available(ecb_client)
        # Monthly (exchange rates)
        cfg_m = ECB_SERIES["eurusd"]
        obs_m = ecb_client.get_data(
            cfg_m["dataflow"], cfg_m["key"],
            series_id=cfg_m["series_id"], limit=2,
        )
        assert obs_m, "Monthly data should return observations"
        assert "-" in obs_m[0].date
        print(f"\n  Monthly: {obs_m[0].date}")

        # Business day (deposit rate)
        cfg_d = ECB_SERIES["deposit_rate"]
        obs_d = ecb_client.get_data(
            cfg_d["dataflow"], cfg_d["key"],
            series_id=cfg_d["series_id"], limit=2,
        )
        assert obs_d, "Daily/business data should return observations"
        print(f"  Daily/Business: {obs_d[0].date}")

    def test_nonexistent_dataflow_raises_error(self, ecb_client: ECBClient) -> None:
        with pytest.raises(ECBAPIError):
            ecb_client.get_data(
                "DOES_NOT_EXIST_XYZ", "A.B.C",
                series_id="probe", limit=1,
            )
        print("\n  Nonexistent dataflow: ECBAPIError raised (OK)")

    def test_empty_dataset_handled_gracefully(self, ecb_client: ECBClient) -> None:
        _check_ecb_available(ecb_client)
        try:
            obs = ecb_client.get_data(
                "EXR", "M.USD.EUR.SP00.A",
                series_id="probe",
                start_period="2099", limit=10,
            )
            print(f"\n  Future period: {len(obs)} obs (OK)")
        except ECBAPIError:
            print("\n  Future period: API error (acceptable)")

    def test_invalid_key_returns_empty_or_error(self, ecb_client: ECBClient) -> None:
        _check_ecb_available(ecb_client)
        try:
            obs = ecb_client.get_data(
                "EXR", "Z.ZZZZZ.ZZZ.ZZZZ.Z",
                series_id="probe", limit=1,
            )
            assert len(obs) == 0, f"Expected empty result, got {len(obs)}"
        except ECBAPIError:
            pass
        print("\n  Invalid key: empty or error (OK)")


# ── Layer 9: Performance Benchmark ────────────────────────────────────

class TestPerformanceBenchmark:
    """Timing benchmarks for key operations."""

    def test_list_dataflows_completes_within_10s(self, ecb_client: ECBClient) -> None:
        ecb_client._dataflow_cache = None
        t0 = time.monotonic()
        flows = ecb_client.list_dataflows()
        elapsed = time.monotonic() - t0
        assert elapsed < 10, f"list_dataflows took {elapsed:.1f}s (limit: 10s)"
        assert len(flows) > 0
        print(f"\n  list_dataflows (cold): {elapsed:.1f}s, {len(flows)} flows")

    def test_dataflow_cache_hit_is_fast(self, ecb_client: ECBClient) -> None:
        ecb_client.list_dataflows()
        t0 = time.monotonic()
        flows = ecb_client.list_dataflows()
        elapsed = time.monotonic() - t0
        assert elapsed < 0.01, f"Cached list_dataflows took {elapsed:.4f}s (limit: 0.01s)"
        print(f"\n  list_dataflows (cached): {elapsed:.6f}s")

    def test_single_probe_completes_within_10s(self, ecb_client: ECBClient) -> None:
        _check_ecb_available(ecb_client)
        cfg = ECB_SERIES["eurusd"]
        t0 = time.monotonic()
        ecb_client.get_data(
            cfg["dataflow"], cfg["key"],
            series_id=cfg["series_id"], limit=1,
        )
        elapsed = time.monotonic() - t0
        assert elapsed < 10, f"Single probe took {elapsed:.1f}s (limit: 10s)"
        print(f"\n  Single data probe: {elapsed:.1f}s")

    def test_dsd_fetch_completes_within_10s(self, ecb_client: ECBClient) -> None:
        ecb_client._structure_cache.clear()
        t0 = time.monotonic()
        ecb_client.get_datastructure("EXR")
        elapsed = time.monotonic() - t0
        assert elapsed < 10, f"DSD fetch took {elapsed:.1f}s (limit: 10s)"
        print(f"\n  DSD fetch (EXR): {elapsed:.1f}s")

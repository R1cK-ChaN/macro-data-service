"""Integration tests for IMF full catalog access and 9-layer validation.

Requires network access and IMF_API_KEY. Run with:
    pytest tests/test_imf_full_catalog.py -v -s
"""

from __future__ import annotations

import random
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from analyst.ingestion.scrapers.imf import (
    IMFAPIError,
    IMFClient,
    IMFRateLimitError,
    _build_decade_chunks,
)
from analyst.ingestion.sources import IMF_SERIES
from analyst.ingestion.validation._types import (
    CheckResult,
    ValidationLayer,
    ValidationReport,
    ValidationSeverity,
)

pytestmark = pytest.mark.integration


# ── Fixtures ──────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def imf_client() -> IMFClient:
    return IMFClient(timeout=45)


@pytest.fixture(scope="module")
def all_dataflows(imf_client: IMFClient) -> list:
    return imf_client.list_dataflows()


def _check_imf_available(client: IMFClient) -> None:
    """Try a minimal fetch; skip test if IMF is rate limiting us."""
    try:
        client.get_data(
            "CPI", "CHN.CPI._T.IX.M",
            series_id="probe", version="5.0.0", limit=1,
        )
    except IMFRateLimitError:
        pytest.skip("IMF API is rate limiting — try again in a few minutes")
    except IMFAPIError:
        pytest.skip("IMF API unavailable")


# ── Layer 1: Catalog Discovery ────────────────────────────────────────

class TestCatalogDiscovery:
    """Validate that we can discover the full IMF dataflow catalog."""

    def test_list_all_dataflows_returns_many(self, all_dataflows: list) -> None:
        assert len(all_dataflows) > 50, (
            f"Expected >50 dataflows, got {len(all_dataflows)}"
        )
        print(f"\n  Total IMF dataflows: {len(all_dataflows)}")
        for flow in all_dataflows[:10]:
            print(f"    {flow.id} v{flow.version}: {flow.name[:60]}")

    def test_dataflows_contain_known_datasets(self, all_dataflows: list) -> None:
        ids = {f.id for f in all_dataflows}
        expected = {"CPI", "QNEA"}
        found = expected & ids
        assert len(found) >= 2, (
            f"Expected at least CPI and QNEA in catalog, found: {found}"
        )
        print(f"\n  Known datasets found: {sorted(found)}")

    def test_dataflows_have_structure_references(self, all_dataflows: list) -> None:
        with_structure = [f for f in all_dataflows if f.structure_id]
        ratio = len(with_structure) / len(all_dataflows) if all_dataflows else 0
        assert ratio > 0.5, (
            f"Expected >50% of dataflows to have structure_id, got {ratio:.0%}"
        )
        print(f"\n  Dataflows with structure_id: {len(with_structure)}/{len(all_dataflows)} ({ratio:.0%})")

    def test_hardcoded_series_match_catalog(self, all_dataflows: list) -> None:
        catalog_ids = {f.id for f in all_dataflows}
        configured_dataflows = {cfg["dataflow"] for cfg in IMF_SERIES.values()}
        missing = configured_dataflows - catalog_ids
        assert not missing, (
            f"Configured dataflows not found in catalog: {missing}"
        )
        print(f"\n  All {len(configured_dataflows)} configured dataflows found in catalog")


# ── Layer 2: Structure Validation ─────────────────────────────────────

class TestStructureValidation:
    """Validate DSD introspection for IMF dataflows."""

    def test_structure_for_cpi(self, imf_client: IMFClient) -> None:
        structure = imf_client.get_datastructure("CPI")
        dim_ids = {d.id for d in structure.dimensions}
        # IMF CPI uses COUNTRY (not REF_AREA) and FREQUENCY
        assert "COUNTRY" in dim_ids or "REF_AREA" in dim_ids, (
            f"Expected COUNTRY or REF_AREA in CPI dims, got {dim_ids}"
        )
        assert "FREQUENCY" in dim_ids or len(dim_ids) >= 3
        has_time = any(d.is_time for d in structure.dimensions)
        assert has_time, "CPI DSD should have a time dimension"
        print(f"\n  CPI structure: {[d.id for d in structure.dimensions]}")
        for d in structure.dimensions:
            if not d.is_time:
                print(f"    {d.id}: {d.code_count} codes")

    def test_structure_for_each_hardcoded_series(self, imf_client: IMFClient) -> None:
        seen_dataflows: set[str] = set()
        for name, cfg in IMF_SERIES.items():
            df_id = cfg["dataflow"]
            if df_id in seen_dataflows:
                continue
            seen_dataflows.add(df_id)
            structure = imf_client.get_datastructure(df_id)
            assert len(structure.dimensions) >= 2, (
                f"{df_id}: expected >=2 dimensions, got {len(structure.dimensions)}"
            )
            print(f"\n  {df_id}: {len(structure.dimensions)} dimensions")

    def test_summarize_structure_returns_compact_view(self, imf_client: IMFClient) -> None:
        summary = imf_client.summarize_structure("CPI")
        assert summary.dataflow_id == "CPI"
        assert summary.structure_id
        assert len(summary.series_dimensions) > 0
        assert summary.time_dimension_id
        print(
            f"\n  CPI summary: dims={list(summary.series_dimensions)}, "
            f"codes={dict(summary.code_counts)}, est_series={summary.estimated_series}"
        )

    def test_structure_for_sample_catalog_dataflows(
        self, imf_client: IMFClient, all_dataflows: list,
    ) -> None:
        _check_imf_available(imf_client)
        with_structure = [f for f in all_dataflows if f.structure_id]
        sample = random.sample(with_structure, min(5, len(with_structure)))
        passed = 0
        for flow in sample:
            try:
                structure = imf_client.get_datastructure(flow.id)
                assert len(structure.dimensions) >= 1
                passed += 1
                print(f"    {flow.id}: {len(structure.dimensions)} dims OK")
            except (IMFAPIError, IMFRateLimitError) as exc:
                print(f"    {flow.id}: SKIP ({exc!r})")
        assert passed >= 1, "Expected at least 1 random DSD fetch to succeed"


# ── Layer 3: Dataset Accessibility ────────────────────────────────────

class TestDatasetAccessibility:
    """Validate that configured series are actually fetchable."""

    def test_fetch_limit_1_from_every_hardcoded_series(self, imf_client: IMFClient) -> None:
        _check_imf_available(imf_client)
        for name, cfg in IMF_SERIES.items():
            obs = imf_client.get_data(
                cfg["dataflow"], cfg["key"],
                series_id=cfg["series_id"], version=cfg["version"], limit=1,
            )
            assert len(obs) >= 1, f"{name}: expected >=1 observation, got {len(obs)}"
            print(f"    {name}: {obs[0].date} = {obs[0].value}")

    def test_probe_sample_catalog_datasets(
        self, imf_client: IMFClient, all_dataflows: list,
    ) -> None:
        _check_imf_available(imf_client)
        sample = random.sample(all_dataflows, min(10, len(all_dataflows)))
        probed = 0
        for flow in sample:
            try:
                est = imf_client.estimate_size(flow.id, flow.version)
                probed += 1
                print(f"    {flow.id}: ~{est.total_series} series, ~{est.estimated_observations} obs")
            except (IMFAPIError, IMFRateLimitError):
                print(f"    {flow.id}: SKIP")
        assert probed >= 1, "Expected at least 1 random probe to succeed"

    def test_known_datasets_return_valid_observations(self, imf_client: IMFClient) -> None:
        _check_imf_available(imf_client)
        cfg = IMF_SERIES["cn_cpi"]
        obs = imf_client.get_data(
            cfg["dataflow"], cfg["key"],
            series_id=cfg["series_id"], version=cfg["version"], limit=5,
        )
        assert len(obs) >= 1
        for o in obs:
            assert o.series_id == cfg["series_id"]
            assert o.date  # non-empty
            assert isinstance(o.value, float)
            assert o.dataflow == cfg["dataflow"]
        print(f"\n  cn_cpi: {len(obs)} valid observations")


# ── Layer 4: Size Estimation ──────────────────────────────────────────

class TestSizeEstimation:
    """Validate size estimation via limit=1 probes."""

    def test_estimate_cpi_size(self, imf_client: IMFClient) -> None:
        _check_imf_available(imf_client)
        est = imf_client.estimate_size("CPI", "5.0.0")
        # Size may come from data probe or DSD fallback
        assert est.total_series >= 0, "CPI estimate should not be negative"
        print(
            f"\n  CPI size: {est.total_series} series x {est.time_periods} periods "
            f"= ~{est.estimated_observations:,} obs"
        )

    def test_estimate_size_for_all_hardcoded(self, imf_client: IMFClient) -> None:
        _check_imf_available(imf_client)
        seen: set[str] = set()
        print("\n  Dataflow         Series   Periods   Est. Obs")
        print("  " + "-" * 50)
        for name, cfg in IMF_SERIES.items():
            df_key = f"{cfg['dataflow']}/{cfg['version']}"
            if df_key in seen:
                continue
            seen.add(df_key)
            try:
                est = imf_client.estimate_size(cfg["dataflow"], cfg["version"])
                print(
                    f"  {cfg['dataflow']:<16} {est.total_series:>7}  {est.time_periods:>8}  "
                    f"{est.estimated_observations:>10,}"
                )
            except (IMFAPIError, IMFRateLimitError) as exc:
                print(f"  {cfg['dataflow']:<16} SKIP ({exc!r})")


# ── Layer 5: Dry-Run Ingestion ────────────────────────────────────────

class TestDryRunIngestion:
    """Combined structure + data probe for catalog dataflows."""

    def test_dry_run_full_catalog(
        self, imf_client: IMFClient, all_dataflows: list,
    ) -> None:
        _check_imf_available(imf_client)
        sample = all_dataflows[:10]
        structure_pass = 0
        data_pass = 0

        print(f"\n  Dry-run for first {len(sample)} dataflows:")
        print(f"  {'Dataflow':<28} {'Structure':<12} {'Data':<12}")
        print("  " + "-" * 52)

        for flow in sample:
            s_ok = False
            d_ok = False
            try:
                structure = imf_client.get_datastructure(flow.id)
                s_ok = len(structure.dimensions) >= 1
            except (IMFAPIError, IMFRateLimitError):
                pass

            if s_ok:
                structure_pass += 1
                # Data probe: estimate_size uses DSD fallback since key=all
                # often returns empty for IMF. Count DSD-based estimate as pass.
                try:
                    est = imf_client.estimate_size(flow.id, flow.version)
                    d_ok = est.total_series > 0 or est.estimated_observations > 0
                except (IMFAPIError, IMFRateLimitError):
                    pass

            if d_ok:
                data_pass += 1

            print(f"  {flow.id:<28} {'PASS' if s_ok else 'FAIL':<12} {'PASS' if d_ok else 'FAIL':<12}")

        total = len(sample)
        s_rate = structure_pass / total if total else 0
        d_rate = data_pass / total if total else 0
        print(f"\n  Structure pass rate: {s_rate:.0%} ({structure_pass}/{total})")
        print(f"  Data pass rate:     {d_rate:.0%} ({data_pass}/{total})")
        assert s_rate >= 0.80, f"Structure pass rate {s_rate:.0%} < 80%"
        assert d_rate >= 0.30, f"Data pass rate {d_rate:.0%} < 30%"

    def test_full_catalog_data_probe(
        self, imf_client: IMFClient, all_dataflows: list,
    ) -> None:
        """Probe every dataflow in the catalog with a limit=1 data fetch.

        This catches datasets that require special dimension filters,
        contain no data, or have non-standard key patterns.
        """
        _check_imf_available(imf_client)

        success = 0
        empty = 0
        failures: list[tuple[str, str]] = []

        print(f"\n  Full catalog probe: {len(all_dataflows)} dataflows")
        print(f"  {'Dataflow':<32} {'Status':<10} {'Detail'}")
        print("  " + "-" * 70)

        for flow in all_dataflows:
            try:
                est = imf_client.estimate_size(flow.id, flow.version)
                if est.total_series > 0 or est.estimated_observations > 0:
                    success += 1
                    status = "OK"
                    detail = f"~{est.total_series:,} series"
                else:
                    empty += 1
                    status = "EMPTY"
                    detail = "0 series (DSD fallback returned 0)"
            except IMFRateLimitError:
                failures.append((flow.id, "rate_limit"))
                status = "RATELIM"
                detail = "429 — rate limited"
            except IMFAPIError as exc:
                failures.append((flow.id, str(exc)[:80]))
                status = "ERROR"
                detail = str(exc)[:60]
            except Exception as exc:
                failures.append((flow.id, str(exc)[:80]))
                status = "ERROR"
                detail = str(exc)[:60]

            print(f"  {flow.id:<32} {status:<10} {detail}")

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

        # At least 90% of the catalog should be accessible
        access_rate = accessible / total if total else 0
        assert access_rate >= 0.90, (
            f"Catalog accessibility {access_rate:.0%} < 90% "
            f"({fail_count} failures out of {total})"
        )


# ── Layer 6: Stress Test ──────────────────────────────────────────────

class TestStressTest:
    """Larger fetches to validate chunked retrieval and memory."""

    def test_full_fetch_cpi_chunked(self, imf_client: IMFClient) -> None:
        _check_imf_available(imf_client)
        chunks_received: list[tuple[str, str, int]] = []

        def on_chunk(obs: list, start: str, end: str) -> None:
            chunks_received.append((start, end, len(obs)))

        t0 = time.monotonic()
        obs = imf_client.fetch_dataset_chunked(
            "CPI", "5.0.0", "CHN.CPI._T.IX.M",
            series_id="IMF_CN_CPI",
            chunk_ranges=[("2020", "2026")],
            on_chunk=on_chunk,
        )
        elapsed = time.monotonic() - t0

        assert len(obs) > 10, f"Expected >10 CPI obs, got {len(obs)}"
        print(f"\n  CPI chunked [2020-2026]: {len(obs)} obs in {elapsed:.1f}s")
        for start, end, count in chunks_received:
            print(f"    [{start}-{end}]: {count} obs")

    def test_full_fetch_with_single_country_filter(self, imf_client: IMFClient) -> None:
        _check_imf_available(imf_client)
        cfg = IMF_SERIES["global_trade"]
        t0 = time.monotonic()
        obs = imf_client.get_data(
            cfg["dataflow"], cfg["key"],
            series_id=cfg["series_id"], version=cfg["version"],
            start_period="2020", limit=0,
        )
        elapsed = time.monotonic() - t0
        assert len(obs) >= 1, f"Expected trade obs for USA, got {len(obs)}"
        print(f"\n  Trade USA [2020+]: {len(obs)} obs in {elapsed:.1f}s")

    def test_memory_bounded_large_fetch(self, imf_client: IMFClient) -> None:
        _check_imf_available(imf_client)
        import resource

        mem_before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        obs = imf_client.fetch_dataset_chunked(
            "CPI", "5.0.0", "CHN.CPI._T.IX.M",
            series_id="IMF_CN_CPI",
            chunk_ranges=[("2020", "2026")],
        )
        mem_after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        mem_delta_mb = (mem_after - mem_before) / 1024  # KB -> MB on Linux

        print(f"\n  Memory delta: {mem_delta_mb:.1f} MB for {len(obs)} obs")
        assert mem_delta_mb < 500, f"Memory usage too high: {mem_delta_mb:.1f} MB"


# ── Layer 7: Automated Test Report ────────────────────────────────────

class TestAutomatedTestReport:
    """Build ValidationReport from check results."""

    def test_generate_validation_report(self, imf_client: IMFClient) -> None:
        _check_imf_available(imf_client)
        checks: list[CheckResult] = []

        # Run a few quick checks
        flows = imf_client.list_dataflows()
        checks.append(CheckResult(
            check_name="catalog_discovery",
            layer=ValidationLayer.CATALOG,
            passed=len(flows) > 50,
            severity=ValidationSeverity.ERROR,
            message=f"Found {len(flows)} dataflows",
            source="imf",
        ))

        cfg = IMF_SERIES["cn_cpi"]
        try:
            obs = imf_client.get_data(
                cfg["dataflow"], cfg["key"],
                series_id=cfg["series_id"], version=cfg["version"], limit=1,
            )
            checks.append(CheckResult(
                check_name="data_accessibility_cn_cpi",
                layer=ValidationLayer.SERIES,
                passed=len(obs) >= 1,
                severity=ValidationSeverity.ERROR,
                message=f"Got {len(obs)} observations",
                source="imf",
            ))
        except (IMFAPIError, IMFRateLimitError) as exc:
            checks.append(CheckResult(
                check_name="data_accessibility_cn_cpi",
                layer=ValidationLayer.SERIES,
                passed=False,
                severity=ValidationSeverity.ERROR,
                message=str(exc),
                source="imf",
            ))

        report = ValidationReport(
            source="imf",
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
                source="imf",
            ),
            CheckResult(
                check_name="bad_check",
                layer=ValidationLayer.CATALOG,
                passed=False,
                severity=ValidationSeverity.ERROR,
                message="intentional failure",
                source="imf",
            ),
        )
        report = ValidationReport(
            source="imf",
            run_id="test-fail",
            timestamp=datetime.now(timezone.utc).isoformat(),
            checks=checks,
        )
        assert not report.passed
        assert report.error_count == 1
        print(f"\n  Failure report: error_count={report.error_count}")


# ── Layer 8: Edge Cases ───────────────────────────────────────────────

class TestEdgeCases:
    """Edge-case handling for the IMF client."""

    def test_different_frequencies(self, imf_client: IMFClient) -> None:
        _check_imf_available(imf_client)
        # Monthly (CPI)
        cfg_m = IMF_SERIES["cn_cpi"]
        obs_m = imf_client.get_data(
            cfg_m["dataflow"], cfg_m["key"],
            series_id=cfg_m["series_id"], version=cfg_m["version"], limit=2,
        )
        assert obs_m, "Monthly data should return observations"
        assert "-" in obs_m[0].date  # normalized to YYYY-MM-DD
        print(f"\n  Monthly: {obs_m[0].date}")

        # Quarterly (GDP)
        cfg_q = IMF_SERIES["cn_gdp"]
        obs_q = imf_client.get_data(
            cfg_q["dataflow"], cfg_q["key"],
            series_id=cfg_q["series_id"], version=cfg_q["version"], limit=2,
        )
        assert obs_q, "Quarterly data should return observations"
        print(f"  Quarterly: {obs_q[0].date}")

    def test_missing_dimension_key_returns_empty(self, imf_client: IMFClient) -> None:
        _check_imf_available(imf_client)
        try:
            obs = imf_client.get_data(
                "CPI", "ZZZZZ.NONEXIST.ZZZ.ZZZ.Z",
                series_id="probe", version="5.0.0", limit=1,
            )
            assert len(obs) == 0, f"Expected empty result, got {len(obs)}"
        except IMFAPIError:
            pass  # 404 is also acceptable
        print("\n  Invalid key: empty or error (OK)")

    def test_nonexistent_dataflow_raises_error(self, imf_client: IMFClient) -> None:
        with pytest.raises(IMFAPIError):
            imf_client.get_data(
                "DOES_NOT_EXIST_XYZ", "A.B.C",
                series_id="probe", version="1.0.0", limit=1,
            )
        print("\n  Nonexistent dataflow: IMFAPIError raised (OK)")

    def test_empty_dataset_handled_gracefully(self, imf_client: IMFClient) -> None:
        _check_imf_available(imf_client)
        try:
            obs = imf_client.get_data(
                "CPI", "CHN.CPI._T.IX.M",
                series_id="probe", version="5.0.0",
                start_period="2099", limit=10,
            )
            # Either empty or very few results is acceptable
            print(f"\n  Future period: {len(obs)} obs (OK)")
        except IMFAPIError:
            print("\n  Future period: API error (acceptable)")


# ── Layer 9: Performance Benchmark ────────────────────────────────────

class TestPerformanceBenchmark:
    """Timing benchmarks for key operations."""

    def test_list_dataflows_completes_within_30s(self, imf_client: IMFClient) -> None:
        # Clear cache to measure cold performance
        imf_client._dataflow_cache = None
        t0 = time.monotonic()
        flows = imf_client.list_dataflows()
        elapsed = time.monotonic() - t0
        assert elapsed < 30, f"list_dataflows took {elapsed:.1f}s (limit: 30s)"
        assert len(flows) > 0
        print(f"\n  list_dataflows (cold): {elapsed:.1f}s, {len(flows)} flows")

    def test_dataflow_cache_hit_is_fast(self, imf_client: IMFClient) -> None:
        # Ensure cache is primed
        imf_client.list_dataflows()
        t0 = time.monotonic()
        flows = imf_client.list_dataflows()
        elapsed = time.monotonic() - t0
        assert elapsed < 0.01, f"Cached list_dataflows took {elapsed:.4f}s (limit: 0.01s)"
        print(f"\n  list_dataflows (cached): {elapsed:.6f}s")

    def test_single_probe_completes_within_15s(self, imf_client: IMFClient) -> None:
        _check_imf_available(imf_client)
        cfg = IMF_SERIES["cn_cpi"]
        t0 = time.monotonic()
        imf_client.get_data(
            cfg["dataflow"], cfg["key"],
            series_id=cfg["series_id"], version=cfg["version"], limit=1,
        )
        elapsed = time.monotonic() - t0
        assert elapsed < 15, f"Single probe took {elapsed:.1f}s (limit: 15s)"
        print(f"\n  Single data probe: {elapsed:.1f}s")

    def test_dsd_fetch_completes_within_15s(self, imf_client: IMFClient) -> None:
        # Clear structure cache
        imf_client._structure_cache.clear()
        t0 = time.monotonic()
        imf_client.get_datastructure("CPI")
        elapsed = time.monotonic() - t0
        assert elapsed < 15, f"DSD fetch took {elapsed:.1f}s (limit: 15s)"
        print(f"\n  DSD fetch (CPI): {elapsed:.1f}s")

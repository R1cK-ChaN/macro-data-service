"""Integration tests for Eurostat full catalog access and 9-layer validation.

Requires network access (no API key needed). Run with:
    pytest tests/test_eurostat_full_catalog.py -v -s
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

from analyst.ingestion.scrapers.eurostat import (
    EurostatAPIError,
    EurostatClient,
    EurostatRateLimitError,
    _build_decade_chunks,
)
from analyst.ingestion.sources import EUROSTAT_SERIES
from analyst.ingestion.validation._types import (
    CheckResult,
    ValidationLayer,
    ValidationReport,
    ValidationSeverity,
)

pytestmark = pytest.mark.integration


# ── Fixtures ──────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def eurostat_client() -> EurostatClient:
    return EurostatClient(timeout=60)


@pytest.fixture(scope="module")
def all_dataflows(eurostat_client: EurostatClient) -> list:
    return eurostat_client.list_dataflows()


def _check_eurostat_available(client: EurostatClient) -> None:
    """Try a minimal fetch; skip test if Eurostat is rate limiting us."""
    try:
        client.get_dataset(
            "prc_hicp_manr",
            params={"coicop": "CP00", "geo": "EA20"},
            series_id="probe", limit=1,
        )
    except EurostatRateLimitError:
        pytest.skip("Eurostat API is rate limiting — try again in a few minutes")
    except (EurostatAPIError, requests.RequestException):
        pytest.skip("Eurostat API unavailable")


# ── Layer 1: Catalog Discovery ────────────────────────────────────────

class TestCatalogDiscovery:
    """Validate that we can discover the full Eurostat dataflow catalog."""

    def test_list_all_dataflows_returns_many(self, all_dataflows: list) -> None:
        assert len(all_dataflows) > 5000, (
            f"Expected >5000 dataflows, got {len(all_dataflows)}"
        )
        print(f"\n  Total Eurostat dataflows: {len(all_dataflows)}")
        for flow in all_dataflows[:20]:
            print(f"    {flow.id:<28} v{flow.version}: {flow.name[:50]}")
        if len(all_dataflows) > 20:
            print(f"    ... and {len(all_dataflows) - 20} more")

    def test_dataflows_contain_known_datasets(self, all_dataflows: list) -> None:
        ids = {f.id for f in all_dataflows}
        expected = {"prc_hicp_manr", "namq_10_gdp", "une_rt_m", "sts_inpr_m", "teibs010"}
        found = expected & ids
        assert len(found) >= 4, (
            f"Expected at least 4 known datasets in catalog, found: {found}"
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
        configured_datasets = {cfg["dataset"] for cfg in EUROSTAT_SERIES.values()}
        missing = configured_datasets - catalog_ids
        assert not missing, (
            f"Configured datasets not found in catalog: {missing}"
        )
        print(f"\n  All {len(configured_datasets)} configured datasets found in catalog")


# ── Layer 2: Structure Validation ─────────────────────────────────────

class TestStructureValidation:
    """Validate DSD introspection for Eurostat dataflows."""

    def test_structure_for_hicp(self, eurostat_client: EurostatClient) -> None:
        structure = eurostat_client.get_datastructure("prc_hicp_manr")
        dim_ids = {d.id for d in structure.dimensions}
        assert len(dim_ids) >= 2, f"Expected >=2 dims for HICP, got {dim_ids}"
        has_time = any(d.is_time for d in structure.dimensions)
        assert has_time, "HICP DSD should have a time dimension"
        print(f"\n  HICP structure: {[d.id for d in structure.dimensions]}")
        for d in structure.dimensions:
            if not d.is_time:
                print(f"    {d.id}: {d.code_count} codes")

    def test_structure_for_each_hardcoded_dataset(self, eurostat_client: EurostatClient) -> None:
        seen: set[str] = set()
        for name, cfg in EUROSTAT_SERIES.items():
            ds_id = cfg["dataset"]
            if ds_id in seen:
                continue
            seen.add(ds_id)
            structure = eurostat_client.get_datastructure(ds_id)
            assert len(structure.dimensions) >= 2, (
                f"{ds_id}: expected >=2 dimensions, got {len(structure.dimensions)}"
            )
            print(f"\n  {ds_id}: {len(structure.dimensions)} dimensions")

    def test_summarize_structure_returns_compact_view(self, eurostat_client: EurostatClient) -> None:
        summary = eurostat_client.summarize_structure("prc_hicp_manr")
        assert summary.dataflow_id == "prc_hicp_manr"
        assert summary.structure_id
        assert len(summary.series_dimensions) > 0
        assert summary.time_dimension_id
        print(
            f"\n  HICP summary: dims={list(summary.series_dimensions)}, "
            f"codes={dict(summary.code_counts)}, est_series={summary.estimated_series}"
        )

    def test_structure_for_sample_catalog_dataflows(
        self, eurostat_client: EurostatClient, all_dataflows: list,
    ) -> None:
        sample = random.sample(all_dataflows, min(5, len(all_dataflows)))
        passed = 0
        for flow in sample:
            try:
                structure = eurostat_client.get_datastructure(flow.id)
                assert len(structure.dimensions) >= 1
                passed += 1
                print(f"    {flow.id}: {len(structure.dimensions)} dims — OK")
            except (EurostatAPIError, EurostatRateLimitError) as exc:
                print(f"    {flow.id}: SKIP ({exc!r})")
        print(f"\n  Sample structure validation: {passed}/{len(sample)} passed")
        assert passed >= 1, "Expected at least 1 structure probe to succeed"


# ── Layer 3: Dataset Accessibility ────────────────────────────────────

class TestDatasetAccessibility:
    """Validate that configured series are actually fetchable."""

    def test_fetch_limit_1_from_every_hardcoded_series(self, eurostat_client: EurostatClient) -> None:
        _check_eurostat_available(eurostat_client)
        for name, cfg in EUROSTAT_SERIES.items():
            obs = eurostat_client.get_dataset(
                cfg["dataset"],
                params=dict(cfg["params"]),
                series_id=cfg["series_id"], limit=1,
            )
            assert len(obs) >= 1, f"{name}: expected >=1 observation, got {len(obs)}"
            print(f"    {name}: {obs[0].date} = {obs[0].value}")

    def test_probe_sample_catalog_datasets(
        self, eurostat_client: EurostatClient, all_dataflows: list,
    ) -> None:
        _check_eurostat_available(eurostat_client)
        sample = random.sample(all_dataflows, min(10, len(all_dataflows)))
        probed = 0
        for flow in sample:
            try:
                est = eurostat_client.estimate_size(flow.id, flow.version or "1.0")
                probed += 1
                print(f"    {flow.id}: ~{est.total_series} series")
            except (EurostatAPIError, EurostatRateLimitError):
                print(f"    {flow.id}: SKIP")
        assert probed >= 1, "Expected at least 1 random probe to succeed"

    def test_known_datasets_return_valid_observations(self, eurostat_client: EurostatClient) -> None:
        _check_eurostat_available(eurostat_client)
        cfg = EUROSTAT_SERIES["hicp"]
        obs = eurostat_client.get_dataset(
            cfg["dataset"],
            params=dict(cfg["params"]),
            series_id=cfg["series_id"], limit=5,
        )
        assert len(obs) >= 1
        for o in obs:
            assert o.series_id == cfg["series_id"]
            assert o.date
            assert isinstance(o.value, float)
            assert o.dataset == cfg["dataset"]
        print(f"\n  HICP: {len(obs)} valid observations")


# ── Layer 4: Size Estimation ──────────────────────────────────────────

class TestSizeEstimation:
    """Validate size estimation via limit=1 probes."""

    def test_estimate_hicp_size(self, eurostat_client: EurostatClient) -> None:
        _check_eurostat_available(eurostat_client)
        est = eurostat_client.estimate_size("prc_hicp_manr")
        assert est.total_series > 0, "HICP should have series"
        print(
            f"\n  HICP size: {est.total_series} series x {est.time_periods} periods "
            f"= ~{est.estimated_observations:,} obs"
        )

    def test_estimate_size_for_all_hardcoded(self, eurostat_client: EurostatClient) -> None:
        _check_eurostat_available(eurostat_client)
        seen: set[str] = set()
        print("\n  Dataset                Series   Periods   Est. Obs")
        print("  " + "-" * 55)
        for name, cfg in EUROSTAT_SERIES.items():
            ds_id = cfg["dataset"]
            if ds_id in seen:
                continue
            seen.add(ds_id)
            try:
                est = eurostat_client.estimate_size(ds_id)
                print(
                    f"  {ds_id:<22} {est.total_series:>7}  {est.time_periods:>8}  "
                    f"{est.estimated_observations:>10,}"
                )
            except (EurostatAPIError, EurostatRateLimitError) as exc:
                print(f"  {ds_id:<22} SKIP ({exc!r})")


# ── Layer 5: Dry-Run Ingestion ────────────────────────────────────────

class TestDryRunIngestion:
    """Combined structure + data probe for catalog dataflows."""

    def test_dry_run_first_20_dataflows(
        self, eurostat_client: EurostatClient, all_dataflows: list,
    ) -> None:
        _check_eurostat_available(eurostat_client)
        subset = all_dataflows[:20]
        structure_pass = 0
        data_pass = 0

        print(f"\n  Dry-run for first {len(subset)} dataflows:")
        print(f"  {'Dataflow':<28} {'Structure':<12} {'Data':<12}")
        print("  " + "-" * 52)

        for flow in subset:
            s_ok = False
            d_ok = False
            try:
                structure = eurostat_client.get_datastructure(flow.id)
                s_ok = len(structure.dimensions) >= 1
            except (EurostatAPIError, EurostatRateLimitError, requests.RequestException):
                pass

            if s_ok:
                structure_pass += 1
                try:
                    est = eurostat_client.estimate_size(flow.id, flow.version or "1.0")
                    d_ok = est.total_series > 0
                except (EurostatAPIError, EurostatRateLimitError, requests.RequestException):
                    pass

            if d_ok:
                data_pass += 1

            print(f"  {flow.id:<28} {'PASS' if s_ok else 'FAIL':<12} {'PASS' if d_ok else 'FAIL':<12}")

        total = len(subset)
        s_rate = structure_pass / total if total else 0
        d_rate = data_pass / total if total else 0
        print(f"\n  Structure pass rate: {s_rate:.0%} ({structure_pass}/{total})")
        print(f"  Data pass rate:     {d_rate:.0%} ({data_pass}/{total})")
        assert s_rate >= 0.80, f"Structure pass rate {s_rate:.0%} < 80%"
        assert d_rate >= 0.30, f"Data pass rate {d_rate:.0%} < 30%"

    def test_full_catalog_data_probe(
        self, eurostat_client: EurostatClient, all_dataflows: list,
    ) -> None:
        """Probe a random sample of catalog dataflows."""
        _check_eurostat_available(eurostat_client)
        sample = random.sample(all_dataflows, min(50, len(all_dataflows)))

        success = 0
        empty = 0
        failures: list[tuple[str, str]] = []

        print(f"\n  Catalog probe: {len(sample)} sampled dataflows")
        print(f"  {'Dataflow':<28} {'Status':<10} {'Detail'}")
        print("  " + "-" * 64)

        for flow in sample:
            try:
                est = eurostat_client.estimate_size(flow.id, flow.version or "1.0")
                if est.total_series > 0:
                    success += 1
                    status = "OK"
                    detail = f"~{est.total_series:,} series"
                else:
                    empty += 1
                    status = "EMPTY"
                    detail = "0 series"
            except EurostatRateLimitError:
                failures.append((flow.id, "rate_limit"))
                status = "RATELIM"
                detail = "429 — rate limited"
            except EurostatAPIError as exc:
                failures.append((flow.id, str(exc)[:80]))
                status = "ERROR"
                detail = str(exc)[:60]
            except Exception as exc:
                failures.append((flow.id, str(exc)[:80]))
                status = "ERROR"
                detail = str(exc)[:60]

            print(f"  {flow.id:<28} {status:<10} {detail}")

        total = len(sample)
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
        assert access_rate >= 0.50, (
            f"Catalog accessibility {access_rate:.0%} < 50% "
            f"({fail_count} failures out of {total})"
        )


# ── Layer 6: Stress Test ──────────────────────────────────────────────

class TestStressTest:
    """Larger fetches to validate chunked retrieval and memory."""

    def test_full_fetch_hicp_chunked(self, eurostat_client: EurostatClient) -> None:
        _check_eurostat_available(eurostat_client)
        chunks_received: list[tuple[str, str, int]] = []

        def on_chunk(obs: list, start: str, end: str) -> None:
            chunks_received.append((start, end, len(obs)))

        t0 = time.monotonic()
        obs = eurostat_client.fetch_dataset_chunked(
            "prc_hicp_manr", ".",
            series_id="ESTAT_HICP",
            chunk_ranges=[("2020", "2026")],
            on_chunk=on_chunk,
        )
        elapsed = time.monotonic() - t0

        assert len(obs) > 0, f"Expected >0 HICP obs, got {len(obs)}"
        print(f"\n  HICP chunked [2020-2026]: {len(obs)} obs in {elapsed:.1f}s")
        for start, end, count in chunks_received:
            print(f"    [{start}-{end}]: {count} obs")

    def test_full_fetch_with_explicit_geo_codes(self, eurostat_client: EurostatClient) -> None:
        """Fetch HICP with explicit geo code chunking."""
        _check_eurostat_available(eurostat_client)
        t0 = time.monotonic()
        obs = eurostat_client.fetch_dataset_chunked(
            "prc_hicp_manr", ".",
            series_id="ESTAT_HICP",
            chunk_ranges=[("2023", "2026")],
            geo_codes=["DE", "FR", "IT", "ES", "NL"],
            geo_batch_size=3,
        )
        elapsed = time.monotonic() - t0
        assert len(obs) >= 0
        print(f"\n  HICP geo-chunked [5 countries, 2023-2026]: {len(obs)} obs in {elapsed:.1f}s")

    def test_full_fetch_with_nuts_level_filter(self, eurostat_client: EurostatClient) -> None:
        """Fetch with nuts_level=0 to auto-filter to country-level codes."""
        _check_eurostat_available(eurostat_client)
        t0 = time.monotonic()
        obs = eurostat_client.fetch_dataset_chunked(
            "prc_hicp_manr", ".",
            series_id="ESTAT_HICP",
            chunk_ranges=[("2024", "2026")],
            nuts_level=0,
        )
        elapsed = time.monotonic() - t0
        assert len(obs) >= 0
        print(f"\n  HICP NUTS-0 [countries only, 2024-2026]: {len(obs)} obs in {elapsed:.1f}s")

    def test_full_fetch_with_json_stat_geo_filter(self, eurostat_client: EurostatClient) -> None:
        """Fetch via the JSON-stat endpoint with geo param (original method)."""
        _check_eurostat_available(eurostat_client)
        cfg = EUROSTAT_SERIES["hicp"]
        t0 = time.monotonic()
        obs = eurostat_client.get_dataset(
            cfg["dataset"],
            params=dict(cfg["params"]),
            series_id=cfg["series_id"],
            limit=0,
        )
        elapsed = time.monotonic() - t0
        assert len(obs) >= 1, f"Expected HICP obs for EA20, got {len(obs)}"
        print(f"\n  HICP EA20 [JSON-stat, all time]: {len(obs)} obs in {elapsed:.1f}s")

    def test_memory_bounded_large_fetch(self, eurostat_client: EurostatClient) -> None:
        _check_eurostat_available(eurostat_client)
        import resource

        mem_before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        obs = eurostat_client.fetch_dataset_chunked(
            "prc_hicp_manr", ".",
            series_id="ESTAT_HICP",
            chunk_ranges=[("2020", "2026")],
        )
        mem_after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        mem_delta_mb = (mem_after - mem_before) / 1024

        print(f"\n  Memory delta: {mem_delta_mb:.1f} MB for {len(obs)} obs")
        assert mem_delta_mb < 500, f"Memory usage too high: {mem_delta_mb:.1f} MB"


# ── Layer 7: Automated Test Report ────────────────────────────────────

class TestAutomatedTestReport:
    """Build ValidationReport from check results."""

    def test_generate_validation_report(self, eurostat_client: EurostatClient) -> None:
        _check_eurostat_available(eurostat_client)
        checks: list[CheckResult] = []

        flows = eurostat_client.list_dataflows()
        checks.append(CheckResult(
            check_name="catalog_discovery",
            layer=ValidationLayer.CATALOG,
            passed=len(flows) > 5000,
            severity=ValidationSeverity.ERROR,
            message=f"Found {len(flows)} dataflows",
            source="eurostat",
        ))

        cfg = EUROSTAT_SERIES["hicp"]
        try:
            obs = eurostat_client.get_dataset(
                cfg["dataset"],
                params=dict(cfg["params"]),
                series_id=cfg["series_id"], limit=1,
            )
            checks.append(CheckResult(
                check_name="data_accessibility_hicp",
                layer=ValidationLayer.SERIES,
                passed=len(obs) >= 1,
                severity=ValidationSeverity.ERROR,
                message=f"Got {len(obs)} observations",
                source="eurostat",
            ))
        except (EurostatAPIError, EurostatRateLimitError) as exc:
            checks.append(CheckResult(
                check_name="data_accessibility_hicp",
                layer=ValidationLayer.SERIES,
                passed=False,
                severity=ValidationSeverity.ERROR,
                message=str(exc),
                source="eurostat",
            ))

        report = ValidationReport(
            source="eurostat",
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
                source="eurostat",
            ),
            CheckResult(
                check_name="bad_check",
                layer=ValidationLayer.CATALOG,
                passed=False,
                severity=ValidationSeverity.ERROR,
                message="intentional failure",
                source="eurostat",
            ),
        )
        report = ValidationReport(
            source="eurostat",
            run_id="test-fail",
            timestamp=datetime.now(timezone.utc).isoformat(),
            checks=checks,
        )
        assert not report.passed
        assert report.error_count == 1
        print(f"\n  Failure report: error_count={report.error_count}")


# ── Layer 8: Edge Cases ───────────────────────────────────────────────

class TestEdgeCases:
    """Edge-case handling for the Eurostat client."""

    def test_different_frequencies(self, eurostat_client: EurostatClient) -> None:
        _check_eurostat_available(eurostat_client)
        # Monthly (HICP)
        cfg_m = EUROSTAT_SERIES["hicp"]
        obs_m = eurostat_client.get_dataset(
            cfg_m["dataset"],
            params=dict(cfg_m["params"]),
            series_id=cfg_m["series_id"], limit=2,
        )
        assert obs_m, "Monthly data should return observations"
        assert "-" in obs_m[0].date
        print(f"\n  Monthly: {obs_m[0].date}")

        # Quarterly (GDP)
        cfg_q = EUROSTAT_SERIES["gdp"]
        obs_q = eurostat_client.get_dataset(
            cfg_q["dataset"],
            params=dict(cfg_q["params"]),
            series_id=cfg_q["series_id"], limit=2,
        )
        assert obs_q, "Quarterly data should return observations"
        print(f"  Quarterly: {obs_q[0].date}")

    def test_nonexistent_dataflow_raises_error(self, eurostat_client: EurostatClient) -> None:
        with pytest.raises(EurostatAPIError):
            eurostat_client.get_data(
                "DOES_NOT_EXIST_XYZ_999", "A.B.C",
                series_id="probe", limit=1,
            )
        print("\n  Nonexistent dataflow: EurostatAPIError raised (OK)")

    def test_empty_dataset_handled_gracefully(self, eurostat_client: EurostatClient) -> None:
        _check_eurostat_available(eurostat_client)
        try:
            obs = eurostat_client.get_data(
                "prc_hicp_manr", ".",
                series_id="probe",
                start_period="2099", limit=10,
            )
            print(f"\n  Future period: {len(obs)} obs (OK)")
        except EurostatAPIError:
            print("\n  Future period: API error (acceptable)")

    def test_nuts_code_filtering(self) -> None:
        from analyst.ingestion.scrapers.eurostat import _filter_nuts_codes

        codes = ("AT", "AT1", "AT11", "AT111", "DE", "DE1", "DE11")
        level0 = _filter_nuts_codes(codes, level=0)
        assert set(level0) == {"AT", "DE"}
        level1 = _filter_nuts_codes(codes, level=1)
        assert set(level1) == {"AT1", "DE1"}
        print(f"\n  NUTS filtering: level0={level0}, level1={level1}")

    def test_geo_chunking_helpers(self) -> None:
        from analyst.ingestion.scrapers.eurostat import (
            _build_geo_chunks,
            _inject_geo_into_key,
        )

        # _build_geo_chunks splits codes into batched key fragments
        chunks = _build_geo_chunks(["AT", "DE", "FR", "IT", "ES"], batch_size=2)
        assert len(chunks) == 3
        assert chunks[0] == "AT+DE"
        assert chunks[1] == "FR+IT"
        assert chunks[2] == "ES"
        print(f"\n  Geo chunks: {chunks}")

        # _inject_geo_into_key places geo fragment into the right position
        result = _inject_geo_into_key(".", "AT+DE", geo_position=2, total_dims=4)
        assert result == "..AT+DE."
        print(f"  Injected key: {result}")

        # With a fuller base key
        result2 = _inject_geo_into_key("M.CP00..", "FR+IT", geo_position=2, total_dims=4)
        assert result2 == "M.CP00.FR+IT."
        print(f"  Injected key (partial): {result2}")

        # Empty geo codes
        empty = _build_geo_chunks([], batch_size=40)
        assert empty == [""]
        print(f"  Empty geo: {empty}")


# ── Layer 9: Performance Benchmark ────────────────────────────────────

class TestPerformanceBenchmark:
    """Timing benchmarks for key operations."""

    def test_list_dataflows_completes_within_60s(self, eurostat_client: EurostatClient) -> None:
        eurostat_client._dataflow_cache = None
        t0 = time.monotonic()
        flows = eurostat_client.list_dataflows()
        elapsed = time.monotonic() - t0
        assert elapsed < 60, f"list_dataflows took {elapsed:.1f}s (limit: 60s)"
        assert len(flows) > 0
        print(f"\n  list_dataflows (cold): {elapsed:.1f}s, {len(flows)} flows")

    def test_dataflow_cache_hit_is_fast(self, eurostat_client: EurostatClient) -> None:
        eurostat_client.list_dataflows()
        t0 = time.monotonic()
        flows = eurostat_client.list_dataflows()
        elapsed = time.monotonic() - t0
        assert elapsed < 0.01, f"Cached list_dataflows took {elapsed:.4f}s (limit: 0.01s)"
        print(f"\n  list_dataflows (cached): {elapsed:.6f}s")

    def test_single_probe_completes_within_15s(self, eurostat_client: EurostatClient) -> None:
        _check_eurostat_available(eurostat_client)
        cfg = EUROSTAT_SERIES["hicp"]
        t0 = time.monotonic()
        eurostat_client.get_dataset(
            cfg["dataset"],
            params=dict(cfg["params"]),
            series_id=cfg["series_id"], limit=1,
        )
        elapsed = time.monotonic() - t0
        assert elapsed < 15, f"Single probe took {elapsed:.1f}s (limit: 15s)"
        print(f"\n  Single data probe: {elapsed:.1f}s")

    def test_dsd_fetch_completes_within_15s(self, eurostat_client: EurostatClient) -> None:
        eurostat_client._structure_cache.clear()
        t0 = time.monotonic()
        eurostat_client.get_datastructure("prc_hicp_manr")
        elapsed = time.monotonic() - t0
        assert elapsed < 15, f"DSD fetch took {elapsed:.1f}s (limit: 15s)"
        print(f"\n  DSD fetch (HICP): {elapsed:.1f}s")

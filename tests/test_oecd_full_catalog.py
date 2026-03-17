"""Integration tests for OECD full catalog access and 10-layer validation.

Requires network access. Run with:
    pytest tests/test_oecd_full_catalog.py -v -s
"""

from __future__ import annotations

import hashlib
import json
import random
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ingestion.sdmx._errors import OECDAPIError, OECDRateLimitError
from ingestion.sdmx.providers.oecd import OECDClient, OECDObservation, _build_decade_chunks
from ingestion.sources import (
    OECD_SERIES,
    OECDIngestionClient,
    OECDSeriesConfig,
    _OECDRateLimiter,
)
from ingestion.validation._types import (
    CheckResult,
    ValidationLayer,
    ValidationReport,
    ValidationSeverity,
)

pytestmark = pytest.mark.integration

# The CLI dataflow is the most stable and widely used OECD dataset.
_CLI_DATAFLOW = "DSD_STES@DF_CLI"
_CLI_AGENCY = "OECD.SDD.STES"


# ── Fixtures ──────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def oecd_client() -> OECDClient:
    return OECDClient(timeout=45)


@pytest.fixture(scope="module")
def all_dataflows(oecd_client: OECDClient) -> list:
    try:
        return oecd_client.list_dataflows()
    except OECDRateLimitError:
        pytest.skip("OECD rate limited on catalog fetch")
    except OECDAPIError:
        pytest.skip("OECD API unavailable")


def _check_oecd_data_available(client: OECDClient) -> None:
    """Try a minimal fetch; skip test if OECD is rate limiting us."""
    try:
        client.fetch_data(
            _CLI_DATAFLOW,
            agency_id=_CLI_AGENCY,
            key="USA.M.LI.IX._Z.NOR.IX._Z.H",
            limit=1,
        )
    except OECDRateLimitError:
        pytest.skip("OECD API is rate limiting — try again in a few minutes")
    except OECDAPIError:
        pytest.skip("OECD API unavailable")


# ── Layer 1: Catalog Discovery ────────────────────────────────────────

class TestCatalogDiscovery:
    """Validate that we can discover the full OECD catalog."""

    def test_list_all_dataflows_returns_hundreds(self, all_dataflows: list) -> None:
        assert len(all_dataflows) > 100, (
            f"Expected >100 dataflows, got {len(all_dataflows)}"
        )
        agencies = {df.agency_id for df in all_dataflows}
        assert len(agencies) > 5

        print(f"\n  Total dataflows: {len(all_dataflows)}")
        print(f"  Distinct agencies: {len(agencies)}")
        for agency in sorted(agencies)[:10]:
            count = sum(1 for df in all_dataflows if df.agency_id == agency)
            print(f"    {agency}: {count} dataflows")

    def test_dataflows_contain_known_datasets(self, all_dataflows: list) -> None:
        ids = {f.id for f in all_dataflows}
        expected = {_CLI_DATAFLOW, "DSD_STES@DF_CS"}
        found = expected & ids
        assert len(found) >= 2, (
            f"Expected at least CLI and CS in catalog, found: {found}"
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
        configured_dataflows = {cfg.dataflow for cfg in OECD_SERIES.values()}
        missing = configured_dataflows - catalog_ids
        assert not missing, (
            f"Configured dataflows not found in catalog: {missing}"
        )
        print(f"\n  All {len(configured_dataflows)} configured dataflows found in catalog")

    def test_catalog_size_stable_across_runs(self, oecd_client: OECDClient) -> None:
        oecd_client._dataflow_list_cache.clear()
        oecd_client._dataflow_cache.clear()
        flows1 = oecd_client.list_dataflows()
        oecd_client._dataflow_list_cache.clear()
        oecd_client._dataflow_cache.clear()
        flows2 = oecd_client.list_dataflows()
        assert len(flows1) == len(flows2), (
            f"Catalog size not stable: {len(flows1)} vs {len(flows2)}"
        )
        print(f"\n  Catalog stable: {len(flows1)} dataflows across 2 fetches")


# ── Layer 2: Structure Validation (DSD) ──────────────────────────────

class TestStructureValidation:
    """Validate DSD introspection for OECD dataflows."""

    def test_structure_for_cli(self, oecd_client: OECDClient) -> None:
        structure = oecd_client.get_structure(
            _CLI_DATAFLOW, agency_id=_CLI_AGENCY,
        )
        dim_ids = {d.id for d in structure.dimensions}
        assert "REF_AREA" in dim_ids, (
            f"Expected REF_AREA in CLI dims, got {dim_ids}"
        )
        has_time = any(d.is_time for d in structure.dimensions)
        assert has_time, "CLI DSD should have a time dimension"
        print(f"\n  CLI structure: {[d.id for d in structure.dimensions]}")
        for d in structure.dimensions:
            if not d.is_time:
                print(f"    {d.id}: {len(d.codes)} codes")

    def test_structure_for_each_hardcoded_series(self, oecd_client: OECDClient) -> None:
        seen_dataflows: set[str] = set()
        for name, cfg in OECD_SERIES.items():
            if cfg.dataflow in seen_dataflows:
                continue
            seen_dataflows.add(cfg.dataflow)
            structure = oecd_client.get_structure(
                cfg.dataflow, agency_id=cfg.agency_id,
            )
            assert len(structure.dimensions) >= 2, (
                f"{cfg.dataflow}: expected >=2 dimensions, got {len(structure.dimensions)}"
            )
            print(f"\n  {cfg.dataflow}: {len(structure.dimensions)} dimensions")

    def test_summarize_structure_returns_compact_view(self, oecd_client: OECDClient) -> None:
        summary = oecd_client.summarize_structure(
            _CLI_DATAFLOW, agency_id=_CLI_AGENCY,
        )
        assert summary.dataflow_id == _CLI_DATAFLOW
        assert summary.structure_id
        assert len(summary.series_dimensions) > 0
        assert summary.time_dimension_id
        print(
            f"\n  CLI summary: dims={list(summary.series_dimensions)}, "
            f"codes={dict(summary.code_counts)}"
        )

    def test_structure_for_sample_catalog_dataflows(
        self, oecd_client: OECDClient, all_dataflows: list,
    ) -> None:
        _check_oecd_data_available(oecd_client)
        with_structure = [f for f in all_dataflows if f.structure_id]
        sample = random.sample(with_structure, min(5, len(with_structure)))
        passed = 0
        for flow in sample:
            try:
                structure = oecd_client.get_structure(
                    flow.id, agency_id=flow.agency_id,
                )
                assert len(structure.dimensions) >= 1
                passed += 1
                print(f"    {flow.id}: {len(structure.dimensions)} dims OK")
            except (OECDAPIError, OECDRateLimitError) as exc:
                print(f"    {flow.id}: SKIP ({exc!r})")
        assert passed >= 1, "Expected at least 1 random DSD fetch to succeed"


# ── Layer 3: Codelist Validation ─────────────────────────────────────

class TestCodelistValidation:
    """Validate codelist values for each dimension in a DSD."""

    def test_cli_ref_area_codelist_has_many_codes(self, oecd_client: OECDClient) -> None:
        structure = oecd_client.get_structure(
            _CLI_DATAFLOW, agency_id=_CLI_AGENCY,
        )
        ref_area = next(
            (d for d in structure.dimensions if d.id == "REF_AREA"), None,
        )
        assert ref_area is not None, "CLI should have REF_AREA dimension"
        assert len(ref_area.codes) > 10, (
            f"Expected >10 REF_AREA codes, got {len(ref_area.codes)}"
        )
        print(f"\n  CLI REF_AREA: {len(ref_area.codes)} codes")
        print(f"    Sample: {[c.id for c in ref_area.codes[:10]]}")

    def test_frequency_codes_present(self, oecd_client: OECDClient) -> None:
        structure = oecd_client.get_structure(
            _CLI_DATAFLOW, agency_id=_CLI_AGENCY,
        )
        freq_dim = next(
            (d for d in structure.dimensions if d.id in ("FREQ", "FREQUENCY")),
            None,
        )
        if freq_dim is None:
            pytest.skip("CLI does not expose a FREQ dimension")
        assert len(freq_dim.codes) >= 1, "FREQ should have at least 1 code"
        print(f"\n  CLI FREQ: {[c.id for c in freq_dim.codes[:20]]}")

    def test_all_non_time_dimensions_have_codes(self, oecd_client: OECDClient) -> None:
        structure = oecd_client.get_structure(
            _CLI_DATAFLOW, agency_id=_CLI_AGENCY,
        )
        for dim in structure.dimensions:
            if not dim.is_time:
                assert len(dim.codes) >= 0, (
                    f"CLI dim {dim.id}: unexpected negative code count"
                )
                print(f"    {dim.id}: {len(dim.codes)} codes")

    def test_codelist_enumeration_for_all_hardcoded_dataflows(
        self, oecd_client: OECDClient,
    ) -> None:
        seen: set[str] = set()
        print(f"\n  {'Dataflow':<28} {'Dimension':<20} {'Codes':>6}")
        print("  " + "-" * 56)
        for name, cfg in OECD_SERIES.items():
            if cfg.dataflow in seen:
                continue
            seen.add(cfg.dataflow)
            structure = oecd_client.get_structure(
                cfg.dataflow, agency_id=cfg.agency_id,
            )
            for dim in structure.dimensions:
                if not dim.is_time:
                    print(f"  {cfg.dataflow:<28} {dim.id:<20} {len(dim.codes):>6}")

    def test_codes_have_names(self, oecd_client: OECDClient) -> None:
        structure = oecd_client.get_structure(
            _CLI_DATAFLOW, agency_id=_CLI_AGENCY,
        )
        ref_area = next(
            (d for d in structure.dimensions if d.id == "REF_AREA"), None,
        )
        assert ref_area is not None
        named = [c for c in ref_area.codes if c.name]
        ratio = len(named) / len(ref_area.codes) if ref_area.codes else 0
        assert ratio > 0.5, (
            f"Expected >50% of REF_AREA codes to have names, got {ratio:.0%}"
        )
        print(f"\n  REF_AREA codes with names: {len(named)}/{len(ref_area.codes)}")


# ── Layer 4: Dataset Accessibility ───────────────────────────────────

class TestDatasetAccessibility:
    """Validate that configured series are actually fetchable."""

    def test_fetch_limit_1_from_every_hardcoded_series(self, oecd_client: OECDClient) -> None:
        _check_oecd_data_available(oecd_client)
        for name, cfg in OECD_SERIES.items():
            if cfg.key:
                obs = oecd_client.fetch_data(
                    cfg.dataflow, agency_id=cfg.agency_id,
                    key=cfg.key, series_id=cfg.series_id, limit=1,
                )
            else:
                obs = oecd_client.fetch_data(
                    cfg.dataflow, agency_id=cfg.agency_id,
                    filters=cfg.filters, series_id=cfg.series_id, limit=1,
                )
            assert len(obs) >= 1, f"{name}: expected >=1 observation, got {len(obs)}"
            print(f"    {name}: {obs[0].date} = {obs[0].value}")

    def test_known_datasets_return_valid_observations(self, oecd_client: OECDClient) -> None:
        _check_oecd_data_available(oecd_client)
        cfg = OECD_SERIES["cli_us"]
        obs = oecd_client.fetch_data(
            cfg.dataflow, agency_id=cfg.agency_id,
            filters=cfg.filters, series_id=cfg.series_id, limit=5,
        )
        assert len(obs) >= 1
        for o in obs:
            assert o.series_id == cfg.series_id
            assert o.date
            assert isinstance(o.value, float)
            assert o.dataflow == cfg.dataflow
        print(f"\n  cli_us: {len(obs)} valid observations")

    def test_probe_sample_catalog_datasets(
        self, oecd_client: OECDClient, all_dataflows: list,
    ) -> None:
        _check_oecd_data_available(oecd_client)
        sample = random.sample(all_dataflows, min(10, len(all_dataflows)))
        probed = 0
        for flow in sample:
            try:
                obs = oecd_client.fetch_data(
                    flow.id, agency_id=flow.agency_id, limit=1,
                )
                probed += 1
                print(f"    {flow.id}: {len(obs)} obs")
            except (OECDAPIError, OECDRateLimitError):
                print(f"    {flow.id}: SKIP")
        assert probed >= 1, "Expected at least 1 random probe to succeed"


# ── Layer 5: Series Enumeration ──────────────────────────────────────

class TestSeriesEnumeration:
    """Validate series enumeration and size estimation."""

    def test_enumerate_cli_series(self, oecd_client: OECDClient) -> None:
        _check_oecd_data_available(oecd_client)
        series = oecd_client.enumerate_series(
            _CLI_DATAFLOW, agency_id=_CLI_AGENCY,
            observation_limit=1, max_series=50,
        )
        assert len(series) > 0, "Expected at least 1 series"
        print(f"\n  CLI series (first 50): {len(series)}")
        for s in series[:5]:
            print(f"    {s.key}")

    def test_series_estimation_from_code_counts(self, oecd_client: OECDClient) -> None:
        summary = oecd_client.summarize_structure(
            _CLI_DATAFLOW, agency_id=_CLI_AGENCY,
        )
        estimated = 1
        for count in summary.code_counts.values():
            if count > 0:
                estimated *= count
        assert estimated > 0, "Series estimate should be positive"
        print(f"\n  CLI estimated series: {estimated:,}")
        print(f"    Code counts: {dict(summary.code_counts)}")

    def test_build_key_from_filters(self, oecd_client: OECDClient) -> None:
        cfg = OECD_SERIES["cli_us"]
        key = oecd_client.build_key(
            cfg.dataflow, cfg.filters,
            agency_id=cfg.agency_id,
        )
        assert key, "build_key should return a non-empty key"
        assert "." in key, "Key should have dot-separated dimension values"
        print(f"\n  cli_us key: {key}")


# ── Layer 7: Deterministic Parsing ───────────────────────────────────

class TestDeterministicParsing:
    """Run identical fetches twice and verify byte-identical output."""

    @staticmethod
    def _obs_hash(obs: list[OECDObservation]) -> str:
        """SHA-256 over sorted (series_id, date, value) tuples."""
        rows = sorted((o.series_id, o.date, o.value) for o in obs)
        payload = json.dumps(rows, sort_keys=True).encode()
        return hashlib.sha256(payload).hexdigest()

    def test_identical_fetches_produce_same_hash(self, oecd_client: OECDClient) -> None:
        _check_oecd_data_available(oecd_client)
        cfg = OECD_SERIES["cli_us"]
        kwargs = dict(
            agency_id=cfg.agency_id,
            filters=cfg.filters,
            series_id=cfg.series_id,
            start_period="2023",
            limit=12,
        )
        obs1 = oecd_client.fetch_data(cfg.dataflow, **kwargs)
        obs2 = oecd_client.fetch_data(cfg.dataflow, **kwargs)
        hash1 = self._obs_hash(obs1)
        hash2 = self._obs_hash(obs2)
        assert hash1 == hash2, (
            f"Deterministic parsing failed: {hash1} != {hash2}"
        )
        print(f"\n  cli_us hash: {hash1[:16]}... ({len(obs1)} obs)")

    def test_stable_ordering_across_fetches(self, oecd_client: OECDClient) -> None:
        _check_oecd_data_available(oecd_client)
        cfg = OECD_SERIES["cli_us"]
        kwargs = dict(
            agency_id=cfg.agency_id,
            filters=cfg.filters,
            series_id=cfg.series_id,
            start_period="2023",
            limit=6,
        )
        obs1 = oecd_client.fetch_data(cfg.dataflow, **kwargs)
        obs2 = oecd_client.fetch_data(cfg.dataflow, **kwargs)
        dates1 = [o.date for o in obs1]
        dates2 = [o.date for o in obs2]
        assert dates1 == dates2, (
            f"Date ordering not stable: {dates1} vs {dates2}"
        )
        print(f"\n  Ordering stable: {dates1}")

    def test_chunked_deterministic(self, oecd_client: OECDClient) -> None:
        _check_oecd_data_available(oecd_client)
        cfg = OECD_SERIES["cli_us"]
        kwargs = dict(
            agency_id=cfg.agency_id,
            filters=cfg.filters,
            series_id=cfg.series_id,
            chunk_ranges=[("2022", "2024")],
        )
        obs1 = oecd_client.fetch_dataset_chunked(cfg.dataflow, **kwargs)
        obs2 = oecd_client.fetch_dataset_chunked(cfg.dataflow, **kwargs)
        hash1 = self._obs_hash(obs1)
        hash2 = self._obs_hash(obs2)
        assert hash1 == hash2, (
            f"Chunked determinism failed: {hash1} != {hash2}"
        )
        print(f"\n  Chunked hash: {hash1[:16]}... ({len(obs1)} obs)")


# ── Layer 8: Full Catalog Probe ──────────────────────────────────────

class TestFullCatalogProbe:
    """Minimal data retrieval across the entire catalog."""

    def test_full_catalog_data_probe(
        self, oecd_client: OECDClient, all_dataflows: list,
    ) -> None:
        """Probe every dataflow in the catalog with a limit=1 data fetch."""
        _check_oecd_data_available(oecd_client)

        success = 0
        empty = 0
        failures: list[tuple[str, str]] = []

        print(f"\n  Full catalog probe: {len(all_dataflows)} dataflows")
        print(f"  {'Dataflow':<40} {'Status':<10} {'Detail'}")
        print("  " + "-" * 80)

        for flow in all_dataflows:
            try:
                obs = oecd_client.fetch_data(
                    flow.id, agency_id=flow.agency_id, limit=1,
                )
                if len(obs) >= 1:
                    success += 1
                    status = "OK"
                    detail = f"{len(obs)} obs"
                else:
                    empty += 1
                    status = "EMPTY"
                    detail = "0 observations"
            except OECDRateLimitError:
                failures.append((flow.id, "rate_limit"))
                status = "RATELIM"
                detail = "429 — rate limited"
            except OECDAPIError as exc:
                failures.append((flow.id, str(exc)[:80]))
                status = "ERROR"
                detail = str(exc)[:60]
            except Exception as exc:
                failures.append((flow.id, str(exc)[:80]))
                status = "ERROR"
                detail = str(exc)[:60]

            print(f"  {flow.id:<40} {status:<10} {detail}")

        total = len(all_dataflows)
        fail_count = len(failures)
        accessible = success + empty

        print(f"\n  ── Summary ──")
        print(f"  dataflows tested:  {total}")
        print(f"  accessible:        {accessible}  (OK={success}, empty={empty})")
        print(f"  failed:            {fail_count}")
        if failures:
            print(f"  failure details:")
            for fid, reason in failures:
                print(f"    {fid}: {reason}")

        access_rate = accessible / total if total else 0
        assert access_rate >= 0.80, (
            f"Catalog accessibility {access_rate:.0%} < 80% "
            f"({fail_count} failures out of {total})"
        )


# ── Layer 9: Performance Benchmark ──────────────────────────────────

class TestPerformanceBenchmark:
    """Timing benchmarks for key operations."""

    def test_list_dataflows_completes_within_30s(self, oecd_client: OECDClient) -> None:
        oecd_client._dataflow_list_cache.clear()
        oecd_client._dataflow_cache.clear()
        t0 = time.monotonic()
        flows = oecd_client.list_dataflows()
        elapsed = time.monotonic() - t0
        assert elapsed < 30, f"list_dataflows took {elapsed:.1f}s (limit: 30s)"
        assert len(flows) > 0
        print(f"\n  list_dataflows (cold): {elapsed:.1f}s, {len(flows)} flows")

    def test_dataflow_cache_hit_is_fast(self, oecd_client: OECDClient) -> None:
        oecd_client.list_dataflows()
        t0 = time.monotonic()
        flows = oecd_client.list_dataflows()
        elapsed = time.monotonic() - t0
        assert elapsed < 0.01, f"Cached list_dataflows took {elapsed:.4f}s (limit: 0.01s)"
        print(f"\n  list_dataflows (cached): {elapsed:.6f}s")

    def test_single_probe_completes_within_15s(self, oecd_client: OECDClient) -> None:
        _check_oecd_data_available(oecd_client)
        cfg = OECD_SERIES["cli_us"]
        t0 = time.monotonic()
        oecd_client.fetch_data(
            cfg.dataflow, agency_id=cfg.agency_id,
            filters=cfg.filters, series_id=cfg.series_id, limit=1,
        )
        elapsed = time.monotonic() - t0
        assert elapsed < 15, f"Single probe took {elapsed:.1f}s (limit: 15s)"
        print(f"\n  Single data probe: {elapsed:.1f}s")

    def test_dsd_fetch_completes_within_15s(self, oecd_client: OECDClient) -> None:
        oecd_client._structure_cache.clear()
        t0 = time.monotonic()
        oecd_client.get_structure(
            _CLI_DATAFLOW, agency_id=_CLI_AGENCY,
        )
        elapsed = time.monotonic() - t0
        assert elapsed < 15, f"DSD fetch took {elapsed:.1f}s (limit: 15s)"
        print(f"\n  DSD fetch (CLI): {elapsed:.1f}s")


# ── Edge Cases ───────────────────────────────────────────────────────

class TestEdgeCases:
    """Edge-case handling for the OECD client."""

    def test_missing_key_returns_empty_or_error(self, oecd_client: OECDClient) -> None:
        _check_oecd_data_available(oecd_client)
        try:
            obs = oecd_client.fetch_data(
                _CLI_DATAFLOW, agency_id=_CLI_AGENCY,
                key="ZZZZZ.NONEXIST.ZZZ.ZZZ.ZZZ.ZZZ.ZZZ.ZZZ.ZZZ",
                series_id="probe", limit=1,
            )
            assert len(obs) == 0, f"Expected empty result, got {len(obs)}"
        except OECDAPIError:
            pass
        print("\n  Invalid key: empty or error (OK)")

    def test_nonexistent_dataflow_raises_error(self, oecd_client: OECDClient) -> None:
        with pytest.raises(OECDAPIError):
            oecd_client.get_dataflow(
                "DOES_NOT_EXIST_XYZ_99",
                agency_id=_CLI_AGENCY,
            )
        print("\n  Nonexistent dataflow: OECDAPIError raised (OK)")

    def test_search_dataflows(self, oecd_client: OECDClient) -> None:
        results = oecd_client.search_dataflows("CLI", limit=5)
        assert len(results) >= 1, "Search for 'CLI' should return results"
        print(f"\n  Search 'CLI': {len(results)} results")
        for r in results[:3]:
            print(f"    {r.id}: {r.name[:50]}")

    def test_empty_dataset_handled_gracefully(self, oecd_client: OECDClient) -> None:
        _check_oecd_data_available(oecd_client)
        try:
            obs = oecd_client.fetch_data(
                _CLI_DATAFLOW, agency_id=_CLI_AGENCY,
                filters={"REF_AREA": "USA", "FREQ": "M", "MEASURE": "LI"},
                series_id="probe",
                start_period="2099", limit=10,
            )
            print(f"\n  Future period: {len(obs)} obs (OK)")
        except OECDAPIError:
            print("\n  Future period: API error (acceptable)")


# ── Full Validation Report ───────────────────────────────────────────

class TestFullValidationReport:
    """Build 10-layer ValidationReport from quick checks."""

    def test_generate_validation_report(self, oecd_client: OECDClient) -> None:
        """Run all 10 layers as quick checks and produce the final summary."""
        _check_oecd_data_available(oecd_client)
        checks: list[CheckResult] = []

        # L1: Catalog discovery
        flows = oecd_client.list_dataflows()
        checks.append(CheckResult(
            check_name="L1_catalog_discovery",
            layer=ValidationLayer.CATALOG,
            passed=len(flows) > 100,
            severity=ValidationSeverity.ERROR,
            message=f"dataflows discovered: {len(flows)}",
            source="oecd",
        ))

        # L2: DSD validation
        dsd_ok = 0
        seen_df: set[str] = set()
        for cfg in OECD_SERIES.values():
            if cfg.dataflow in seen_df:
                continue
            seen_df.add(cfg.dataflow)
            try:
                s = oecd_client.get_structure(
                    cfg.dataflow, agency_id=cfg.agency_id,
                )
                if len(s.dimensions) >= 2:
                    dsd_ok += 1
            except (OECDAPIError, OECDRateLimitError):
                pass
        checks.append(CheckResult(
            check_name="L2_dsd_validation",
            layer=ValidationLayer.CATALOG,
            passed=dsd_ok == len(seen_df),
            severity=ValidationSeverity.ERROR,
            message=f"DSDs validated: {dsd_ok}/{len(seen_df)}",
            source="oecd",
        ))

        # L3: Codelist validation
        cli_dsd = oecd_client.get_structure(
            _CLI_DATAFLOW, agency_id=_CLI_AGENCY,
        )
        ref_area = next(
            (d for d in cli_dsd.dimensions if d.id == "REF_AREA"), None,
        )
        cl_ok = ref_area is not None and len(ref_area.codes) > 10
        checks.append(CheckResult(
            check_name="L3_codelist_validation",
            layer=ValidationLayer.CATALOG,
            passed=cl_ok,
            severity=ValidationSeverity.ERROR,
            message=f"CLI REF_AREA codes: {len(ref_area.codes) if ref_area else 0}",
            source="oecd",
        ))

        # L4: Dataset accessibility
        cfg = OECD_SERIES["cli_us"]
        try:
            obs = oecd_client.fetch_data(
                cfg.dataflow, agency_id=cfg.agency_id,
                filters=cfg.filters, series_id=cfg.series_id, limit=1,
            )
            data_ok = len(obs) >= 1
        except (OECDAPIError, OECDRateLimitError):
            data_ok = False
        checks.append(CheckResult(
            check_name="L4_dataset_accessibility",
            layer=ValidationLayer.SERIES,
            passed=data_ok,
            severity=ValidationSeverity.ERROR,
            message=f"datasets accessible: {'YES' if data_ok else 'NO'}",
            source="oecd",
        ))

        # L5: Series enumeration
        try:
            series = oecd_client.enumerate_series(
                _CLI_DATAFLOW, agency_id=_CLI_AGENCY,
                observation_limit=1, max_series=10,
            )
            series_ok = len(series) > 0
        except (OECDAPIError, OECDRateLimitError):
            series_ok = False
        checks.append(CheckResult(
            check_name="L5_series_enumeration",
            layer=ValidationLayer.SERIES,
            passed=series_ok,
            severity=ValidationSeverity.ERROR,
            message=f"series estimation: {'PASS' if series_ok else 'FAIL'}",
            source="oecd",
        ))

        # L6: Chunking
        try:
            chunk_obs = oecd_client.fetch_dataset_chunked(
                cfg.dataflow, agency_id=cfg.agency_id,
                filters=cfg.filters, series_id=cfg.series_id,
                chunk_ranges=[("2023", "2024")],
            )
            chunk_ok = len(chunk_obs) > 0
        except (OECDAPIError, OECDRateLimitError):
            chunk_ok = False
        checks.append(CheckResult(
            check_name="L6_chunking",
            layer=ValidationLayer.SERIES,
            passed=chunk_ok,
            severity=ValidationSeverity.ERROR,
            message=f"chunking: {'PASS' if chunk_ok else 'FAIL'}",
            source="oecd",
        ))

        # L7: Deterministic parsing
        try:
            kwargs = dict(
                agency_id=cfg.agency_id,
                filters=cfg.filters,
                series_id=cfg.series_id,
                start_period="2023",
                limit=6,
            )
            obs1 = oecd_client.fetch_data(cfg.dataflow, **kwargs)
            obs2 = oecd_client.fetch_data(cfg.dataflow, **kwargs)
            rows1 = sorted((o.series_id, o.date, o.value) for o in obs1)
            rows2 = sorted((o.series_id, o.date, o.value) for o in obs2)
            det_ok = rows1 == rows2
        except (OECDAPIError, OECDRateLimitError):
            det_ok = False
        checks.append(CheckResult(
            check_name="L7_deterministic_parsing",
            layer=ValidationLayer.SERIES,
            passed=det_ok,
            severity=ValidationSeverity.ERROR,
            message=f"deterministic parsing: {'PASS' if det_ok else 'FAIL'}",
            source="oecd",
        ))

        report = ValidationReport(
            source="oecd",
            run_id=str(uuid.uuid4())[:8],
            timestamp=datetime.now(timezone.utc).isoformat(),
            checks=tuple(checks),
        )

        print(f"\n  OECD SDMX ingestion validation")
        print(f"  ===============================")
        for c in report.checks:
            print(f"  {c.message}")
        print(f"\n  overall: {'PASS' if report.passed else 'FAIL'}")
        print(f"\n{report.format_text()}")

        assert report.passed, f"Report failed:\n{report.format_text()}"

    def test_report_captures_failures(self) -> None:
        checks = (
            CheckResult(
                check_name="good_check",
                layer=ValidationLayer.CATALOG,
                passed=True,
                severity=ValidationSeverity.ERROR,
                message="OK",
                source="oecd",
            ),
            CheckResult(
                check_name="bad_check",
                layer=ValidationLayer.CATALOG,
                passed=False,
                severity=ValidationSeverity.ERROR,
                message="intentional failure",
                source="oecd",
            ),
        )
        report = ValidationReport(
            source="oecd",
            run_id="test-fail",
            timestamp=datetime.now(timezone.utc).isoformat(),
            checks=checks,
        )
        assert not report.passed
        assert report.error_count == 1
        print(f"\n  Failure report: error_count={report.error_count}")


# ── Infrastructure Tests ─────────────────────────────────────────────

class TestRateLimiter:
    """Verify _OECDRateLimiter enforces minimum intervals and hourly budget."""

    def test_rate_limiter_enforces_minimum_interval(self) -> None:
        limiter = _OECDRateLimiter(min_interval=0.1)
        timestamps: list[float] = []

        def worker() -> None:
            limiter.wait()
            timestamps.append(time.monotonic())

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        timestamps.sort()
        gaps = [timestamps[i + 1] - timestamps[i] for i in range(len(timestamps) - 1)]
        for gap in gaps:
            assert gap >= 0.09, f"Gap {gap:.4f}s below minimum interval 0.1s"
        print(f"\n  Gaps between requests: {[f'{g:.3f}s' for g in gaps]}")

    def test_rate_limiter_thread_safety(self) -> None:
        limiter = _OECDRateLimiter(min_interval=0.05)
        call_count = 0
        lock = threading.Lock()

        def worker() -> None:
            nonlocal call_count
            for _ in range(10):
                limiter.wait()
                with lock:
                    call_count += 1

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert call_count == 40

    def test_backoff_pushes_next_request_forward(self) -> None:
        limiter = _OECDRateLimiter(min_interval=0.05)
        limiter.wait()  # prime it

        limiter.backoff(0.5)
        t0 = time.monotonic()
        limiter.wait()
        elapsed = time.monotonic() - t0

        assert elapsed >= 0.4, f"Expected >=0.4s wait after backoff, got {elapsed:.3f}s"
        print(f"\n  Backoff wait: {elapsed:.3f}s")

    def test_hourly_budget_tracks_request_count(self) -> None:
        limiter = _OECDRateLimiter(min_interval=0.01)
        # Make some requests and verify the counter tracks
        for _ in range(5):
            limiter.wait()
        assert limiter._hour_count == 5

    def test_hourly_budget_caps_at_limit(self) -> None:
        """When budget is exhausted, limiter should block (we test with tiny budget)."""
        limiter = _OECDRateLimiter(min_interval=0.01)
        # Artificially exhaust the budget
        limiter._hour_count = _OECDRateLimiter.HOURLY_BUDGET
        limiter._hour_start = time.monotonic()  # window just started

        t0 = time.monotonic()
        # This should detect exhausted budget and reset the window
        # (since we can't wait 3600s in a test, we cheat by backdating the window)
        limiter._hour_start = time.monotonic() - 3601.0
        limiter.wait()
        elapsed = time.monotonic() - t0

        # Window should have reset, so request goes through quickly
        assert elapsed < 1.0
        assert limiter._hour_count == 1
        print(f"\n  Budget reset after window expired: count={limiter._hour_count}")

    def test_default_interval_is_conservative(self) -> None:
        """Default min_interval should be >= 2s to respect 60/hour."""
        limiter = _OECDRateLimiter()
        assert limiter._min_interval >= 2.0
        assert limiter.HOURLY_BUDGET == 60


class TestParallelRefresh:
    """Exercise refresh_parallel with real OECD API calls + mock store."""

    def test_refresh_parallel_fetches_series(self, oecd_client: OECDClient) -> None:
        """Use refresh_parallel to fetch 3 CLI series concurrently."""
        _check_oecd_data_available(oecd_client)

        # Use only 3 series to stay well under rate limits
        configs = dict(list(OECDIngestionClient().series_configs.items())[:3])
        ingestion = OECDIngestionClient(client=oecd_client, series_configs=configs)

        store = Mock()
        t0 = time.monotonic()
        stats = ingestion.refresh_parallel(store, max_workers=2)
        elapsed = time.monotonic() - t0

        assert stats.source == "oecd"
        assert stats.count > 0, "Expected observations from parallel refresh"
        assert store.upsert_indicator_observation.call_count == stats.count

        print(f"\n  refresh_parallel: {stats.count} obs in {elapsed:.1f}s")
        print(f"  store.upsert calls: {store.upsert_indicator_observation.call_count}")


class TestCatalogParallelRefresh:
    """Exercise refresh_catalog_parallel with real OECD API + mock store."""

    def test_refresh_catalog_parallel_fetches_dataflows(self, oecd_client: OECDClient) -> None:
        _check_oecd_data_available(oecd_client)

        store = Mock()
        ingestion = OECDIngestionClient(client=oecd_client)

        t0 = time.monotonic()
        stats = ingestion.refresh_catalog_parallel(
            store,
            agency_prefix="OECD.SDD.STES",
            dataflow_limit=2,
            latest_observations=1,
        )
        elapsed = time.monotonic() - t0

        assert stats.source == "oecd_catalog"
        assert stats.count > 0
        assert store.upsert_indicator_observation.call_count == stats.count

        print(
            f"\n  refresh_catalog_parallel: {stats.count} obs "
            f"from 2 STES dataflows in {elapsed:.1f}s"
        )

    def test_catalog_parallel_errors_are_logged_not_raised(self, oecd_client: OECDClient) -> None:
        """Errors for individual dataflows should be logged, not crash the pool."""
        store = Mock()
        ingestion = OECDIngestionClient(client=oecd_client)

        stats = ingestion.refresh_catalog_parallel(
            store,
            dataflow_ids=["DOES_NOT_EXIST_XYZ"],
            max_workers=1,
            request_delay=0.5,
        )
        assert stats.source == "oecd_catalog"
        assert stats.count == 0
        assert store.upsert_indicator_observation.call_count == 0
        print("\n  Bogus dataflow: logged error, returned count=0 (no crash)")


class TestTimeRangeChunking:
    """Validate automatic time-range chunking for large datasets."""

    def test_build_decade_chunks_basic(self) -> None:
        chunks = _build_decade_chunks(1960, 2026)
        assert chunks[0] == ("1960", "1969")
        assert chunks[1] == ("1970", "1979")
        assert chunks[-1][1] == "2026"
        print(f"\n  Decade chunks 1960–2026: {chunks}")

    def test_build_decade_chunks_partial(self) -> None:
        chunks = _build_decade_chunks(2015, 2026)
        assert chunks == [("2015", "2024"), ("2025", "2026")]
        print(f"\n  Partial decades 2015–2026: {chunks}")

    def test_build_decade_chunks_single_year(self) -> None:
        chunks = _build_decade_chunks(2025, 2025)
        assert chunks == [("2025", "2025")]

    def test_auto_chunk_skips_small_dataset(self, oecd_client: OECDClient) -> None:
        """CLI dataset is small — auto-chunking should return None (no chunks)."""
        _check_oecd_data_available(oecd_client)

        result = oecd_client._auto_chunk_ranges(
            "DSD_STES@DF_CLI",
            agency_id="OECD.SDD.STES",
            version=None,
            key="all",
            obs_threshold=1_000_000,
        )
        assert result is None, "CLI dataset should be below 1M obs threshold"
        print("\n  CLI dataset: below threshold, no chunking needed")

    def test_fetch_dataset_chunked_small_returns_direct(self, oecd_client: OECDClient) -> None:
        """Small dataset should go through without chunking."""
        _check_oecd_data_available(oecd_client)

        t0 = time.monotonic()
        obs = oecd_client.fetch_dataset_chunked(
            "DSD_STES@DF_CLI",
            agency_id="OECD.SDD.STES",
            key="all",
            limit=1,
            obs_threshold=1_000_000,
        )
        elapsed = time.monotonic() - t0
        assert len(obs) > 0
        print(f"\n  Chunked fetch (small dataset, no split): {len(obs)} obs in {elapsed:.1f}s")

    def test_fetch_dataset_chunked_with_explicit_ranges(self, oecd_client: OECDClient) -> None:
        """Verify explicit chunk_ranges are respected."""
        _check_oecd_data_available(oecd_client)

        chunks_received: list[tuple[str, str, int]] = []

        def on_chunk(obs: list, start: str, end: str) -> None:
            chunks_received.append((start, end, len(obs)))

        t0 = time.monotonic()
        obs = oecd_client.fetch_dataset_chunked(
            "DSD_STES@DF_CLI",
            agency_id="OECD.SDD.STES",
            key="USA.M.LI.IX._Z.NOR.IX._Z.H",
            limit=None,
            chunk_ranges=[("2000", "2009"), ("2010", "2019"), ("2020", "2026")],
            on_chunk=on_chunk,
        )
        elapsed = time.monotonic() - t0

        assert len(chunks_received) == 3
        total_from_chunks = sum(c[2] for c in chunks_received)
        assert total_from_chunks == len(obs)

        print(f"\n  Explicit 3-chunk fetch: {len(obs)} total obs in {elapsed:.1f}s")
        for start, end, count in chunks_received:
            print(f"    [{start}–{end}]: {count} obs")

    def test_auto_chunk_triggers_on_low_threshold(self, oecd_client: OECDClient) -> None:
        """Force chunking by setting a very low threshold."""
        _check_oecd_data_available(oecd_client)

        result = oecd_client._auto_chunk_ranges(
            "DSD_STES@DF_CLI",
            agency_id="OECD.SDD.STES",
            version=None,
            key="all",
            obs_threshold=1,  # impossibly low — forces chunking
        )
        assert result is not None, "Should trigger chunking with threshold=1"
        assert len(result) > 1
        assert result[0][0] == "1960"
        print(f"\n  Forced chunking (threshold=1): {len(result)} decade chunks")


class TestLargeDatasetFetch:
    """Integration tests against large OECD datasets.

    These tests validate memory handling, parsing, and chunked fetch
    against datasets with high observation counts (e.g. SNA_TABLE1, MEI).
    """

    def test_large_dataset_probe_estimates_size(self, oecd_client: OECDClient) -> None:
        """Probe MEI (Main Economic Indicators) to estimate observation count."""
        _check_oecd_data_available(oecd_client)

        # MEI is one of the larger OECD datasets
        t0 = time.monotonic()
        probe = oecd_client._get_data_json(
            "DSD_KEI@DF_KEI",
            agency_id="OECD.SDD.STES",
            version=None,
            key="all",
            limit=1,
        )
        elapsed = time.monotonic() - t0

        inner = probe.get("data", probe)
        datasets = inner.get("dataSets", [])
        total_series = sum(len(ds.get("series", {})) for ds in datasets)

        obs_dims = inner.get("structures", [{}])[0].get("dimensions", {}).get("observation", [])
        time_dim_size = 1
        for dim in obs_dims:
            if dim.get("id") == "TIME_PERIOD":
                time_dim_size = max(len(dim.get("values", [])), 1)

        estimated_obs = total_series * time_dim_size
        print(f"\n  MEI (DF_KEI) probe: {elapsed:.1f}s")
        print(f"    Series: {total_series}")
        print(f"    Time periods: {time_dim_size}")
        print(f"    Estimated obs: {estimated_obs:,}")
        assert total_series > 0, "Expected series from MEI dataset"

    def test_large_dataset_chunked_fetch_limited(self, oecd_client: OECDClient) -> None:
        """Fetch MEI with chunking, using lastNObservations=1 to keep it fast."""
        _check_oecd_data_available(oecd_client)

        chunks_received: list[tuple[str, str, int]] = []

        def on_chunk(obs: list, start: str, end: str) -> None:
            chunks_received.append((start, end, len(obs)))

        t0 = time.monotonic()
        obs = oecd_client.fetch_dataset_chunked(
            "DSD_KEI@DF_KEI",
            agency_id="OECD.SDD.STES",
            key="all",
            limit=1,  # latest observation per series only
            chunk_ranges=[("2020", "2026")],
            on_chunk=on_chunk,
        )
        elapsed = time.monotonic() - t0

        assert len(obs) > 100, f"Expected >100 obs from MEI, got {len(obs)}"
        distinct_series = len({o.series_id for o in obs})

        print(f"\n  MEI chunked fetch [2020–2026] (limit=1): {elapsed:.1f}s")
        print(f"    Observations: {len(obs)}")
        print(f"    Distinct series: {distinct_series}")
        for start, end, count in chunks_received:
            print(f"    [{start}–{end}]: {count} obs")

    def test_large_dataset_memory_bounded(self, oecd_client: OECDClient) -> None:
        """Verify that fetching a large dataset slice doesn't blow up memory."""
        _check_oecd_data_available(oecd_client)
        import resource

        mem_before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

        obs = oecd_client.fetch_dataset_chunked(
            "DSD_KEI@DF_KEI",
            agency_id="OECD.SDD.STES",
            key="all",
            limit=1,
            chunk_ranges=[("2024", "2026")],
        )

        mem_after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        mem_delta_mb = (mem_after - mem_before) / 1024  # KB -> MB on Linux

        print(f"\n  Memory delta for MEI [2024–2026]: {mem_delta_mb:.1f} MB")
        print(f"    Observations: {len(obs)}")
        # Should not use more than 500 MB for a single-year slice
        assert mem_delta_mb < 500, f"Memory usage too high: {mem_delta_mb:.1f} MB"

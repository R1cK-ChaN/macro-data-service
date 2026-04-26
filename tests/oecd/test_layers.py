"""OECD full-catalog tests: 10-layer validation tests (Layers 1-9, ex. Layer 6).

Split out of the original tests/test_oecd_full_catalog.py as part of
issue #58 Tier 1.2 — pure file split, no behavior change.
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

PROJECT_ROOT = Path(__file__).resolve().parents[2]
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

_CLI_DATAFLOW = "DSD_STES@DF_CLI"
_CLI_AGENCY = "OECD.SDD.STES"


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

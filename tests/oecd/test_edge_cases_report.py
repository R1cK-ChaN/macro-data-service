"""OECD full-catalog tests: edge cases + cross-layer aggregate report.

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

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

pytestmark = pytest.mark.integration

PROJECT_ROOT = Path(__file__).resolve().parents[1]
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

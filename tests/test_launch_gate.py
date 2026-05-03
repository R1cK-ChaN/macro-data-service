from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ingestion.quality import launch_gate as gate
from ingestion.validation import ValidationStore
from ingestion.validation._types import (
    CheckResult,
    ValidationLayer,
    ValidationReport,
    ValidationSeverity,
)
from storage import SQLiteEngineStore


def _passing_report() -> ValidationReport:
    check = CheckResult(
        check_name="concept_exists",
        layer=ValidationLayer.CONCEPT,
        passed=True,
        severity=ValidationSeverity.INFO,
        message="ok",
        source="CPI_US",
        timestamp="2026-05-03T00:00:00+00:00",
    )
    return ValidationReport(
        source="concept:CPI_US",
        run_id="concept-pass",
        timestamp="2026-05-03T00:00:00+00:00",
        checks=(check,),
    )


def _clean_digest() -> dict[str, Any]:
    return {
        "concepts_covered": 1,
        "concepts_total": 1,
        "coverage_pct": 100.0,
        "confirmed_24h": 1,
        "error_sources": [],
    }


def _manifest(*, macro_rows: int = 1, macro_launch_state: str = "available") -> dict[str, Any]:
    macro_status = "available" if macro_rows > 0 else "empty"
    return {
        "version": "v1",
        "generated_at": "2026-05-03T00:00:00+00:00",
        "summary": {"total": 2, "available": 1, "empty": 1},
        "datasets": [
            {
                "dataset": "macro_timeseries",
                "label": "Macro time series",
                "status": macro_status,
                "launch_state": macro_launch_state,
                "row_count": macro_rows,
                "quality_status": "unknown",
                "last_quality_run": None,
                "storage": "indicator_vintages",
            },
            {
                "dataset": "market_bars",
                "label": "Market bars",
                "status": "empty",
                "launch_state": "empty",
                "row_count": 0,
                "quality_status": "unknown",
                "last_quality_run": None,
                "storage": "clickhouse.bars_1d",
            },
        ],
    }


def _degraded_manifest() -> dict[str, Any]:
    manifest = _manifest(macro_rows=1)
    manifest["datasets"][0]["status"] = "degraded"
    manifest["datasets"][0]["quality_status"] = "fail"
    return manifest


class FakeStore:
    def __init__(self, db_path: Path, manifest: dict[str, Any]) -> None:
        self.db_path = db_path
        self._manifest = manifest
        self.seeded = False

    def seed_concept_map(self) -> None:
        self.seeded = True

    def get_data_manifest(self, *, market_bars: dict[str, Any]) -> dict[str, Any]:
        return self._manifest


class FakeValidationStore:
    def __init__(self) -> None:
        self.saved_reports: list[dict[str, Any]] = []
        self.saved_checks: list[list[dict[str, Any]]] = []
        self.closed = False

    def save_report(self, report: dict[str, Any]) -> None:
        self.saved_reports.append(report)

    def save_check_results(self, checks: list[dict[str, Any]]) -> None:
        self.saved_checks.append(checks)

    def close(self) -> None:
        self.closed = True


class FakeEngine:
    def __init__(self, reports: list[ValidationReport]) -> None:
        self._reports = reports

    def validate_all_concepts(self, store: FakeStore) -> list[ValidationReport]:
        return self._reports


def _patch_gate(
    monkeypatch,
    *,
    reports: list[ValidationReport],
    manifest: dict[str, Any],
    validation_store: FakeValidationStore,
) -> None:
    monkeypatch.setattr(
        gate,
        "SQLiteEngineStore",
        lambda db_path: FakeStore(db_path, manifest),
    )
    monkeypatch.setattr(gate, "ValidationStore", lambda db_path: validation_store)
    monkeypatch.setattr(
        gate,
        "ValidationEngine",
        lambda validation_store: FakeEngine(reports),
    )


def test_launch_gate_blocks_shadow_coverage_drop_and_persists_quality(
    tmp_path: Path,
    monkeypatch,
) -> None:
    validation_store = FakeValidationStore()
    _patch_gate(
        monkeypatch,
        reports=[_passing_report()],
        manifest=_manifest(),
        validation_store=validation_store,
    )

    result = gate.run_launch_gate(
        engine_db=tmp_path / "engine.db",
        digest_loader=lambda db: {
            **_clean_digest(),
            "concepts_covered": 0,
            "concepts_total": 1,
        },
        update_issue=False,
        secret_log_names=(),
        log_path=tmp_path / "launch_gate.log",
        state_path=tmp_path / "state.json",
    )

    assert result.status == "blocked"
    assert any(f.kind == "coverage_drop" for f in result.findings)
    saved = validation_store.saved_reports[-1]
    assert saved["source"] == gate.LAUNCH_GATE_QUALITY_SOURCE
    assert saved["passed"] is False
    assert saved["error_count"] >= 1
    log_payload = json.loads((tmp_path / "launch_gate.log").read_text().splitlines()[-1])
    assert log_payload["status"] == "blocked"


def test_launch_gate_treats_zero_check_validation_report_as_blocker(
    tmp_path: Path,
    monkeypatch,
) -> None:
    validation_store = FakeValidationStore()
    zero_check = ValidationReport(
        source="concept:CPI_US",
        run_id="zero",
        timestamp="2026-05-03T00:00:00+00:00",
        checks=(),
    )
    _patch_gate(
        monkeypatch,
        reports=[zero_check],
        manifest=_manifest(),
        validation_store=validation_store,
    )

    result = gate.run_launch_gate(
        engine_db=tmp_path / "engine.db",
        digest_loader=lambda db: _clean_digest(),
        update_issue=False,
        secret_log_names=(),
        log_path=tmp_path / "launch_gate.log",
        state_path=tmp_path / "state.json",
    )

    assert result.status == "blocked"
    assert result.concept_zero_check_count == 1
    assert any(f.kind == "zero_check_validation" for f in result.findings)


def test_launch_gate_blocks_available_dataset_with_empty_inventory(
    tmp_path: Path,
    monkeypatch,
) -> None:
    validation_store = FakeValidationStore()
    _patch_gate(
        monkeypatch,
        reports=[_passing_report()],
        manifest=_manifest(macro_rows=0, macro_launch_state="available"),
        validation_store=validation_store,
    )

    result = gate.run_launch_gate(
        engine_db=tmp_path / "engine.db",
        digest_loader=lambda db: _clean_digest(),
        update_issue=False,
        secret_log_names=(),
        log_path=tmp_path / "launch_gate.log",
        state_path=tmp_path / "state.json",
    )

    assert result.status == "blocked"
    assert any(f.kind == "dataset_inventory_empty" for f in result.findings)


def test_launch_gate_ignores_stale_manifest_degraded_status(
    tmp_path: Path,
    monkeypatch,
) -> None:
    validation_store = FakeValidationStore()
    _patch_gate(
        monkeypatch,
        reports=[_passing_report()],
        manifest=_degraded_manifest(),
        validation_store=validation_store,
    )

    result = gate.run_launch_gate(
        engine_db=tmp_path / "engine.db",
        digest_loader=lambda db: _clean_digest(),
        update_issue=False,
        secret_log_names=(),
        log_path=tmp_path / "launch_gate.log",
        state_path=tmp_path / "state.json",
    )

    assert result.status == "green"
    assert not any(f.kind == "manifest_dataset_degraded" for f in result.findings)


def test_manifest_uses_latest_global_data_quality_report(tmp_path: Path) -> None:
    store = SQLiteEngineStore(db_path=tmp_path / "engine.db")
    validation = ValidationStore(str(store.db_path))
    try:
        validation.save_report({
            "source": "macro_timeseries",
            "run_id": "macro-pass",
            "timestamp": "2026-05-03T10:00:00+00:00",
            "passed": True,
            "error_count": 0,
            "warning_count": 0,
            "total_checks": 1,
            "duration_ms": 1,
        })
        validation.save_report({
            "source": gate.LAUNCH_GATE_QUALITY_SOURCE,
            "run_id": "gate-fail",
            "timestamp": "2026-05-03T11:00:00+00:00",
            "passed": False,
            "error_count": 1,
            "warning_count": 0,
            "total_checks": 1,
            "duration_ms": 1,
        })

        rows = {
            row["dataset"]: row
            for row in store.get_data_manifest(market_bars={})["datasets"]
        }
    finally:
        validation.close()

    assert rows["macro_timeseries"]["quality_status"] == "fail"
    assert rows["macro_timeseries"]["last_quality_run"] == "2026-05-03T11:00:00+00:00"


def test_launch_gate_digest_loader_is_packaged(
    tmp_path: Path,
    monkeypatch,
) -> None:
    store = SQLiteEngineStore(db_path=tmp_path / "engine.db")
    store.seed_concept_map()
    monkeypatch.chdir(tmp_path)
    checks: list[gate.LaunchGateCheck] = []
    findings = []

    digest = gate._run_digest_check(
        db_path=store.db_path,
        digest_loader=None,
        checks=checks,
        findings=findings,
    )

    assert digest["concepts_total"] > 0
    assert checks[0].name == "shadow_digest_coverage"

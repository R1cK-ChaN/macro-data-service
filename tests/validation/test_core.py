"""Validation framework tests: framework types — CheckResult / ValidationReport / ValidationStore / ValidationEngine.

Split out of the original tests/test_validation_types.py as part of
issue #58 Tier 1.2 — pure file split, no behavior change.
"""

from __future__ import annotations

import os
import pytest
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from ingestion.validation import (
    CheckResult,
    ValidationConfig,
    ValidationEngine,
    ValidationLayer,
    ValidationReport,
    ValidationSeverity,
    ValidationStore,
)


class TestCheckResult:
    def test_create(self):
        r = CheckResult(
            check_name="test",
            layer=ValidationLayer.SCHEMA,
            passed=True,
            severity=ValidationSeverity.INFO,
            message="ok",
            source="worldbank",
        )
        assert r.passed is True
        assert r.layer == ValidationLayer.SCHEMA
        assert r.severity == ValidationSeverity.INFO

    def test_frozen(self):
        r = CheckResult(
            check_name="test",
            layer=ValidationLayer.SCHEMA,
            passed=True,
            severity=ValidationSeverity.INFO,
            message="ok",
        )
        with pytest.raises(AttributeError):
            r.passed = False  # type: ignore[misc]


class TestValidationReport:
    def test_passed_with_no_checks(self):
        report = ValidationReport(
            source="test", run_id="abc", timestamp="2024-01-01"
        )
        assert report.passed is True
        assert report.error_count == 0
        assert report.warning_count == 0

    def test_passed_with_all_passing(self):
        checks = (
            CheckResult("a", ValidationLayer.SCHEMA, True, ValidationSeverity.INFO, "ok"),
            CheckResult("b", ValidationLayer.SERIES, True, ValidationSeverity.ERROR, "ok"),
        )
        report = ValidationReport(
            source="test", run_id="abc", timestamp="2024-01-01", checks=checks
        )
        assert report.passed is True
        assert report.error_count == 0

    def test_failed_with_error(self):
        checks = (
            CheckResult("a", ValidationLayer.SCHEMA, True, ValidationSeverity.INFO, "ok"),
            CheckResult("b", ValidationLayer.SERIES, False, ValidationSeverity.ERROR, "bad"),
        )
        report = ValidationReport(
            source="test", run_id="abc", timestamp="2024-01-01", checks=checks
        )
        assert report.passed is False
        assert report.error_count == 1

    def test_passed_with_warning_only(self):
        checks = (
            CheckResult("a", ValidationLayer.SCHEMA, False, ValidationSeverity.WARNING, "warn"),
        )
        report = ValidationReport(
            source="test", run_id="abc", timestamp="2024-01-01", checks=checks
        )
        assert report.passed is True
        assert report.warning_count == 1

    def test_failed_with_critical(self):
        checks = (
            CheckResult("a", ValidationLayer.CATALOG, False, ValidationSeverity.CRITICAL, "fail"),
        )
        report = ValidationReport(
            source="test", run_id="abc", timestamp="2024-01-01", checks=checks
        )
        assert report.passed is False
        assert report.error_count == 1

    def test_to_dict(self):
        checks = (
            CheckResult("a", ValidationLayer.SCHEMA, True, ValidationSeverity.INFO, "ok"),
        )
        report = ValidationReport(
            source="test", run_id="abc", timestamp="2024-01-01", checks=checks
        )
        d = report.to_dict()
        assert d["source"] == "test"
        assert d["passed"] is True
        assert d["total_checks"] == 1
        assert len(d["checks"]) == 1

    def test_format_text(self):
        checks = (
            CheckResult("schema_check", ValidationLayer.SCHEMA, True, ValidationSeverity.INFO, "ok"),
            CheckResult("series_check", ValidationLayer.SERIES, False, ValidationSeverity.ERROR, "bad"),
        )
        report = ValidationReport(
            source="test", run_id="abc", timestamp="2024-01-01", checks=checks
        )
        text = report.format_text()
        assert "FAIL" in text
        assert "schema_check" in text
        assert "series_check" in text


class TestValidationStore:
    @pytest.fixture
    def store(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        s = ValidationStore(db_path)
        yield s
        s.close()

    def test_save_and_get_report(self, store: ValidationStore):
        report_dict = {
            "source": "worldbank",
            "run_id": "abc123",
            "timestamp": "2024-01-01T00:00:00",
            "passed": True,
            "error_count": 0,
            "warning_count": 0,
            "total_checks": 1,
            "duration_ms": 42,
            "checks": [],
        }
        store.save_report(report_dict)
        result = store.get_latest_report("worldbank")
        assert result is not None
        assert result["run_id"] == "abc123"
        assert result["passed"] is True

    def test_list_reports(self, store: ValidationStore):
        for i in range(3):
            store.save_report({
                "source": "worldbank",
                "run_id": f"run_{i}",
                "timestamp": f"2024-01-0{i + 1}T00:00:00",
                "passed": True,
                "error_count": 0,
                "warning_count": 0,
                "total_checks": 1,
                "duration_ms": 42,
                "checks": [],
            })
        reports = store.list_reports("worldbank")
        assert len(reports) == 3
        # Most recent first
        assert reports[0]["run_id"] == "run_2"

    def test_save_and_get_baseline(self, store: ValidationStore):
        store.save_baseline(
            "worldbank", "SP.POP.TOTL", "schema_fingerprint",
            {"field_names": ["date", "value"]},
            "2024-01-01",
        )
        result = store.get_baseline("worldbank", "SP.POP.TOTL", "schema_fingerprint")
        assert result is not None
        assert result["field_names"] == ["date", "value"]

    def test_baseline_upsert(self, store: ValidationStore):
        store.save_baseline(
            "worldbank", "SP.POP.TOTL", "schema_fingerprint",
            {"version": 1},
            "2024-01-01",
        )
        store.save_baseline(
            "worldbank", "SP.POP.TOTL", "schema_fingerprint",
            {"version": 2},
            "2024-01-02",
        )
        result = store.get_baseline("worldbank", "SP.POP.TOTL", "schema_fingerprint")
        assert result is not None
        assert result["version"] == 2

    def test_save_and_get_history(self, store: ValidationStore):
        from datetime import datetime, UTC

        now_iso = datetime.now(UTC).isoformat()
        checks = [
            {
                "source": "worldbank",
                "check_name": "schema_consistent",
                "layer": "schema",
                "passed": True,
                "severity": "info",
                "message": "ok",
                "series_id": "",
                "timestamp": now_iso,
                "details": {},
            }
        ]
        store.save_check_results(checks)
        history = store.get_history("worldbank", days=30)
        assert len(history) >= 1

    def test_get_latest_report_returns_none(self, store: ValidationStore):
        assert store.get_latest_report("nonexistent") is None


class TestValidationEngine:
    @pytest.fixture
    def engine(self, tmp_path):
        store = ValidationStore(str(tmp_path / "val.db"))
        e = ValidationEngine(store)
        yield e
        store.close()

    def test_validate_post_store_empty(self, engine: ValidationEngine):
        report = engine.validate_post_store("test_source", None)
        assert report.passed is True
        assert len(report.checks) == 0

    def test_validate_post_store_with_observations(self, engine: ValidationEngine):
        config = ValidationConfig(source="test", enable_series=True)
        engine.set_config(config)
        obs = [
            {"date": "2023-01-01", "value": 1.0},
            {"date": "2023-02-01", "value": 2.0},
            {"date": "2023-03-01", "value": 3.0},
        ]
        report = engine.validate_post_store(
            "test", None, stored_observations={"SERIES_1": obs}
        )
        assert report.source == "test"
        assert len(report.checks) > 0

    def test_validate_full_empty(self, engine: ValidationEngine):
        report = engine.validate_full("test")
        assert report.passed is True

    def test_config_defaults(self, engine: ValidationEngine):
        config = engine.get_config("unknown_source")
        assert config.enable_schema is True
        assert config.enable_series is True
        assert config.enable_catalog is False
        assert config.enable_anomaly is False

    def test_should_fail(self, engine: ValidationEngine):
        assert engine.should_fail("test") is False
        engine.set_config(ValidationConfig(source="test", fail_on_error=True))
        assert engine.should_fail("test") is True

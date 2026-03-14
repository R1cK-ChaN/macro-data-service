"""Tests for the validation framework types, store, and engine."""

from __future__ import annotations

import json
import os
import tempfile

import pytest

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from analyst.ingestion.validation import (
    CheckResult,
    ValidationConfig,
    ValidationEngine,
    ValidationLayer,
    ValidationReport,
    ValidationSeverity,
    ValidationStore,
)


# ── CheckResult ──────────────────────────────────────────────────


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


# ── ValidationReport ─────────────────────────────────────────────


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


# ── ValidationStore ──────────────────────────────────────────────


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


# ── ValidationEngine ─────────────────────────────────────────────


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


# ── Schema validation ────────────────────────────────────────────


class TestSchemaValidation:
    @pytest.fixture
    def store(self, tmp_path):
        s = ValidationStore(str(tmp_path / "schema.db"))
        yield s
        s.close()

    def test_first_run_captures_baseline(self, store: ValidationStore):
        from analyst.ingestion.validation._schema import check_schema

        items = [
            {"date": "2023", "value": 1.0, "country": "USA"},
            {"date": "2024", "value": 2.0, "country": "CHN"},
        ]
        results = check_schema("worldbank", "indicators", items, store)
        assert len(results) == 1
        assert results[0].check_name == "schema_baseline_captured"
        assert results[0].passed is True

        baseline = store.get_baseline("worldbank", "indicators", "schema_fingerprint")
        assert baseline is not None

    def test_consistent_schema(self, store: ValidationStore):
        from analyst.ingestion.validation._schema import check_schema

        items = [{"date": "2023", "value": 1.0, "country": "USA"}]
        check_schema("worldbank", "indicators", items, store)

        results = check_schema("worldbank", "indicators", items, store)
        assert any(r.check_name == "schema_consistent" for r in results)
        assert all(r.passed for r in results)

    def test_field_removed(self, store: ValidationStore):
        from analyst.ingestion.validation._schema import check_schema

        items_v1 = [{"date": "2023", "value": 1.0, "country": "USA"}]
        check_schema("worldbank", "indicators", items_v1, store)

        items_v2 = [{"date": "2023", "value": 1.0}]
        results = check_schema("worldbank", "indicators", items_v2, store)
        removed = [r for r in results if r.check_name == "schema_fields_removed"]
        assert len(removed) == 1
        assert removed[0].passed is False
        assert "country" in removed[0].details.get("removed_fields", [])

    def test_field_added(self, store: ValidationStore):
        from analyst.ingestion.validation._schema import check_schema

        items_v1 = [{"date": "2023", "value": 1.0}]
        check_schema("worldbank", "indicators", items_v1, store)

        items_v2 = [{"date": "2023", "value": 1.0, "note": "revised"}]
        results = check_schema("worldbank", "indicators", items_v2, store)
        added = [r for r in results if r.check_name == "schema_fields_added"]
        assert len(added) == 1
        assert added[0].passed is True


# ── Series validation ────────────────────────────────────────────


class TestSeriesValidation:
    def test_empty_series(self):
        from analyst.ingestion.validation._series import check_series_integrity

        results = check_series_integrity("worldbank", "TEST", [])
        assert any(r.check_name == "series_empty" for r in results)
        assert any(not r.passed for r in results)

    def test_row_count_match(self):
        from analyst.ingestion.validation._series import check_series_integrity

        obs = [{"date": f"2023-0{i}-01", "value": float(i)} for i in range(1, 6)]
        results = check_series_integrity("worldbank", "TEST", obs, expected_count=5)
        row_check = [r for r in results if r.check_name == "series_row_count"]
        assert len(row_check) == 1
        assert row_check[0].passed is True

    def test_row_count_mismatch(self):
        from analyst.ingestion.validation._series import check_series_integrity

        obs = [{"date": "2023-01-01", "value": 1.0}]
        results = check_series_integrity("worldbank", "TEST", obs, expected_count=5)
        row_check = [r for r in results if r.check_name == "series_row_count"]
        assert row_check[0].passed is False

    def test_missing_rate(self):
        from analyst.ingestion.validation._series import check_series_integrity

        obs = [
            {"date": "2023-01-01", "value": 1.0},
            {"date": "2023-02-01", "value": None},
            {"date": "2023-03-01", "value": 3.0},
        ]
        results = check_series_integrity("worldbank", "TEST", obs, max_missing_rate=0.2)
        missing = [r for r in results if r.check_name == "series_missing_rate"]
        assert len(missing) == 1
        assert missing[0].passed is False  # 33% > 20%

    def test_year_coverage(self):
        from analyst.ingestion.validation._series import check_series_integrity

        obs = [{"date": f"{y}-01-01", "value": 1.0} for y in range(1960, 2024)]
        results = check_series_integrity(
            "worldbank", "TEST", obs,
            expected_min_year=1960,
            expected_max_year=2023,
        )
        assert all(r.passed for r in results)


# ── Anomaly detection ────────────────────────────────────────────


class TestAnomalyDetection:
    @pytest.fixture
    def store(self, tmp_path):
        s = ValidationStore(str(tmp_path / "anomaly.db"))
        yield s
        s.close()

    def test_first_run_captures_baseline(self, store: ValidationStore):
        from analyst.ingestion.validation._anomaly import (
            check_anomalies,
            compute_series_profile,
        )

        obs = [{"date": f"2023-0{i}-01", "value": float(i)} for i in range(1, 7)]
        profile = compute_series_profile("TEST", "src", obs)
        results = check_anomalies("src", "TEST", profile, store)
        assert len(results) == 1
        assert results[0].check_name == "anomaly_baseline_captured"

    def test_no_anomaly(self, store: ValidationStore):
        from analyst.ingestion.validation._anomaly import (
            check_anomalies,
            compute_series_profile,
        )

        obs = [{"date": f"2023-0{i}-01", "value": float(i)} for i in range(1, 7)]
        profile = compute_series_profile("TEST", "src", obs)
        check_anomalies("src", "TEST", profile, store)

        results = check_anomalies("src", "TEST", profile, store)
        assert all(r.passed for r in results)

    def test_mean_shift_detected(self, store: ValidationStore):
        from analyst.ingestion.validation._anomaly import (
            check_anomalies,
            compute_series_profile,
        )

        obs_baseline = [{"date": f"2023-0{i}-01", "value": 100.0 + i} for i in range(1, 7)]
        profile_baseline = compute_series_profile("TEST", "src", obs_baseline)
        check_anomalies("src", "TEST", profile_baseline, store)

        obs_shifted = [{"date": f"2024-0{i}-01", "value": 200.0 + i} for i in range(1, 7)]
        profile_shifted = compute_series_profile("TEST", "src", obs_shifted)
        results = check_anomalies("src", "TEST", profile_shifted, store)
        mean_shift = [r for r in results if r.check_name == "anomaly_mean_shift"]
        assert len(mean_shift) == 1
        assert mean_shift[0].passed is False

    def test_compute_profile(self):
        from analyst.ingestion.validation._anomaly import compute_series_profile

        obs = [
            {"date": "2023-01-01", "value": 10.0},
            {"date": "2023-02-01", "value": 20.0},
            {"date": "2023-03-01", "value": 30.0},
        ]
        profile = compute_series_profile("TEST", "src", obs)
        assert profile.observation_count == 3
        assert profile.value_mean == 20.0
        assert profile.value_min == 10.0
        assert profile.value_max == 30.0
        assert profile.missing_rate == 0.0
        assert profile.date_min == "2023-01-01"
        assert profile.date_max == "2023-03-01"


# ── Catalog completeness ─────────────────────────────────────────


class TestCatalogCompleteness:
    def test_matching_counts(self):
        from analyst.ingestion.validation._catalog import (
            CatalogExpectation,
            check_catalog_completeness,
        )

        expectations = [
            CatalogExpectation("sources", lambda: 71, lambda: 71),
            CatalogExpectation("topics", lambda: 21, lambda: 21),
        ]
        results = check_catalog_completeness("worldbank", expectations)
        assert all(r.passed for r in results)

    def test_missing_items(self):
        from analyst.ingestion.validation._catalog import (
            CatalogExpectation,
            check_catalog_completeness,
        )

        expectations = [
            CatalogExpectation("indicators", lambda: 29470, lambda: 29400),
        ]
        results = check_catalog_completeness("worldbank", expectations)
        assert len(results) == 1
        assert results[0].passed is False

    def test_tolerance(self):
        from analyst.ingestion.validation._catalog import (
            CatalogExpectation,
            check_catalog_completeness,
        )

        expectations = [
            CatalogExpectation("topics", lambda: 21, lambda: 18, tolerance=5),
        ]
        results = check_catalog_completeness("worldbank", expectations)
        assert results[0].passed is True

    def test_api_error_handled(self):
        from analyst.ingestion.validation._catalog import (
            CatalogExpectation,
            check_catalog_completeness,
        )

        def raise_error():
            raise ConnectionError("API down")

        expectations = [CatalogExpectation("sources", raise_error, lambda: 0)]
        results = check_catalog_completeness("worldbank", expectations)
        assert len(results) == 1
        assert results[0].passed is False
        assert "API down" in results[0].message


# ── Cross-source checks ─────────────────────────────────────────


class TestCrossSource:
    def test_matching_values(self):
        from analyst.ingestion.validation._cross_source import (
            CrossSourcePair,
            check_cross_source,
        )

        pair = CrossSourcePair("a", "b", "level", 1.0, "test")
        obs_a = [{"date": "2023-01", "value": 100.0}, {"date": "2023-02", "value": 101.0}]
        obs_b = [{"date": "2023-01", "value": 100.5}, {"date": "2023-02", "value": 101.5}]
        results = check_cross_source(pair, obs_a, obs_b)
        level = [r for r in results if r.check_name == "cross_source_level"]
        assert len(level) == 1
        assert level[0].passed is True

    def test_divergent_values(self):
        from analyst.ingestion.validation._cross_source import (
            CrossSourcePair,
            check_cross_source,
        )

        pair = CrossSourcePair("a", "b", "level", 1.0, "test")
        obs_a = [{"date": "2023-01", "value": 100.0}]
        obs_b = [{"date": "2023-01", "value": 200.0}]
        results = check_cross_source(pair, obs_a, obs_b)
        level = [r for r in results if r.check_name == "cross_source_level"]
        assert level[0].passed is False

    def test_direction_agreement(self):
        from analyst.ingestion.validation._cross_source import (
            CrossSourcePair,
            check_cross_source,
        )

        pair = CrossSourcePair("a", "b", "direction", 5.0, "test")
        obs_a = [
            {"date": "2023-01", "value": 10.0},
            {"date": "2023-02", "value": 12.0},
            {"date": "2023-03", "value": 15.0},
        ]
        obs_b = [
            {"date": "2023-01", "value": 100.0},
            {"date": "2023-02", "value": 120.0},
            {"date": "2023-03", "value": 150.0},
        ]
        results = check_cross_source(pair, obs_a, obs_b)
        direction = [r for r in results if r.check_name == "cross_source_direction"]
        assert len(direction) == 1
        assert direction[0].passed is True

    def test_empty_data(self):
        from analyst.ingestion.validation._cross_source import (
            CrossSourcePair,
            check_cross_source,
        )

        pair = CrossSourcePair("a", "b", "level", 1.0, "test")
        results = check_cross_source(pair, [], [])
        assert any(r.check_name == "cross_source_data_available" for r in results)
        assert any(not r.passed for r in results)


# ── Data diff ────────────────────────────────────────────────────


class TestDataDiff:
    def test_identical_data(self):
        from analyst.ingestion.validation._diff import check_data_diff

        obs = [{"date": "2023-01-01", "value": 1.0}, {"date": "2023-02-01", "value": 2.0}]
        results = check_data_diff("worldbank", "TEST", obs, obs)
        hash_check = [r for r in results if r.check_name == "diff_hash_match"]
        assert hash_check[0].passed is True
        assert len(results) == 1  # only hash match, no diff results

    def test_revision_detected(self):
        from analyst.ingestion.validation._diff import check_data_diff

        api = [{"date": "2023-01-01", "value": 1.5}]
        db = [{"date": "2023-01-01", "value": 1.0}]
        results = check_data_diff("worldbank", "TEST", api, db)
        revised = [r for r in results if r.check_name == "diff_revised_values"]
        assert len(revised) == 1

    def test_new_dates(self):
        from analyst.ingestion.validation._diff import check_data_diff

        api = [
            {"date": "2023-01-01", "value": 1.0},
            {"date": "2023-02-01", "value": 2.0},
        ]
        db = [{"date": "2023-01-01", "value": 1.0}]
        results = check_data_diff("worldbank", "TEST", api, db)
        new = [r for r in results if r.check_name == "diff_new_dates"]
        assert len(new) == 1

    def test_removed_dates(self):
        from analyst.ingestion.validation._diff import check_data_diff

        api = [{"date": "2023-01-01", "value": 1.0}]
        db = [
            {"date": "2023-01-01", "value": 1.0},
            {"date": "2023-02-01", "value": 2.0},
        ]
        results = check_data_diff("worldbank", "TEST", api, db)
        removed = [r for r in results if r.check_name == "diff_removed_dates"]
        assert len(removed) == 1
        assert removed[0].passed is False


# ── Volume checks ────────────────────────────────────────────────


class TestVolumeChecks:
    @pytest.fixture
    def store(self, tmp_path):
        s = ValidationStore(str(tmp_path / "volume.db"))
        yield s
        s.close()

    def test_volume_in_range(self):
        from analyst.ingestion.validation._volume import VolumeExpectation, check_volume

        exp = VolumeExpectation("fred", min_rows=100, max_rows=10000)
        results = check_volume("fred", 500, expectation=exp)
        assert len(results) == 1
        assert results[0].passed is True
        assert results[0].check_name == "volume_in_range"

    def test_volume_below_minimum(self):
        from analyst.ingestion.validation._volume import VolumeExpectation, check_volume

        exp = VolumeExpectation("fred", min_rows=1000)
        results = check_volume("fred", 50, expectation=exp)
        below = [r for r in results if r.check_name == "volume_below_minimum"]
        assert len(below) == 1
        assert below[0].passed is False

    def test_volume_above_maximum(self):
        from analyst.ingestion.validation._volume import VolumeExpectation, check_volume

        exp = VolumeExpectation("fred", min_rows=100, max_rows=1000)
        results = check_volume("fred", 5000, expectation=exp)
        above = [r for r in results if r.check_name == "volume_above_maximum"]
        assert len(above) == 1
        assert above[0].passed is False

    def test_volume_regression_detected(self, store: ValidationStore):
        from analyst.ingestion.validation._volume import check_volume

        # First run: establish baseline
        check_volume("fred", 1000, validation_store=store)
        # Second run: 50% drop
        results = check_volume("fred", 500, validation_store=store)
        regression = [r for r in results if r.check_name == "volume_regression"]
        assert len(regression) == 1
        assert regression[0].passed is False  # 50% < 80% threshold

    def test_volume_no_regression(self, store: ValidationStore):
        from analyst.ingestion.validation._volume import check_volume

        check_volume("fred", 1000, validation_store=store)
        results = check_volume("fred", 950, validation_store=store)
        regression = [r for r in results if r.check_name == "volume_regression"]
        assert len(regression) == 1
        assert regression[0].passed is True  # 95% > 80% threshold

    def test_volume_batch(self):
        from analyst.ingestion.validation._volume import (
            VolumeExpectation,
            check_volume_batch,
        )

        expectations = {
            "fred": VolumeExpectation("fred", min_rows=100),
            "oecd": VolumeExpectation("oecd", min_rows=50),
        }
        results = check_volume_batch(
            {"fred": 200, "oecd": 10},
            expectations=expectations,
        )
        fred_ok = [r for r in results if r.source == "fred" and r.passed]
        oecd_fail = [r for r in results if r.source == "oecd" and not r.passed]
        assert len(fred_ok) >= 1
        assert len(oecd_fail) >= 1


# ── Freshness checks ────────────────────────────────────────────


class TestFreshnessChecks:
    def test_fresh_data(self):
        from datetime import datetime as dt, timedelta, UTC as _UTC
        from analyst.ingestion.validation._freshness import (
            FreshnessExpectation,
            check_freshness,
        )

        yesterday = (dt.now(_UTC) - timedelta(days=1)).strftime("%Y-%m-%d")
        exp = FreshnessExpectation("fred", max_staleness_days=7)
        results = check_freshness("fred", yesterday, exp)
        assert len(results) == 1
        assert results[0].passed is True

    def test_stale_data(self):
        from analyst.ingestion.validation._freshness import (
            FreshnessExpectation,
            check_freshness,
        )

        exp = FreshnessExpectation("fred", max_staleness_days=7)
        results = check_freshness("fred", "2020-01-01", exp)
        assert len(results) == 1
        assert results[0].passed is False

    def test_no_date(self):
        from analyst.ingestion.validation._freshness import check_freshness

        results = check_freshness("fred", "")
        assert any(r.check_name == "freshness_no_data" for r in results)
        assert any(not r.passed for r in results)

    def test_annual_data_with_long_threshold(self):
        from datetime import datetime as dt, timedelta, UTC as _UTC
        from analyst.ingestion.validation._freshness import (
            FreshnessExpectation,
            check_freshness,
        )

        # World Bank annual data — 300 days ago is still fresh at 400-day threshold
        old_date = (dt.now(_UTC) - timedelta(days=300)).strftime("%Y-%m-%d")
        exp = FreshnessExpectation("worldbank", max_staleness_days=400)
        results = check_freshness("worldbank", old_date, exp)
        assert results[0].passed is True

    def test_freshness_batch(self):
        from datetime import datetime as dt, timedelta, UTC as _UTC
        from analyst.ingestion.validation._freshness import (
            FreshnessExpectation,
            check_freshness_batch,
        )

        yesterday = (dt.now(_UTC) - timedelta(days=1)).strftime("%Y-%m-%d")
        expectations = {
            "fred": FreshnessExpectation("fred", max_staleness_days=7),
            "worldbank": FreshnessExpectation("worldbank", max_staleness_days=400),
        }
        results = check_freshness_batch(
            {"fred": yesterday, "worldbank": "2020-01-01"},
            expectations=expectations,
        )
        fred = [r for r in results if r.source == "fred"]
        wb = [r for r in results if r.source == "worldbank"]
        assert fred[0].passed is True
        assert wb[0].passed is False  # 2020 is >400 days stale


# ── Revision monitoring ──────────────────────────────────────────


class TestRevisionMonitoring:
    @pytest.fixture
    def store(self, tmp_path):
        s = ValidationStore(str(tmp_path / "revision.db"))
        yield s
        s.close()

    def test_no_vintages(self):
        from analyst.ingestion.validation._revision import check_revisions

        results = check_revisions("fred", "GDP", [])
        assert len(results) == 1
        assert results[0].check_name == "revision_no_vintages"

    def test_no_revisions(self):
        from analyst.ingestion.validation._revision import check_revisions

        vintages = [
            {"observation_date": "2023-01-01", "vintage_date": "2023-02-01", "value": 100.0},
            {"observation_date": "2023-04-01", "vintage_date": "2023-05-01", "value": 200.0},
        ]
        results = check_revisions("fred", "GDP", vintages)
        rate = [r for r in results if r.check_name == "revision_rate"]
        assert len(rate) == 1
        assert rate[0].passed is True
        assert "0 %" in rate[0].message or "0%" in rate[0].message

    def test_revisions_detected(self):
        from analyst.ingestion.validation._revision import check_revisions

        vintages = [
            {"observation_date": "2023-01-01", "vintage_date": "2023-02-01", "value": 100.0},
            {"observation_date": "2023-01-01", "vintage_date": "2023-03-01", "value": 105.0},
            {"observation_date": "2023-01-01", "vintage_date": "2023-04-01", "value": 103.0},
        ]
        results = check_revisions("fred", "GDP", vintages)
        rate = [r for r in results if r.check_name == "revision_rate"]
        assert rate[0].passed is False or "1/1" in rate[0].message
        magnitude = [r for r in results if r.check_name == "revision_magnitude"]
        assert len(magnitude) == 1
        assert magnitude[0].details["max_magnitude"] == 5.0

    def test_revision_summary(self):
        from analyst.ingestion.validation._revision import compute_revision_summary

        vintages = [
            {"observation_date": "2023-01-01", "vintage_date": "2023-02-01", "value": 100.0},
            {"observation_date": "2023-01-01", "vintage_date": "2023-05-01", "value": 110.0},
            {"observation_date": "2023-04-01", "vintage_date": "2023-05-01", "value": 200.0},
            {"observation_date": "2023-04-01", "vintage_date": "2023-08-01", "value": 200.0},
        ]
        summary = compute_revision_summary("GDP", "fred", vintages)
        assert summary.total_vintages == 4
        assert summary.revised_dates == 1  # only 2023-01-01 was revised
        assert summary.total_revision_count == 1
        assert summary.max_revision_magnitude == 10.0
        assert summary.latest_vintage_date == "2023-08-01"

    def test_revision_rate_spike(self, store: ValidationStore):
        from analyst.ingestion.validation._revision import check_revisions

        # First run: low revision rate
        vintages_v1 = [
            {"observation_date": "2023-01-01", "vintage_date": "2023-02-01", "value": 100.0},
            {"observation_date": "2023-04-01", "vintage_date": "2023-05-01", "value": 200.0},
        ]
        check_revisions("fred", "GDP", vintages_v1, store)

        # Second run: high revision rate (every date revised)
        vintages_v2 = [
            {"observation_date": "2023-01-01", "vintage_date": "2023-02-01", "value": 100.0},
            {"observation_date": "2023-01-01", "vintage_date": "2023-06-01", "value": 150.0},
            {"observation_date": "2023-04-01", "vintage_date": "2023-05-01", "value": 200.0},
            {"observation_date": "2023-04-01", "vintage_date": "2023-06-01", "value": 250.0},
        ]
        results = check_revisions("fred", "GDP", vintages_v2, store)
        spike = [r for r in results if r.check_name == "revision_rate_spike"]
        # Rate went from 0% to 100%: spike > 20pp
        assert len(spike) == 1
        assert spike[0].passed is False

    def test_magnitude_threshold(self):
        from analyst.ingestion.validation._revision import check_revisions

        vintages = [
            {"observation_date": "2023-01-01", "vintage_date": "2023-02-01", "value": 100.0},
            {"observation_date": "2023-01-01", "vintage_date": "2023-03-01", "value": 200.0},
        ]
        results = check_revisions("fred", "GDP", vintages, max_revision_magnitude=50.0)
        threshold = [r for r in results if r.check_name == "revision_magnitude_threshold"]
        assert len(threshold) == 1
        assert threshold[0].passed is False  # 100 > 50

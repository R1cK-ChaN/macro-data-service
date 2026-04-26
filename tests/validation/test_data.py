"""Validation framework tests: data-shape validation layers — schema / series / anomaly detection.

Split out of the original tests/test_validation_types.py as part of
issue #58 Tier 1.2 — pure file split, no behavior change.
"""

from __future__ import annotations

import os
import pytest
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from ingestion.validation import (
    ValidationStore,
)


class TestSchemaValidation:
    @pytest.fixture
    def store(self, tmp_path):
        s = ValidationStore(str(tmp_path / "schema.db"))
        yield s
        s.close()

    def test_first_run_captures_baseline(self, store: ValidationStore):
        from ingestion.validation._schema import check_schema

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
        from ingestion.validation._schema import check_schema

        items = [{"date": "2023", "value": 1.0, "country": "USA"}]
        check_schema("worldbank", "indicators", items, store)

        results = check_schema("worldbank", "indicators", items, store)
        assert any(r.check_name == "schema_consistent" for r in results)
        assert all(r.passed for r in results)

    def test_field_removed(self, store: ValidationStore):
        from ingestion.validation._schema import check_schema

        items_v1 = [{"date": "2023", "value": 1.0, "country": "USA"}]
        check_schema("worldbank", "indicators", items_v1, store)

        items_v2 = [{"date": "2023", "value": 1.0}]
        results = check_schema("worldbank", "indicators", items_v2, store)
        removed = [r for r in results if r.check_name == "schema_fields_removed"]
        assert len(removed) == 1
        assert removed[0].passed is False
        assert "country" in removed[0].details.get("removed_fields", [])

    def test_field_added(self, store: ValidationStore):
        from ingestion.validation._schema import check_schema

        items_v1 = [{"date": "2023", "value": 1.0}]
        check_schema("worldbank", "indicators", items_v1, store)

        items_v2 = [{"date": "2023", "value": 1.0, "note": "revised"}]
        results = check_schema("worldbank", "indicators", items_v2, store)
        added = [r for r in results if r.check_name == "schema_fields_added"]
        assert len(added) == 1
        assert added[0].passed is True


class TestSeriesValidation:
    def test_empty_series(self):
        from ingestion.validation._series import check_series_integrity

        results = check_series_integrity("worldbank", "TEST", [])
        assert any(r.check_name == "series_empty" for r in results)
        assert any(not r.passed for r in results)

    def test_row_count_match(self):
        from ingestion.validation._series import check_series_integrity

        obs = [{"date": f"2023-0{i}-01", "value": float(i)} for i in range(1, 6)]
        results = check_series_integrity("worldbank", "TEST", obs, expected_count=5)
        row_check = [r for r in results if r.check_name == "series_row_count"]
        assert len(row_check) == 1
        assert row_check[0].passed is True

    def test_row_count_mismatch(self):
        from ingestion.validation._series import check_series_integrity

        obs = [{"date": "2023-01-01", "value": 1.0}]
        results = check_series_integrity("worldbank", "TEST", obs, expected_count=5)
        row_check = [r for r in results if r.check_name == "series_row_count"]
        assert row_check[0].passed is False

    def test_missing_rate(self):
        from ingestion.validation._series import check_series_integrity

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
        from ingestion.validation._series import check_series_integrity

        obs = [{"date": f"{y}-01-01", "value": 1.0} for y in range(1960, 2024)]
        results = check_series_integrity(
            "worldbank", "TEST", obs,
            expected_min_year=1960,
            expected_max_year=2023,
        )
        assert all(r.passed for r in results)


class TestAnomalyDetection:
    @pytest.fixture
    def store(self, tmp_path):
        s = ValidationStore(str(tmp_path / "anomaly.db"))
        yield s
        s.close()

    def test_first_run_captures_baseline(self, store: ValidationStore):
        from ingestion.validation._anomaly import (
            check_anomalies,
            compute_series_profile,
        )

        obs = [{"date": f"2023-0{i}-01", "value": float(i)} for i in range(1, 7)]
        profile = compute_series_profile("TEST", "src", obs)
        results = check_anomalies("src", "TEST", profile, store)
        assert len(results) == 1
        assert results[0].check_name == "anomaly_baseline_captured"

    def test_no_anomaly(self, store: ValidationStore):
        from ingestion.validation._anomaly import (
            check_anomalies,
            compute_series_profile,
        )

        obs = [{"date": f"2023-0{i}-01", "value": float(i)} for i in range(1, 7)]
        profile = compute_series_profile("TEST", "src", obs)
        check_anomalies("src", "TEST", profile, store)

        results = check_anomalies("src", "TEST", profile, store)
        assert all(r.passed for r in results)

    def test_mean_shift_detected(self, store: ValidationStore):
        from ingestion.validation._anomaly import (
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
        from ingestion.validation._anomaly import compute_series_profile

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

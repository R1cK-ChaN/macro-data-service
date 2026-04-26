"""Validation framework tests: completeness + comparison layers — catalog completeness / cross-source / data diff / volume / freshness.

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


class TestCatalogCompleteness:
    def test_matching_counts(self):
        from ingestion.validation._catalog import (
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
        from ingestion.validation._catalog import (
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
        from ingestion.validation._catalog import (
            CatalogExpectation,
            check_catalog_completeness,
        )

        expectations = [
            CatalogExpectation("topics", lambda: 21, lambda: 18, tolerance=5),
        ]
        results = check_catalog_completeness("worldbank", expectations)
        assert results[0].passed is True

    def test_api_error_handled(self):
        from ingestion.validation._catalog import (
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


class TestCrossSource:
    def test_matching_values(self):
        from ingestion.validation._cross_source import (
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
        from ingestion.validation._cross_source import (
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
        from ingestion.validation._cross_source import (
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
        from ingestion.validation._cross_source import (
            CrossSourcePair,
            check_cross_source,
        )

        pair = CrossSourcePair("a", "b", "level", 1.0, "test")
        results = check_cross_source(pair, [], [])
        assert any(r.check_name == "cross_source_data_available" for r in results)
        assert any(not r.passed for r in results)


class TestDataDiff:
    def test_identical_data(self):
        from ingestion.validation._diff import check_data_diff

        obs = [{"date": "2023-01-01", "value": 1.0}, {"date": "2023-02-01", "value": 2.0}]
        results = check_data_diff("worldbank", "TEST", obs, obs)
        hash_check = [r for r in results if r.check_name == "diff_hash_match"]
        assert hash_check[0].passed is True
        assert len(results) == 1  # only hash match, no diff results

    def test_revision_detected(self):
        from ingestion.validation._diff import check_data_diff

        api = [{"date": "2023-01-01", "value": 1.5}]
        db = [{"date": "2023-01-01", "value": 1.0}]
        results = check_data_diff("worldbank", "TEST", api, db)
        revised = [r for r in results if r.check_name == "diff_revised_values"]
        assert len(revised) == 1

    def test_new_dates(self):
        from ingestion.validation._diff import check_data_diff

        api = [
            {"date": "2023-01-01", "value": 1.0},
            {"date": "2023-02-01", "value": 2.0},
        ]
        db = [{"date": "2023-01-01", "value": 1.0}]
        results = check_data_diff("worldbank", "TEST", api, db)
        new = [r for r in results if r.check_name == "diff_new_dates"]
        assert len(new) == 1

    def test_removed_dates(self):
        from ingestion.validation._diff import check_data_diff

        api = [{"date": "2023-01-01", "value": 1.0}]
        db = [
            {"date": "2023-01-01", "value": 1.0},
            {"date": "2023-02-01", "value": 2.0},
        ]
        results = check_data_diff("worldbank", "TEST", api, db)
        removed = [r for r in results if r.check_name == "diff_removed_dates"]
        assert len(removed) == 1
        assert removed[0].passed is False


class TestVolumeChecks:
    @pytest.fixture
    def store(self, tmp_path):
        s = ValidationStore(str(tmp_path / "volume.db"))
        yield s
        s.close()

    def test_volume_in_range(self):
        from ingestion.validation._volume import VolumeExpectation, check_volume

        exp = VolumeExpectation("fred", min_rows=100, max_rows=10000)
        results = check_volume("fred", 500, expectation=exp)
        assert len(results) == 1
        assert results[0].passed is True
        assert results[0].check_name == "volume_in_range"

    def test_volume_below_minimum(self):
        from ingestion.validation._volume import VolumeExpectation, check_volume

        exp = VolumeExpectation("fred", min_rows=1000)
        results = check_volume("fred", 50, expectation=exp)
        below = [r for r in results if r.check_name == "volume_below_minimum"]
        assert len(below) == 1
        assert below[0].passed is False

    def test_volume_above_maximum(self):
        from ingestion.validation._volume import VolumeExpectation, check_volume

        exp = VolumeExpectation("fred", min_rows=100, max_rows=1000)
        results = check_volume("fred", 5000, expectation=exp)
        above = [r for r in results if r.check_name == "volume_above_maximum"]
        assert len(above) == 1
        assert above[0].passed is False

    def test_volume_regression_detected(self, store: ValidationStore):
        from ingestion.validation._volume import check_volume

        # First run: establish baseline
        check_volume("fred", 1000, validation_store=store)
        # Second run: 50% drop
        results = check_volume("fred", 500, validation_store=store)
        regression = [r for r in results if r.check_name == "volume_regression"]
        assert len(regression) == 1
        assert regression[0].passed is False  # 50% < 80% threshold

    def test_volume_no_regression(self, store: ValidationStore):
        from ingestion.validation._volume import check_volume

        check_volume("fred", 1000, validation_store=store)
        results = check_volume("fred", 950, validation_store=store)
        regression = [r for r in results if r.check_name == "volume_regression"]
        assert len(regression) == 1
        assert regression[0].passed is True  # 95% > 80% threshold

    def test_volume_batch(self):
        from ingestion.validation._volume import (
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


class TestFreshnessChecks:
    def test_fresh_data(self):
        from datetime import datetime as dt, timedelta, UTC as _UTC
        from ingestion.validation._freshness import (
            FreshnessExpectation,
            check_freshness,
        )

        yesterday = (dt.now(_UTC) - timedelta(days=1)).strftime("%Y-%m-%d")
        exp = FreshnessExpectation("fred", max_staleness_days=7)
        results = check_freshness("fred", yesterday, exp)
        assert len(results) == 1
        assert results[0].passed is True

    def test_stale_data(self):
        from ingestion.validation._freshness import (
            FreshnessExpectation,
            check_freshness,
        )

        exp = FreshnessExpectation("fred", max_staleness_days=7)
        results = check_freshness("fred", "2020-01-01", exp)
        assert len(results) == 1
        assert results[0].passed is False

    def test_no_date(self):
        from ingestion.validation._freshness import check_freshness

        results = check_freshness("fred", "")
        assert any(r.check_name == "freshness_no_data" for r in results)
        assert any(not r.passed for r in results)

    def test_annual_data_with_long_threshold(self):
        from datetime import datetime as dt, timedelta, UTC as _UTC
        from ingestion.validation._freshness import (
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
        from ingestion.validation._freshness import (
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

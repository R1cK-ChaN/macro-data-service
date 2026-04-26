"""Validation framework tests: lineage layers — revision monitoring / lineage validation / dimension validation.

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


class TestRevisionMonitoring:
    @pytest.fixture
    def store(self, tmp_path):
        s = ValidationStore(str(tmp_path / "revision.db"))
        yield s
        s.close()

    def test_no_vintages(self):
        from ingestion.validation._revision import check_revisions

        results = check_revisions("fred", "GDP", [])
        assert len(results) == 1
        assert results[0].check_name == "revision_no_vintages"

    def test_no_revisions(self):
        from ingestion.validation._revision import check_revisions

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
        from ingestion.validation._revision import check_revisions

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
        from ingestion.validation._revision import compute_revision_summary

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
        from ingestion.validation._revision import check_revisions

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
        from ingestion.validation._revision import check_revisions

        vintages = [
            {"observation_date": "2023-01-01", "vintage_date": "2023-02-01", "value": 100.0},
            {"observation_date": "2023-01-01", "vintage_date": "2023-03-01", "value": 200.0},
        ]
        results = check_revisions("fred", "GDP", vintages, max_revision_magnitude=50.0)
        threshold = [r for r in results if r.check_name == "revision_magnitude_threshold"]
        assert len(threshold) == 1
        assert threshold[0].passed is False  # 100 > 50


class TestLineageValidation:
    def test_complete_lineage(self):
        from ingestion.validation._lineage import check_lineage

        obs = [
            {"source": "fred", "series_id": "CPIAUCSL", "date": "2023-01-01", "value": 100.0},
            {"source": "fred", "series_id": "CPIAUCSL", "date": "2023-02-01", "value": 101.0},
        ]
        results = check_lineage("fred", obs)
        assert all(r.passed for r in results)

    def test_missing_source(self):
        from ingestion.validation._lineage import check_lineage

        obs = [
            {"source": "", "series_id": "CPIAUCSL", "date": "2023-01-01", "value": 100.0},
            {"source": "fred", "series_id": "CPIAUCSL", "date": "2023-02-01", "value": 101.0},
        ]
        results = check_lineage("fred", obs)
        src = [r for r in results if r.check_name == "lineage_source"]
        assert len(src) == 1
        assert src[0].passed is False

    def test_missing_series_id(self):
        from ingestion.validation._lineage import check_lineage

        obs = [
            {"source": "fred", "series_id": "", "date": "2023-01-01", "value": 100.0},
        ]
        results = check_lineage("fred", obs)
        sid = [r for r in results if r.check_name == "lineage_series_id"]
        assert sid[0].passed is False

    def test_missing_date(self):
        from ingestion.validation._lineage import check_lineage

        obs = [
            {"source": "fred", "series_id": "CPIAUCSL", "date": "", "value": 100.0},
        ]
        results = check_lineage("fred", obs)
        d = [r for r in results if r.check_name == "lineage_date"]
        assert d[0].passed is False

    def test_invalid_date_format(self):
        from ingestion.validation._lineage import check_lineage

        obs = [
            {"source": "fred", "series_id": "CPIAUCSL", "date": "abc", "value": 100.0},
        ]
        results = check_lineage("fred", obs)
        d = [r for r in results if r.check_name == "lineage_date"]
        assert d[0].passed is False

    def test_family_id_required(self):
        from ingestion.validation._lineage import check_lineage

        obs = [
            {"source": "fred", "series_id": "CPIAUCSL", "date": "2023-01-01", "value": 100.0, "obs_family_id": None},
        ]
        results = check_lineage("fred", obs, require_family_id=True)
        fid = [r for r in results if r.check_name == "lineage_family_id"]
        assert len(fid) == 1
        assert fid[0].passed is False

    def test_family_id_present(self):
        from ingestion.validation._lineage import check_lineage

        obs = [
            {"source": "fred", "series_id": "CPIAUCSL", "date": "2023-01-01",
             "value": 100.0, "obs_family_id": "us.inflation.cpi_all"},
        ]
        results = check_lineage("fred", obs, require_family_id=True)
        fid = [r for r in results if r.check_name == "lineage_family_id"]
        assert fid[0].passed is True

    def test_empty_list(self):
        from ingestion.validation._lineage import check_lineage

        results = check_lineage("fred", [])
        assert results == []


class TestDimensionValidation:
    def test_valid_dimensions(self):
        from ingestion.validation._dimensions import check_dimensions

        families = [
            {
                "family_id": "us.inflation.cpi_all",
                "source_id": "fred",
                "frequency": "monthly",
                "unit": "index",
                "seasonal_adjustment": "sa",
                "country_code": "US",
            },
            {
                "family_id": "us.rates.fed_funds",
                "source_id": "fred",
                "frequency": "daily",
                "unit": "percent",
                "seasonal_adjustment": "none",
                "country_code": "US",
            },
        ]
        results = check_dimensions("fred", families)
        assert all(r.passed for r in results)

    def test_invalid_frequency(self):
        from ingestion.validation._dimensions import check_dimensions

        families = [
            {
                "family_id": "test.bad",
                "source_id": "fred",
                "frequency": "biweekly",
                "unit": "percent",
                "seasonal_adjustment": "sa",
                "country_code": "US",
            },
        ]
        results = check_dimensions("fred", families)
        freq = [r for r in results if r.check_name == "dimension_frequency"]
        assert freq[0].passed is False

    def test_invalid_unit(self):
        from ingestion.validation._dimensions import check_dimensions

        families = [
            {
                "family_id": "test.bad",
                "source_id": "fred",
                "frequency": "monthly",
                "unit": "bushels_per_acre",
                "seasonal_adjustment": "sa",
                "country_code": "US",
            },
        ]
        results = check_dimensions("fred", families)
        unit = [r for r in results if r.check_name == "dimension_unit"]
        assert unit[0].passed is False

    def test_invalid_seasonal_adjustment(self):
        from ingestion.validation._dimensions import check_dimensions

        families = [
            {
                "family_id": "test.bad",
                "source_id": "fred",
                "frequency": "monthly",
                "unit": "percent",
                "seasonal_adjustment": "double_adjusted",
                "country_code": "US",
            },
        ]
        results = check_dimensions("fred", families)
        sa = [r for r in results if r.check_name == "dimension_seasonal_adjustment"]
        assert sa[0].passed is False

    def test_unrecognized_country_code(self):
        from ingestion.validation._dimensions import check_dimensions

        families = [
            {
                "family_id": "test.bad",
                "source_id": "fred",
                "frequency": "monthly",
                "unit": "percent",
                "seasonal_adjustment": "sa",
                "country_code": "ZZ",
            },
        ]
        results = check_dimensions("fred", families)
        cc = [r for r in results if r.check_name == "dimension_country_code"]
        assert cc[0].passed is False

    def test_invalid_source_id(self):
        from ingestion.validation._dimensions import check_dimensions

        families = [
            {
                "family_id": "test.bad",
                "source_id": "unknown_provider",
                "frequency": "monthly",
                "unit": "percent",
                "seasonal_adjustment": "sa",
                "country_code": "US",
            },
        ]
        results = check_dimensions("fred", families)
        sid = [r for r in results if r.check_name == "dimension_source_id"]
        assert sid[0].passed is False

    def test_empty_families(self):
        from ingestion.validation._dimensions import check_dimensions

        results = check_dimensions("fred", [])
        assert results == []

    def test_multiple_errors(self):
        from ingestion.validation._dimensions import check_dimensions

        families = [
            {
                "family_id": "test.bad",
                "source_id": "xyz",
                "frequency": "biweekly",
                "unit": "bushels",
                "seasonal_adjustment": "triple",
                "country_code": "ZZ",
            },
        ]
        results = check_dimensions("fred", families)
        failed = [r for r in results if not r.passed]
        assert len(failed) == 5  # all five dimensions fail

"""Tests for the normalization layer and concept_map cross-source registry."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from ingestion.normalization import Normalizer, normalize_observation_date
from ingestion.types import CanonicalRow, RawObservation, RawSeries
from storage.sqlite import ConceptMapRecord, ObsFamilyRecord, SQLiteEngineStore


# ── Fixtures ──────────────────────────────────────────────────────────


@pytest.fixture()
def sample_family() -> ObsFamilyRecord:
    return ObsFamilyRecord(
        family_id="us.inflation.cpi_all",
        source_id="fred",
        provider_series_id="CPIAUCSL",
        canonical_name="CPI All Urban Consumers",
        short_name="CPI",
        unit="index",
        frequency="monthly",
        seasonal_adjustment="sa",
        country_code="US",
        topic_code="inflation",
        category="consumer_prices",
        is_active=True,
        has_vintages=True,
    )


@pytest.fixture()
def sample_raw_series() -> RawSeries:
    return RawSeries(
        source="fred",
        series_id="CPIAUCSL",
        observations=(
            RawObservation(date="2024-01-01", value=312.3),
            RawObservation(date="2024-02-01", value=313.1),
            RawObservation(date="2024-03-01", value=314.0),
        ),
        fetched_at="2024-04-01T00:00:00+00:00",
        series_metadata={"name": "CPI All Urban", "category": "inflation", "freq": "monthly"},
    )


@pytest.fixture()
def store() -> SQLiteEngineStore:
    with tempfile.TemporaryDirectory() as td:
        db_path = Path(td) / "test.db"
        s = SQLiteEngineStore(db_path)
        yield s


# ── Normalizer tests ─────────────────────────────────────────────────


class TestNormalizerWithFamily:
    def test_normalize_uses_family_metadata(self, sample_family, sample_raw_series):
        lookup = {("fred", "CPIAUCSL"): sample_family}
        normalizer = Normalizer(family_lookup=lookup)
        rows = normalizer.normalize(sample_raw_series)

        assert len(rows) == 3
        r = rows[0]
        assert isinstance(r, CanonicalRow)
        assert r.series_id == "CPIAUCSL"
        assert r.source == "fred"
        assert r.date == "2024-01-01"
        assert r.value == 312.3
        assert r.country_code == "US"
        assert r.frequency == "monthly"
        assert r.concept == "inflation"
        assert r.unit == "index"
        assert r.seasonal_adjustment == "sa"
        assert r.obs_family_id == "us.inflation.cpi_all"

    def test_normalize_batch(self, sample_family, sample_raw_series):
        lookup = {("fred", "CPIAUCSL"): sample_family}
        normalizer = Normalizer(family_lookup=lookup)
        rows = normalizer.normalize_batch([sample_raw_series, sample_raw_series])
        assert len(rows) == 6


class TestNormalizerFallback:
    def test_normalize_falls_back_to_metadata(self):
        normalizer = Normalizer(family_lookup={})
        series = RawSeries(
            source="custom",
            series_id="CUSTOM_1",
            observations=(
                RawObservation(date="2024-01-01", value=99.0),
            ),
            fetched_at="2024-04-01T00:00:00+00:00",
            series_metadata={
                "category": "growth",
                "freq": "quarterly",
                "country_code": "JP",
                "unit": "percent",
            },
        )
        rows = normalizer.normalize(series)
        assert len(rows) == 1
        r = rows[0]
        assert r.country_code == "JP"
        assert r.frequency == "quarterly"
        assert r.concept == "growth"
        assert r.unit == "percent"
        assert r.seasonal_adjustment == "none"
        assert r.obs_family_id is None

    def test_frequency_alias_mapping(self):
        normalizer = Normalizer(family_lookup={})
        series = RawSeries(
            source="test", series_id="T",
            observations=(RawObservation(date="2024-01-01", value=1.0),),
            fetched_at="", series_metadata={"freq": "Q"},
        )
        rows = normalizer.normalize(series)
        assert rows[0].frequency == "quarterly"

    def test_empty_observations(self):
        normalizer = Normalizer(family_lookup={})
        series = RawSeries(
            source="test", series_id="T",
            observations=(), fetched_at="", series_metadata={},
        )
        assert normalizer.normalize(series) == []


class TestBuildFamilyLookup:
    def test_build_lookup(self, sample_family):
        lookup = Normalizer.build_family_lookup([sample_family])
        assert ("fred", "CPIAUCSL") in lookup
        assert lookup[("fred", "CPIAUCSL")].unit == "index"


# ── Concept map tests ────────────────────────────────────────────────


class TestConceptMapSeed:
    def test_seed_creates_concepts(self, store):
        store.seed_concept_map()
        concepts = store.list_concepts()
        assert "CPI_US" in concepts
        assert "UNEMP_US" in concepts
        assert len(concepts) >= 10

    def test_seed_is_idempotent(self, store):
        store.seed_concept_map()
        store.seed_concept_map()
        concepts = store.list_concepts()
        assert len(concepts) >= 10


class TestConceptMapQueries:
    def test_get_concept_series_returns_mappings(self, store):
        store.seed_concept_map()
        mappings = store.get_concept_series("CPI_US")
        assert len(mappings) == 2
        sources = {m.source_id for m in mappings}
        assert sources == {"fred", "bls"}
        assert all(isinstance(m, ConceptMapRecord) for m in mappings)

    def test_get_concept_series_unknown_returns_empty(self, store):
        store.seed_concept_map()
        assert store.get_concept_series("NONEXISTENT") == []

    def test_list_concepts_by_country(self, store):
        store.seed_concept_map()
        us = store.list_concepts(country_code="US")
        assert "CPI_US" in us
        assert "CPI_CN" not in us

        cn = store.list_concepts(country_code="CN")
        assert "CPI_CN" in cn
        assert "CPI_US" not in cn

    def test_policy_rate_has_cross_check(self, store):
        store.seed_concept_map()
        mappings = store.get_concept_series("POLICY_RATE_US")
        roles = {m.role for m in mappings}
        assert "primary" in roles
        assert "cross_check" in roles

    def test_cpi_eu_has_two_sources(self, store):
        store.seed_concept_map()
        mappings = store.get_concept_series("CPI_EU")
        sources = {m.source_id for m in mappings}
        assert sources == {"imf", "eurostat"}

    def test_get_concept_observations_empty_db(self, store):
        store.seed_concept_map()
        obs = store.get_concept_observations("CPI_US")
        assert obs == []


# ── Date normalization tests ─────────────────────────────────────────


class TestNormalizeObservationDateDaily:
    def test_passthrough(self):
        assert normalize_observation_date("2024-01-15", "daily") == "2024-01-15"

    def test_passthrough_weekday(self):
        assert normalize_observation_date("2024-03-22", "daily") == "2024-03-22"


class TestNormalizeObservationDateMonthly:
    def test_already_first_of_month(self):
        assert normalize_observation_date("2024-01-01", "monthly") == "2024-01-01"

    def test_mid_month_snaps_to_first(self):
        assert normalize_observation_date("2024-01-15", "monthly") == "2024-01-01"

    def test_yyyy_mm_format(self):
        assert normalize_observation_date("2024-01", "monthly") == "2024-01-01"

    def test_imf_format(self):
        assert normalize_observation_date("2025-M09", "monthly") == "2025-09-01"

    def test_eurostat_no_dash(self):
        assert normalize_observation_date("2024M01", "monthly") == "2024-01-01"


class TestNormalizeObservationDateQuarterly:
    def test_quarter_format_dash(self):
        assert normalize_observation_date("2024-Q1", "quarterly") == "2024-01-01"
        assert normalize_observation_date("2024-Q2", "quarterly") == "2024-04-01"
        assert normalize_observation_date("2024-Q3", "quarterly") == "2024-07-01"
        assert normalize_observation_date("2024-Q4", "quarterly") == "2024-10-01"

    def test_quarter_format_no_dash(self):
        assert normalize_observation_date("2024Q1", "quarterly") == "2024-01-01"
        assert normalize_observation_date("2024Q3", "quarterly") == "2024-07-01"

    def test_bea_quarter_end_to_start(self):
        """BEA returns quarter-end dates (03-31, 06-30, etc.)."""
        assert normalize_observation_date("2024-03-31", "quarterly") == "2024-01-01"
        assert normalize_observation_date("2024-06-30", "quarterly") == "2024-04-01"
        assert normalize_observation_date("2024-09-30", "quarterly") == "2024-07-01"
        assert normalize_observation_date("2024-12-31", "quarterly") == "2024-10-01"

    def test_already_quarter_start(self):
        assert normalize_observation_date("2024-01-01", "quarterly") == "2024-01-01"
        assert normalize_observation_date("2024-04-01", "quarterly") == "2024-04-01"


class TestNormalizeObservationDateAnnual:
    def test_bare_year(self):
        assert normalize_observation_date("2023", "annual") == "2023-01-01"

    def test_full_date_snaps_to_jan1(self):
        assert normalize_observation_date("2024-12-31", "annual") == "2024-01-01"
        assert normalize_observation_date("2024-06-15", "annual") == "2024-01-01"

    def test_already_jan1(self):
        assert normalize_observation_date("2024-01-01", "annual") == "2024-01-01"


class TestNormalizeObservationDateWeekly:
    def test_iso_week(self):
        # 2024-W01 Monday is 2024-01-01
        assert normalize_observation_date("2024-W01", "weekly") == "2024-01-01"

    def test_iso_week_mid_year(self):
        # 2024-W26 Monday is 2024-06-24
        assert normalize_observation_date("2024-W26", "weekly") == "2024-06-24"

    def test_daily_date_passthrough_for_weekly(self):
        assert normalize_observation_date("2024-01-15", "weekly") == "2024-01-15"


class TestNormalizeObservationDateSemester:
    def test_semester(self):
        assert normalize_observation_date("2024-S1", "") == "2024-01-01"
        assert normalize_observation_date("2024-S2", "") == "2024-07-01"


class TestNormalizeObservationDateEdgeCases:
    def test_empty_string(self):
        assert normalize_observation_date("", "monthly") == ""

    def test_whitespace(self):
        assert normalize_observation_date("  2024-01  ", "monthly") == "2024-01-01"

    def test_unknown_format_passthrough(self):
        assert normalize_observation_date("not-a-date", "monthly") == "not-a-date"


class TestNormalizerDateIntegration:
    """Verify the Normalizer actually applies date normalization."""

    def test_monthly_dates_normalized(self, sample_family):
        """BLS sends 2024-01-15, normalizer should snap to 2024-01-01."""
        lookup = {("fred", "CPIAUCSL"): sample_family}
        normalizer = Normalizer(family_lookup=lookup)
        series = RawSeries(
            source="fred",
            series_id="CPIAUCSL",
            observations=(
                RawObservation(date="2024-01-15", value=312.3),
            ),
            fetched_at="",
            series_metadata={},
        )
        rows = normalizer.normalize(series)
        # sample_family.frequency == "monthly" → snaps to first of month
        assert rows[0].date == "2024-01-01"

    def test_quarterly_bea_end_to_start(self):
        """BEA sends 2024-03-31 for Q1, normalizer should snap to 2024-01-01."""
        family = ObsFamilyRecord(
            family_id="us.growth.gdp_real", source_id="fred",
            provider_series_id="GDPC1", canonical_name="Real GDP",
            short_name="GDP", unit="billions_usd", frequency="quarterly",
            seasonal_adjustment="saar", country_code="US",
            topic_code="growth", category="output",
            is_active=True, has_vintages=True,
        )
        normalizer = Normalizer(family_lookup={("fred", "GDPC1"): family})
        series = RawSeries(
            source="fred", series_id="GDPC1",
            observations=(RawObservation(date="2024-03-31", value=22000.0),),
            fetched_at="", series_metadata={},
        )
        rows = normalizer.normalize(series)
        assert rows[0].date == "2024-01-01"

    def test_cross_source_dates_align(self):
        """CPI from FRED (2024-01-01) and IMF (2024-M01) should align."""
        normalizer = Normalizer(family_lookup={})

        fred_series = RawSeries(
            source="fred", series_id="CPIAUCSL",
            observations=(RawObservation(date="2024-01-01", value=312.3),),
            fetched_at="", series_metadata={"freq": "monthly"},
        )
        imf_series = RawSeries(
            source="imf", series_id="IMF_CN_CPI",
            observations=(RawObservation(date="2024-M01", value=102.5),),
            fetched_at="", series_metadata={"freq": "monthly"},
        )

        fred_rows = normalizer.normalize(fred_series)
        imf_rows = normalizer.normalize(imf_series)
        assert fred_rows[0].date == imf_rows[0].date == "2024-01-01"

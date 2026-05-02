"""Tests for the normalization layer and concept_map cross-source registry."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from ingestion.normalization import Normalizer, normalize_observation_date
from ingestion.types import CanonicalRow, RawObservation, RawSeries
from ingestion.validation import ValidationEngine, ValidationStore
from ingestion.validation._types import ValidationLayer, ValidationSeverity
from storage.sqlite import ConceptMapRecord, IndicatorObservationRecord, ObsFamilyRecord, ResolvedObservation, SQLiteEngineStore


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

        de = store.list_concepts(country_code="DE")
        assert "DE_GOVT_10Y" in de
        assert "CPI_US" not in de

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


# ── Helpers ───────────────────────────────────────────────────────────


def _insert_obs(store: SQLiteEngineStore, series_id: str, source: str, date: str, value: float) -> None:
    store.upsert_indicator_observation(
        IndicatorObservationRecord(series_id=series_id, source=source, date=date, value=value)
    )


# ── Series stats tests ───────────────────────────────────────────────


class TestSeriesStats:
    def test_get_series_stats_empty(self, store):
        stats = store.get_series_stats("fred", "CPIAUCSL")
        assert stats["count"] == 0
        assert stats["min_date"] is None
        assert stats["max_date"] is None
        assert stats["latest_value"] is None

    def test_get_series_stats_with_data(self, store):
        _insert_obs(store, "CPIAUCSL", "fred", "2024-01-01", 312.3)
        _insert_obs(store, "CPIAUCSL", "fred", "2024-02-01", 313.1)
        _insert_obs(store, "CPIAUCSL", "fred", "2024-03-01", 314.0)
        stats = store.get_series_stats("fred", "CPIAUCSL")
        assert stats["count"] == 3
        assert stats["min_date"] == "2024-01-01"
        assert stats["max_date"] == "2024-03-01"
        assert stats["latest_value"] == 314.0

    def test_get_concept_stats(self, store):
        store.seed_concept_map()
        _insert_obs(store, "CPIAUCSL", "fred", "2024-01-01", 312.3)
        stats = store.get_concept_stats("CPI_US")
        assert len(stats) == 2  # fred + bls
        fred_stat = next(s for s in stats if s["source"] == "fred")
        assert fred_stat["count"] == 1
        assert fred_stat["series_id"] == "CPIAUCSL"
        bls_stat = next(s for s in stats if s["source"] == "bls")
        assert bls_stat["count"] == 0


# ── Concept validation E2E tests ─────────────────────────────────────


@pytest.fixture()
def validation_engine(store):
    validation_store = ValidationStore(str(store.db_path))
    return ValidationEngine(validation_store)


class TestValidateConceptNotFound:
    def test_unknown_concept_fails(self, store, validation_engine):
        store.seed_concept_map()
        report = validation_engine.validate_concept("NONEXISTENT", store)
        assert not report.passed
        assert report.error_count >= 1
        assert report.source == "concept:NONEXISTENT"
        assert any(c.check_name == "concept_exists" and not c.passed for c in report.checks)

    def test_report_format(self, store, validation_engine):
        store.seed_concept_map()
        report = validation_engine.validate_concept("NONEXISTENT", store)
        text = report.format_text()
        assert "NONEXISTENT" in text
        assert "FAIL" in text


class TestValidateConceptEmptyDB:
    def test_concept_with_no_data(self, store, validation_engine):
        store.seed_concept_map()
        report = validation_engine.validate_concept("CPI_US", store)
        assert report.source == "concept:CPI_US"
        # concept_exists should pass (it's in the map)
        exists_check = next(c for c in report.checks if c.check_name == "concept_exists")
        assert exists_check.passed
        # completeness checks should show no data
        completeness = [c for c in report.checks if c.check_name == "concept_completeness"]
        assert len(completeness) == 2  # fred + bls
        assert all(not c.passed for c in completeness)
        # coverage should fail (0 sources with data)
        coverage = next(c for c in report.checks if c.check_name == "concept_source_coverage")
        assert not coverage.passed
        assert coverage.details["sources_with_data"] == 0

    def test_concept_with_partial_data(self, store, validation_engine):
        store.seed_concept_map()
        _insert_obs(store, "CPIAUCSL", "fred", "2024-01-01", 312.3)
        report = validation_engine.validate_concept("CPI_US", store)
        # coverage should pass (at least 1 source)
        coverage = next(c for c in report.checks if c.check_name == "concept_source_coverage")
        assert coverage.passed
        assert coverage.details["sources_with_data"] == 1


class TestValidateConceptWithData:
    def test_concept_full_data_both_sources(self, store, validation_engine):
        store.seed_concept_map()
        # FRED CPI
        for i, v in enumerate([312.3, 313.1, 314.0], start=1):
            _insert_obs(store, "CPIAUCSL", "fred", f"2024-0{i}-01", v)
        # BLS CPI
        for i, v in enumerate([312.0, 312.8, 313.5], start=1):
            _insert_obs(store, "CUUR0000SA0", "bls", f"2024-0{i}-01", v)

        report = validation_engine.validate_concept("CPI_US", store)
        assert report.source == "concept:CPI_US"

        # Both completeness checks pass
        completeness = [c for c in report.checks if c.check_name == "concept_completeness"]
        assert all(c.passed for c in completeness)

        # Coverage passes
        coverage = next(c for c in report.checks if c.check_name == "concept_source_coverage")
        assert coverage.passed
        assert coverage.details["sources_with_data"] == 2

        # Cross-source check should be present
        cross = [c for c in report.checks if c.layer == ValidationLayer.CROSS_SOURCE]
        assert len(cross) > 0

    def test_cross_source_divergent_values(self, store, validation_engine):
        store.seed_concept_map()
        # FRED CPI: 300
        _insert_obs(store, "CPIAUCSL", "fred", "2024-01-01", 300.0)
        # BLS CPI: 400 (very different!)
        _insert_obs(store, "CUUR0000SA0", "bls", "2024-01-01", 400.0)

        report = validation_engine.validate_concept(
            "CPI_US", store, value_tolerance_pct=1.0,
        )
        cross = [c for c in report.checks if c.layer == ValidationLayer.CROSS_SOURCE]
        # Should flag the divergence
        assert any(not c.passed for c in cross)


class TestValidateConceptFreshness:
    def test_stale_data_flagged(self, store, validation_engine):
        store.seed_concept_map()
        # Very old data
        _insert_obs(store, "CPIAUCSL", "fred", "2020-01-01", 260.0)

        report = validation_engine.validate_concept(
            "CPI_US", store, max_staleness_days=90,
        )
        freshness = [c for c in report.checks if c.check_name == "freshness_check"]
        assert len(freshness) >= 1
        assert any(not c.passed for c in freshness)


class TestValidateAllConcepts:
    def test_validates_all(self, store, validation_engine):
        store.seed_concept_map()
        reports = validation_engine.validate_all_concepts(store)
        concepts = store.list_concepts()
        assert len(reports) == len(concepts)
        assert all(r.source.startswith("concept:") for r in reports)

    def test_filter_by_country(self, store, validation_engine):
        store.seed_concept_map()
        reports = validation_engine.validate_all_concepts(store, country_code="US")
        sources = {r.source for r in reports}
        assert len(reports) > 0
        assert "concept:CPI_US" in sources
        assert "concept:WTI_CRUDE" in sources
        assert "concept:CPI_CN" not in sources
        assert "concept:DE_GOVT_10Y" not in sources

    def test_to_dict_round_trip(self, store, validation_engine):
        store.seed_concept_map()
        reports = validation_engine.validate_all_concepts(store)
        for report in reports:
            d = report.to_dict()
            assert "source" in d
            assert "checks" in d
            assert isinstance(d["checks"], list)


# ── Priority & resolution tests ──────────────────────────────────────


class TestPriorityColumn:
    def test_priority_column_persisted(self, store):
        store.seed_concept_map()
        mappings = store.get_concept_series("CPI_US")
        assert len(mappings) == 2
        for m in mappings:
            assert m.priority > 0, f"{m.source_id} has priority=0 after seed"

    def test_priority_ordering(self, store):
        store.seed_concept_map()
        mappings = store.get_concept_series("CPI_US")
        # bls should come first (priority=1), fred second (priority=2)
        assert mappings[0].source_id == "bls"
        assert mappings[0].priority == 1
        assert mappings[1].source_id == "fred"
        assert mappings[1].priority == 2

    def test_policy_rate_priority_nyfed_first(self, store):
        store.seed_concept_map()
        mappings = store.get_concept_series("POLICY_RATE_US")
        assert mappings[0].source_id == "nyfed"
        assert mappings[0].priority == 1
        priorities = [m.priority for m in mappings]
        assert priorities == sorted(priorities)

    def test_cpi_eu_eurostat_before_imf(self, store):
        store.seed_concept_map()
        mappings = store.get_concept_series("CPI_EU")
        assert mappings[0].source_id == "eurostat"
        assert mappings[0].priority == 1
        assert mappings[1].source_id == "imf"
        assert mappings[1].priority == 2


class TestResolveIndicator:
    def test_resolve_picks_highest_priority(self, store):
        store.seed_concept_map()
        # BLS (p=1) has data
        _insert_obs(store, "CUUR0000SA0", "bls", "2024-01-01", 312.0)
        # FRED (p=2) has data
        _insert_obs(store, "CPIAUCSL", "fred", "2024-01-01", 312.3)

        obs = store.resolve_indicator("CPI_US", date="2024-01-01")
        assert obs is not None
        assert isinstance(obs, ResolvedObservation)
        assert obs.source_id == "bls"
        assert obs.priority == 1
        assert obs.value == 312.0
        assert obs.alternates == 1  # fred also has data

    def test_resolve_falls_back(self, store):
        store.seed_concept_map()
        # Only FRED (p=2) has data, BLS (p=1) does not
        _insert_obs(store, "CPIAUCSL", "fred", "2024-01-01", 312.3)

        obs = store.resolve_indicator("CPI_US", date="2024-01-01")
        assert obs is not None
        assert obs.source_id == "fred"
        assert obs.priority == 2
        assert obs.value == 312.3
        assert obs.alternates == 0

    def test_resolve_latest_date(self, store):
        store.seed_concept_map()
        _insert_obs(store, "CUUR0000SA0", "bls", "2024-01-01", 312.0)
        _insert_obs(store, "CUUR0000SA0", "bls", "2024-02-01", 313.0)

        obs = store.resolve_indicator("CPI_US")
        assert obs is not None
        assert obs.date == "2024-02-01"
        assert obs.value == 313.0

    def test_resolve_no_data_returns_none(self, store):
        store.seed_concept_map()
        obs = store.resolve_indicator("CPI_US", date="2024-01-01")
        assert obs is None

    def test_resolve_unknown_concept_returns_none(self, store):
        store.seed_concept_map()
        obs = store.resolve_indicator("NONEXISTENT")
        assert obs is None


class TestResolveIndicatorHistory:
    def test_resolve_history_mixed_fallback(self, store):
        store.seed_concept_map()
        # BLS has Jan and Feb
        _insert_obs(store, "CUUR0000SA0", "bls", "2024-01-01", 312.0)
        _insert_obs(store, "CUUR0000SA0", "bls", "2024-02-01", 313.0)
        # FRED has Jan, Feb, and Mar (BLS gap in Mar)
        _insert_obs(store, "CPIAUCSL", "fred", "2024-01-01", 312.3)
        _insert_obs(store, "CPIAUCSL", "fred", "2024-02-01", 313.1)
        _insert_obs(store, "CPIAUCSL", "fred", "2024-03-01", 314.0)

        results = store.resolve_indicator_history("CPI_US", limit=12)
        assert len(results) == 3

        # Most recent first
        assert results[0].date == "2024-03-01"
        assert results[0].source_id == "fred"  # fallback — bls has no Mar data
        assert results[0].alternates == 0

        assert results[1].date == "2024-02-01"
        assert results[1].source_id == "bls"  # bls wins (p=1)
        assert results[1].alternates == 1  # fred also has Feb

        assert results[2].date == "2024-01-01"
        assert results[2].source_id == "bls"
        assert results[2].alternates == 1

    def test_resolve_history_alternates_count(self, store):
        store.seed_concept_map()
        # All three sources have data for UNEMP_US on same date
        _insert_obs(store, "LNS14000000", "bls", "2024-01-01", 3.7)
        _insert_obs(store, "UNRATE", "fred", "2024-01-01", 3.7)
        _insert_obs(store, "OECD_UNEMP_US", "oecd", "2024-01-01", 3.8)

        results = store.resolve_indicator_history("UNEMP_US", limit=5)
        assert len(results) == 1
        assert results[0].source_id == "bls"
        assert results[0].alternates == 2  # fred + oecd

    def test_resolve_history_empty(self, store):
        store.seed_concept_map()
        results = store.resolve_indicator_history("CPI_US")
        assert results == []

    def test_resolve_history_respects_limit(self, store):
        store.seed_concept_map()
        for i in range(1, 7):
            _insert_obs(store, "CUUR0000SA0", "bls", f"2024-0{i}-01", 310.0 + i)
        results = store.resolve_indicator_history("CPI_US", limit=3)
        assert len(results) == 3
        assert results[0].date == "2024-06-01"
        assert results[2].date == "2024-04-01"

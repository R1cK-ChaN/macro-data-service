"""Tests for issue #114 P3 — `as_of` PIT reads + calendar fallback.

Covers:
* `resolve_indicator(date, as_of)` selects the latest vintage with
  ``vintage_date <= as_of``.
* `resolve_indicator_history(as_of=...)` reconstructs the projection at
  the requested cutoff.
* Calendar fallback augments shallow projections with
  ``cal_econ_event.actual`` rows tagged ``provenance='calendar'``.
"""

from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

import pytest

from storage.sqlite import (
    IndicatorVintageRecord,
    SQLiteEngineStore,
)


@pytest.fixture()
def store() -> SQLiteEngineStore:
    with tempfile.TemporaryDirectory() as td:
        s = SQLiteEngineStore(Path(td) / "test.db")
        s.seed_concept_map()
        yield s


def _vintage(store, *, source, series_id, obs_date, vintage_date, value, q="native_pit"):
    store.upsert_indicator_vintage(
        IndicatorVintageRecord(
            series_id=series_id, source=source,
            observation_date=obs_date, vintage_date=vintage_date,
            value=value, vintage_quality=q,
        )
    )


class TestAsOfSingle:
    def test_as_of_picks_latest_vintage_at_or_before_cutoff(self, store):
        # Two vintages of CPIAUCSL for obs_date 2024-01-01:
        #   first published 2024-02-15 = 312.0
        #   revised 2024-04-25 = 312.5
        _vintage(store, source="fred", series_id="CPIAUCSL",
                 obs_date="2024-01-01", vintage_date="2024-02-15", value=312.0)
        _vintage(store, source="fred", series_id="CPIAUCSL",
                 obs_date="2024-01-01", vintage_date="2024-04-25", value=312.5)
        # PIT read as of 2024-03-01 → first print only.
        obs = store.resolve_indicator(
            "CPI_US", date="2024-01-01", as_of="2024-03-01",
        )
        assert obs is not None
        assert obs.value == 312.0
        # PIT read as of 2024-05-01 → revised value.
        obs = store.resolve_indicator(
            "CPI_US", date="2024-01-01", as_of="2024-05-01",
        )
        assert obs.value == 312.5
        # No as_of → latest vintage (revised).
        obs = store.resolve_indicator("CPI_US", date="2024-01-01")
        assert obs.value == 312.5

    def test_as_of_with_no_date_picks_latest_obs_at_cutoff(self, store):
        _vintage(store, source="fred", series_id="CPIAUCSL",
                 obs_date="2024-01-01", vintage_date="2024-02-15", value=312.0)
        _vintage(store, source="fred", series_id="CPIAUCSL",
                 obs_date="2024-02-01", vintage_date="2024-03-15", value=313.1)
        # As-of 2024-02-20: only Jan visible (Feb's vintage is 2024-03-15).
        obs = store.resolve_indicator("CPI_US", as_of="2024-02-20")
        assert obs is not None
        assert obs.date == "2024-01-01"
        assert obs.value == 312.0

    def test_as_of_returns_none_before_any_vintage(self, store):
        _vintage(store, source="fred", series_id="CPIAUCSL",
                 obs_date="2024-01-01", vintage_date="2024-02-15", value=312.0)
        obs = store.resolve_indicator(
            "CPI_US", date="2024-01-01", as_of="2024-01-01",
        )
        assert obs is None


class TestAsOfHistory:
    def test_history_at_cutoff(self, store):
        # Three observation_dates, two vintages each (revisions land
        # later in the timeline).
        for obs, first_vd, first_val, rev_vd, rev_val in [
            ("2024-01-01", "2024-02-15", 100.0, "2024-04-25", 101.0),
            ("2024-02-01", "2024-03-15", 200.0, "2024-04-25", 201.0),
            ("2024-03-01", "2024-04-15", 300.0, "2024-05-25", 303.0),
        ]:
            _vintage(store, source="fred", series_id="CPIAUCSL",
                     obs_date=obs, vintage_date=first_vd, value=first_val)
            _vintage(store, source="fred", series_id="CPIAUCSL",
                     obs_date=obs, vintage_date=rev_vd, value=rev_val)
        # As-of 2024-04-20: only first prints visible.
        results = store.resolve_indicator_history(
            "CPI_US", limit=12, as_of="2024-04-20",
        )
        # Mar=300 first vintage 2024-04-15 ≤ 2024-04-20 ✓
        # Feb first vintage 2024-03-15 ≤ 2024-04-20 ✓
        # Jan first vintage 2024-02-15 ≤ 2024-04-20 ✓
        assert len(results) == 3
        by_date = {r.date: r.value for r in results}
        assert by_date == {"2024-01-01": 100.0, "2024-02-01": 200.0, "2024-03-01": 300.0}
        # As-of 2024-05-30: revised values for Jan/Feb (rev=2024-04-25)
        # and revised Mar (rev=2024-05-25).
        results = store.resolve_indicator_history(
            "CPI_US", limit=12, as_of="2024-05-30",
        )
        by_date = {r.date: r.value for r in results}
        assert by_date == {"2024-01-01": 101.0, "2024-02-01": 201.0, "2024-03-01": 303.0}


class TestProvenanceField:
    def test_default_provenance_is_native(self, store):
        _vintage(store, source="fred", series_id="CPIAUCSL",
                 obs_date="2024-01-01", vintage_date="2024-02-15", value=312.0)
        obs = store.resolve_indicator("CPI_US", date="2024-01-01")
        assert obs.provenance == "native"


class TestCalendarFallback:
    """Issue #114 P3: when the projection is shallow (<24 rows), augment
    from `cal_econ_event.actual`."""

    def _seed_calendar_event(self, store, *, country, title, ref_date, actual):
        # Insert a calendar row directly — the production seeder is too
        # heavy for a unit test.
        with store._connection(commit=True) as conn:
            conn.execute(
                """
                INSERT INTO cal_econ_event (
                    provider, provider_event_id, event_time_utc,
                    event_time_precision, reference_date, reference_label,
                    country_code, indicator_id, category, title,
                    importance, currency, unit, actual, previous, revised,
                    forecast, consensus_forecast, ticker, source, source_url,
                    content_hash, last_update_epoch_ms, observed_at_epoch_ms,
                    created_at, updated_at, event_type
                )
                VALUES ('te', ?, ?, 'datetime', ?, '', ?, NULL, ?, ?,
                        'high', 'USD', '%', ?, '', NULL, '', '', '', '', '',
                        '', 0, 0, '2026-01-01', '2026-01-01', '')
                """,
                (
                    f"id-{ref_date}-{title}",
                    ref_date + "T00:00:00", ref_date,
                    country, title, title, actual,
                ),
            )

    def test_fallback_fires_when_projection_below_threshold(self, store):
        # Only 3 native vintages — far below the 24-row threshold.
        _vintage(store, source="bls", series_id="CUUR0000SA0",
                 obs_date="2024-12-01", vintage_date="2024-12-15", value=312.0)
        _vintage(store, source="bls", series_id="CUUR0000SA0",
                 obs_date="2024-11-01", vintage_date="2024-11-15", value=311.0)
        _vintage(store, source="bls", series_id="CUUR0000SA0",
                 obs_date="2024-10-01", vintage_date="2024-10-15", value=310.0)
        # Two calendar events for older months.
        self._seed_calendar_event(
            store, country="US", title="Inflation Rate YoY",
            ref_date="2024-09-01", actual="3.2",
        )
        self._seed_calendar_event(
            store, country="US", title="Inflation Rate YoY",
            ref_date="2024-08-01", actual="3.4",
        )
        results = store.resolve_indicator_history("CPI_US", limit=12)
        # 3 native + 2 calendar = 5 rows.
        assert len(results) == 5
        provenance = [r.provenance for r in results]
        assert provenance.count("native") == 3
        assert provenance.count("calendar") == 2
        # Calendar rows tagged with source_id = 'cal_econ_event'.
        cal_rows = [r for r in results if r.provenance == "calendar"]
        for r in cal_rows:
            assert r.source_id == "cal_econ_event"
            assert r.role == "calendar_fallback"

    def test_fallback_dedupes_against_native_dates(self, store):
        _vintage(store, source="bls", series_id="CUUR0000SA0",
                 obs_date="2024-09-01", vintage_date="2024-09-15", value=312.5)
        # Calendar event for the same month — should NOT shadow the
        # native row; native wins, fallback skips this date.
        self._seed_calendar_event(
            store, country="US", title="Inflation Rate YoY",
            ref_date="2024-09-01", actual="3.2",
        )
        results = store.resolve_indicator_history("CPI_US", limit=12)
        sept = [r for r in results if r.date == "2024-09-01"]
        assert len(sept) == 1
        assert sept[0].provenance == "native"
        assert sept[0].value == 312.5

    def test_fallback_skips_when_no_keyword_bridge(self, store):
        # M3_GROWTH_EU is not in the calendar-keyword bridge.
        results = store.resolve_indicator_history("M3_GROWTH_EU", limit=12)
        assert results == []  # no native, no fallback bridge

    def test_fallback_does_not_fire_when_projection_full(self, store):
        # Seed 24 native vintages — at-threshold, no augment.
        for i in range(1, 25):
            _vintage(
                store, source="bls", series_id="CUUR0000SA0",
                obs_date=f"2024-{i:02d}-01" if i <= 12 else f"2025-{i-12:02d}-01",
                vintage_date=f"2024-{i:02d}-15" if i <= 12 else f"2025-{i-12:02d}-15",
                value=300.0 + i,
            )
        # Add a calendar row that should NOT surface.
        self._seed_calendar_event(
            store, country="US", title="Inflation Rate YoY",
            ref_date="2023-01-01", actual="6.5",
        )
        results = store.resolve_indicator_history("CPI_US", limit=30)
        assert all(r.provenance == "native" for r in results)
        assert "2023-01-01" not in {r.date for r in results}

    def test_fallback_threshold_uses_full_projection_not_limit(self, store):
        # Seed 30 native vintages so full projection > 24 threshold —
        # but request limit=12. Codex P3 round 1 caught the bug where
        # ``len(results) < 24`` checked the sliced list, so default
        # limit=12 would always trip fallback.
        for i in range(30):
            obs = f"2023-{(i%12)+1:02d}-{((i//12)*10 + 1):02d}"
            _vintage(
                store, source="bls", series_id="CUUR0000SA0",
                obs_date=obs, vintage_date=obs, value=300.0 + i,
            )
        # Calendar row that should NOT surface (full projection deep enough).
        self._seed_calendar_event(
            store, country="US", title="Inflation Rate YoY",
            ref_date="2020-01-01", actual="2.4",
        )
        results = store.resolve_indicator_history("CPI_US", limit=12)
        assert len(results) == 12
        assert all(r.provenance == "native" for r in results)

    def test_fallback_respects_as_of_cutoff(self, store):
        # Native: shallow (3 rows). Calendar events span across cutoff.
        _vintage(store, source="bls", series_id="CUUR0000SA0",
                 obs_date="2024-12-01", vintage_date="2024-12-15", value=312.0)
        # Pre-cutoff calendar event.
        self._seed_calendar_event(
            store, country="US", title="Inflation Rate YoY",
            ref_date="2024-09-01", actual="3.2",
        )
        # Post-cutoff event — should NOT leak into a PIT read.
        self._seed_calendar_event(
            store, country="US", title="Inflation Rate YoY",
            ref_date="2025-01-01", actual="3.5",
        )
        results = store.resolve_indicator_history(
            "CPI_US", limit=12, as_of="2024-10-01",
        )
        dates = {r.date for r in results}
        assert "2024-09-01" in dates
        assert "2025-01-01" not in dates


class TestAsOfNormalisation:
    def test_iso_timestamp_vintage_matches_same_day_cutoff(self, store):
        """Codex P3 round 1: ``synthetic_snapshot`` writes
        ``vintage_date = utc_now()`` (full ISO with timestamp). A
        cutoff like ``as_of='2026-05-02'`` must include
        ``2026-05-02T12:00:00+00:00`` rows; raw text comparison would
        exclude them."""
        _vintage(
            store, source="fred", series_id="CPIAUCSL",
            obs_date="2026-04-01",
            vintage_date="2026-05-02T12:30:00+00:00",
            value=320.0, q="synthetic_snapshot",
        )
        obs = store.resolve_indicator(
            "CPI_US", date="2026-04-01", as_of="2026-05-02",
        )
        assert obs is not None
        assert obs.value == 320.0

    def test_iso_timestamp_vintage_excluded_when_cutoff_is_earlier(self, store):
        _vintage(
            store, source="fred", series_id="CPIAUCSL",
            obs_date="2026-04-01",
            vintage_date="2026-05-02T12:30:00+00:00",
            value=320.0, q="synthetic_snapshot",
        )
        obs = store.resolve_indicator(
            "CPI_US", date="2026-04-01", as_of="2026-05-01",
        )
        assert obs is None


class TestEuroAreaCountryCode:
    """``cal_econ_event`` stores Eurozone aggregate rows with
    ``country_code='EU'`` (not ``'EA'``). The bridge must match."""

    def _seed_calendar_event(self, store, *, country, title, ref_date, actual):
        with store._connection(commit=True) as conn:
            conn.execute(
                """
                INSERT INTO cal_econ_event (
                    provider, provider_event_id, event_time_utc,
                    event_time_precision, reference_date, reference_label,
                    country_code, indicator_id, category, title,
                    importance, currency, unit, actual, previous, revised,
                    forecast, consensus_forecast, ticker, source, source_url,
                    content_hash, last_update_epoch_ms, observed_at_epoch_ms,
                    created_at, updated_at, event_type
                )
                VALUES ('te', ?, ?, 'datetime', ?, '', ?, NULL, ?, ?,
                        'high', 'EUR', '%', ?, '', NULL, '', '', '', '', '',
                        '', 0, 0, '2026-01-01', '2026-01-01', '')
                """,
                (
                    f"id-eu-{ref_date}-{title}",
                    ref_date + "T00:00:00", ref_date,
                    country, title, title, actual,
                ),
            )

    def test_cpi_eu_picks_up_country_code_eu(self, store):
        # No native series for CPI_EU; only calendar fallback.
        self._seed_calendar_event(
            store, country="EU", title="Inflation Rate YoY",
            ref_date="2024-12-01", actual="2.2",
        )
        results = store.resolve_indicator_history("CPI_EU", limit=12)
        assert len(results) == 1
        assert results[0].provenance == "calendar"
        assert results[0].value == 2.2

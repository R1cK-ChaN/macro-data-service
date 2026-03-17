"""Tests for the release-calendar layer: resolvers, storage, integration."""
from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

# Ensure src is on path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from ingestion.release_schedule import (
    _next_day_of_month,
    _next_weekday_of_month,
    _next_quarter_lag,
    _next_daily,
    _next_weekly,
    _next_fixed_dates,
    _next_approximate_window,
    _next_monthly_lag,
    next_expected_release,
    is_due,
    check_due_concepts,
    expected_reference_period,
    is_data_fresh,
    compute_next_retry,
    RETRY_BACKOFF_SECONDS,
    MAX_RETRIES,
    STATUS_PENDING,
    STATUS_WAITING,
    STATUS_FETCHED,
    STATUS_CONFIRMED,
    STATUS_STALE,
    STATUS_FAILED,
)


# ── Resolver unit tests (no DB) ──────────────────────────────────────

class TestNextDayOfMonth:
    def test_future(self):
        """ref Mar 1, day=12 -> Mar 12"""
        ref = datetime(2026, 3, 1, tzinfo=timezone.utc)
        result = _next_day_of_month({"day": 12, "tolerance_days": 3}, ref)
        assert result is not None
        assert result.month == 3
        assert result.day == 12

    def test_past(self):
        """ref Mar 15, day=12 -> Apr 12 (since Mar 12 already passed)"""
        ref = datetime(2026, 3, 15, tzinfo=timezone.utc)
        result = _next_day_of_month({"day": 12, "tolerance_days": 3}, ref)
        assert result is not None
        assert result.month == 4
        # Apr 12 2026 is a Sunday → shifted to Apr 13 (Monday)
        assert result.day == 13

    def test_weekend_shift(self):
        """day falls on Sat -> Mon"""
        # 2026-01-10 is a Saturday
        ref = datetime(2026, 1, 1, tzinfo=timezone.utc)
        result = _next_day_of_month({"day": 10}, ref)
        assert result is not None
        assert result.day == 12  # Mon Jan 12


class TestNextWeekdayOfMonth:
    def test_first_friday_future(self):
        """ref Mar 1 2026 -> Mar 6 (1st Fri)"""
        ref = datetime(2026, 3, 1, tzinfo=timezone.utc)
        result = _next_weekday_of_month({"weekday": 4, "ordinal": 1}, ref)
        assert result is not None
        assert result.month == 3
        assert result.day == 6
        assert result.weekday() == 4  # Friday

    def test_first_friday_past(self):
        """ref Mar 8 -> Apr 3 (1st Fri of Apr)"""
        ref = datetime(2026, 3, 8, tzinfo=timezone.utc)
        result = _next_weekday_of_month({"weekday": 4, "ordinal": 1}, ref)
        assert result is not None
        assert result.month == 4
        assert result.day == 3
        assert result.weekday() == 4


class TestNextQuarterLag:
    def test_after_quarter_end(self):
        """ref Apr 1, lag=30 -> Apr 30 (Q1 end = Mar 31, + 30 = Apr 30)"""
        ref = datetime(2026, 4, 1, tzinfo=timezone.utc)
        result = _next_quarter_lag({"lag_days": 30}, ref)
        assert result is not None
        assert result.month == 4
        assert result.day == 30

    def test_mid_quarter(self):
        """ref Feb 15, lag=30 -> Apr 30 (next Q end = Mar 31)"""
        ref = datetime(2026, 2, 15, tzinfo=timezone.utc)
        result = _next_quarter_lag({"lag_days": 30}, ref)
        assert result is not None
        assert result.month == 4
        assert result.day == 30


class TestNextDaily:
    def test_weekday(self):
        """ref Mon -> Tue"""
        ref = datetime(2026, 3, 16, 12, 0, tzinfo=timezone.utc)  # Mon
        result = _next_daily({}, ref)
        assert result is not None
        assert result.weekday() == 1  # Tue

    def test_friday(self):
        """ref Fri -> Mon"""
        ref = datetime(2026, 3, 20, 12, 0, tzinfo=timezone.utc)  # Fri
        result = _next_daily({}, ref)
        assert result is not None
        assert result.weekday() == 0  # Mon
        assert result.day == 23


class TestNextWeekly:
    def test_thursday_same_week(self):
        """ref Mon, weekday=3 (Thu) -> Thu same week"""
        ref = datetime(2026, 3, 16, tzinfo=timezone.utc)  # Mon
        result = _next_weekly({"weekday": 3}, ref)
        assert result is not None
        assert result.day == 19  # Thu
        assert result.weekday() == 3

    def test_past_weekday(self):
        """ref Fri, weekday=3 -> next Thu"""
        ref = datetime(2026, 3, 20, tzinfo=timezone.utc)  # Fri
        result = _next_weekly({"weekday": 3}, ref)
        assert result is not None
        assert result.day == 26  # next Thu
        assert result.weekday() == 3


class TestNextFixedDates:
    def test_returns_first_future(self):
        ref = datetime(2026, 3, 15, tzinfo=timezone.utc)
        dates = ["2026-03-10T00:00:00+00:00", "2026-03-20T00:00:00+00:00", "2026-04-10T00:00:00+00:00"]
        result = _next_fixed_dates({"dates": dates}, ref)
        assert result is not None
        assert result.day == 20

    def test_exhausted(self):
        ref = datetime(2026, 5, 1, tzinfo=timezone.utc)
        dates = ["2026-03-10T00:00:00+00:00", "2026-03-20T00:00:00+00:00"]
        result = _next_fixed_dates({"dates": dates}, ref)
        assert result is None


class TestNextApproximateWindow:
    def test_returns_midpoint(self):
        ref = datetime(2026, 3, 1, tzinfo=timezone.utc)
        result = _next_approximate_window({"month_offset": 3, "window_days": 30}, ref)
        assert result is not None
        assert result > ref


class TestNextMonthlyLag:
    def test_basic(self):
        ref = datetime(2026, 3, 1, tzinfo=timezone.utc)
        result = _next_monthly_lag({"lag_months": 1, "day": 15, "tolerance_days": 5}, ref)
        assert result is not None
        assert result > ref
        # Should be around the 15th of a month
        assert 13 <= result.day <= 17  # ±weekend shift


class TestNextExpectedRelease:
    def test_dispatch(self):
        ref = datetime(2026, 3, 1, tzinfo=timezone.utc)
        result = next_expected_release("daily", {}, reference=ref)
        assert result is not None
        assert result > ref

    def test_unknown_type(self):
        result = next_expected_release("bogus", {})
        assert result is None


class TestIsDue:
    def test_within_window(self):
        now = datetime(2026, 3, 12, 10, 0, tzinfo=timezone.utc)
        nxt = "2026-03-12T11:30:00+00:00"  # 90 min from now
        assert is_due(nxt, now=now, window_minutes=120) is True

    def test_outside_window(self):
        now = datetime(2026, 3, 12, 10, 0, tzinfo=timezone.utc)
        nxt = "2026-03-15T11:30:00+00:00"
        assert is_due(nxt, now=now, window_minutes=120) is False

    def test_empty(self):
        assert is_due("") is False

    def test_past_is_due(self):
        now = datetime(2026, 3, 12, 10, 0, tzinfo=timezone.utc)
        nxt = "2026-03-10T10:00:00+00:00"
        assert is_due(nxt, now=now, window_minutes=120) is True


class TestCheckDueConcepts:
    def test_filters(self):
        @dataclass
        class FakeSchedule:
            concept_id: str
            next_expected: str

        now = datetime(2026, 3, 12, 10, 0, tzinfo=timezone.utc)
        schedules = [
            FakeSchedule("CPI_US", "2026-03-12T11:00:00+00:00"),  # due (60 min)
            FakeSchedule("NFP_US", "2026-03-20T12:00:00+00:00"),  # not due
            FakeSchedule("SOFR_US", "2026-03-11T00:00:00+00:00"),  # past → due
        ]
        due = check_due_concepts(schedules, now=now, window_minutes=120)
        assert len(due) == 2
        ids = {s.concept_id for s in due}
        assert ids == {"CPI_US", "SOFR_US"}


# ── Storage tests (temp DB) ──────────────────────────────────────────

class TestReleaseScheduleStorage:
    @pytest.fixture()
    def store(self, tmp_path):
        from storage.sqlite import SQLiteEngineStore
        return SQLiteEngineStore(db_path=tmp_path / "test.db")

    def test_seed_creates_schedules(self, store):
        store.seed_release_schedules()
        schedules = store.list_release_schedules()
        assert len(schedules) > 80  # all 86 concepts

    def test_seed_is_idempotent(self, store):
        store.seed_release_schedules()
        store.seed_release_schedules()
        schedules = store.list_release_schedules()
        # Should still be same count, not doubled
        concept_ids = [s.concept_id for s in schedules]
        assert len(concept_ids) == len(set(concept_ids))

    def test_upsert_roundtrip(self, store):
        from storage.sqlite import ReleaseScheduleRecord
        now = datetime.now(timezone.utc).isoformat()
        rec = ReleaseScheduleRecord(
            concept_id="TEST_CONCEPT",
            rule_type="daily",
            rule_json={},
            frequency="daily",
            release_time_utc="",
            timezone="",
            source_authority="manual",
            confidence="pattern",
            next_expected="2026-03-18T00:00:00+00:00",
            last_released="",
            last_checked="",
            is_active=True,
            notes="test",
            created_at=now,
            updated_at=now,
        )
        store.upsert_release_schedule(rec)
        got = store.get_release_schedule("TEST_CONCEPT")
        assert got is not None
        assert got.concept_id == "TEST_CONCEPT"
        assert got.rule_type == "daily"
        assert got.confidence == "pattern"

    def test_list_due_before(self, store):
        from storage.sqlite import ReleaseScheduleRecord
        now = datetime.now(timezone.utc).isoformat()
        for i, nxt in enumerate(["2026-03-10T00:00:00", "2026-03-20T00:00:00", "2026-04-01T00:00:00"]):
            store.upsert_release_schedule(ReleaseScheduleRecord(
                concept_id=f"TEST_{i}",
                rule_type="daily",
                rule_json={},
                frequency="daily",
                release_time_utc="",
                timezone="",
                source_authority="manual",
                confidence="pattern",
                next_expected=nxt,
                last_released="",
                last_checked="",
                is_active=True,
                created_at=now,
                updated_at=now,
            ))
        due = store.list_release_schedules(due_before="2026-03-15T00:00:00")
        assert len(due) == 1
        assert due[0].concept_id == "TEST_0"

    def test_update_timestamps(self, store):
        from storage.sqlite import ReleaseScheduleRecord
        now = datetime.now(timezone.utc).isoformat()
        store.upsert_release_schedule(ReleaseScheduleRecord(
            concept_id="TS_TEST",
            rule_type="daily",
            rule_json={},
            frequency="daily",
            release_time_utc="",
            timezone="",
            source_authority="manual",
            confidence="pattern",
            next_expected="",
            last_released="",
            last_checked="",
            is_active=True,
            created_at=now,
            updated_at=now,
        ))
        store.update_release_timestamps(
            "TS_TEST",
            next_expected="2026-04-01T00:00:00",
            last_checked="2026-03-17T12:00:00",
        )
        got = store.get_release_schedule("TS_TEST")
        assert got is not None
        assert got.next_expected == "2026-04-01T00:00:00"
        assert got.last_checked == "2026-03-17T12:00:00"


# ── Integration tests ────────────────────────────────────────────────

class TestIntegration:
    @pytest.fixture()
    def store(self, tmp_path):
        from storage.sqlite import SQLiteEngineStore
        return SQLiteEngineStore(db_path=tmp_path / "test.db")

    def test_recompute_populates_next_expected(self, store):
        """After seed + recompute, all schedules have non-empty next_expected."""
        store.seed_release_schedules()
        schedules = store.list_release_schedules()

        from ingestion.release_schedule import next_expected_release
        import json as _json
        for s in schedules:
            rule = s.rule_json if isinstance(s.rule_json, dict) else _json.loads(s.rule_json)
            nxt = next_expected_release(s.rule_type, rule)
            if nxt is not None:
                store.update_release_timestamps(
                    s.concept_id, next_expected=nxt.isoformat(),
                )

        updated = store.list_release_schedules()
        empty = [s for s in updated if not s.next_expected]
        assert len(empty) == 0, f"Concepts with empty next_expected: {[s.concept_id for s in empty]}"


# ── Availability / freshness tests ───────────────────────────────────

class TestExpectedReferencePeriod:
    def test_daily(self):
        ref = datetime(2026, 3, 17, 12, 0, tzinfo=timezone.utc)  # Tuesday
        period = expected_reference_period("daily", reference=ref)
        assert period == "2026-03-16"  # yesterday (Monday)

    def test_daily_skips_weekend(self):
        ref = datetime(2026, 3, 16, 12, 0, tzinfo=timezone.utc)  # Monday
        period = expected_reference_period("daily", reference=ref)
        assert period == "2026-03-13"  # Friday

    def test_weekly(self):
        ref = datetime(2026, 3, 17, tzinfo=timezone.utc)
        period = expected_reference_period("weekly", reference=ref)
        assert period == "2026-03-10"

    def test_monthly(self):
        ref = datetime(2026, 3, 17, tzinfo=timezone.utc)
        period = expected_reference_period("monthly", reference=ref)
        assert period == "2026-02-01"

    def test_quarterly(self):
        ref = datetime(2026, 4, 15, tzinfo=timezone.utc)
        period = expected_reference_period("quarterly", reference=ref)
        assert period == "2026-01-01"  # previous quarter start

    def test_annual(self):
        ref = datetime(2026, 6, 1, tzinfo=timezone.utc)
        period = expected_reference_period("annual", reference=ref)
        assert period == "2025-01-01"


class TestIsDataFresh:
    def test_fresh_daily(self):
        ref = datetime(2026, 3, 17, 12, 0, tzinfo=timezone.utc)
        assert is_data_fresh("2026-03-16", "daily", reference=ref) is True

    def test_stale_daily(self):
        ref = datetime(2026, 3, 17, 12, 0, tzinfo=timezone.utc)
        assert is_data_fresh("2026-03-13", "daily", reference=ref) is False

    def test_fresh_monthly(self):
        ref = datetime(2026, 3, 17, tzinfo=timezone.utc)
        assert is_data_fresh("2026-02-15", "monthly", reference=ref) is True

    def test_stale_monthly(self):
        ref = datetime(2026, 3, 17, tzinfo=timezone.utc)
        assert is_data_fresh("2026-01-15", "monthly", reference=ref) is False

    def test_none_date(self):
        assert is_data_fresh(None, "daily") is False

    def test_empty_string(self):
        assert is_data_fresh("", "monthly") is False


class TestComputeNextRetry:
    def test_first_retry(self):
        ref = datetime(2026, 3, 17, 12, 0, tzinfo=timezone.utc)
        nxt = compute_next_retry(0, reference=ref)
        assert nxt is not None
        assert nxt == ref + timedelta(seconds=60)

    def test_second_retry(self):
        ref = datetime(2026, 3, 17, 12, 0, tzinfo=timezone.utc)
        nxt = compute_next_retry(1, reference=ref)
        assert nxt is not None
        assert nxt == ref + timedelta(seconds=300)

    def test_exhausted(self):
        nxt = compute_next_retry(MAX_RETRIES)
        assert nxt is None

    def test_backoff_progression(self):
        ref = datetime(2026, 3, 17, 12, 0, tzinfo=timezone.utc)
        delays = []
        for i in range(MAX_RETRIES):
            nxt = compute_next_retry(i, reference=ref)
            assert nxt is not None
            delays.append((nxt - ref).total_seconds())
        # Delays should match RETRY_BACKOFF_SECONDS
        assert delays == [float(d) for d in RETRY_BACKOFF_SECONDS]


# ── Release status storage tests ─────────────────────────────────────

class TestReleaseStatusStorage:
    @pytest.fixture()
    def store(self, tmp_path):
        from storage.sqlite import SQLiteEngineStore
        return SQLiteEngineStore(db_path=tmp_path / "test.db")

    def test_upsert_and_get(self, store):
        from storage.sqlite import ReleaseStatusRecord
        now = datetime.now(timezone.utc).isoformat()
        rec = ReleaseStatusRecord(
            concept_id="CPI_US",
            release_date="2026-03-12T12:30:00",
            status=STATUS_PENDING,
            expected_period="2026-02-01",
            created_at=now,
            updated_at=now,
        )
        store.upsert_release_status(rec)
        got = store.get_release_status("CPI_US", "2026-03-12T12:30:00")
        assert got is not None
        assert got.status == STATUS_PENDING
        assert got.expected_period == "2026-02-01"

    def test_update_partial(self, store):
        from storage.sqlite import ReleaseStatusRecord
        now = datetime.now(timezone.utc).isoformat()
        store.upsert_release_status(ReleaseStatusRecord(
            concept_id="NFP_US",
            release_date="2026-04-03T12:30:00",
            status=STATUS_PENDING,
            created_at=now,
            updated_at=now,
        ))
        store.update_release_status(
            "NFP_US", "2026-04-03T12:30:00",
            status=STATUS_WAITING,
            attempt_count=1,
            next_retry="2026-04-03T12:31:00",
            error="data not fresh yet",
        )
        got = store.get_release_status("NFP_US", "2026-04-03T12:30:00")
        assert got is not None
        assert got.status == STATUS_WAITING
        assert got.attempt_count == 1
        assert got.next_retry == "2026-04-03T12:31:00"
        assert got.error == "data not fresh yet"

    def test_list_pending_retries(self, store):
        from storage.sqlite import ReleaseStatusRecord
        now = datetime.now(timezone.utc).isoformat()
        # One pending with retry in the past
        store.upsert_release_status(ReleaseStatusRecord(
            concept_id="A", release_date="2026-03-12",
            status=STATUS_WAITING, next_retry="2026-03-17T10:00:00",
            created_at=now, updated_at=now,
        ))
        # One pending with retry in the future
        store.upsert_release_status(ReleaseStatusRecord(
            concept_id="B", release_date="2026-03-12",
            status=STATUS_WAITING, next_retry="2026-03-20T10:00:00",
            created_at=now, updated_at=now,
        ))
        # One already confirmed (should not appear)
        store.upsert_release_status(ReleaseStatusRecord(
            concept_id="C", release_date="2026-03-12",
            status=STATUS_CONFIRMED, next_retry="",
            created_at=now, updated_at=now,
        ))
        pending = store.list_release_statuses(
            pending_retry_before="2026-03-17T12:00:00",
        )
        assert len(pending) == 1
        assert pending[0].concept_id == "A"

    def test_get_latest_release_status(self, store):
        from storage.sqlite import ReleaseStatusRecord
        now = datetime.now(timezone.utc).isoformat()
        store.upsert_release_status(ReleaseStatusRecord(
            concept_id="CPI_US", release_date="2026-02-12",
            status=STATUS_CONFIRMED, created_at=now, updated_at=now,
        ))
        store.upsert_release_status(ReleaseStatusRecord(
            concept_id="CPI_US", release_date="2026-03-12",
            status=STATUS_WAITING, created_at=now, updated_at=now,
        ))
        latest = store.get_latest_release_status("CPI_US")
        assert latest is not None
        assert latest.release_date == "2026-03-12"
        assert latest.status == STATUS_WAITING

    def test_list_all_latest(self, store):
        from storage.sqlite import ReleaseStatusRecord
        now = datetime.now(timezone.utc).isoformat()
        # Two releases for CPI, one for NFP
        store.upsert_release_status(ReleaseStatusRecord(
            concept_id="CPI_US", release_date="2026-02-12",
            status=STATUS_CONFIRMED, created_at=now, updated_at=now,
        ))
        store.upsert_release_status(ReleaseStatusRecord(
            concept_id="CPI_US", release_date="2026-03-12",
            status=STATUS_FETCHED, created_at=now, updated_at=now,
        ))
        store.upsert_release_status(ReleaseStatusRecord(
            concept_id="NFP_US", release_date="2026-03-06",
            status=STATUS_CONFIRMED, created_at=now, updated_at=now,
        ))
        latest = store.list_all_latest_release_statuses()
        assert len(latest) == 2
        by_concept = {r.concept_id: r for r in latest}
        assert by_concept["CPI_US"].release_date == "2026-03-12"
        assert by_concept["CPI_US"].status == STATUS_FETCHED
        assert by_concept["NFP_US"].status == STATUS_CONFIRMED

    def test_status_transition_flow(self, store):
        """Simulate PENDING → WAITING → CONFIRMED lifecycle."""
        from storage.sqlite import ReleaseStatusRecord
        now = datetime.now(timezone.utc).isoformat()
        # 1. Create PENDING
        store.upsert_release_status(ReleaseStatusRecord(
            concept_id="SOFR_US", release_date="2026-03-17",
            status=STATUS_PENDING, expected_period="2026-03-16",
            created_at=now, updated_at=now,
        ))
        # 2. Update to WAITING (first attempt, data not fresh)
        store.update_release_status(
            "SOFR_US", "2026-03-17",
            status=STATUS_WAITING, attempt_count=1,
            next_retry="2026-03-17T08:31:00",
            data_date="2026-03-14", error="data not fresh yet",
        )
        got = store.get_release_status("SOFR_US", "2026-03-17")
        assert got.status == STATUS_WAITING
        assert got.attempt_count == 1
        # 3. Update to CONFIRMED (second attempt, data fresh)
        store.update_release_status(
            "SOFR_US", "2026-03-17",
            status=STATUS_CONFIRMED, attempt_count=2,
            next_retry="", data_date="2026-03-16",
            source_used="nyfed", error="",
        )
        got = store.get_release_status("SOFR_US", "2026-03-17")
        assert got.status == STATUS_CONFIRMED
        assert got.data_date == "2026-03-16"
        assert got.source_used == "nyfed"
        assert got.error == ""

"""Tests for the daily parity comparator (issue #22 P3)."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from ingestion.calendar.parity_daily import (
    AgencyReport,
    LagAfterParity,
    MissingRelease,
    ValueMismatch,
    compare_daily,
)
from storage.sqlite import SQLiteEngineStore

EVENT_DEFAULTS = {
    "event_time_precision": "datetime",
    "reference_label": "",
    "category": "",
    "importance": None,
    "currency": "",
    "unit": "",
    "previous": None,
    "revised": None,
    "forecast": None,
    "consensus_forecast": None,
    "ticker": "",
    "source": "",
    "source_url": "",
    "content_hash": "hash",
    "last_update_epoch_ms": None,
    "observed_at_epoch_ms": 0,
    "created_at": "2026-04-25T00:00:00+00:00",
    "updated_at": "2026-04-25T00:00:00+00:00",
    "indicator_id": None,
}


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _insert_event(
    store: SQLiteEngineStore, *,
    provider: str, provider_event_id: str, country_code: str, title: str,
    reference_date: str | None, event_time_utc: str, actual: str | None,
) -> None:
    row = {
        **EVENT_DEFAULTS,
        "provider": provider,
        "provider_event_id": provider_event_id,
        "country_code": country_code,
        "title": title,
        "reference_date": reference_date,
        "event_time_utc": event_time_utc,
        "actual": actual,
    }
    with store.get_connection() as conn:
        conn.execute(
            """
            INSERT INTO cal_econ_event (
                provider, provider_event_id, event_time_utc,
                event_time_precision, reference_date, reference_label,
                country_code, indicator_id, category, title,
                importance, currency, unit, actual, previous, revised,
                forecast, consensus_forecast, ticker, source, source_url,
                content_hash, last_update_epoch_ms, observed_at_epoch_ms,
                created_at, updated_at
            ) VALUES (
                :provider, :provider_event_id, :event_time_utc,
                :event_time_precision, :reference_date, :reference_label,
                :country_code, :indicator_id, :category, :title,
                :importance, :currency, :unit, :actual, :previous, :revised,
                :forecast, :consensus_forecast, :ticker, :source, :source_url,
                :content_hash, :last_update_epoch_ms, :observed_at_epoch_ms,
                :created_at, :updated_at
            )
            """,
            row,
        )
        conn.commit()


def _insert_vintage(
    store: SQLiteEngineStore, *,
    provider: str, event_id: str, observed_at: str, actual: str | None,
) -> None:
    with store.get_connection() as conn:
        conn.execute(
            """
            INSERT INTO calendar_event_vintages
                (event_id, provider, vintage_date, observed_at,
                 actual, forecast, previous, metadata_json, scraped_at)
            VALUES (?, ?, ?, ?, ?, NULL, NULL, '{}', ?)
            """,
            (event_id, provider, observed_at, observed_at, actual, observed_at),
        )
        conn.commit()


# ---------------------------------------------------------------------------
# Match
# ---------------------------------------------------------------------------


def test_clean_run_returns_empty(store: SQLiteEngineStore) -> None:
    _insert_event(
        store, provider="tradingeconomics", provider_event_id="te-1",
        country_code="US", title="Unemployment Rate",
        reference_date="2026-03-31T00:00:00",
        event_time_utc="2026-04-24T12:30:00+00:00",
        actual="3.1",
    )
    _insert_event(
        store, provider="bls", provider_event_id="bls-1",
        country_code="US", title="Unemployment Rate",
        reference_date="2026-03-01",
        event_time_utc="2026-04-24T12:30:00+00:00",
        actual="3.1",
    )
    _insert_vintage(
        store, provider="bls", event_id="bls-1",
        observed_at="2026-04-24T12:30:00+00:00", actual="3.1",
    )

    with store.get_connection() as conn:
        reports = compare_daily(conn, target_date=date(2026, 4, 24))
    assert reports == []


def test_te_only_release_reports_missing(store: SQLiteEngineStore) -> None:
    _insert_event(
        store, provider="tradingeconomics", provider_event_id="te-2",
        country_code="US", title="Unemployment Rate",
        reference_date="2026-03-31T00:00:00",
        event_time_utc="2026-04-24T12:30:00+00:00",
        actual="200000",
    )

    with store.get_connection() as conn:
        reports = compare_daily(conn, target_date=date(2026, 4, 24))

    assert len(reports) == 1
    report = reports[0]
    assert report.agency_id == "BLS"
    assert len(report.missing) == 1
    assert report.missing[0].canonical == "UNEMPLOYMENT_RATE"
    assert report.missing[0].te_actual == "200000"


def test_value_mismatch_reports_difference(store: SQLiteEngineStore) -> None:
    _insert_event(
        store, provider="tradingeconomics", provider_event_id="te-3",
        country_code="US", title="Unemployment Rate",
        reference_date="2026-03-31T00:00:00",
        event_time_utc="2026-04-24T12:30:00+00:00",
        actual="200",
    )
    _insert_event(
        store, provider="bls", provider_event_id="bls-3",
        country_code="US", title="Unemployment Rate",
        reference_date="2026-03-01",
        event_time_utc="2026-04-24T12:30:00+00:00",
        actual="195",
    )
    _insert_vintage(
        store, provider="bls", event_id="bls-3",
        observed_at="2026-04-24T12:30:00+00:00", actual="195",
    )
    # A later revision lands; comparator should still use the first
    # release.
    _insert_vintage(
        store, provider="bls", event_id="bls-3",
        observed_at="2026-04-25T12:30:00+00:00", actual="200",
    )

    with store.get_connection() as conn:
        reports = compare_daily(conn, target_date=date(2026, 4, 24))

    assert len(reports) == 1
    report = reports[0]
    assert report.agency_id == "BLS"
    assert len(report.mismatches) == 1
    mm = report.mismatches[0]
    assert mm.canonical == "UNEMPLOYMENT_RATE"
    assert mm.te_actual == "200"
    assert mm.agency_actual == "195"
    assert mm.parse_failed is False
    assert "TE=200" in mm.note and "BLS=195" in mm.note


def test_clean_factor_flags_alignment_bug(store: SQLiteEngineStore) -> None:
    # TE quotes 1000 and BLS quotes 1 — clean ×1000 factor flagged.
    _insert_event(
        store, provider="tradingeconomics", provider_event_id="te-4",
        country_code="US", title="Unemployment Rate",
        reference_date="2026-03-31T00:00:00",
        event_time_utc="2026-04-24T12:30:00+00:00",
        actual="1000",
    )
    _insert_event(
        store, provider="bls", provider_event_id="bls-4",
        country_code="US", title="Unemployment Rate",
        reference_date="2026-03-01",
        event_time_utc="2026-04-24T12:30:00+00:00",
        actual="1",
    )
    _insert_vintage(
        store, provider="bls", event_id="bls-4",
        observed_at="2026-04-24T12:30:00+00:00", actual="1",
    )

    with store.get_connection() as conn:
        reports = compare_daily(conn, target_date=date(2026, 4, 24))

    assert reports[0].mismatches[0].likely_alignment_bug is True


def test_pct_suffix_normalisation(store: SQLiteEngineStore) -> None:
    _insert_event(
        store, provider="tradingeconomics", provider_event_id="te-5",
        country_code="US", title="Unemployment Rate",
        reference_date="2026-03-31T00:00:00",
        event_time_utc="2026-04-24T12:30:00+00:00",
        actual="3.10%",
    )
    _insert_event(
        store, provider="bls", provider_event_id="bls-5",
        country_code="US", title="Unemployment Rate",
        reference_date="2026-03-01",
        event_time_utc="2026-04-24T12:30:00+00:00",
        actual="3.1",
    )
    _insert_vintage(
        store, provider="bls", event_id="bls-5",
        observed_at="2026-04-24T12:30:00+00:00", actual="3.1",
    )

    with store.get_connection() as conn:
        reports = compare_daily(conn, target_date=date(2026, 4, 24))

    assert reports == []


def test_first_release_used_not_revision(store: SQLiteEngineStore) -> None:
    """Even if a later revision matches TE, the first-release vintage is
    what we compare against — that's the parity contract."""
    _insert_event(
        store, provider="tradingeconomics", provider_event_id="te-6",
        country_code="US", title="Unemployment Rate",
        reference_date="2026-03-31T00:00:00",
        event_time_utc="2026-04-24T12:30:00+00:00",
        actual="210",
    )
    _insert_event(
        store, provider="bls", provider_event_id="bls-6",
        country_code="US", title="Unemployment Rate",
        reference_date="2026-03-01",
        event_time_utc="2026-04-24T12:30:00+00:00",
        actual="210",
    )
    # First release was 200; revised to 210 the next day.
    _insert_vintage(
        store, provider="bls", event_id="bls-6",
        observed_at="2026-04-24T12:30:00+00:00", actual="200",
    )
    _insert_vintage(
        store, provider="bls", event_id="bls-6",
        observed_at="2026-04-25T12:00:00+00:00", actual="210",
    )

    with store.get_connection() as conn:
        reports = compare_daily(conn, target_date=date(2026, 4, 24))

    assert len(reports) == 1
    assert reports[0].mismatches[0].agency_actual == "200"


def test_missing_first_release_records_parse_failed(
    store: SQLiteEngineStore,
) -> None:
    _insert_event(
        store, provider="tradingeconomics", provider_event_id="te-7",
        country_code="US", title="Unemployment Rate",
        reference_date="2026-03-31T00:00:00",
        event_time_utc="2026-04-24T12:30:00+00:00",
        actual="200",
    )
    _insert_event(
        store, provider="bls", provider_event_id="bls-7",
        country_code="US", title="Unemployment Rate",
        reference_date="2026-03-01",
        event_time_utc="2026-04-24T12:30:00+00:00",
        actual=None,
    )
    # No vintage rows — the agency event exists but never recorded an
    # actual via the vintage path.

    with store.get_connection() as conn:
        reports = compare_daily(conn, target_date=date(2026, 4, 24))

    assert len(reports) == 1
    mm = reports[0].mismatches[0]
    assert mm.parse_failed is True
    assert "vintage" in mm.note


def test_vintage_after_parity_run_classified_as_lag(
    store: SQLiteEngineStore,
) -> None:
    """First-release vintage observed_at > parity_run_time → LagAfterParity,
    not ValueMismatch. Issue #38."""
    _insert_event(
        store, provider="tradingeconomics", provider_event_id="te-9",
        country_code="US", title="Unemployment Rate",
        reference_date="2026-03-31T00:00:00",
        event_time_utc="2026-04-24T03:30:00+00:00",
        actual="3.1",
    )
    _insert_event(
        store, provider="bls", provider_event_id="bls-9",
        country_code="US", title="Unemployment Rate",
        reference_date="2026-03-01",
        event_time_utc="2026-04-24T03:30:00+00:00",
        actual=None,
    )
    # Sweep wrote the vintage at 07:15 UTC — after the 06:00 parity cron.
    _insert_vintage(
        store, provider="bls", event_id="bls-9",
        observed_at="2026-04-24T07:15:00+00:00", actual="3.1",
    )

    parity_run = datetime(2026, 4, 24, 6, 0, tzinfo=timezone.utc)
    with store.get_connection() as conn:
        reports = compare_daily(
            conn, target_date=date(2026, 4, 24), now_utc=parity_run,
        )

    assert len(reports) == 1
    report = reports[0]
    assert report.clean is True
    assert report.empty is False
    assert len(report.lag_after_parity) == 1
    lag = report.lag_after_parity[0]
    assert lag.agency_event_id == "bls-9"
    assert lag.first_observed_at == "2026-04-24T07:15:00+00:00"
    assert lag.te_actual == "3.1"


def test_vintage_before_parity_run_falls_through_to_normal_logic(
    store: SQLiteEngineStore,
) -> None:
    """observed_at <= parity_run_time → normal value comparison applies."""
    _insert_event(
        store, provider="tradingeconomics", provider_event_id="te-10",
        country_code="US", title="Unemployment Rate",
        reference_date="2026-03-31T00:00:00",
        event_time_utc="2026-04-24T03:30:00+00:00",
        actual="3.1",
    )
    _insert_event(
        store, provider="bls", provider_event_id="bls-10",
        country_code="US", title="Unemployment Rate",
        reference_date="2026-03-01",
        event_time_utc="2026-04-24T03:30:00+00:00",
        actual="3.1",
    )
    _insert_vintage(
        store, provider="bls", event_id="bls-10",
        observed_at="2026-04-24T05:55:00+00:00", actual="3.2",
    )

    parity_run = datetime(2026, 4, 24, 6, 0, tzinfo=timezone.utc)
    with store.get_connection() as conn:
        reports = compare_daily(
            conn, target_date=date(2026, 4, 24), now_utc=parity_run,
        )

    assert len(reports) == 1
    assert reports[0].lag_after_parity == []
    assert len(reports[0].mismatches) == 1


def test_late_vintage_with_mismatched_value_still_classified_as_lag(
    store: SQLiteEngineStore,
) -> None:
    """Per the issue spec the lag classification is timing-based, not
    value-based: a late vintage that disagrees with TE is still bucketed
    under lag (no drift filed) but its agency_actual is captured so the
    structured log shows the disagreement for operator triage."""
    _insert_event(
        store, provider="tradingeconomics", provider_event_id="te-12",
        country_code="US", title="Unemployment Rate",
        reference_date="2026-03-31T00:00:00",
        event_time_utc="2026-04-24T03:30:00+00:00",
        actual="3.1",
    )
    _insert_event(
        store, provider="bls", provider_event_id="bls-12",
        country_code="US", title="Unemployment Rate",
        reference_date="2026-03-01",
        event_time_utc="2026-04-24T03:30:00+00:00",
        actual=None,
    )
    _insert_vintage(
        store, provider="bls", event_id="bls-12",
        observed_at="2026-04-24T07:15:00+00:00", actual="3.4",
    )

    parity_run = datetime(2026, 4, 24, 6, 0, tzinfo=timezone.utc)
    with store.get_connection() as conn:
        reports = compare_daily(
            conn, target_date=date(2026, 4, 24), now_utc=parity_run,
        )
    assert len(reports) == 1
    report = reports[0]
    assert report.mismatches == []
    assert len(report.lag_after_parity) == 1
    lag = report.lag_after_parity[0]
    assert lag.te_actual == "3.1"
    assert lag.agency_actual == "3.4"


def test_lag_only_report_returned_with_clean_true(
    store: SQLiteEngineStore,
) -> None:
    """A report whose only entries are lag_after_parity is still
    returned (so the structured log can record the count) but
    ``clean`` is True so the filer skips drift handling."""
    _insert_event(
        store, provider="tradingeconomics", provider_event_id="te-11",
        country_code="US", title="Unemployment Rate",
        reference_date="2026-03-31T00:00:00",
        event_time_utc="2026-04-24T03:30:00+00:00",
        actual="3.1",
    )
    _insert_event(
        store, provider="bls", provider_event_id="bls-11",
        country_code="US", title="Unemployment Rate",
        reference_date="2026-03-01",
        event_time_utc="2026-04-24T03:30:00+00:00",
        actual=None,
    )
    _insert_vintage(
        store, provider="bls", event_id="bls-11",
        observed_at="2026-04-24T07:15:00+00:00", actual="3.1",
    )

    parity_run = datetime(2026, 4, 24, 6, 0, tzinfo=timezone.utc)
    with store.get_connection() as conn:
        reports = compare_daily(
            conn, target_date=date(2026, 4, 24), now_utc=parity_run,
        )
    assert len(reports) == 1
    assert reports[0].clean is True
    assert reports[0].total_lag_after_parity == 1


def test_unowned_country_indicator_ignored(store: SQLiteEngineStore) -> None:
    _insert_event(
        store, provider="tradingeconomics", provider_event_id="te-8",
        country_code="ZZ", title="Mystery Indicator",
        reference_date="2026-03-31T00:00:00",
        event_time_utc="2026-04-24T12:30:00+00:00",
        actual="42",
    )
    with store.get_connection() as conn:
        reports = compare_daily(conn, target_date=date(2026, 4, 24))
    assert reports == []

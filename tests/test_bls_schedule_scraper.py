"""Tests for the BLS release-schedule scraper (issue #9 P1a).

Fixture HTML lives in ``tests/fixtures/bls_schedule/`` — slices of
the real ``bls.gov/schedule/news_release/<series>.htm`` pages
captured 2026-04-21. No real HTTP in CI.

Covers:

- Parser: rows extracted correctly; Reference Month promoted to
  first-of-month; date/time combined with DST-aware UTC conversion.
- Schedule entry → (raw, event) records: ``provider_event_id``
  anchors on reference-period date so it matches the API-side id.
- Projector: schedule rows insert with ``actual=NULL`` +
  ``precision='datetime'``; later API-side writes preserve the
  scheduled datetime.
- Merge scenarios in both orders (schedule-first, API-first) — the
  end state is one row with ``actual`` set and ``precision='datetime'``.
- Service op ``calendar_econ_schedule_bls`` — dry_run plan, fixture
  injection via ``html_fetcher`` seam.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from ingestion.calendar.bls_api import (
    INDICATOR_REGISTRY,
    BLSScheduleEntry,
    BLSScheduleParseError,
    SCHEDULE_URL_SLUG,
    parse_observation,
    parse_schedule_html,
    project_events,
    project_schedule_events,
    schedule_bls_calendar,
    schedule_entry_to_records,
    store_raw,
)
from ingestion.timeseries.scrapers.bls import BLSObservation
from storage.sqlite import SQLiteEngineStore


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "bls_schedule"


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _fixture_html(name: str) -> str:
    return (FIXTURE_DIR / name).read_text(encoding="utf-8")


# ──────────────────────────────────────────────────────────────────────────
# parse_schedule_html
# ──────────────────────────────────────────────────────────────────────────


def test_parse_cpi_schedule_extracts_all_rows() -> None:
    html = _fixture_html("cpi.html")
    entries = parse_schedule_html(html, series_id="CUUR0000SA0")
    # Every BLS schedule page lists ~13 months forward; the fixture was
    # captured 2026-04-21, so expect at least a handful of entries.
    assert len(entries) >= 10
    # Reference months are always first-of-month dates.
    assert all(e.reference_date.endswith("-01") for e in entries)


def test_parse_schedule_uses_dst_aware_eastern_time() -> None:
    """BLS publishes at 8:30 AM ET. Summer entries must be UTC-4 (EDT),
    winter entries UTC-5 (EST)."""
    entries = parse_schedule_html(
        _fixture_html("cpi.html"), series_id="CUUR0000SA0",
    )
    by_release_date = {e.release_date: e for e in entries}
    # One winter sample + one summer sample (pick any release dates
    # present in the fixture to guarantee both sides of DST).
    winter = next(
        e for d, e in by_release_date.items()
        if d.startswith("2026-01") or d.startswith("2026-02")
    )
    summer = next(
        e for d, e in by_release_date.items()
        if d.startswith("2026-06") or d.startswith("2026-07")
    )
    # 8:30 AM EST = 13:30 UTC
    assert "T13:30" in winter.event_time_utc
    # 8:30 AM EDT = 12:30 UTC
    assert "T12:30" in summer.event_time_utc


def test_parse_nfp_schedule_extracts_entries() -> None:
    html = _fixture_html("empsit.html")
    entries = parse_schedule_html(html, series_id="CES0000000001")
    assert len(entries) >= 10
    for entry in entries:
        assert entry.series_id == "CES0000000001"


def test_parse_raises_on_missing_table() -> None:
    with pytest.raises(BLSScheduleParseError):
        parse_schedule_html("<html><body>no table</body></html>", series_id="CUUR0000SA0")


def test_parse_unknown_series_rejected() -> None:
    html = _fixture_html("cpi.html")
    with pytest.raises(KeyError):
        parse_schedule_html(html, series_id="UNKNOWN")


# ──────────────────────────────────────────────────────────────────────────
# schedule_entry_to_records
# ──────────────────────────────────────────────────────────────────────────


def test_schedule_entry_id_matches_api_side_id() -> None:
    """The key property for cross-source merge: the same reference
    period hashes to the same ``provider_event_id`` whether the row
    came from the schedule scraper or the API."""
    entry = BLSScheduleEntry(
        series_id="CUUR0000SA0",
        reference_date="2024-04-01",
        reference_label="April 2024",
        release_date="2024-05-15",
        release_time_local="08:30 AM",
        event_time_utc="2024-05-15T12:30:00+00:00",
    )
    _, schedule_event = schedule_entry_to_records(entry, snapshot_epoch_ms=0)

    api_obs = BLSObservation(
        series_id="CUUR0000SA0",
        date="2024-04-01",
        value=312.5,
        period="M04",
    )
    _, api_event = parse_observation(api_obs, snapshot_epoch_ms=0)
    assert schedule_event.provider_event_id == api_event.provider_event_id


def test_schedule_event_record_shape() -> None:
    entry = BLSScheduleEntry(
        series_id="CUUR0000SA0",
        reference_date="2024-04-01",
        reference_label="April 2024",
        release_date="2024-05-15",
        release_time_local="08:30 AM",
        event_time_utc="2024-05-15T12:30:00+00:00",
    )
    raw, event = schedule_entry_to_records(entry, snapshot_epoch_ms=1_700_000_000_000)
    assert event.event_time_precision == "datetime"
    assert event.event_time_utc == "2024-05-15T12:30:00+00:00"
    assert event.actual is None
    assert event.title == "Consumer Price Index"
    assert event.importance == "high"
    assert event.source_url.endswith("/cpi.htm")
    assert raw.provider == "bls"


# ──────────────────────────────────────────────────────────────────────────
# Cross-source merge
# ──────────────────────────────────────────────────────────────────────────


def _schedule_records(reference_date: str):
    entry = BLSScheduleEntry(
        series_id="CUUR0000SA0",
        reference_date=reference_date,
        reference_label="April 2024",
        release_date="2024-05-15",
        release_time_local="08:30 AM",
        event_time_utc="2024-05-15T12:30:00+00:00",
    )
    return schedule_entry_to_records(entry, snapshot_epoch_ms=1_700_000_000_000)


def _api_records(reference_date: str):
    obs = BLSObservation(
        series_id="CUUR0000SA0",
        date=reference_date,
        value=312.5,
        period="M04",
    )
    return parse_observation(obs, snapshot_epoch_ms=1_700_000_100_000)


def test_merge_schedule_then_api_keeps_datetime_and_gains_actual(
    store: SQLiteEngineStore,
) -> None:
    raw_sched, evt_sched = _schedule_records("2024-04-01")
    raw_api, evt_api = _api_records("2024-04-01")

    with store._connection(commit=True) as conn:
        store_raw(conn, [raw_sched])
        project_schedule_events(conn, [evt_sched])
        store_raw(conn, [raw_api])
        project_events(conn, [evt_api])

    with store._connection(commit=False) as conn:
        row = conn.execute(
            "SELECT event_time_utc, event_time_precision, actual "
            "FROM cal_econ_event WHERE provider='bls'"
        ).fetchone()
    assert tuple(row) == (
        "2024-05-15T12:30:00+00:00",
        "datetime",
        "312.5",
    )


def test_merge_api_then_schedule_promotes_precision_and_keeps_actual(
    store: SQLiteEngineStore,
) -> None:
    raw_api, evt_api = _api_records("2024-04-01")
    raw_sched, evt_sched = _schedule_records("2024-04-01")

    with store._connection(commit=True) as conn:
        store_raw(conn, [raw_api])
        project_events(conn, [evt_api])
        store_raw(conn, [raw_sched])
        project_schedule_events(conn, [evt_sched])

    with store._connection(commit=False) as conn:
        row = conn.execute(
            "SELECT event_time_utc, event_time_precision, actual "
            "FROM cal_econ_event WHERE provider='bls'"
        ).fetchone()
    assert tuple(row) == (
        "2024-05-15T12:30:00+00:00",
        "datetime",
        "312.5",
    )


def test_single_row_after_merge(store: SQLiteEngineStore) -> None:
    """Both sides, both orderings → exactly one row per event."""
    raw_sched, evt_sched = _schedule_records("2024-04-01")
    raw_api, evt_api = _api_records("2024-04-01")

    with store._connection(commit=True) as conn:
        store_raw(conn, [raw_sched])
        project_schedule_events(conn, [evt_sched])
        store_raw(conn, [raw_api])
        project_events(conn, [evt_api])

    with store._connection(commit=False) as conn:
        (n,) = conn.execute(
            "SELECT COUNT(*) FROM cal_econ_event WHERE provider='bls'"
        ).fetchone()
    assert n == 1


def test_schedule_merge_does_not_clobber_api_freshness(
    store: SQLiteEngineStore,
) -> None:
    """Codex P2 — a schedule scrape must not overwrite the API-side
    freshness column (``observed_at_epoch_ms``), which
    :func:`project_events` uses to gate revision ordering. Scenario:

      t=100 — API writes value=312.5, observed_at=100.
      t=200 — schedule scrape arrives, bumps observed_at to 200 (bug).
      t=150 — late-arriving API revision value=313.0. With the bug,
              the freshness gate ``150 >= 200`` FAILS and the
              revision is silently dropped.

    After the fix, the schedule merge preserves observed_at=100, so
    the 150-revision passes the gate and updates ``actual`` to 313.0.
    """
    obs_v1 = BLSObservation(
        series_id="CUUR0000SA0", date="2024-04-01", value=312.5, period="M04",
    )
    raw_v1, evt_v1 = parse_observation(
        obs_v1, snapshot_epoch_ms=100, observed_at_epoch_ms=100,
    )

    entry = BLSScheduleEntry(
        series_id="CUUR0000SA0",
        reference_date="2024-04-01",
        reference_label="April 2024",
        release_date="2024-05-15",
        release_time_local="08:30 AM",
        event_time_utc="2024-05-15T12:30:00+00:00",
    )
    raw_sched, evt_sched = schedule_entry_to_records(
        entry, snapshot_epoch_ms=200, observed_at_epoch_ms=200,
    )

    obs_v2 = BLSObservation(
        series_id="CUUR0000SA0", date="2024-04-01", value=313.0, period="M04",
    )
    raw_v2, evt_v2 = parse_observation(
        obs_v2, snapshot_epoch_ms=150, observed_at_epoch_ms=150,
    )

    with store._connection(commit=True) as conn:
        store_raw(conn, [raw_v1])
        project_events(conn, [evt_v1])
        store_raw(conn, [raw_sched])
        project_schedule_events(conn, [evt_sched])
        store_raw(conn, [raw_v2])
        project_events(conn, [evt_v2])

    with store._connection(commit=False) as conn:
        row = conn.execute(
            "SELECT actual, event_time_precision, observed_at_epoch_ms "
            "FROM cal_econ_event WHERE provider='bls'"
        ).fetchone()
    # API revision v2 landed (actual updated) and schedule precision
    # is preserved. observed_at reflects v2's 150 (API-side freshness).
    assert tuple(row) == ("313.0", "datetime", 150)


# ──────────────────────────────────────────────────────────────────────────
# Service op
# ──────────────────────────────────────────────────────────────────────────


def test_service_op_dry_run(store: SQLiteEngineStore) -> None:
    from macro_data.service import LocalMacroDataService

    svc = LocalMacroDataService(store=store)
    result = svc.invoke("calendar_econ_schedule_bls", {"dry_run": True})
    assert result["dry_run"] is True
    assert set(result["series_planned"]) == set(SCHEDULE_URL_SLUG.keys())
    assert result["series_unknown"] == []


def test_schedule_fetcher_uses_injected_html(store: SQLiteEngineStore) -> None:
    """Tests use the ``html_fetcher`` seam to feed fixture HTML
    without hitting bls.gov."""
    cpi_html = _fixture_html("cpi.html")
    empsit_html = _fixture_html("empsit.html")

    def fake_fetcher(series_id, *, session=None):
        return {
            "CUUR0000SA0":   cpi_html,
            "CES0000000001": empsit_html,
        }[series_id]

    with store._connection(commit=True) as conn:
        summary = schedule_bls_calendar(
            conn, dry_run=False, html_fetcher=fake_fetcher,
        )
    assert set(summary.series_ok) == {"CUUR0000SA0", "CES0000000001"}
    assert summary.entries_parsed >= 20  # ≥10 from each fixture
    assert summary.events_upserted == summary.entries_parsed
    # All schedule rows carry datetime precision.
    with store._connection(commit=False) as conn:
        rows = conn.execute(
            "SELECT DISTINCT event_time_precision "
            "FROM cal_econ_event WHERE provider='bls'"
        ).fetchall()
    assert [tuple(r) for r in rows] == [("datetime",)]

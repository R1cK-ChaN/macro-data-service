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


def _p1c_fake_fetcher():
    """Fake fetcher mapping every :data:`SCHEDULE_URL_SLUG` series id to
    the appropriate fixture HTML. Tests feed this into
    :func:`schedule_bls_calendar` to drive the full P1c whitelist
    without touching bls.gov."""
    cpi_html = _fixture_html("cpi.html")
    empsit_html = _fixture_html("empsit.html")
    jolts_html = _fixture_html("jolts.html")
    eci_html = _fixture_html("eci.html")
    prod_html = _fixture_html("prod2.html")
    slug_to_html = {
        "cpi.htm":     cpi_html,
        "empsit.htm":  empsit_html,
        "jolts.htm":   jolts_html,
        "eci.htm":     eci_html,
        "prod2.htm":   prod_html,
    }

    def fake_fetcher(series_id, *, session=None):
        return slug_to_html[SCHEDULE_URL_SLUG[series_id]]

    return fake_fetcher


def test_schedule_fetcher_uses_injected_html(store: SQLiteEngineStore) -> None:
    """Fake ``html_fetcher`` drives the full P1c whitelist — every
    series id in :data:`SCHEDULE_URL_SLUG` resolves to fixture HTML."""
    fake_fetcher = _p1c_fake_fetcher()

    with store._connection(commit=True) as conn:
        summary = schedule_bls_calendar(
            conn, dry_run=False, html_fetcher=fake_fetcher,
        )
    assert set(summary.series_ok) == set(SCHEDULE_URL_SLUG.keys())
    assert summary.events_upserted == summary.entries_parsed
    # Every schedule row carries datetime precision — the schedule
    # scraper always resolves a wall-clock release time.
    with store._connection(commit=False) as conn:
        rows = conn.execute(
            "SELECT DISTINCT event_time_precision "
            "FROM cal_econ_event WHERE provider='bls'"
        ).fetchall()
    assert [tuple(r) for r in rows] == [("datetime",)]


# ──────────────────────────────────────────────────────────────────────────
# P1c fixtures — new schedule pages + quarterly reference parsing
# ──────────────────────────────────────────────────────────────────────────


def test_parse_jolts_schedule_extracts_ten_am_release_time() -> None:
    """JOLTS releases at 10:00 AM ET, not the BLS-default 8:30 AM.
    The DST-aware converter still delivers correct UTC."""
    entries = parse_schedule_html(
        _fixture_html("jolts.html"), series_id="JTS000000000000000JOL",
    )
    assert len(entries) >= 10
    # Winter sample: Jan. 07, 2026 = 10:00 AM EST = 15:00 UTC.
    winter = next(e for e in entries if e.release_date == "2026-01-07")
    assert "T15:00" in winter.event_time_utc
    # Summer sample: Jul. 07, 2026 = 10:00 AM EDT = 14:00 UTC.
    summer = next(e for e in entries if e.release_date == "2026-07-07")
    assert "T14:00" in summer.event_time_utc


def test_parse_eci_schedule_uses_end_of_quarter_reference_date() -> None:
    """ECI is quarterly; reference_date must land on end-of-quarter
    so the schedule-side event id matches the API-side obs.date
    (BLSClient normalises ``Q03 → YYYY-09-30``)."""
    entries = parse_schedule_html(
        _fixture_html("eci.html"), series_id="CIU1010000000000A",
    )
    assert len(entries) >= 4
    by_release = {e.release_date: e for e in entries}
    # 3rd Quarter 2025 → reference_date 2025-09-30.
    q3 = by_release["2025-10-31"]
    assert q3.reference_date == "2025-09-30"
    assert q3.reference_label == "3rd Quarter 2025"
    # 4th Quarter 2025 → reference_date 2025-12-31.
    q4 = by_release["2026-01-30"]
    assert q4.reference_date == "2025-12-31"
    assert q4.reference_label == "4th Quarter 2025"


def test_parse_productivity_schedule_preserves_stage_preliminary_vs_revised() -> None:
    """Productivity and Costs publishes preliminary then revised
    versions of the same quarter. Both rows share ``reference_date``
    but land under *distinct* provider_event_ids because the
    ``release_stage`` rides into the synthesised id — the two
    scheduled calendar events are semantically distinct, so the
    later scrape must not overwrite the earlier one."""
    entries = parse_schedule_html(
        _fixture_html("prod2.html"), series_id="PRS85006092",
    )
    by_label = {e.reference_label: e for e in entries}
    preliminary = by_label["4th Quarter 2025 (Preliminary)"]
    revised = by_label["4th Quarter 2025 (Revised)"]
    # Same reference_date (end-of-Q4).
    assert preliminary.reference_date == revised.reference_date == "2025-12-31"
    # Distinct stage, distinct release date.
    assert preliminary.release_stage == "preliminary"
    assert revised.release_stage == "revised"
    assert preliminary.release_date != revised.release_date
    # Distinct provider_event_ids via stage-qualified synthesis.
    _, prelim_event = schedule_entry_to_records(
        preliminary, snapshot_epoch_ms=1_700_000_000_000,
    )
    _, revised_event = schedule_entry_to_records(
        revised, snapshot_epoch_ms=1_700_000_000_000,
    )
    assert prelim_event.provider_event_id != revised_event.provider_event_id


def test_schedule_run_keeps_both_prod_releases_per_quarter(
    store: SQLiteEngineStore,
) -> None:
    """End-to-end: a full prod2 scrape lands one cal_econ_event row
    per (quarter, stage) pair, not one per quarter."""
    fake_fetcher = _p1c_fake_fetcher()

    with store._connection(commit=True) as conn:
        schedule_bls_calendar(
            conn,
            series_ids=["PRS85006092"],
            dry_run=False,
            html_fetcher=fake_fetcher,
        )
    with store._connection(commit=False) as conn:
        (count,) = conn.execute(
            "SELECT COUNT(*) FROM cal_econ_event "
            "WHERE provider='bls' AND title LIKE 'Nonfarm Business%'"
        ).fetchone()
    # prod2 fixture holds 8 entries (4 quarters × 2 stages).
    assert count == 8


def test_parse_reference_period_supports_q_short_form() -> None:
    """``"Q3 2025"`` should parse to end-of-quarter identically to
    ``"3rd Quarter 2025"`` (occasional BLS format variant)."""
    html = (
        "<table class='release-list'><tbody>"
        "<tr><td>Q3 2025</td><td>Oct. 31, 2025</td><td>08:30 AM</td></tr>"
        "</tbody></table>"
    )
    entries = parse_schedule_html(html, series_id="CIU1010000000000A")
    assert len(entries) == 1
    assert entries[0].reference_date == "2025-09-30"


def test_schedule_entry_ids_differentiate_indicators_sharing_empsit() -> None:
    """Codex P1c — Unemployment Rate / AHE / AWH all piggyback on
    ``empsit.htm``. Their schedule rows must resolve to *distinct*
    provider_event_ids so they don't clobber the NFP row (and each
    other) in cal_econ_event."""
    empsit = _fixture_html("empsit.html")
    per_series = {}
    for sid in ("CES0000000001", "LNS14000000", "CES0500000003", "CES0500000002"):
        entries = parse_schedule_html(empsit, series_id=sid)
        _, event = schedule_entry_to_records(
            entries[0], snapshot_epoch_ms=1_700_000_000_000,
        )
        per_series[sid] = event.provider_event_id
    assert len(set(per_series.values())) == 4


def test_schedule_run_fetches_each_slug_only_once(
    store: SQLiteEngineStore,
) -> None:
    """Codex P1c — the four empsit.htm piggybackers (NFP / UR / AHE / AWH)
    must not trigger four separate HTTP fetches of the same page. The
    scraper dedupes by slug so bls.gov sees one request per unique
    page per run."""
    fake_fetcher = _p1c_fake_fetcher()
    fetch_calls: list[str] = []

    def counting_fetcher(series_id, *, session=None):
        fetch_calls.append(series_id)
        return fake_fetcher(series_id, session=session)

    with store._connection(commit=True) as conn:
        schedule_bls_calendar(
            conn, dry_run=False, html_fetcher=counting_fetcher,
        )

    # One fetch per unique slug in SCHEDULE_URL_SLUG.
    unique_slugs = set(SCHEDULE_URL_SLUG.values())
    assert len(fetch_calls) == len(unique_slugs)


def test_schedule_run_writes_distinct_rows_per_empsit_indicator(
    store: SQLiteEngineStore,
) -> None:
    """End-to-end: one ``empsit.htm`` scrape yields one row per CES/CPS
    indicator mapped to it, none of them clobbering each other."""
    fake_fetcher = _p1c_fake_fetcher()

    with store._connection(commit=True) as conn:
        schedule_bls_calendar(
            conn,
            series_ids=[
                "CES0000000001", "LNS14000000",
                "CES0500000003", "CES0500000002",
            ],
            dry_run=False,
            html_fetcher=fake_fetcher,
        )

    with store._connection(commit=False) as conn:
        rows = conn.execute(
            "SELECT title, COUNT(*) FROM cal_econ_event "
            "WHERE provider='bls' GROUP BY title"
        ).fetchall()
    titles = {r[0]: r[1] for r in rows}
    # All four bundled indicators present with identical row counts
    # (one per release on empsit.htm).
    assert set(titles.keys()) == {
        "Nonfarm Payrolls",
        "Unemployment Rate",
        "Average Hourly Earnings — Total Private",
        "Average Weekly Hours — Total Private",
    }
    counts = set(titles.values())
    assert len(counts) == 1  # identical row counts across the four


# ──────────────────────────────────────────────────────────────────────────
# Schedule-only audit-freshness (BLS P1c Productivity follow-up)
# ──────────────────────────────────────────────────────────────────────────


def test_schedule_rescrape_refreshes_productivity_observed_at(
    store: SQLiteEngineStore,
) -> None:
    """Productivity is ``api_fetch=False`` — there's no API side to
    protect, so schedule re-scrapes must refresh the audit field
    (``observed_at_epoch_ms``) the same way ``project_events`` does.

    The pre-fix path routed every schedule row through
    ``project_schedule_events``, which deliberately does *not* touch
    ``observed_at_epoch_ms`` on conflict — a value-side freshness
    guard designed for API-merged indicators. Schedule-only
    Productivity rows therefore froze their audit field on the first
    scrape and every subsequent re-scrape was invisible in the audit
    trail. The partition-by-``api_fetch`` fix (matching BEA P2a)
    routes schedule-only rows through ``project_events`` instead so
    the audit reflects the latest scrape time.
    """
    fake_fetcher = _p1c_fake_fetcher()

    first_snapshot = 1_700_000_000_000
    second_snapshot = 1_700_000_100_000

    with store._connection(commit=True) as conn:
        schedule_bls_calendar(
            conn,
            series_ids=["PRS85006092"],
            dry_run=False,
            html_fetcher=fake_fetcher,
            snapshot_epoch_ms=first_snapshot,
        )
    with store._connection(commit=True) as conn:
        schedule_bls_calendar(
            conn,
            series_ids=["PRS85006092"],
            dry_run=False,
            html_fetcher=fake_fetcher,
            snapshot_epoch_ms=second_snapshot,
        )

    with store._connection(commit=False) as conn:
        observed_values = {
            row[0] for row in conn.execute(
                "SELECT DISTINCT observed_at_epoch_ms "
                "FROM cal_econ_event "
                "WHERE provider='bls' AND title LIKE 'Nonfarm Business%'"
            ).fetchall()
        }
    assert observed_values == {second_snapshot}


def test_schedule_rescrape_preserves_cpi_observed_at(
    store: SQLiteEngineStore,
) -> None:
    """Conjugate to the Productivity test: CPI is ``api_fetch=True``,
    so its rows still flow through ``project_schedule_events`` and
    the audit field stays at the first-write value — otherwise a
    later schedule re-scrape could let a stale API value overwrite a
    fresher one by bumping the freshness gate."""
    fake_fetcher = _p1c_fake_fetcher()

    first_snapshot = 1_700_000_000_000
    second_snapshot = 1_700_000_100_000

    with store._connection(commit=True) as conn:
        schedule_bls_calendar(
            conn,
            series_ids=["CUUR0000SA0"],
            dry_run=False,
            html_fetcher=fake_fetcher,
            snapshot_epoch_ms=first_snapshot,
        )
    with store._connection(commit=True) as conn:
        schedule_bls_calendar(
            conn,
            series_ids=["CUUR0000SA0"],
            dry_run=False,
            html_fetcher=fake_fetcher,
            snapshot_epoch_ms=second_snapshot,
        )

    with store._connection(commit=False) as conn:
        observed_values = {
            row[0] for row in conn.execute(
                "SELECT DISTINCT observed_at_epoch_ms "
                "FROM cal_econ_event "
                "WHERE provider='bls' AND title LIKE 'Consumer Price Index%'"
            ).fetchall()
        }
    assert observed_values == {first_snapshot}

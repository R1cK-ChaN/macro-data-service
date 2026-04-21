"""Mocked tests for the NBS calendar connector (issue #9 P5).

Fixture HTML lives in ``tests/fixtures/nbs_calendar/`` — a real NBS
yearly-calendar article captured 2026-04-21. No real HTTP in CI.

Covers:

- Parser: every month's scheduled CPI release extracted; empty-
  month markers (``"……"``) skipped; day + weekday cells parsed.
- ``release_entry_to_records``: ``provider_event_id`` anchors on
  the release-date + canonical indicator so schedule / value
  upgrades land on the same id; event time combines date + NBS
  09:30 local → Asia/Shanghai UTC+8.
- Projector: schedule rows land with ``precision='datetime'`` +
  ``actual=NULL``; upsert is idempotent; merge rule preserves
  datetime precision when a later ``approximate`` row arrives.
- Fetcher: dry-run returns the plan; zero-parse guard fires in
  execute mode; fixture injection via the ``html_fetcher`` seam
  succeeds.
- Service op ``calendar_econ_fetch_nbs`` — dry-run plan + execute-
  mode wiring.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from ingestion.calendar.nbs_api import (
    INDICATOR_REGISTRY,
    NBS_CALENDAR_URL_BASE,
    NBSCalendarEventRecord,
    NBSCalendarParseError,
    NBSReleaseEntry,
    fetch_nbs_calendar,
    parse_nbs_calendar_html,
    project_events,
    release_entry_to_records,
    store_raw,
)
from ingestion.calendar.nbs_api.parser import PROVIDER, _content_hash
from storage.sqlite import SQLiteEngineStore


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "nbs_calendar"
FIXTURE_CALENDAR_URL = (
    "https://www.stats.gov.cn/english/PressRelease/ReleaseCalendar/"
    "202512/t20251226_1962154.html"
)


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _fixture_html(name: str) -> str:
    return (FIXTURE_DIR / name).read_text(encoding="utf-8")


# ──────────────────────────────────────────────────────────────────────────
# INDICATOR_REGISTRY
# ──────────────────────────────────────────────────────────────────────────


def test_registry_ships_single_anchor() -> None:
    assert set(INDICATOR_REGISTRY.keys()) == {"CPI"}
    spec = INDICATOR_REGISTRY["CPI"]
    assert spec.country_code == "CN"
    assert spec.importance == "high"
    assert "consumer price" in spec.label_fragment.lower()


# ──────────────────────────────────────────────────────────────────────────
# parse_nbs_calendar_html
# ──────────────────────────────────────────────────────────────────────────


def test_parse_2026_calendar_extracts_twelve_cpi_releases() -> None:
    entries = parse_nbs_calendar_html(_fixture_html("nbs_2026.html"))
    assert len(entries) == 12
    months = [e.month for e in entries]
    assert months == sorted(months)
    assert months == list(range(1, 13))
    for entry in entries:
        assert entry.year == 2026
        assert entry.indicator == "CPI"
        assert entry.release_time_local == "9:30"


def test_parse_carries_day_and_weekday() -> None:
    entries = parse_nbs_calendar_html(_fixture_html("nbs_2026.html"))
    january = [e for e in entries if e.month == 1][0]
    # NBS 2026 CPI: 9 January 2026 (Friday).
    assert january.day == 9
    assert january.weekday_label.lower().startswith("fri")


def test_parse_uses_year_override_when_provided() -> None:
    entries = parse_nbs_calendar_html(
        _fixture_html("nbs_2026.html"), year_override=1999,
    )
    assert all(e.year == 1999 for e in entries)


def test_parse_skips_empty_month_markers() -> None:
    # Synthetic minimal table: four months with "……" → expect
    # only the months that carry a date.
    html = """
    <table class="trs_word_table">
      <tr>
        <td>No.</td><td>Content</td>
        <td>Jan.</td><td>Feb.</td><td>Mar.</td><td>Apr.</td>
        <td>May</td><td>Jun.</td><td>Jul.</td><td>Aug.</td>
        <td>Sep.</td><td>Oct.</td><td>Nov.</td><td>Dec.</td>
      </tr>
      <tr>
        <td>1</td>
        <td>Monthly Report on Consumer Price Index (CPI)</td>
        <td>9/Fri</td><td>……</td><td>9/Mon</td><td>……</td>
        <td>……</td><td>……</td><td>……</td><td>……</td>
        <td>……</td><td>……</td><td>……</td><td>……</td>
      </tr>
      <tr>
        <td>9:30</td><td>9:30</td><td>9:30</td><td>9:30</td>
        <td>9:30</td><td>9:30</td><td>9:30</td><td>9:30</td>
        <td>9:30</td><td>9:30</td><td>9:30</td><td>9:30</td>
      </tr>
    </table>
    """
    entries = parse_nbs_calendar_html(html, year_override=2030)
    assert len(entries) == 2
    assert [e.month for e in entries] == [1, 3]


def test_parse_raises_when_table_missing() -> None:
    with pytest.raises(NBSCalendarParseError):
        parse_nbs_calendar_html("<html></html>", year_override=2026)


def test_parse_raises_without_year() -> None:
    html = """
    <table class="trs_word_table">
      <tr><td>No.</td><td>Content</td>
        <td>Jan.</td><td>Feb.</td><td>Mar.</td><td>Apr.</td>
        <td>May</td><td>Jun.</td><td>Jul.</td><td>Aug.</td>
        <td>Sep.</td><td>Oct.</td><td>Nov.</td><td>Dec.</td>
      </tr>
    </table>
    """
    with pytest.raises(NBSCalendarParseError):
        parse_nbs_calendar_html(html)


def test_parse_raises_on_malformed_day_cell() -> None:
    html = """
    <title>Regular Press Release Calendar of NBS in 2026</title>
    <table class="trs_word_table">
      <tr>
        <td>No.</td><td>Content</td>
        <td>Jan.</td><td>Feb.</td><td>Mar.</td><td>Apr.</td>
        <td>May</td><td>Jun.</td><td>Jul.</td><td>Aug.</td>
        <td>Sep.</td><td>Oct.</td><td>Nov.</td><td>Dec.</td>
      </tr>
      <tr>
        <td>1</td>
        <td>Monthly Report on Consumer Price Index (CPI)</td>
        <td>garbage</td><td>……</td><td>……</td><td>……</td>
        <td>……</td><td>……</td><td>……</td><td>……</td>
        <td>……</td><td>……</td><td>……</td><td>……</td>
      </tr>
      <tr>
        <td>9:30</td><td>9:30</td><td>9:30</td><td>9:30</td>
        <td>9:30</td><td>9:30</td><td>9:30</td><td>9:30</td>
        <td>9:30</td><td>9:30</td><td>9:30</td><td>9:30</td>
      </tr>
    </table>
    """
    with pytest.raises(NBSCalendarParseError):
        parse_nbs_calendar_html(html)


# ──────────────────────────────────────────────────────────────────────────
# release_entry_to_records
# ──────────────────────────────────────────────────────────────────────────


def _entry(
    *,
    year: int = 2026,
    month: int = 1,
    day: int = 9,
    release_time: str = "9:30",
    indicator: str = "CPI",
    weekday: str = "Fri",
) -> NBSReleaseEntry:
    return NBSReleaseEntry(
        year=year,
        month=month,
        day=day,
        release_time_local=release_time,
        indicator=indicator,
        weekday_label=weekday,
    )


def test_event_time_is_shanghai_local_plus_utc8() -> None:
    # 9 Jan 2026 09:30 Asia/Shanghai = 01:30 UTC (no DST in CN).
    _, event = release_entry_to_records(
        _entry(), snapshot_epoch_ms=1_700_000_000,
    )
    assert event.event_time_precision == "datetime"
    assert event.event_time_utc.startswith("2026-01-09T01:30")


def test_event_record_carries_calendar_url() -> None:
    _, event = release_entry_to_records(
        _entry(),
        snapshot_epoch_ms=1_700_000_000,
        calendar_url=FIXTURE_CALENDAR_URL,
    )
    assert event.source_url == FIXTURE_CALENDAR_URL


def test_event_record_defaults_to_calendar_index_when_no_url() -> None:
    _, event = release_entry_to_records(
        _entry(), snapshot_epoch_ms=1_700_000_000,
    )
    assert event.source_url == NBS_CALENDAR_URL_BASE


def test_provider_event_id_stable_across_snapshots() -> None:
    """The id must not depend on the snapshot epoch — only on the
    release-date + canonical indicator — so re-fetches upsert instead
    of duplicating."""
    raw_a, event_a = release_entry_to_records(
        _entry(), snapshot_epoch_ms=1_700_000_000,
    )
    raw_b, event_b = release_entry_to_records(
        _entry(), snapshot_epoch_ms=1_800_000_000,
    )
    assert raw_a.provider_event_id == raw_b.provider_event_id
    assert event_a.provider_event_id == event_b.provider_event_id


def test_content_hash_changes_when_note_reference_is_added() -> None:
    """Note-only revisions — NBS keeps the release day/time but tags
    the cell with ``Note5`` etc. — must produce a different content
    hash so a rescrape registers the new audit row instead of silently
    collapsing the revision."""
    plain = _entry(weekday="Mon")
    noted = NBSReleaseEntry(
        year=plain.year, month=plain.month, day=plain.day,
        release_time_local=plain.release_time_local,
        indicator=plain.indicator, weekday_label=plain.weekday_label,
        date_cell=f"{plain.day}/{plain.weekday_label} Note5",
    )
    raw_a, _ = release_entry_to_records(plain, snapshot_epoch_ms=1_700_000_000)
    raw_b, _ = release_entry_to_records(noted, snapshot_epoch_ms=1_700_000_000)
    assert raw_a.content_hash != raw_b.content_hash
    # provider_event_id unchanged — same (indicator, release-date) key.
    assert raw_a.provider_event_id == raw_b.provider_event_id


def test_content_hash_changes_when_release_time_flips() -> None:
    payload_a = {
        "release_date":       "2026-01-09",
        "release_time_local": "9:30",
        "event_time_utc":     "2026-01-09T01:30:00+00:00",
        "indicator":          "CPI",
    }
    payload_b = {**payload_a, "release_time_local": "10:00"}
    assert _content_hash(payload_a) != _content_hash(payload_b)


def test_record_shape_is_schedule_only() -> None:
    raw, event = release_entry_to_records(
        _entry(), snapshot_epoch_ms=1_700_000_000,
    )
    assert raw.provider == PROVIDER
    assert event.provider == PROVIDER
    assert event.country_code == "CN"
    assert event.source == "National Bureau of Statistics of China"
    assert event.currency == "CNY"
    assert event.actual is None
    assert event.previous is None
    assert event.forecast is None
    assert event.reference_label == "2026-01"


def test_unknown_indicator_is_rejected() -> None:
    with pytest.raises(KeyError):
        release_entry_to_records(
            _entry(indicator="PPI"),
            snapshot_epoch_ms=1_700_000_000,
        )


# ──────────────────────────────────────────────────────────────────────────
# Projector
# ──────────────────────────────────────────────────────────────────────────


def test_store_raw_is_idempotent(store: SQLiteEngineStore) -> None:
    raw, _ = release_entry_to_records(
        _entry(), snapshot_epoch_ms=1_700_000_000,
    )
    with store._connection(commit=True) as conn:
        first = store_raw(conn, [raw])
        second = store_raw(conn, [raw])
    assert first == 1
    assert second == 0


def test_project_events_inserts_single_row(store: SQLiteEngineStore) -> None:
    _, event = release_entry_to_records(
        _entry(), snapshot_epoch_ms=1_700_000_000,
    )
    with store._connection(commit=True) as conn:
        changed = project_events(conn, [event])
    assert changed == 1
    with store._connection(commit=False) as conn:
        total = conn.execute(
            "SELECT COUNT(*) FROM cal_econ_event WHERE provider=?", (PROVIDER,),
        ).fetchone()[0]
    assert total == 1


def test_project_events_monotonicity(store: SQLiteEngineStore) -> None:
    _, newer = release_entry_to_records(
        _entry(), snapshot_epoch_ms=2_000_000_000,
        observed_at_epoch_ms=2_000_000_000,
    )
    _, older = release_entry_to_records(
        _entry(), snapshot_epoch_ms=1_000_000_000,
        observed_at_epoch_ms=1_000_000_000,
    )
    with store._connection(commit=True) as conn:
        project_events(conn, [newer])
        project_events(conn, [older])
        observed = conn.execute(
            "SELECT observed_at_epoch_ms FROM cal_econ_event WHERE provider=?",
            (PROVIDER,),
        ).fetchone()[0]
    assert observed == 2_000_000_000


def test_project_events_updates_datetime_on_schedule_revision(
    store: SQLiteEngineStore,
) -> None:
    """NBS republishes the yearly calendar with a revised release time.
    Both writes are ``datetime``-precision; the newer one must win so
    the calendar reflects the corrected time rather than silently
    strand the stale value while ``observed_at`` advances."""
    _, first = release_entry_to_records(
        _entry(release_time="9:30"),
        snapshot_epoch_ms=1_000_000_000,
        observed_at_epoch_ms=1_000_000_000,
    )
    _, revised = release_entry_to_records(
        _entry(release_time="10:00"),
        snapshot_epoch_ms=2_000_000_000,
        observed_at_epoch_ms=2_000_000_000,
    )
    with store._connection(commit=True) as conn:
        project_events(conn, [first])
        project_events(conn, [revised])
        row = conn.execute(
            "SELECT event_time_utc, event_time_precision "
            "FROM cal_econ_event WHERE provider=?",
            (PROVIDER,),
        ).fetchone()
    assert row[1] == "datetime"
    # 10:00 Asia/Shanghai == 02:00 UTC — not the stale 01:30.
    assert row[0].startswith("2026-01-09T02:00")


def test_project_events_preserves_datetime_on_merge(
    store: SQLiteEngineStore,
) -> None:
    _, scheduled = release_entry_to_records(
        _entry(), snapshot_epoch_ms=1_500_000_000,
    )
    with store._connection(commit=True) as conn:
        project_events(conn, [scheduled])

    approx = NBSCalendarEventRecord(
        provider=scheduled.provider,
        provider_event_id=scheduled.provider_event_id,
        event_time_utc="2026-01-09T00:00:00+00:00",
        event_time_precision="approximate",
        reference_date=scheduled.reference_date,
        reference_label=scheduled.reference_label,
        country_code=scheduled.country_code,
        indicator_id=None,
        category=scheduled.category,
        title=scheduled.title,
        importance=scheduled.importance,
        currency=scheduled.currency,
        unit=scheduled.unit,
        actual=None,
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source=scheduled.source,
        source_url=scheduled.source_url,
        content_hash=scheduled.content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=2_000_000_000,
    )
    with store._connection(commit=True) as conn:
        project_events(conn, [approx])
        row = conn.execute(
            "SELECT event_time_utc, event_time_precision "
            "FROM cal_econ_event WHERE provider=?",
            (PROVIDER,),
        ).fetchone()
    assert row[1] == "datetime"
    assert row[0] == scheduled.event_time_utc


# ──────────────────────────────────────────────────────────────────────────
# Fetcher
# ──────────────────────────────────────────────────────────────────────────


def test_fetch_dry_run_returns_indicator_plan(store: SQLiteEngineStore) -> None:
    with store._connection(commit=False) as conn:
        summary = fetch_nbs_calendar(
            conn, calendar_url=FIXTURE_CALENDAR_URL, dry_run=True,
        )
    assert summary.dry_run is True
    assert summary.indicators_planned == list(INDICATOR_REGISTRY.keys())
    assert summary.entries_parsed == 0
    assert summary.calendar_url == FIXTURE_CALENDAR_URL


def test_fetch_projects_fixture_into_events(store: SQLiteEngineStore) -> None:
    captured_url: list[str] = []

    def _fake_fetcher(url: str) -> str:
        captured_url.append(url)
        return _fixture_html("nbs_2026.html")

    with store._connection(commit=True) as conn:
        summary = fetch_nbs_calendar(
            conn,
            calendar_url=FIXTURE_CALENDAR_URL,
            dry_run=False,
            html_fetcher=_fake_fetcher,
            snapshot_epoch_ms=1_700_000_000,
        )
    assert captured_url == [FIXTURE_CALENDAR_URL]
    assert summary.entries_parsed == 12
    assert summary.rows_raw_inserted == 12
    assert summary.events_upserted == 12
    with store._connection(commit=False) as conn:
        rows = conn.execute(
            "SELECT COUNT(*) FROM cal_econ_event WHERE provider=?", (PROVIDER,),
        ).fetchone()[0]
    assert rows == 12


def test_fetch_twice_is_idempotent(store: SQLiteEngineStore) -> None:
    def _fake_fetcher(_url: str) -> str:
        return _fixture_html("nbs_2026.html")

    with store._connection(commit=True) as conn:
        fetch_nbs_calendar(
            conn, calendar_url=FIXTURE_CALENDAR_URL, dry_run=False,
            html_fetcher=_fake_fetcher, snapshot_epoch_ms=1_700_000_000,
        )
        second = fetch_nbs_calendar(
            conn, calendar_url=FIXTURE_CALENDAR_URL, dry_run=False,
            html_fetcher=_fake_fetcher, snapshot_epoch_ms=1_700_000_000,
        )
    assert second.rows_raw_inserted == 0


def test_fetch_raises_on_zero_parsed_entries(
    store: SQLiteEngineStore,
) -> None:
    def _empty_page(_url: str) -> str:
        return (
            "<html><title>Regular Press Release Calendar of NBS in 2026"
            "</title><body>Access Denied</body></html>"
        )

    with store._connection(commit=True) as conn:
        with pytest.raises(NBSCalendarParseError):
            fetch_nbs_calendar(
                conn, calendar_url=FIXTURE_CALENDAR_URL, dry_run=False,
                html_fetcher=_empty_page, snapshot_epoch_ms=1_700_000_000,
            )


# ──────────────────────────────────────────────────────────────────────────
# Service op wiring
# ──────────────────────────────────────────────────────────────────────────


def test_service_op_dry_run_returns_plan(store: SQLiteEngineStore) -> None:
    from macro_data.service import LocalMacroDataService

    svc = LocalMacroDataService(store=store)
    result = svc.invoke("calendar_econ_fetch_nbs", {"dry_run": True})
    assert result["dry_run"] is True
    assert result["stopped_reason"] == "dry_run"
    assert result["indicators_planned"] == ["CPI"]


def test_service_op_execute_requires_calendar_url(
    store: SQLiteEngineStore,
) -> None:
    from macro_data.service import LocalMacroDataService

    svc = LocalMacroDataService(store=store)
    result = svc.invoke("calendar_econ_fetch_nbs", {"dry_run": False})
    assert "error" in result

"""Tests for the ECB press-calendar scraper (issue #9 P3a).

Fixture HTML under ``tests/fixtures/ecb_schedule/`` covers both
surfaces:

- ``gc_meetings.html`` — Governing Council monetary-policy meeting
  dates (two years, ~8 meetings each).
- ``economic_bulletin.html`` — Economic Bulletin publication dates
  (two years, ~7 issues each).

No real HTTP.

Covers:

- Parser: dates extracted from both ``<li>`` and ``<tr>`` structures;
  duplicate dates deduped; default times applied when absent.
- Schedule entry → (raw, event) records: schedule-only ECB
  indicators (``ECB_MP_DECISION`` / ``ECB_BULLETIN``) surface under
  distinct ``provider_event_id`` — not merged with the SDMX rate
  lane, which anchors on effective dates.
- Projector: schedule rows land with ``actual=NULL`` +
  ``precision='datetime'``.
- Service op ``calendar_econ_schedule_ecb`` — dry_run + fixture
  injection via ``gc_meetings_fetcher`` / ``bulletin_fetcher`` seams.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ingestion.calendar.ecb_api import (
    ECB_BULLETIN_SPEC,
    ECB_BULLETIN_URL,
    ECB_GC_MEETINGS_URL,
    ECB_MP_DECISION_SPEC,
    ECBScheduleEntry,
    ECBScheduleParseError,
    parse_bulletin_html,
    parse_gc_meetings_html,
    schedule_ecb_calendar,
    schedule_entry_to_records,
)
from storage.sqlite import SQLiteEngineStore


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "ecb_schedule"


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _fixture(name: str) -> str:
    return (FIXTURE_DIR / name).read_text(encoding="utf-8")


# ──────────────────────────────────────────────────────────────────────────
# parse_gc_meetings_html
# ──────────────────────────────────────────────────────────────────────────


def test_parse_gc_meetings_extracts_all_dates() -> None:
    entries = parse_gc_meetings_html(_fixture("gc_meetings.html"))
    # Fixture has 8 MP meetings × 2 years = 16 entries; the two
    # non-MP rows (non-monetary-policy GC meeting, General Council
    # meeting) are excluded. All dates distinct.
    assert len(entries) == 16
    assert all(e.kind == "mp_decision" for e in entries)
    assert len({e.event_date for e in entries}) == 16


def test_parse_gc_meetings_excludes_non_mp_rows() -> None:
    """Codex P3a R1 — the mgcgc page can reference General Council /
    non-monetary-policy meetings alongside MP decisions. Without the
    exclude guard those dates would land as rate-decision events,
    which is wrong."""
    entries = parse_gc_meetings_html(_fixture("gc_meetings.html"))
    dates = {e.event_date for e in entries}
    # Fixture's non-MP rows use these dates; they must not surface.
    assert "2026-02-25" not in dates  # non-monetary policy meeting
    assert "2026-05-20" not in dates  # general council meeting


def test_parse_gc_meetings_inline_exclude_markers() -> None:
    html = (
        "<ul>"
        "<li>Thursday, 22 January 2026</li>"
        "<li>Tuesday, 10 February 2026 — General Council meeting</li>"
        "<li>Wednesday, 25 February 2026 — non-monetary policy meeting</li>"
        "<li>Thursday, 5 March 2026 — supervisory board session</li>"
        "</ul>"
    )
    entries = parse_gc_meetings_html(html)
    kept_dates = {e.event_date for e in entries}
    assert kept_dates == {"2026-01-22"}


def test_parse_gc_meetings_excludes_date_only_rows_under_non_mp_heading() -> None:
    """Codex P3a R2 — when ECB groups non-monetary-policy or General
    Council meetings under a dedicated section heading with date-only
    ``<li>``/``<tr>`` rows underneath, the exclude must follow the
    section context. Per-row markers alone miss this shape."""
    html = (
        "<h2>Monetary policy meetings</h2>"
        "<ul>"
        "<li>Thursday, 22 January 2026</li>"
        "<li>Thursday, 5 March 2026</li>"
        "</ul>"
        "<h2>Non-monetary policy meetings</h2>"
        "<ul>"
        "<li>Wednesday, 25 February 2026</li>"
        "<li>Wednesday, 29 April 2026</li>"
        "</ul>"
        "<h2>General Council meetings</h2>"
        "<ul>"
        "<li>Thursday, 12 March 2026</li>"
        "</ul>"
    )
    entries = parse_gc_meetings_html(html)
    kept_dates = {e.event_date for e in entries}
    assert kept_dates == {"2026-01-22", "2026-03-05"}


def test_parse_bulletin_keeps_rows_under_non_mp_heading() -> None:
    """Section-level exclude is scoped to ``kind='mp_decision'`` — the
    Bulletin page should still parse every dated row regardless of
    surrounding headings (Bulletin releases aren't tied to decision
    dates and don't need the non-MP filter)."""
    html = (
        "<h2>Non-monetary policy meetings</h2>"
        "<ul>"
        "<li>5 February 2026 — Economic Bulletin Issue 1/2026</li>"
        "</ul>"
    )
    entries = parse_bulletin_html(html)
    assert len(entries) == 1
    assert entries[0].event_date == "2026-02-05"


def test_parse_gc_meetings_default_time_is_14_15() -> None:
    """ECB rate decisions default to 14:15 CET / CEST."""
    entries = parse_gc_meetings_html(_fixture("gc_meetings.html"))
    for e in entries:
        assert e.release_time_local == "14:15"


def test_parse_gc_meetings_dst_aware() -> None:
    """January meeting = 14:15 CET = 13:15 UTC;
    July meeting = 14:15 CEST = 12:15 UTC."""
    entries = parse_gc_meetings_html(_fixture("gc_meetings.html"))
    by_date = {e.event_date: e for e in entries}
    winter = by_date["2026-01-22"]
    summer = by_date["2026-07-23"]
    assert "T13:15" in winter.event_time_utc
    assert "T12:15" in summer.event_time_utc


def test_parse_bulletin_extracts_all_dates() -> None:
    entries = parse_bulletin_html(_fixture("economic_bulletin.html"))
    # 7 issues × 2 years = 14 entries.
    assert len(entries) == 14
    assert all(e.kind == "bulletin" for e in entries)
    assert len({e.event_date for e in entries}) == 14


def test_parse_bulletin_default_time_is_10_00() -> None:
    entries = parse_bulletin_html(_fixture("economic_bulletin.html"))
    for e in entries:
        assert e.release_time_local == "10:00"


def test_parse_raises_when_no_dated_rows() -> None:
    with pytest.raises(ECBScheduleParseError):
        parse_gc_meetings_html(
            "<html><body><p>no dates here</p></body></html>"
        )


def test_parse_surfaces_row_issues_for_bad_time() -> None:
    """An explicit time that can't be converted shows up in row_issues
    — we keep scraping but flag the bad row."""
    html = (
        "<ul>"
        "<li>Thursday, 22 January 2026 at 30:99 CET</li>"
        "</ul>"
    )
    issues: list[str] = []
    with pytest.raises(ECBScheduleParseError):
        parse_gc_meetings_html(html, row_issues=issues)
    assert any("30:99" in msg for msg in issues)


def test_parse_dedupes_repeated_dates() -> None:
    """If BEA / ECB's page accidentally ships the same date twice —
    perhaps in a summary + an expanded view — we only emit one row."""
    html = (
        "<ul>"
        "<li>Thursday, 22 January 2026</li>"
        "<li>Thursday, 22 January 2026</li>"
        "<li>Thursday, 5 March 2026</li>"
        "</ul>"
    )
    entries = parse_gc_meetings_html(html)
    assert len(entries) == 2


# ──────────────────────────────────────────────────────────────────────────
# schedule_entry_to_records
# ──────────────────────────────────────────────────────────────────────────


def test_record_shape_for_mp_decision() -> None:
    entries = parse_gc_meetings_html(_fixture("gc_meetings.html"))
    jan = next(e for e in entries if e.event_date == "2026-01-22")
    raw, event = schedule_entry_to_records(
        jan, snapshot_epoch_ms=1_700_000_000_000,
    )
    assert event.provider == "ecb"
    assert event.event_time_precision == "datetime"
    assert event.actual is None
    assert event.title == "ECB Monetary Policy Decision"
    assert event.importance == "high"
    assert event.currency == "EUR"
    assert event.source_url == ECB_GC_MEETINGS_URL
    assert raw.provider == "ecb"


def test_record_shape_for_bulletin() -> None:
    entries = parse_bulletin_html(_fixture("economic_bulletin.html"))
    feb = next(e for e in entries if e.event_date == "2026-02-05")
    _, event = schedule_entry_to_records(
        feb, snapshot_epoch_ms=1_700_000_000_000,
    )
    assert event.title == "ECB Economic Bulletin"
    assert event.importance == "medium"
    assert event.source_url == ECB_BULLETIN_URL


def test_mp_decision_and_bulletin_ids_differ_even_on_same_date() -> None:
    """Both indicator tokens canonicalize distinctly, so a MP decision
    and a Bulletin release that happen to fall on the same date don't
    collide in cal_econ_event."""
    decision = ECBScheduleEntry(
        kind="mp_decision",
        event_date="2026-02-05",
        release_time_local="14:15",
        event_time_utc="2026-02-05T13:15:00+00:00",
        reference_label="mp",
    )
    bulletin = ECBScheduleEntry(
        kind="bulletin",
        event_date="2026-02-05",
        release_time_local="10:00",
        event_time_utc="2026-02-05T09:00:00+00:00",
        reference_label="bull",
    )
    _, d_event = schedule_entry_to_records(decision, snapshot_epoch_ms=0)
    _, b_event = schedule_entry_to_records(bulletin, snapshot_epoch_ms=0)
    assert d_event.provider_event_id != b_event.provider_event_id


# ──────────────────────────────────────────────────────────────────────────
# schedule_ecb_calendar
# ──────────────────────────────────────────────────────────────────────────


def test_schedule_run_lands_both_surfaces(
    store: SQLiteEngineStore,
) -> None:
    gc = _fixture("gc_meetings.html")
    bull = _fixture("economic_bulletin.html")

    def fake_gc(*, session=None):
        return gc

    def fake_bull(*, session=None):
        return bull

    with store._connection(commit=True) as conn:
        summary = schedule_ecb_calendar(
            conn, dry_run=False,
            gc_meetings_fetcher=fake_gc,
            bulletin_fetcher=fake_bull,
        )
    assert summary.fetch_errors == {}
    assert summary.mp_decision_entries == 16
    assert summary.bulletin_entries == 14
    assert summary.entries_parsed == 30
    assert summary.events_upserted == 30

    with store._connection(commit=False) as conn:
        counts = dict(conn.execute(
            "SELECT title, COUNT(*) FROM cal_econ_event "
            "WHERE provider='ecb' GROUP BY title"
        ).fetchall())
    assert counts == {
        "ECB Monetary Policy Decision": 16,
        "ECB Economic Bulletin":        14,
    }


def test_schedule_run_one_page_failure_does_not_stop_other(
    store: SQLiteEngineStore,
) -> None:
    """Upstream failure on one ECB page doesn't abort the other —
    ``fetch_errors`` surfaces the failed page, the successful page
    still lands rows."""
    gc = _fixture("gc_meetings.html")

    def fake_gc(*, session=None):
        return gc

    def exploding_bull(*, session=None):
        raise RuntimeError("upstream timeout")

    with store._connection(commit=True) as conn:
        summary = schedule_ecb_calendar(
            conn, dry_run=False,
            gc_meetings_fetcher=fake_gc,
            bulletin_fetcher=exploding_bull,
        )
    assert summary.mp_decision_entries == 16
    assert summary.bulletin_entries == 0
    assert summary.fetch_errors == {"bulletin": "upstream timeout"}
    assert summary.events_upserted == 16


def test_schedule_dry_run_does_no_work(store: SQLiteEngineStore) -> None:
    calls: list[None] = []

    def never(*, session=None):
        calls.append(None)
        return ""

    with store._connection(commit=False) as conn:
        summary = schedule_ecb_calendar(
            conn, dry_run=True,
            gc_meetings_fetcher=never, bulletin_fetcher=never,
        )
    assert summary.dry_run is True
    assert calls == []


# ──────────────────────────────────────────────────────────────────────────
# Service op wiring
# ──────────────────────────────────────────────────────────────────────────


def test_service_op_dry_run(store: SQLiteEngineStore) -> None:
    from macro_data.service import LocalMacroDataService

    svc = LocalMacroDataService(store=store)
    result = svc.invoke("calendar_econ_schedule_ecb", {"dry_run": True})
    assert result["dry_run"] is True
    assert result["stopped_reason"] == "dry_run"


def test_schedule_indicator_specs_are_distinct() -> None:
    """Sanity: the two schedule-only ECB specs don't share their
    canonical token — otherwise they'd collide in provider_event_id
    synthesis for same-date events."""
    assert ECB_MP_DECISION_SPEC.indicator != ECB_BULLETIN_SPEC.indicator
    assert ECB_MP_DECISION_SPEC.title != ECB_BULLETIN_SPEC.title


def test_mp_decision_indicator_has_registered_canonical_alias() -> None:
    """Codex P3a R1 P2 — the MP-decision token must round-trip through
    ``canonicalize_indicator`` to a stable uppercase form. Without the
    alias, the token falls through to the normalized string and a
    future alias addition would change the provider_event_id hash,
    duplicating every previously-written decision event."""
    from ingestion.calendar._official_shared import canonicalize_indicator

    token = canonicalize_indicator(ECB_MP_DECISION_SPEC.indicator)
    assert token == "ECB_MP_DECISION"
    bulletin = canonicalize_indicator(ECB_BULLETIN_SPEC.indicator)
    assert bulletin == "ECB_BULLETIN"

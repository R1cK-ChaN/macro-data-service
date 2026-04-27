"""Mocked tests for the Fed speeches calendar connector (issue #56 P1).

The captured fixtures live in ``tests/fixtures/fed_speeches/`` —
``2026.htm`` and ``2025.htm`` were recorded live on 2026-04-27 from
``federalreserve.gov/newsevents/speech/<YYYY>-speeches.htm``. Both
carry the ``eventlist__time`` / ``eventlist__event`` row layout the
parser anchors on; 2026 is mid-year (~30 entries through April),
2025 carries the full calendar year (~350 entries).

No real HTTP in CI — every test injects the ``html_fetcher`` seam.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ingestion.calendar.fed_speeches_api import (
    FedSpeechesArchiveParseError,
    fetch_fed_speeches_calendar,
    parse_speeches_archive,
    speech_to_records,
)
from ingestion.calendar.fed_speeches_api.parser import PROVIDER
from storage.sqlite import SQLiteEngineStore


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "fed_speeches"


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _archive_html(year: int) -> str:
    return (FIXTURE_DIR / f"{year}.htm").read_text(encoding="utf-8")


# ── parser ───────────────────────────────────────────────────────


def test_parse_speeches_archive_extracts_2026_entries() -> None:
    """The 2026 fixture (captured 2026-04-27) lists ~30 speeches by
    Board members through April 21. Verify a known entry parses
    cleanly and the row count is in the expected ballpark."""
    speeches = parse_speeches_archive(_archive_html(2026))
    assert len(speeches) >= 25
    # Christopher Waller's 2026-04-17 Auburn lecture appears in the
    # fixture with a fully-populated speaker + venue paragraph.
    waller = next(
        s for s in speeches
        if s.delivery_date.isoformat() == "2026-04-17"
        and s.slug.startswith("waller")
    )
    assert waller.title == "One Transitory Shock After Another"
    assert waller.speaker == "Governor Christopher J. Waller"
    assert "Auburn University" in (waller.venue or "")
    assert waller.url.endswith("/newsevents/speech/waller20260417a.htm")


def test_parse_speeches_archive_handles_future_event_without_speaker() -> None:
    """Future-event rows on the archive page (only ``Watch Live``,
    transcript not yet posted) omit the ``news__speaker`` paragraph.
    The parser must still emit the row with ``speaker=None`` rather
    than dropping it."""
    html = (
        '<div class="row">'
        '<div class="col-xs-3 col-md-2 eventlist__time">'
        '<time>5/1/2026</time></div>'
        '<div class="col-xs-9 col-md-10 eventlist__event">'
        '<p><a href="/newsevents/speech/powell20260501a.htm">'
        '<em>Future Speech Title</em></a></p>'
        "<p><a class='watchLive' href='https://example.com'>Watch Live</a></p>"
        "</div></div>"
    )
    speeches = parse_speeches_archive(html)
    assert len(speeches) == 1
    assert speeches[0].speaker is None
    assert speeches[0].venue is None
    assert speeches[0].title == "Future Speech Title"


def test_parse_speeches_archive_orders_by_delivery_date_ascending() -> None:
    """Output must be sorted by delivery date ascending so a
    downstream caller can paginate or eyeball the upcoming entries
    without re-sorting."""
    speeches = parse_speeches_archive(_archive_html(2025))
    iso_list = [s.delivery_date.isoformat() for s in speeches]
    assert iso_list == sorted(iso_list)


def test_parse_speeches_archive_handles_unicode_in_title() -> None:
    """The Fed page renders curly quotes (``“…”``) and em-dashes in
    titles. After HTML unescape the parser must preserve them as-is
    so the stored ``cal_econ_event.title`` matches the published
    text."""
    speeches = parse_speeches_archive(_archive_html(2026))
    cook = next(
        s for s in speeches
        if s.delivery_date.isoformat() == "2026-02-24"
        and s.slug.startswith("cook")
    )
    assert "AI and Productivity" in cook.title


def test_parse_speeches_archive_raises_on_empty_listing() -> None:
    """A page that drops the ``eventlist__time`` row layout entirely
    (maintenance window, layout migration) signals upstream drift."""
    with pytest.raises(FedSpeechesArchiveParseError, match="zero entries"):
        parse_speeches_archive(
            "<html><body><h1>maintenance window</h1></body></html>",
        )


# ── projection ───────────────────────────────────────────────────


def test_speech_to_records_anchors_on_delivery_date_with_date_precision() -> None:
    """Each row projects with ``event_time_precision='date'`` and a
    midnight-UTC sortable anchor on the delivery date."""
    speeches = parse_speeches_archive(_archive_html(2026))
    waller = next(
        s for s in speeches
        if s.delivery_date.isoformat() == "2026-04-17"
        and s.slug.startswith("waller")
    )
    raw_rec, event_rec = speech_to_records(
        waller, snapshot_epoch_ms=1_800_000_000_000,
    )
    assert event_rec.country_code == "US"
    assert event_rec.currency == "USD"
    assert event_rec.actual is None  # schedule-only slice
    assert event_rec.event_time_precision == "date"
    assert event_rec.event_time_utc.startswith("2026-04-17T00:00:00")
    assert event_rec.reference_date == "2026-04-17"
    assert event_rec.reference_label == "April 2026"
    assert event_rec.title.startswith("Fed Speech — Governor Christopher J. Waller:")
    assert "One Transitory Shock" in event_rec.title
    assert event_rec.source == "US Federal Reserve"
    assert event_rec.source_url == waller.url
    assert raw_rec.provider == PROVIDER
    # Event id stable across re-projection.
    _, event_rec_again = speech_to_records(
        waller, snapshot_epoch_ms=2_000_000_000_000,
    )
    assert event_rec.provider_event_id == event_rec_again.provider_event_id


def test_speech_to_records_distinct_provider_ids_per_speech() -> None:
    """The slug is unique per speech; same-day speeches by different
    speakers must not collide."""
    speeches = parse_speeches_archive(_archive_html(2025))
    ids = {
        speech_to_records(s, snapshot_epoch_ms=1_800_000_000_000)[1].provider_event_id
        for s in speeches
    }
    assert len(ids) == len(speeches)


def test_speech_to_records_falls_back_to_bare_title_when_speaker_missing() -> None:
    """Future-event rows without a speaker line still project — the
    title format collapses to ``"Fed Speech: <title>"``."""
    html = (
        '<div class="row">'
        '<div class="col-xs-3 col-md-2 eventlist__time">'
        '<time>5/1/2026</time></div>'
        '<div class="col-xs-9 col-md-10 eventlist__event">'
        '<p><a href="/newsevents/speech/powell20260501a.htm">'
        '<em>Future Speech</em></a></p></div></div>'
    )
    [speech] = parse_speeches_archive(html)
    _, event_rec = speech_to_records(
        speech, snapshot_epoch_ms=1_800_000_000_000,
    )
    assert event_rec.title == "Fed Speech: Future Speech"


# ── full fetch driver ───────────────────────────────────────────


def test_fetch_fed_speeches_calendar_writes_one_event_per_speech(
    store: SQLiteEngineStore,
) -> None:
    """Combined 2025 + 2026 fixture sweep should land ~150 rows.
    The Board archive only carries Governors / Vice Chairs / Chair
    speeches (regional Reserve Bank presidents live on a separate
    surface); 2025 carried ~120 Board speeches, 2026 ~30 through
    April. Use ≥ to absorb any later historical addition."""
    def fetcher(year: int) -> str:
        return _archive_html(year)

    with store._connection(commit=True) as conn:
        summary = fetch_fed_speeches_calendar(
            conn,
            dry_run=False,
            html_fetcher=fetcher,
            years=[2025, 2026],
            snapshot_epoch_ms=1_800_000_000_000,
        )
    assert summary.fetch_error is None
    assert summary.speeches_parsed >= 100
    assert summary.events_upserted == summary.speeches_parsed
    assert summary.per_year_errors == {}


def test_fetch_fed_speeches_calendar_continues_when_one_year_fails(
    store: SQLiteEngineStore,
) -> None:
    """A single-year fetch failure (e.g. 2027 404 in early 2026)
    must not abort the sweep — the surviving year still lands."""
    def fetcher(year: int) -> str:
        if year == 2027:
            raise RuntimeError("simulated 404")
        return _archive_html(year)

    with store._connection(commit=True) as conn:
        summary = fetch_fed_speeches_calendar(
            conn,
            dry_run=False,
            html_fetcher=fetcher,
            years=[2026, 2027],
            snapshot_epoch_ms=1_800_000_000_000,
        )
    assert summary.fetch_error is None
    assert 2027 in summary.per_year_errors
    assert summary.speeches_parsed >= 25


def test_fetch_fed_speeches_calendar_records_fetch_error_when_all_years_fail(
    store: SQLiteEngineStore,
) -> None:
    def broken(year: int) -> str:
        raise RuntimeError(f"simulated 503 for {year}")

    with store._connection(commit=True) as conn:
        summary = fetch_fed_speeches_calendar(
            conn,
            dry_run=False,
            html_fetcher=broken,
            years=[2025, 2026],
        )
    assert summary.fetch_error is not None
    assert summary.events_upserted == 0
    assert set(summary.per_year_errors.keys()) == {2025, 2026}


def test_fetch_fed_speeches_calendar_dry_run_returns_plan(
    store: SQLiteEngineStore,
) -> None:
    with store._connection(commit=False) as conn:
        summary = fetch_fed_speeches_calendar(
            conn, dry_run=True, years=[2025, 2026],
        )
    assert summary.dry_run is True
    assert summary.indicators_planned == ["FED_SPEECHES"]
    assert summary.years_planned == [2025, 2026]


def test_fetch_fed_speeches_calendar_idempotent_on_repeat(
    store: SQLiteEngineStore,
) -> None:
    """The slug-anchored provider_event_id is stable per speech, so
    a second sweep over the same archive writes zero new rows."""
    def fetcher(year: int) -> str:
        return _archive_html(year)
    with store._connection(commit=True) as conn:
        first = fetch_fed_speeches_calendar(
            conn,
            dry_run=False,
            html_fetcher=fetcher,
            years=[2026],
            snapshot_epoch_ms=1_800_000_000_000,
        )
        fetch_fed_speeches_calendar(
            conn,
            dry_run=False,
            html_fetcher=fetcher,
            years=[2026],
            snapshot_epoch_ms=1_800_000_000_001,
        )
    with store._connection(commit=False) as conn:
        rows = conn.execute(
            "SELECT count(*) FROM cal_econ_event WHERE provider=?",
            (PROVIDER,),
        ).fetchone()
    assert rows[0] == first.events_upserted


# ── scheduler + agency wiring ───────────────────────────────────


def test_fed_speeches_listed_in_default_rosters() -> None:
    from ingestion.calendar.scheduler import (
        ALL_CONNECTORS,
        ALL_VALUE_SIDE_CONNECTORS,
    )
    assert "fed-speeches" in ALL_CONNECTORS
    assert "fed-speeches" in ALL_VALUE_SIDE_CONNECTORS


def test_fed_speeches_agency_attribution_provider_only_in_p1() -> None:
    """The Fed speeches connector ships schedule-only events; wiring
    a parity-comparable indicator into the registry would trip the
    parse_failed-on-missing-actual path on every speech."""
    from ingestion.calendar.agency_registry import (
        agency_for,
        provider_to_agency,
    )
    agency = provider_to_agency("fed-speeches")
    assert agency is not None and agency.agency_id == "FED_SPEECHES"
    assert agency.indicators == frozenset()
    assert agency_for("US", "FED_SPEECHES") is None

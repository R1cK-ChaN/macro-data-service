"""Mocked tests for the BoE speeches calendar connector (issue #56 P1).

The captured fixture
``tests/fixtures/boe_speeches/sitemap.html`` was recorded live on
2026-04-27 from ``bankofengland.co.uk/sitemap/speeches`` (Akamai
serves it with a Safari-shaped UA — the fetcher mirrors that UA).
~1,500 speech links across 1997-present, with ~380 in the
current ``/speech/<YYYY>/<month>/<slug>`` shape that the parser
projects.

No real HTTP in CI — every test injects the ``html_fetcher`` seam.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ingestion.calendar.boe_speeches_api import (
    BoeSpeechesSitemapParseError,
    fetch_boe_speeches_calendar,
    parse_speeches_sitemap,
    speech_to_records,
)
from ingestion.calendar.boe_speeches_api.parser import PROVIDER
from storage.sqlite import SQLiteEngineStore


FIXTURE_PATH = (
    Path(__file__).parent / "fixtures" / "boe_speeches" / "sitemap.html"
)


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _sitemap_html() -> str:
    return FIXTURE_PATH.read_text(encoding="utf-8")


# ── parser ───────────────────────────────────────────────────────


def test_parse_speeches_sitemap_extracts_current_format_links() -> None:
    """The parser should pick up only the ``/speech/<YYYY>/<month>/...``
    shape (introduced 2021); legacy 2-segment URLs are out of scope
    in P1 because they lack month precision."""
    speeches = parse_speeches_sitemap(_sitemap_html())
    assert len(speeches) >= 300
    assert all(s.year >= 2021 for s in speeches)
    # Andrew Bailey 2026-02 panel speech is in the fixture as a
    # ``/speech/2026/february/andrew-bailey-...`` row.
    bailey = next(
        s for s in speeches
        if s.year == 2026
        and s.month == 2
        and s.slug.startswith("andrew-bailey")
    )
    assert bailey.url.startswith(
        "https://www.bankofengland.co.uk/speech/2026/february/andrew-bailey",
    )
    assert "Andrew Bailey" in (bailey.speaker or "") or "Bailey" in bailey.title


def test_parse_speeches_sitemap_extracts_speaker_dash_pattern() -> None:
    """Title shaped ``X − speech by Speaker`` (en-dash) splits into
    ``title=X``, ``speaker=Speaker``."""
    html = (
        '<a href="https://www.bankofengland.co.uk/speech/2026/april/'
        'alan-taylor-at-a-joint-conference-at-the-banque-de-france" '
        'class="list-links__link">'
        "Two-way street − speech by Alan Taylor"
        "</a>"
    )
    [speech] = parse_speeches_sitemap(html)
    assert speech.title == "Two-way street"
    assert speech.speaker == "Alan Taylor"


def test_parse_speeches_sitemap_extracts_speaker_colon_pattern() -> None:
    """Title shaped ``Speaker: <description>`` splits into
    ``title=description``, ``speaker=Speaker``."""
    html = (
        '<a href="https://www.bankofengland.co.uk/speech/2026/april/'
        'charlotte-gerken-speech-and-panel-at-the-building-societies" '
        'class="list-links__link">'
        "Charlotte Gerken: Speech and panel at the Building Societies"
        "</a>"
    )
    [speech] = parse_speeches_sitemap(html)
    assert speech.speaker == "Charlotte Gerken"
    assert "Speech and panel" in speech.title


def test_parse_speeches_sitemap_keeps_unparseable_titles() -> None:
    """Titles that don't match any speaker pattern still project, with
    ``speaker=None`` and the full text retained in the title."""
    html = (
        '<a href="https://www.bankofengland.co.uk/speech/2025/march/'
        'mansion-house-mansion-house-mansion-house" '
        'class="list-links__link">'
        "Mansion House Lecture"
        "</a>"
    )
    [speech] = parse_speeches_sitemap(html)
    assert speech.speaker is None
    assert speech.title == "Mansion House Lecture"


def test_parse_speeches_sitemap_orders_by_delivery_date_ascending() -> None:
    speeches = parse_speeches_sitemap(_sitemap_html())
    iso_list = [s.delivery_date.isoformat() for s in speeches]
    assert iso_list == sorted(iso_list)


def test_parse_speeches_sitemap_skips_duplicate_keys() -> None:
    """The sitemap renders some entries twice across navigation
    contexts; the parser collapses on (year, month, slug)."""
    html = (
        '<a href="https://www.bankofengland.co.uk/speech/2026/april/'
        'duplicate-slug" class="list-links__link">First</a>'
        '<a href="https://www.bankofengland.co.uk/speech/2026/april/'
        'duplicate-slug" class="list-links__link">Second</a>'
    )
    speeches = parse_speeches_sitemap(html)
    assert len(speeches) == 1


def test_parse_speeches_sitemap_raises_on_empty_listing() -> None:
    with pytest.raises(BoeSpeechesSitemapParseError, match="zero"):
        parse_speeches_sitemap(
            "<html><body><h1>maintenance</h1></body></html>",
        )


# ── projection ───────────────────────────────────────────────────


def test_speech_to_records_anchors_on_first_of_month() -> None:
    speeches = parse_speeches_sitemap(_sitemap_html())
    bailey = next(
        s for s in speeches
        if s.year == 2026 and s.month == 2 and s.slug.startswith("andrew-bailey")
    )
    raw_rec, event_rec = speech_to_records(
        bailey, snapshot_epoch_ms=1_800_000_000_000,
    )
    assert event_rec.country_code == "UK"
    assert event_rec.currency == "GBP"
    assert event_rec.actual is None
    assert event_rec.event_time_precision == "date"
    assert event_rec.event_time_utc.startswith("2026-02-01T00:00:00")
    assert event_rec.reference_date == "2026-02-01"
    assert event_rec.title.startswith("BoE Speech")
    assert event_rec.source == "Bank of England"
    assert event_rec.source_url == bailey.url
    assert raw_rec.provider == PROVIDER
    _, event_rec_again = speech_to_records(
        bailey, snapshot_epoch_ms=2_000_000_000_000,
    )
    assert event_rec.provider_event_id == event_rec_again.provider_event_id


def test_speech_to_records_distinct_provider_ids_per_speech() -> None:
    speeches = parse_speeches_sitemap(_sitemap_html())
    ids = {
        speech_to_records(s, snapshot_epoch_ms=1_800_000_000_000)[1].provider_event_id
        for s in speeches
    }
    assert len(ids) == len(speeches)


# ── full fetch driver ───────────────────────────────────────────


def test_fetch_boe_speeches_calendar_writes_one_event_per_speech(
    store: SQLiteEngineStore,
) -> None:
    """The 2026-04 fixture lists ~380 current-format speeches."""
    def fetcher() -> str:
        return _sitemap_html()

    with store._connection(commit=True) as conn:
        summary = fetch_boe_speeches_calendar(
            conn,
            dry_run=False,
            html_fetcher=fetcher,
            snapshot_epoch_ms=1_800_000_000_000,
        )
    assert summary.fetch_error is None
    assert summary.speeches_parsed >= 300
    assert summary.events_upserted == summary.speeches_parsed


def test_fetch_boe_speeches_calendar_records_fetch_error_on_outage(
    store: SQLiteEngineStore,
) -> None:
    def broken() -> str:
        raise RuntimeError("simulated 503 from Akamai")

    with store._connection(commit=True) as conn:
        summary = fetch_boe_speeches_calendar(
            conn, dry_run=False, html_fetcher=broken,
        )
    assert summary.fetch_error is not None
    assert summary.events_upserted == 0


def test_fetch_boe_speeches_calendar_records_parse_error_on_drift(
    store: SQLiteEngineStore,
) -> None:
    def drift() -> str:
        return "<html><body><h1>maintenance window</h1></body></html>"

    with store._connection(commit=True) as conn:
        summary = fetch_boe_speeches_calendar(
            conn, dry_run=False, html_fetcher=drift,
        )
    assert summary.fetch_error is not None
    assert summary.events_upserted == 0


def test_fetch_boe_speeches_calendar_dry_run_returns_plan(
    store: SQLiteEngineStore,
) -> None:
    with store._connection(commit=False) as conn:
        summary = fetch_boe_speeches_calendar(conn, dry_run=True)
    assert summary.dry_run is True
    assert summary.indicators_planned == ["BOE_SPEECHES"]


def test_fetch_boe_speeches_calendar_idempotent_on_repeat(
    store: SQLiteEngineStore,
) -> None:
    def fetcher() -> str:
        return _sitemap_html()
    with store._connection(commit=True) as conn:
        first = fetch_boe_speeches_calendar(
            conn, dry_run=False, html_fetcher=fetcher,
            snapshot_epoch_ms=1_800_000_000_000,
        )
        fetch_boe_speeches_calendar(
            conn, dry_run=False, html_fetcher=fetcher,
            snapshot_epoch_ms=1_800_000_000_001,
        )
    with store._connection(commit=False) as conn:
        rows = conn.execute(
            "SELECT count(*) FROM cal_econ_event WHERE provider=?",
            (PROVIDER,),
        ).fetchone()
    assert rows[0] == first.events_upserted


# ── scheduler + agency wiring ───────────────────────────────────


def test_boe_speeches_listed_in_default_rosters() -> None:
    from ingestion.calendar.scheduler import (
        ALL_CONNECTORS,
        ALL_VALUE_SIDE_CONNECTORS,
    )
    assert "boe-speeches" in ALL_CONNECTORS
    assert "boe-speeches" in ALL_VALUE_SIDE_CONNECTORS


def test_boe_speeches_agency_attribution_provider_only_in_p1() -> None:
    from ingestion.calendar.agency_registry import (
        agency_for,
        provider_to_agency,
    )
    agency = provider_to_agency("boe-speeches")
    assert agency is not None and agency.agency_id == "BOE_SPEECHES"
    assert agency.indicators == frozenset()
    assert agency_for("UK", "BOE_SPEECHES") is None

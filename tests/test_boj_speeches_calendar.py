"""Mocked tests for the BoJ speeches calendar connector (issue #56 P1).

The captured fixtures live in ``tests/fixtures/boj_speeches/`` —
``2026.htm`` and ``2025.htm`` were recorded live on 2026-04-27 from
``boj.or.jp/en/about/press/koen_<YYYY>/index.htm``. The pages list
every speech across all ranks; the parser keeps only rate-setting
roles (Governor / Deputy Governor / Member of the Policy Board).

No real HTTP in CI — every test injects the ``html_fetcher`` seam.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ingestion.calendar.boj_speeches_api import (
    BojSpeechesArchiveParseError,
    fetch_boj_speeches_calendar,
    parse_speeches_archive,
    speech_to_records,
)
from ingestion.calendar.boj_speeches_api.parser import PROVIDER
from storage.sqlite import SQLiteEngineStore


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "boj_speeches"


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _archive_html(year: int) -> str:
    return (FIXTURE_DIR / f"{year}.htm").read_text(encoding="utf-8")


# ── parser ───────────────────────────────────────────────────────


def test_parse_speeches_archive_extracts_governor_speech() -> None:
    """The 2026-03-03 UEDA speech is in the fixture as a Governor row."""
    speeches = parse_speeches_archive(_archive_html(2026))
    ueda = next(
        s for s in speeches
        if s.delivery_date.isoformat() == "2026-03-03"
        and "UEDA" in s.speaker.upper()
    )
    assert ueda.role == "Governor"
    assert "Financial Ecosystem" in ueda.title
    assert ueda.url.endswith("/en/about/press/koen_2026/ko260303a.htm")


def test_parse_speeches_archive_filters_out_executive_director() -> None:
    """The 2026 fixture's KAMIYAMA Kazushige row is an Executive
    Director — not a rate-setting role — and must be filtered."""
    speeches = parse_speeches_archive(_archive_html(2026))
    assert all(
        s.role.lower() in {
            "governor", "deputy governor",
            "member of the policy board", "member of policy board",
        }
        for s in speeches
    )
    assert all("KAMIYAMA" not in s.speaker.upper() for s in speeches)


def test_parse_speeches_archive_handles_september_token() -> None:
    """BoJ uses ``Sept.`` (4-letter) for September; the parser must
    map it to month=9 same as the standard ``Sep.``."""
    html = (
        '<tr><td>Sept.&nbsp;29,&nbsp;2025</td>'
        '<td>UEDA Kazuo, Governor</td>'
        '<td><a href="/en/about/press/koen_2025/ko250929a.htm">'
        '"Test"</a></td></tr>'
    )
    [speech] = parse_speeches_archive(html)
    assert speech.delivery_date.month == 9
    assert speech.delivery_date.day == 29


def test_parse_speeches_archive_orders_by_date_ascending() -> None:
    speeches = parse_speeches_archive(_archive_html(2025))
    iso_list = [s.delivery_date.isoformat() for s in speeches]
    assert iso_list == sorted(iso_list)


def test_parse_speeches_archive_strips_quoted_title() -> None:
    """BoJ wraps speech titles in double quotes followed by a
    parenthesised event note. The parser must keep only the title
    text, not the wrapping quotes or trailing event description."""
    html = (
        '<tr><td>Mar.&nbsp;&nbsp;3,&nbsp;2026</td>'
        '<td>UEDA Kazuo, Governor</td>'
        '<td><a href="/en/about/press/koen_2026/ko260303a.htm">'
        '"The New Financial Ecosystem and the Role of Central Banks" '
        '(Remarks at the FIN/SUM 2026)&nbsp;</a></td></tr>'
    )
    [speech] = parse_speeches_archive(html)
    assert speech.title == (
        "The New Financial Ecosystem and the Role of Central Banks"
    )


def test_parse_speeches_archive_raises_on_empty_listing() -> None:
    with pytest.raises(BojSpeechesArchiveParseError, match="zero rows"):
        parse_speeches_archive(
            "<html><body><h1>maintenance</h1></body></html>",
        )


# ── projection ───────────────────────────────────────────────────


def test_speech_to_records_anchors_on_delivery_date_with_date_precision() -> None:
    speeches = parse_speeches_archive(_archive_html(2026))
    ueda = next(
        s for s in speeches
        if s.delivery_date.isoformat() == "2026-03-03"
        and "UEDA" in s.speaker.upper()
    )
    raw_rec, event_rec = speech_to_records(
        ueda, snapshot_epoch_ms=1_800_000_000_000,
    )
    assert event_rec.country_code == "JP"
    assert event_rec.currency == "JPY"
    assert event_rec.actual is None
    assert event_rec.event_time_precision == "date"
    assert event_rec.event_time_utc.startswith("2026-03-03T00:00:00")
    assert event_rec.reference_date == "2026-03-03"
    assert event_rec.title.startswith("BoJ Speech — Governor UEDA Kazuo:")
    assert event_rec.source == "Bank of Japan"
    assert event_rec.source_url == ueda.url
    assert raw_rec.provider == PROVIDER
    _, event_rec_again = speech_to_records(
        ueda, snapshot_epoch_ms=2_000_000_000_000,
    )
    assert event_rec.provider_event_id == event_rec_again.provider_event_id


def test_speech_to_records_distinct_provider_ids_per_speech() -> None:
    speeches = parse_speeches_archive(_archive_html(2025))
    ids = {
        speech_to_records(s, snapshot_epoch_ms=1_800_000_000_000)[1].provider_event_id
        for s in speeches
    }
    assert len(ids) == len(speeches)


# ── full fetch driver ───────────────────────────────────────────


def test_fetch_boj_speeches_calendar_writes_one_event_per_speech(
    store: SQLiteEngineStore,
) -> None:
    """Combined 2025 + 2026 fixture sweep should land tens of rows
    (Policy Board members deliver ~20-40 public speeches per year
    combined; remaining rows are filtered Executive Director / staff
    entries)."""
    def fetcher(year: int) -> str:
        return _archive_html(year)

    with store._connection(commit=True) as conn:
        summary = fetch_boj_speeches_calendar(
            conn,
            dry_run=False,
            html_fetcher=fetcher,
            years=[2025, 2026],
            snapshot_epoch_ms=1_800_000_000_000,
        )
    assert summary.fetch_error is None
    assert summary.speeches_parsed >= 20
    assert summary.events_upserted == summary.speeches_parsed
    assert summary.per_year_errors == {}


def test_fetch_boj_speeches_calendar_continues_when_one_year_fails(
    store: SQLiteEngineStore,
) -> None:
    def fetcher(year: int) -> str:
        if year == 2027:
            raise RuntimeError("simulated 404")
        return _archive_html(year)

    with store._connection(commit=True) as conn:
        summary = fetch_boj_speeches_calendar(
            conn,
            dry_run=False,
            html_fetcher=fetcher,
            years=[2026, 2027],
            snapshot_epoch_ms=1_800_000_000_000,
        )
    assert summary.fetch_error is None
    assert 2027 in summary.per_year_errors
    assert summary.speeches_parsed >= 1


def test_fetch_boj_speeches_calendar_records_fetch_error_when_all_years_fail(
    store: SQLiteEngineStore,
) -> None:
    def broken(year: int) -> str:
        raise RuntimeError(f"simulated 503 for {year}")

    with store._connection(commit=True) as conn:
        summary = fetch_boj_speeches_calendar(
            conn,
            dry_run=False,
            html_fetcher=broken,
            years=[2025, 2026],
        )
    assert summary.fetch_error is not None
    assert summary.events_upserted == 0


def test_fetch_boj_speeches_calendar_dry_run_returns_plan(
    store: SQLiteEngineStore,
) -> None:
    with store._connection(commit=False) as conn:
        summary = fetch_boj_speeches_calendar(
            conn, dry_run=True, years=[2025, 2026],
        )
    assert summary.dry_run is True
    assert summary.indicators_planned == ["BOJ_SPEECHES"]
    assert summary.years_planned == [2025, 2026]


def test_fetch_boj_speeches_calendar_idempotent_on_repeat(
    store: SQLiteEngineStore,
) -> None:
    def fetcher(year: int) -> str:
        return _archive_html(year)
    with store._connection(commit=True) as conn:
        first = fetch_boj_speeches_calendar(
            conn,
            dry_run=False,
            html_fetcher=fetcher,
            years=[2026],
            snapshot_epoch_ms=1_800_000_000_000,
        )
        fetch_boj_speeches_calendar(
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


def test_boj_speeches_listed_in_default_rosters() -> None:
    from ingestion.calendar.scheduler import (
        ALL_CONNECTORS,
        ALL_VALUE_SIDE_CONNECTORS,
    )
    assert "boj-speeches" in ALL_CONNECTORS
    assert "boj-speeches" in ALL_VALUE_SIDE_CONNECTORS


def test_boj_speeches_agency_attribution_provider_only_in_p1() -> None:
    from ingestion.calendar.agency_registry import (
        agency_for,
        provider_to_agency,
    )
    agency = provider_to_agency("boj-speeches")
    assert agency is not None and agency.agency_id == "BOJ_SPEECHES"
    assert agency.indicators == frozenset()
    assert agency_for("JP", "BOJ_SPEECHES") is None

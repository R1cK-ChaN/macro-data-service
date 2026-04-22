"""Tests for the Fed release-dates calendar feed (issue #9 P4a +
P4b-live-follow-up).

Fixture JSON at ``tests/fixtures/fed_releasedates/calendar.json``
covers the three whitelisted indicators (Beige Book, H.4.1, H.8)
plus distractors the parser must ignore: SEP (handled via the FOMC
calendar scraper), G.19 Consumer Credit, H.15 Selected Interest
Rates, an FOMC meeting event, and a Chair speech.

No real HTTP.

Covers:

- Parser: whitelisted events extracted; SEP dropped; off-whitelist
  dropped; unparseable month/day captured in ``row_issues``.
- Entry → (raw, event) records: each release gets a distinct
  ``provider_event_id`` (indicator, release_date).
- Projector: rows land with ``actual=NULL`` +
  ``precision='datetime'`` via the full-writer path.
- Service op ``calendar_econ_fetch_fed_releases`` — dry_run +
  fixture injection via ``html_fetcher`` seam.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ingestion.calendar.fed_api import (
    FED_CALENDAR_JSON_URL,
    FedCalendarJsonParseError,
    FedReleaseEntry,
    INDICATOR_REGISTRY,
    fetch_fed_releasedates,
    parse_fed_calendar_json,
    release_entry_to_records,
)
from storage.sqlite import SQLiteEngineStore


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "fed_releasedates"


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _fixture() -> str:
    return (FIXTURE_DIR / "calendar.json").read_text(encoding="utf-8")


# ──────────────────────────────────────────────────────────────────────────
# Registry additions
# ──────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "series_id,expected_unit,expected_importance",
    [
        ("BEIGE_BOOK", "event", "high"),
        ("FED_H41",    "event", "medium"),
        ("FED_H8",     "event", "low"),
    ],
)
def test_new_registry_entries(
    series_id: str,
    expected_unit: str,
    expected_importance: str,
) -> None:
    spec = INDICATOR_REGISTRY[series_id]
    assert spec.country_code == "US"
    assert spec.unit == expected_unit
    assert spec.importance == expected_importance
    assert spec.category == "Monetary Policy"


# ──────────────────────────────────────────────────────────────────────────
# parse_fed_calendar_json
# ──────────────────────────────────────────────────────────────────────────


def test_parse_extracts_whitelisted_events_only() -> None:
    entries = parse_fed_calendar_json(_fixture())
    counts: dict[str, int] = {}
    for e in entries:
        counts[e.series_id] = counts.get(e.series_id, 0) + 1
    # Fixture: 2 Beige Book, 5 H.4.1 (3+2 across two months),
    # 5 H.8 (3+2) = 12 entries total.
    assert counts == {
        "BEIGE_BOOK": 2,
        "FED_H41":    5,
        "FED_H8":     5,
    }


def test_parse_drops_sep_event() -> None:
    """SEP events are explicitly excluded — they're surfaced via the
    FOMC calendar's ``has_sep`` flag, not as a separate calendar row."""
    entries = parse_fed_calendar_json(_fixture())
    titles = [e.release_title for e in entries]
    assert not any("Economic Projections" in t for t in titles)


def test_parse_drops_off_whitelist_events() -> None:
    """Consumer Credit (G.19) / Selected Interest Rates (H.15) /
    FOMC meetings / speeches aren't in the P4a whitelist — the parser
    must drop them, not mis-label."""
    entries = parse_fed_calendar_json(_fixture())
    titles = [e.release_title for e in entries]
    assert not any("Consumer Credit" in t for t in titles)
    assert not any("Selected Interest Rates" in t for t in titles)
    assert not any("FOMC Meeting" in t for t in titles)
    assert not any("Speech" in t for t in titles)


def test_parse_resolves_dst_correct_times() -> None:
    """H.4.1 at 4:30 PM ET in January = 21:30 UTC (EST).
    Beige Book at 2:00 PM ET in March = 19:00 UTC (ET has moved to
    EDT by the fixture's March 4 date — ET spring-forward 2026-03-08)."""
    entries = parse_fed_calendar_json(_fixture())
    jan_h41 = next(
        e for e in entries
        if e.series_id == "FED_H41" and e.release_date == "2026-01-15"
    )
    assert "T21:30" in jan_h41.event_time_utc
    # March 4, 2026 — before DST spring-forward Mar 8. Still EST:
    # 14:00 EST = 19:00 UTC.
    mar_beige = next(
        e for e in entries
        if e.series_id == "BEIGE_BOOK" and e.release_date == "2026-03-04"
    )
    assert "T19:00" in mar_beige.event_time_utc


def test_parse_drops_non_release_types_silently() -> None:
    """The parser gates matching on ``type in {"Beige", "Stat"}``
    before checking the title. Any other value — ``"Speeches"``,
    ``"Testimony"``, ``"FOMC"``, ``"Conferences"``, or the
    ``"events"`` orphan sub-entries with empty ``month`` — is
    off-scope. 569 orphan sub-entries appeared in the 2026-04-22 live
    capture; surfacing them as row_issues would bury the genuine-drift
    signal. The fixture carries two such stubs (Beige Book + H.4.1)
    plus an FOMC row and a Speech; all four should be silently
    dropped.
    """
    issues: list[str] = []
    entries = parse_fed_calendar_json(_fixture(), row_issues=issues)
    # 12 real entries (2 + 5 + 5) — orphan stubs, FOMC, and Speech
    # are filtered at the type gate.
    assert len(entries) == 12
    assert issues == []


def test_parse_gates_matches_on_type_not_title() -> None:
    """Codex P2 on 2026-04-22 — without the type gate, a governor
    speech titled something like "The Beige Book in a Digital Economy"
    would substring-match the whitelist and project as a spurious
    BEIGE_BOOK release row. The current live feed has zero such
    collisions, but the category routinely publishes titles that
    reference the H.4.1 balance sheet or the Beige Book narrative,
    so the gate keeps pollution out over a multi-year horizon.

    Same structural check for a Testimony row referencing H.4.1 and
    an off-type "events" row titled like a whitelist entry — every
    non-release type must be rejected by the gate, never by title.
    """
    payload = (
        '{"events":['
        '{"title":"The Beige Book in a Digital Economy","time":"2:30 p.m.",'
        '"month":"2026-04","days":"9","type":"Speeches"},'
        '{"title":"Testimony on H.4.1 Balance-Sheet Dynamics",'
        '"time":"10:00 a.m.","month":"2026-04","days":"22","type":"Testimony"},'
        '{"title":"Conference: Interpreting the H.8 Survey",'
        '"time":"9:00 a.m.","month":"2026-05","days":"14","type":"Conferences"}'
        ']}'
    )
    with pytest.raises(FedCalendarJsonParseError):
        parse_fed_calendar_json(payload)


def test_parse_multi_day_events_fan_out() -> None:
    """The feed groups weekly releases into one month-level event with
    ``days`` as a comma-separated string (e.g. H.4.1 Jan
    ``"15, 22, 28"``). The parser must emit one :class:`FedReleaseEntry`
    per day, otherwise only the first date in each month would land."""
    entries = parse_fed_calendar_json(_fixture())
    jan_h41_dates = sorted(
        e.release_date for e in entries
        if e.series_id == "FED_H41" and e.release_date.startswith("2026-01")
    )
    assert jan_h41_dates == ["2026-01-15", "2026-01-22", "2026-01-28"]


def test_parse_raises_when_payload_is_not_json() -> None:
    with pytest.raises(FedCalendarJsonParseError):
        parse_fed_calendar_json("<html>not json</html>")


def test_parse_raises_when_events_array_missing() -> None:
    with pytest.raises(FedCalendarJsonParseError):
        parse_fed_calendar_json('{"other_key": []}')


def test_parse_strips_utf8_bom() -> None:
    """The wire payload is UTF-8 with a leading BOM —
    ``fetch_fed_calendar_json`` strips it, but the parser must also
    tolerate it so tests / ad-hoc reads of an uncleaned capture work."""
    entries = parse_fed_calendar_json("\ufeff" + _fixture())
    assert len(entries) == 12


@pytest.mark.parametrize(
    "time_text",
    ["4:30 PM", "4:30 pm", "4:30 p.m.", "4:30 P.M.", "4:30 PM ET"],
)
def test_parse_accepts_all_pm_suffix_variants(time_text: str) -> None:
    """Codex P4a R2 — dotted ``p.m.`` wasn't matching the time regex,
    so H.4.1 / H.8 releases on rows with that suffix landed 12 hours
    early. All documented PM-suffix variants must round-trip to
    16:30 ET → 21:30 UTC."""
    payload = (
        '{"events":[{"title":"H.4.1 - Factors Affecting Reserve Balances",'
        f'"time":"{time_text}","month":"2026-01","days":"15","type":"Stat"}}]}}'
    )
    entries = parse_fed_calendar_json(payload)
    assert len(entries) == 1
    # 4:30 PM EST = 21:30 UTC.
    assert "T21:30" in entries[0].event_time_utc


def test_parse_bare_time_falls_back_to_default() -> None:
    """Codex P4a R2 — a bare ``4:30`` without AM/PM should fall back
    to the per-indicator default (H.4.1 → ``"4:30 PM"``) rather than
    being read as 04:30 ET."""
    payload = (
        '{"events":[{"title":"H.4.1 - Factors Affecting Reserve Balances",'
        '"time":"4:30","month":"2026-01","days":"15","type":"Stat"}]}'
    )
    entries = parse_fed_calendar_json(payload)
    assert len(entries) == 1
    assert "T21:30" in entries[0].event_time_utc  # 4:30 PM EST


def test_parse_missing_time_falls_back_to_default() -> None:
    """An event with empty ``time`` falls back to the per-indicator
    default rather than dropping the event — the feed is well-formed
    in practice but defensive fallback keeps the pipeline alive on
    an upstream field-drop."""
    payload = (
        '{"events":[{"title":"H.4.1 - Factors Affecting Reserve Balances",'
        '"time":"","month":"2026-01","days":"15","type":"Stat"}]}'
    )
    entries = parse_fed_calendar_json(payload)
    assert len(entries) == 1
    assert "T21:30" in entries[0].event_time_utc


def test_parse_raises_when_no_whitelist_matches() -> None:
    """A feed with zero events matching the whitelist is surfaced as
    a parse error — blue-sky upstream drift catches this."""
    payload = (
        '{"events":[{"title":"Consumer Credit - G.19",'
        '"time":"3:00 p.m.","month":"2026-01","days":"7","type":"Stat"}]}'
    )
    with pytest.raises(FedCalendarJsonParseError):
        parse_fed_calendar_json(payload)


def test_parse_captures_row_issues_for_unparseable_month() -> None:
    """A whitelisted event with a bad month field is surfaced in
    ``row_issues`` rather than silently dropped."""
    payload = (
        '{"events":['
        '{"title":"Beige Book","time":"2:00 p.m.",'
        '"month":"later this year","days":"21","type":"Beige"},'
        '{"title":"Beige Book","time":"2:00 p.m.",'
        '"month":"2026-01","days":"21","type":"Beige"}'
        ']}'
    )
    issues: list[str] = []
    entries = parse_fed_calendar_json(payload, row_issues=issues)
    assert len(entries) == 1
    assert any("unparseable month" in msg for msg in issues)


def test_parse_captures_row_issues_for_unparseable_days() -> None:
    payload = (
        '{"events":['
        '{"title":"Beige Book","time":"2:00 p.m.",'
        '"month":"2026-01","days":"","type":"Beige"},'
        '{"title":"Beige Book","time":"2:00 p.m.",'
        '"month":"2026-01","days":"21","type":"Beige"}'
        ']}'
    )
    issues: list[str] = []
    entries = parse_fed_calendar_json(payload, row_issues=issues)
    assert len(entries) == 1
    assert any("unparseable days" in msg for msg in issues)


def test_parse_raises_when_every_matched_event_fails() -> None:
    """Codex P4a R1 — if every whitelisted event fails row-level
    parsing (feed month/day format drifted on every match), the
    parser must raise so the outage surfaces loudly. ``row_issues``
    alone would be easy to miss on a successful-looking commit."""
    payload = (
        '{"events":['
        '{"title":"Beige Book","time":"2:00 p.m.",'
        '"month":"later this year","days":"21","type":"Beige"},'
        '{"title":"H.4.1 - Factors Affecting Reserve Balances",'
        '"time":"4:30 p.m.","month":"somewhere","days":"15","type":"Stat"}'
        ']}'
    )
    issues: list[str] = []
    with pytest.raises(FedCalendarJsonParseError):
        parse_fed_calendar_json(payload, row_issues=issues)
    # ``row_issues`` still carries per-event detail for diagnosis.
    assert len(issues) == 2


def test_fetch_reports_parse_failure_when_all_events_drift(
    store: SQLiteEngineStore,
) -> None:
    """End-to-end: a feed-wide format drift raises at parse, the
    fetcher captures it in ``fetch_error``, and DB state stays clean."""
    payload = (
        '{"events":['
        '{"title":"Beige Book","time":"2:00 p.m.",'
        '"month":"later this year","days":"21","type":"Beige"}'
        ']}'
    )

    def fake_fetcher():
        return payload

    with store._connection(commit=True) as conn:
        summary = fetch_fed_releasedates(
            conn, dry_run=False, html_fetcher=fake_fetcher,
        )
    assert summary.fetch_error is not None
    assert "every event failed" in summary.fetch_error
    assert summary.entries_parsed == 0


# ──────────────────────────────────────────────────────────────────────────
# release_entry_to_records
# ──────────────────────────────────────────────────────────────────────────


def test_entries_get_distinct_provider_event_ids() -> None:
    """Each fixture row lands under a unique ``provider_event_id``
    — same indicator on different dates, or different indicators
    on the same date, don't collide in cal_econ_event."""
    entries = parse_fed_calendar_json(_fixture())
    ids = set()
    for e in entries:
        _, event = release_entry_to_records(e, snapshot_epoch_ms=0)
        ids.add(event.provider_event_id)
    assert len(ids) == len(entries)


def test_record_shape() -> None:
    entry = FedReleaseEntry(
        series_id="BEIGE_BOOK",
        release_title="Beige Book",
        release_date="2026-01-21",
        release_time_local="2:00 PM",
        event_time_utc="2026-01-21T19:00:00+00:00",
    )
    raw, event = release_entry_to_records(
        entry, snapshot_epoch_ms=1_700_000_000_000,
    )
    assert event.provider == "federal-reserve"
    assert event.event_time_precision == "datetime"
    assert event.actual is None
    assert event.title == "Beige Book"
    assert event.currency == "USD"
    assert event.source_url == FED_CALENDAR_JSON_URL
    assert raw.provider == "federal-reserve"


# ──────────────────────────────────────────────────────────────────────────
# fetch_fed_releasedates
# ──────────────────────────────────────────────────────────────────────────


def test_fetch_lands_all_whitelisted_events(
    store: SQLiteEngineStore,
) -> None:
    payload = _fixture()

    def fake_fetcher():
        return payload

    with store._connection(commit=True) as conn:
        summary = fetch_fed_releasedates(
            conn, dry_run=False, html_fetcher=fake_fetcher,
        )
    assert summary.fetch_error is None
    assert summary.entries_parsed == 12  # 2 + 5 + 5
    assert summary.events_upserted == 12
    assert summary.entries_by_indicator == {
        "BEIGE_BOOK": 2, "FED_H41": 5, "FED_H8": 5,
    }

    with store._connection(commit=False) as conn:
        counts = dict(conn.execute(
            "SELECT title, COUNT(*) FROM cal_econ_event "
            "WHERE provider='federal-reserve' "
            "AND source_url LIKE '%calendar.json%' "
            "GROUP BY title"
        ).fetchall())
    assert set(counts) == {
        "Beige Book",
        "H.4.1 — Factors Affecting Reserve Balances",
        "H.8 — Assets and Liabilities of Commercial Banks",
    }


def test_fetch_reports_fetch_error(
    store: SQLiteEngineStore,
) -> None:
    def exploding_fetcher():
        raise RuntimeError("upstream 404")

    with store._connection(commit=True) as conn:
        summary = fetch_fed_releasedates(
            conn, dry_run=False, html_fetcher=exploding_fetcher,
        )
    assert summary.fetch_error == "upstream 404"
    assert summary.entries_parsed == 0


def test_fetch_dry_run(store: SQLiteEngineStore) -> None:
    calls: list[None] = []

    def never():
        calls.append(None)
        return ""

    with store._connection(commit=False) as conn:
        summary = fetch_fed_releasedates(
            conn, dry_run=True, html_fetcher=never,
        )
    assert summary.dry_run is True
    assert summary.indicators_planned == ["BEIGE_BOOK", "FED_H41", "FED_H8"]
    assert calls == []


# ──────────────────────────────────────────────────────────────────────────
# Service op
# ──────────────────────────────────────────────────────────────────────────


def test_service_op_dry_run(store: SQLiteEngineStore) -> None:
    from macro_data.service import LocalMacroDataService

    svc = LocalMacroDataService(store=store)
    result = svc.invoke(
        "calendar_econ_fetch_fed_releases", {"dry_run": True},
    )
    assert result["dry_run"] is True
    assert result["stopped_reason"] == "dry_run"
    assert result["indicators_planned"] == [
        "BEIGE_BOOK", "FED_H41", "FED_H8",
    ]

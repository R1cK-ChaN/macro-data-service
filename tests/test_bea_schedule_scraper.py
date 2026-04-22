"""Tests for the BEA release-schedule scraper (issue #9 P2a +
P2b-schedule-drift).

Fixture HTML lives in ``tests/fixtures/bea_schedule/news_schedule.html``
— a plausible slice of bea.gov/news/schedule covering GDP (staged
advance / second / third across two quarters) + Personal Income (six
months) + multi-indicator rows that combine the GDP headline with
regional / industry decompositions. Non-whitelisted titles (``GDP by
Industry``, ``GDP by County``, regional rollups, ``Trade in Value
Added``) must be dropped; multi-indicator rows where the headline is
GDP must match even when the tail lists regional / state decompositions.

The fixture mirrors the live 2026 DOM shape — single table with a
``Year 2026`` column header, stacked ``<div class="release-date">``
and ``<small class="text-muted">`` for date + time, and the reference
period inline in the release-title cell after the first comma.

No real HTTP.

Covers:

- Parser: rows extracted correctly; unmatched titles skipped;
  leading-segment exclude rule fires on ``"GDP by Industry"`` /
  ``"GDP by County and Personal Income by County"`` without dropping
  headline-GDP multi-indicator rows; release-stage extracted from the
  title qualifier; TBA placeholders skipped.
- Schedule entry → (raw, event) records: ``provider_event_id`` anchors
  on ``reference_date`` for Personal Income (unstaged) and on
  ``reference_date|stage`` for GDP (staged).
- Projector: schedule rows insert with ``actual=NULL`` +
  ``precision='datetime'``.
- Merge with API-side row: Personal Income schedule row keeps its
  datetime after the API value lands; GDP schedule rows do not
  merge (``api_fetch=False`` at the spec — schedule-only).
- Service op ``calendar_econ_schedule_bea`` — dry_run + fixture
  injection via ``html_fetcher`` seam.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ingestion.calendar.bea_api import (
    BEA_SCHEDULE_URL,
    BEAScheduleEntry,
    BEAScheduleParseError,
    INDICATOR_REGISTRY,
    parse_observation,
    parse_schedule_html,
    project_events,
    project_schedule_events,
    schedule_bea_calendar,
    schedule_entry_to_records,
    store_raw,
)
from ingestion.timeseries.scrapers.bea import BEAObservation
from storage.sqlite import SQLiteEngineStore


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "bea_schedule"


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _fixture_html(name: str = "news_schedule.html") -> str:
    return (FIXTURE_DIR / name).read_text(encoding="utf-8")


# ──────────────────────────────────────────────────────────────────────────
# parse_schedule_html
# ──────────────────────────────────────────────────────────────────────────


def test_parse_extracts_gdp_and_personal_income_rows() -> None:
    entries = parse_schedule_html(_fixture_html())
    # Fixture holds 6 GDP (3 stages × 2 quarters) + 6 Personal Income
    # (6 months) = 12 whitelisted entries; "GDP by Industry" and
    # "State Personal Income" must be dropped.
    series_count: dict[str, int] = {}
    for e in entries:
        series_count[e.series_id] = series_count.get(e.series_id, 0) + 1
    assert series_count == {
        "BEA_NIPA_T10101_1":  6,
        "BEA_NIPA_T20600_1":  6,
    }


def test_parse_skips_gdp_by_industry() -> None:
    """Leading-segment exclude rule prevents ``"GDP by Industry, ..."``
    from substring-matching the headline GDP fragment and getting
    labelled as the wrong indicator. The row-wide substring check
    used to live here would now drop legitimate multi-indicator
    rows — see ``test_parse_keeps_multi_indicator_gdp_rows``."""
    entries = parse_schedule_html(_fixture_html())
    titles = {e.release_title for e in entries}
    assert not any(
        t.lower().split(",", 1)[0].strip().endswith("by industry")
        for t in titles
    )


def test_parse_keeps_multi_indicator_gdp_rows() -> None:
    """The live DOM bundles headline GDP with industry / regional
    decompositions on a single row — e.g.
    ``"GDP (Third Estimate), Industries, Corporate Profits, State
    GDP, and State Personal Income, 1st Quarter 2026"``. Leading-
    segment scoping keeps these rows matched as the headline GDP
    release because ``State GDP`` and ``State Personal Income`` live
    past the first comma. A row-wide substring check (the old
    behaviour) would drop every such row under the regional excludes."""
    entries = parse_schedule_html(_fixture_html())
    # Both Q4-2025 and Q1-2026 "Third Estimate" rows in the fixture
    # are multi-indicator; both should land as GDP third-estimate
    # entries.
    third_estimate_entries = [
        e for e in entries
        if e.series_id == "BEA_NIPA_T10101_1" and e.release_stage == "third"
    ]
    assert len(third_estimate_entries) == 2


def test_parse_skips_out_of_whitelist_releases() -> None:
    """``"GDP by County and Personal Income by County"`` is outside
    the P2a whitelist — the scraper silently drops it rather than
    synthesising a bogus id. The tail ``"Personal Income by County"``
    substring is irrelevant under leading-segment scoping; the veto
    fires on ``"by county"`` in the leading segment itself."""
    entries = parse_schedule_html(_fixture_html())
    titles = {e.release_title for e in entries}
    assert not any(
        t.lower().split(",", 1)[0].strip().startswith("gdp by county")
        for t in titles
    )


@pytest.mark.parametrize(
    "regional_title",
    [
        "GDP by Industry, 1st Quarter 2026",
        "GDP by County and Personal Income by County, 2025",
        "Real Personal Consumption Expenditures by State and Real Personal Income by State, 2024",
    ],
)
def test_parse_skips_regional_decompositions(regional_title: str) -> None:
    """Codex P2a R1 — regional BEA releases (``by State`` / ``by
    County`` / ``by Industry``) share a substring with the headline
    GDP and Personal Income fragments. Without the leading-segment
    excludes they would be mis-labelled as the headline indicator and
    land in cal_econ_event under the wrong provider_event_id."""
    entries = parse_schedule_html(_fixture_html())
    titles = {e.release_title for e in entries}
    assert regional_title not in titles


def test_parse_extracts_release_stage_for_gdp() -> None:
    entries = parse_schedule_html(_fixture_html())
    stages_by_release = {
        (e.release_date, e.reference_label): e.release_stage
        for e in entries if e.series_id == "BEA_NIPA_T10101_1"
    }
    assert stages_by_release[("2026-01-30", "4th Quarter 2025")] == "advance"
    assert stages_by_release[("2026-02-27", "4th Quarter 2025")] == "second"
    assert stages_by_release[("2026-03-27", "4th Quarter 2025")] == "third"


def test_parse_leaves_personal_income_stage_empty() -> None:
    """Personal Income is unstaged — ``release_stage=""`` so the
    schedule-side event_id matches the API-side observation id."""
    entries = parse_schedule_html(_fixture_html())
    for e in entries:
        if e.series_id == "BEA_NIPA_T20600_1":
            assert e.release_stage == ""


def test_parse_gdp_reference_date_is_end_of_quarter() -> None:
    entries = parse_schedule_html(_fixture_html())
    q4 = next(
        e for e in entries
        if e.series_id == "BEA_NIPA_T10101_1"
        and e.release_stage == "advance"
        and e.reference_label == "4th Quarter 2025"
    )
    assert q4.reference_date == "2025-12-31"


def test_parse_personal_income_reference_date_is_first_of_month() -> None:
    entries = parse_schedule_html(_fixture_html())
    pi = next(
        e for e in entries
        if e.series_id == "BEA_NIPA_T20600_1"
        and e.reference_label == "December 2025"
    )
    assert pi.reference_date == "2025-12-01"


def test_parse_uses_dst_aware_eastern_time() -> None:
    """Winter vs summer sanity check — 8:30 AM ET is 13:30 UTC in EST
    and 12:30 UTC in EDT."""
    entries = parse_schedule_html(_fixture_html())
    winter = next(e for e in entries if e.release_date == "2026-01-30")
    summer = next(e for e in entries if e.release_date == "2026-04-29")
    assert "T13:30" in winter.event_time_utc
    assert "T12:30" in summer.event_time_utc


def test_parse_raises_when_no_matching_table_found() -> None:
    with pytest.raises(BEAScheduleParseError):
        parse_schedule_html(
            "<html><body><table><tr><th>Foo</th></tr></table></body></html>"
        )


def test_parse_raises_when_no_table_found() -> None:
    with pytest.raises(BEAScheduleParseError):
        parse_schedule_html("<html><body><p>no table here</p></body></html>")


def test_parse_surfaces_row_issues_for_matched_but_unparseable_reference() -> None:
    """Codex P2a R2 — when a whitelisted title matches but the
    reference period has a format the parser doesn't yet handle (BEA
    occasionally uses ``"4th Quarter and Year 2025"``), the row is
    captured in ``row_issues`` instead of silently dropped. Operator
    sees the drift and can extend the parser."""
    html = (
        '<table id="release-schedule-table">'
        '<thead><tr><th>Year 2026</th><th></th><th>Release</th></tr></thead>'
        '<tbody><tr>'
        '<td class="scheduled-date"><div class="release-date">January 30</div>'
        '<small class="text-muted">8:30 AM</small></td>'
        '<td></td>'
        '<td class="release-title">GDP (Advance Estimate), 4th Quarter and Year 2025</td>'
        "</tr></tbody></table>"
    )
    issues: list[str] = []
    entries = parse_schedule_html(html, row_issues=issues)
    assert entries == []
    assert len(issues) == 1
    assert "GDP" in issues[0]
    assert "4th Quarter and Year 2025" in issues[0]


def test_parse_surfaces_row_issues_when_reference_period_absent() -> None:
    """BEA's new DOM carries the reference period inline after the
    first comma. If a whitelisted title drops the comma altogether
    (``"GDP (Advance Estimate)"`` with no reference tail), the row
    must land in ``row_issues`` rather than silently disappearing —
    that's exactly the kind of upstream drift we want operators to
    notice."""
    html = (
        '<table id="release-schedule-table">'
        '<thead><tr><th>Year 2026</th><th></th><th>Release</th></tr></thead>'
        '<tbody><tr>'
        '<td class="scheduled-date"><div class="release-date">January 30</div>'
        '<small class="text-muted">8:30 AM</small></td>'
        '<td></td>'
        '<td class="release-title">GDP (Advance Estimate)</td>'
        "</tr></tbody></table>"
    )
    issues: list[str] = []
    entries = parse_schedule_html(html, row_issues=issues)
    assert entries == []
    assert len(issues) == 1
    assert "no reference period" in issues[0]


def test_parse_skips_to_be_announced_rows() -> None:
    """The live page emits ``"To Be Announced"`` rows without a
    ``release-date`` div — they're placeholders and shouldn't land
    as scheduleable events or as row_issues."""
    html = (
        '<table id="release-schedule-table">'
        '<thead><tr><th>Year 2026</th><th></th><th>Release</th></tr></thead>'
        '<tbody><tr>'
        '<td class="scheduled-date">'
        '<small class="text-muted">To Be Announced<br/>Spring 2026</small>'
        "</td>"
        '<td></td>'
        '<td class="release-title">GDP (Advance Estimate), 1st Quarter 2026</td>'
        "</tr></tbody></table>"
    )
    issues: list[str] = []
    entries = parse_schedule_html(html, row_issues=issues)
    assert entries == []
    assert issues == []


def test_parse_silently_drops_unmatched_titles() -> None:
    """Titles that don't match any whitelisted fragment are still
    dropped without issue — we don't track every BEA release, only
    the whitelisted ones. Silence here is the right behaviour."""
    html = (
        '<table id="release-schedule-table">'
        '<thead><tr><th>Year 2026</th><th></th><th>Release</th></tr></thead>'
        '<tbody><tr>'
        '<td class="scheduled-date"><div class="release-date">January 30</div>'
        '<small class="text-muted">8:30 AM</small></td>'
        '<td></td>'
        '<td class="release-title">U.S. International Trade in Goods and Services, December 2025</td>'
        "</tr></tbody></table>"
    )
    issues: list[str] = []
    entries = parse_schedule_html(html, row_issues=issues)
    assert entries == []
    assert issues == []


def test_parse_accepts_short_form_quarter_inline_reference() -> None:
    """Codex P2 on 2026-04-22 — ``_parse_reference_period`` accepts
    BEA's documented ``"Q4 2025"`` short-form quarter, but the inline
    extraction regex previously required the word ``"quarter"`` after
    the ``Q4`` token. With the 2026 DOM the inline path is the *only*
    reference-period path, so short-form quarters on any matched row
    would silently drop. Lock the round-trip down here."""
    html = (
        '<table id="release-schedule-table">'
        '<thead><tr><th>Year 2026</th><th></th><th>Release</th></tr></thead>'
        '<tbody><tr>'
        '<td class="scheduled-date"><div class="release-date">January 30</div>'
        '<small class="text-muted">8:30 AM</small></td>'
        '<td></td>'
        '<td class="release-title">GDP (Advance Estimate), Q4 2025</td>'
        "</tr></tbody></table>"
    )
    entries = parse_schedule_html(html)
    assert len(entries) == 1
    assert entries[0].series_id == "BEA_NIPA_T10101_1"
    assert entries[0].release_stage == "advance"
    assert entries[0].reference_label == "Q4 2025"
    assert entries[0].reference_date == "2025-12-31"


def test_parse_accepts_both_gdp_phrasings() -> None:
    """BEA switched from ``"Gross Domestic Product (Advance)"`` to
    ``"GDP (Advance Estimate)"`` in the 2026 DOM refactor. The match
    table covers both so a rewrite in either direction keeps landing
    rows — tested with distinct dates so the IDs don't collide."""
    html = (
        '<table id="release-schedule-table">'
        '<thead><tr><th>Year 2026</th><th></th><th>Release</th></tr></thead>'
        '<tbody>'
        '<tr>'
        '<td class="scheduled-date"><div class="release-date">January 30</div>'
        '<small class="text-muted">8:30 AM</small></td>'
        '<td></td>'
        '<td class="release-title">GDP (Advance Estimate), 4th Quarter 2025</td>'
        "</tr>"
        '<tr>'
        '<td class="scheduled-date"><div class="release-date">April 29</div>'
        '<small class="text-muted">8:30 AM</small></td>'
        '<td></td>'
        '<td class="release-title">Gross Domestic Product (Advance Estimate), 1st Quarter 2026</td>'
        "</tr>"
        "</tbody></table>"
    )
    entries = parse_schedule_html(html)
    assert {e.series_id for e in entries} == {"BEA_NIPA_T10101_1"}
    assert {e.release_stage for e in entries} == {"advance"}
    assert {e.reference_label for e in entries} == {
        "4th Quarter 2025", "1st Quarter 2026",
    }


# ──────────────────────────────────────────────────────────────────────────
# schedule_entry_to_records
# ──────────────────────────────────────────────────────────────────────────


def test_record_shape_for_personal_income() -> None:
    entry = BEAScheduleEntry(
        series_id="BEA_NIPA_T20600_1",
        reference_date="2026-01-01",
        reference_label="January 2026",
        release_title="Personal Income and Outlays",
        release_date="2026-02-27",
        release_time_local="8:30 AM",
        event_time_utc="2026-02-27T13:30:00+00:00",
    )
    raw, event = schedule_entry_to_records(
        entry, snapshot_epoch_ms=1_700_000_000_000,
    )
    assert event.provider == "bea"
    assert event.event_time_precision == "datetime"
    assert event.event_time_utc == "2026-02-27T13:30:00+00:00"
    assert event.actual is None
    assert event.title == "Personal Income"
    assert event.source_url == BEA_SCHEDULE_URL
    assert raw.provider == "bea"


def test_personal_income_schedule_id_matches_api_side_id() -> None:
    """Key property for cross-source merge: same reference period hashes
    to the same ``provider_event_id`` whether the row came from the
    schedule scraper or the API."""
    entry = BEAScheduleEntry(
        series_id="BEA_NIPA_T20600_1",
        reference_date="2024-04-01",
        reference_label="April 2024",
        release_title="Personal Income and Outlays",
        release_date="2024-05-31",
        release_time_local="8:30 AM",
        event_time_utc="2024-05-31T12:30:00+00:00",
    )
    _, schedule_event = schedule_entry_to_records(entry, snapshot_epoch_ms=0)

    api_obs = BEAObservation(
        series_id="BEA_NIPA_T20600_1",
        date="2024-04-01",
        value=23_000.0,
        table_name="T20600",
        line_number="1",
        line_description="Personal income",
    )
    _, api_event = parse_observation(api_obs, snapshot_epoch_ms=0)
    assert schedule_event.provider_event_id == api_event.provider_event_id


def test_gdp_staged_ids_distinct_across_release_stages() -> None:
    """GDP publishes Advance / Second / Third for the same quarter —
    each stage gets its own ``provider_event_id`` so the three staged
    releases don't collide in cal_econ_event."""
    entries = parse_schedule_html(_fixture_html())
    gdp_q4 = [
        e for e in entries
        if e.series_id == "BEA_NIPA_T10101_1"
        and e.reference_label == "4th Quarter 2025"
    ]
    ids = set()
    for e in gdp_q4:
        _, event = schedule_entry_to_records(e, snapshot_epoch_ms=0)
        ids.add(event.provider_event_id)
    assert len(ids) == 3


def test_gdp_staged_id_differs_from_bare_anchor_api_id() -> None:
    """GDP's API observation uses the bare-date anchor; the
    schedule-side uses stage-qualified anchor. That's the
    intentional misalignment that gates ``api_fetch=False`` on GDP."""
    entries = parse_schedule_html(_fixture_html())
    advance = next(
        e for e in entries
        if e.series_id == "BEA_NIPA_T10101_1"
        and e.release_stage == "advance"
        and e.reference_label == "4th Quarter 2025"
    )
    _, schedule_event = schedule_entry_to_records(
        advance, snapshot_epoch_ms=0,
    )
    api_obs = BEAObservation(
        series_id="BEA_NIPA_T10101_1",
        date="2025-12-31",
        value=2.3,
        table_name="T10101",
        line_number="1",
        line_description="Real GDP",
    )
    _, api_event = parse_observation(api_obs, snapshot_epoch_ms=0)
    assert schedule_event.provider_event_id != api_event.provider_event_id


# ──────────────────────────────────────────────────────────────────────────
# Cross-source merge (Personal Income only — GDP is schedule-only)
# ──────────────────────────────────────────────────────────────────────────


def _schedule_pi_records(reference_date: str):
    entry = BEAScheduleEntry(
        series_id="BEA_NIPA_T20600_1",
        reference_date=reference_date,
        reference_label="January 2026",
        release_title="Personal Income and Outlays",
        release_date="2026-02-27",
        release_time_local="8:30 AM",
        event_time_utc="2026-02-27T13:30:00+00:00",
    )
    return schedule_entry_to_records(entry, snapshot_epoch_ms=1_700_000_000_000)


def _api_pi_records(reference_date: str, value: float = 23_456.7):
    obs = BEAObservation(
        series_id="BEA_NIPA_T20600_1",
        date=reference_date,
        value=value,
        table_name="T20600",
        line_number="1",
        line_description="Personal income",
    )
    return parse_observation(obs, snapshot_epoch_ms=1_700_000_100_000)


def test_merge_schedule_then_api_keeps_datetime_and_gains_actual(
    store: SQLiteEngineStore,
) -> None:
    raw_sched, evt_sched = _schedule_pi_records("2026-01-01")
    raw_api, evt_api = _api_pi_records("2026-01-01", value=23_456.7)

    with store._connection(commit=True) as conn:
        store_raw(conn, [raw_sched])
        project_schedule_events(conn, [evt_sched])
        store_raw(conn, [raw_api])
        project_events(conn, [evt_api])

    with store._connection(commit=False) as conn:
        row = conn.execute(
            "SELECT event_time_utc, event_time_precision, actual "
            "FROM cal_econ_event WHERE provider='bea'"
        ).fetchone()
    assert tuple(row) == (
        "2026-02-27T13:30:00+00:00",
        "datetime",
        "23456.7",
    )


def test_merge_api_then_schedule_promotes_precision_and_keeps_actual(
    store: SQLiteEngineStore,
) -> None:
    raw_api, evt_api = _api_pi_records("2026-01-01", value=23_456.7)
    raw_sched, evt_sched = _schedule_pi_records("2026-01-01")

    with store._connection(commit=True) as conn:
        store_raw(conn, [raw_api])
        project_events(conn, [evt_api])
        store_raw(conn, [raw_sched])
        project_schedule_events(conn, [evt_sched])

    with store._connection(commit=False) as conn:
        row = conn.execute(
            "SELECT event_time_utc, event_time_precision, actual "
            "FROM cal_econ_event WHERE provider='bea'"
        ).fetchone()
    assert tuple(row) == (
        "2026-02-27T13:30:00+00:00",
        "datetime",
        "23456.7",
    )


# ──────────────────────────────────────────────────────────────────────────
# schedule_bea_calendar
# ──────────────────────────────────────────────────────────────────────────


def test_schedule_dry_run_returns_plan_without_fetch(
    store: SQLiteEngineStore,
) -> None:
    """Dry-run does no HTTP, no writes — fake fetcher would be called
    otherwise and flip ``fetch_error`` if misconfigured."""
    calls: list[None] = []

    def never_called_fetcher(*, session=None):
        calls.append(None)
        return ""

    with store._connection(commit=False) as conn:
        summary = schedule_bea_calendar(
            conn, dry_run=True, html_fetcher=never_called_fetcher,
        )
    assert summary.dry_run is True
    assert calls == []


def test_schedule_run_lands_rows_for_full_whitelist(
    store: SQLiteEngineStore,
) -> None:
    html = _fixture_html()

    def fake_fetcher(*, session=None):
        return html

    with store._connection(commit=True) as conn:
        summary = schedule_bea_calendar(
            conn, dry_run=False, html_fetcher=fake_fetcher,
        )
    assert summary.fetch_error is None
    assert set(summary.series_ok) == {
        "BEA_NIPA_T10101_1",
        "BEA_NIPA_T20600_1",
    }
    assert summary.entries_parsed == 12  # 6 GDP + 6 Personal Income
    assert summary.events_upserted == summary.entries_parsed

    with store._connection(commit=False) as conn:
        rows = conn.execute(
            "SELECT DISTINCT event_time_precision "
            "FROM cal_econ_event WHERE provider='bea'"
        ).fetchall()
    assert [tuple(r) for r in rows] == [("datetime",)]


def test_schedule_run_updates_audit_fields_for_gdp_revisions(
    store: SQLiteEngineStore,
) -> None:
    """Codex P2a R2 — GDP is ``api_fetch=False``, so the schedule
    lane is the sole writer. A rescrape with a revised release time
    must update ``content_hash`` and ``observed_at_epoch_ms`` too —
    ``project_schedule_events`` preserves those on conflict (they're
    the API-side audit fields), so schedule-only specs need the
    full-writer ``project_events`` path."""
    original_entry = BEAScheduleEntry(
        series_id="BEA_NIPA_T10101_1",
        reference_date="2025-12-31",
        reference_label="4th Quarter 2025",
        release_title="Gross Domestic Product (Advance Estimate)",
        release_date="2026-01-30",
        release_time_local="8:30 AM",
        event_time_utc="2026-01-30T13:30:00+00:00",
        release_stage="advance",
    )
    # Schedule revision: BEA bumped the release time to 10:00 ET.
    revised_entry = BEAScheduleEntry(
        series_id="BEA_NIPA_T10101_1",
        reference_date="2025-12-31",
        reference_label="4th Quarter 2025",
        release_title="Gross Domestic Product (Advance Estimate)",
        release_date="2026-01-30",
        release_time_local="10:00 AM",
        event_time_utc="2026-01-30T15:00:00+00:00",
        release_stage="advance",
    )

    def fetcher_factory(entry):
        ref_label = entry.reference_label
        month_day = {
            "2026-01-30": "January 30",
        }[entry.release_date]
        year = entry.release_date[:4]
        html = (
            f'<table id="release-schedule-table">'
            f'<thead><tr><th>Year {year}</th><th></th><th>Release</th></tr></thead>'
            f'<tbody><tr>'
            f'<td class="scheduled-date">'
            f'<div class="release-date">{month_day}</div>'
            f'<small class="text-muted">{entry.release_time_local}</small>'
            f"</td>"
            f'<td></td>'
            f'<td class="release-title">{entry.release_title}, {ref_label}</td>'
            f"</tr></tbody></table>"
        )
        def fake(*, session=None):
            return html
        return fake

    with store._connection(commit=True) as conn:
        schedule_bea_calendar(
            conn,
            dry_run=False,
            html_fetcher=fetcher_factory(original_entry),
            snapshot_epoch_ms=1_700_000_000_000,
        )
    with store._connection(commit=True) as conn:
        schedule_bea_calendar(
            conn,
            dry_run=False,
            html_fetcher=fetcher_factory(revised_entry),
            snapshot_epoch_ms=1_700_001_000_000,
        )
    with store._connection(commit=False) as conn:
        row = conn.execute(
            "SELECT event_time_utc, content_hash, observed_at_epoch_ms "
            "FROM cal_econ_event WHERE provider='bea'"
        ).fetchone()
    event_time, content_hash, observed_at = row
    # Revised event_time landed.
    assert event_time == "2026-01-30T15:00:00+00:00"
    # Audit fields followed the revision — not frozen at the first
    # scrape's values.
    assert observed_at == 1_700_001_000_000


def test_schedule_run_reports_fetch_error(
    store: SQLiteEngineStore,
) -> None:
    """Fetch failure surfaces via ``fetch_error`` instead of raising —
    the scheduler keeps the DB clean so partial state doesn't leak."""

    def exploding_fetcher(*, session=None):
        raise RuntimeError("upstream timeout")

    with store._connection(commit=True) as conn:
        summary = schedule_bea_calendar(
            conn, dry_run=False, html_fetcher=exploding_fetcher,
        )
    assert summary.fetch_error == "upstream timeout"
    assert summary.entries_parsed == 0
    assert summary.events_upserted == 0


# ──────────────────────────────────────────────────────────────────────────
# Service op
# ──────────────────────────────────────────────────────────────────────────


def test_service_op_dry_run(store: SQLiteEngineStore) -> None:
    from macro_data.service import LocalMacroDataService

    svc = LocalMacroDataService(store=store)
    result = svc.invoke("calendar_econ_schedule_bea", {"dry_run": True})
    assert result["dry_run"] is True
    assert result["stopped_reason"] == "dry_run"


def test_service_op_excludes_non_whitelist_releases() -> None:
    """Sanity check via the INDICATOR_REGISTRY shape: GDP by Industry
    / State Personal Income / PCE aren't registered so the whole
    ingestion path cannot project them by accident."""
    assert "BEA_NIPA_T10101_1" in INDICATOR_REGISTRY  # Real GDP
    assert "BEA_NIPA_T20600_1" in INDICATOR_REGISTRY  # Personal Income
    # PCE deferred — not registered yet.
    for sid in INDICATOR_REGISTRY:
        assert INDICATOR_REGISTRY[sid].dataset == "NIPA"

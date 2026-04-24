"""Mocked tests for the BoJ Tankan calendar connector (issue #14 P1a).

Fixture HTML lives in ``tests/fixtures/boj_tankan_schedule/`` and
``tests/fixtures/boj_tankan_outlines/`` — slices of the real
``boj.or.jp`` pages captured 2026-04-24. No real HTTP in CI.

Covers:

- Schedule parser: yoshi-index rows extracted correctly (release
  date parsed from NBSP-padded cell; reference date resolved from
  the ``tkYYMM.htm`` URL code; forward and wrap cases).
- ``schedule_entry_to_records``: ``provider_event_id`` anchors on
  ``(indicator, reference_date)`` so each release writes two rows
  (Large Mfg + Large Non-Mfg) that the outline-side upgrade upserts
  onto; 08:50 JST → 23:50 UTC of the prior day (JST has no DST).
- Outline parser: current DI extracted from ``row2[col=1]`` under
  the Business-Conditions > Large-Enterprises table; handles the
  sign / parens / blank cell shapes.
- Projector: schedule rows land with ``precision='datetime'`` and
  ``actual=NULL``; the outline-side write fills ``actual`` /
  ``previous`` / ``forecast`` without clobbering the schedule
  datetime.
- Service ops ``calendar_econ_fetch_boj_tankan`` and
  ``calendar_econ_fetch_boj_tankan_values`` — dry-run plans.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from ingestion.calendar.boj_tankan_api import (
    ALL_INDICATORS,
    INDICATOR_REGISTRY,
    OutlineValue,
    SectorDI,
    TANKAN_YOSHI_INDEX_URL,
    TankanOutlineParseError,
    TankanScheduleEntry,
    TankanScheduleParseError,
    build_outline_url,
    fetch_boj_tankan_calendar,
    fetch_boj_tankan_outlines,
    outline_value_to_records,
    parse_outline_html,
    parse_tankan_schedule_html,
    project_events,
    project_schedule_events,
    reference_date_from_yymm,
    schedule_entry_to_records,
    store_raw,
    yymm_from_reference_date,
)
from ingestion.calendar.boj_tankan_api.parser import PROVIDER
from ingestion.calendar.boj_tankan_api.outlines import _content_hash as _outline_hash
from ingestion.calendar.boj_tankan_api.scraper import _content_hash as _schedule_hash
from storage.sqlite import SQLiteEngineStore


SCHEDULE_FIXTURES = Path(__file__).parent / "fixtures" / "boj_tankan_schedule"
OUTLINE_FIXTURES = Path(__file__).parent / "fixtures" / "boj_tankan_outlines"


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _yoshi_fixture() -> str:
    return (SCHEDULE_FIXTURES / "yoshi_index.html").read_text(encoding="utf-8")


def _outline_fixture(name: str) -> str:
    return (OUTLINE_FIXTURES / name).read_text(encoding="utf-8")


# ──────────────────────────────────────────────────────────────────────────
# INDICATOR_REGISTRY
# ──────────────────────────────────────────────────────────────────────────


def test_registry_includes_both_large_enterprise_anchors() -> None:
    assert set(INDICATOR_REGISTRY.keys()) == {
        "TANKAN_LARGE_MFG", "TANKAN_LARGE_NONMFG",
    }
    for spec in INDICATOR_REGISTRY.values():
        assert spec.country_code == "JP"
        assert spec.importance == "high"
        assert spec.unit == "points"
        assert "Tankan" in spec.title
    assert INDICATOR_REGISTRY["TANKAN_LARGE_MFG"].sector == "manufacturing"
    assert INDICATOR_REGISTRY["TANKAN_LARGE_NONMFG"].sector == "nonmanufacturing"


def test_all_indicators_list_is_sorted() -> None:
    # ALL_INDICATORS feeds the service-op dry-run plan; keep it
    # deterministic so operators can diff dry-run output across runs.
    assert ALL_INDICATORS == sorted(INDICATOR_REGISTRY.keys())


# ──────────────────────────────────────────────────────────────────────────
# YYMM codec helpers
# ──────────────────────────────────────────────────────────────────────────


def test_reference_date_from_yymm_resolves_survey_quarter() -> None:
    assert reference_date_from_yymm("2603") == date(2026, 3, 1)
    assert reference_date_from_yymm("2512") == date(2025, 12, 1)
    assert reference_date_from_yymm("2306") == date(2023, 6, 1)


def test_reference_date_from_yymm_rejects_non_quarter_month() -> None:
    # Tankan surveys publish only in March/June/September/December;
    # any other month is a parse-bug indicator.
    with pytest.raises(ValueError):
        reference_date_from_yymm("2601")
    with pytest.raises(ValueError):
        reference_date_from_yymm("26xx")


def test_yymm_roundtrip() -> None:
    for ref in [date(2024, 3, 1), date(2025, 12, 1), date(2026, 9, 1)]:
        assert reference_date_from_yymm(yymm_from_reference_date(ref)) == ref


def test_build_outline_url_matches_live_pattern() -> None:
    assert build_outline_url(date(2026, 3, 1)) == (
        "https://www.boj.or.jp/en/statistics/tk/yoshi/tk2603.htm"
    )


# ──────────────────────────────────────────────────────────────────────────
# parse_tankan_schedule_html
# ──────────────────────────────────────────────────────────────────────────


def test_schedule_parser_extracts_all_rows_from_fixture() -> None:
    entries = parse_tankan_schedule_html(_yoshi_fixture())
    # Fixture captures 12 quarterly releases (covers ~3 years of
    # history on the live page at snapshot time).
    assert len(entries) == 12
    yymms = {e.yymm for e in entries}
    assert "2603" in yymms
    assert "2306" in yymms


def test_schedule_parser_decodes_nbsp_padded_date_cells() -> None:
    """Yoshi index uses multiple &nbsp; to right-align day numbers;
    collapsing them to plain spaces must not break parsing."""
    entries = parse_tankan_schedule_html(_yoshi_fixture())
    by_yymm = {e.yymm: e for e in entries}
    assert by_yymm["2603"].release_date == date(2026, 4, 1)
    assert by_yymm["2512"].release_date == date(2025, 12, 15)
    assert by_yymm["2306"].release_date == date(2023, 7, 3)


def test_schedule_parser_derives_reference_date_from_url() -> None:
    entries = parse_tankan_schedule_html(_yoshi_fixture())
    by_yymm = {e.yymm: e for e in entries}
    assert by_yymm["2603"].reference_date == date(2026, 3, 1)
    assert by_yymm["2512"].reference_date == date(2025, 12, 1)
    assert by_yymm["2506"].reference_date == date(2025, 6, 1)


def test_schedule_parser_raises_on_malformed_date_cell() -> None:
    html = """
    <table><tbody>
    <tr>
      <td>garbage</td>
      <td><a href="/en/statistics/tk/yoshi/tk2603.htm">March 2026 Survey</a></td>
    </tr>
    </tbody></table>
    """
    with pytest.raises(TankanScheduleParseError):
        parse_tankan_schedule_html(html)


def test_schedule_parser_ignores_unrelated_anchors() -> None:
    """The yoshi page carries side-nav links into other BoJ
    statistics (money stock, CGPI, …). Only anchors matching the
    ``/tk/yoshi/tkYYMM.htm`` pattern count as Tankan release rows."""
    html = """
    <a href="/en/statistics/ms/index.htm">Money Stock</a>
    <a href="/en/about/release_2026/index.htm">Other Releases</a>
    <table><tbody>
    <tr>
      <td>Apr.&nbsp;&nbsp;1,&nbsp;2026</td>
      <td><a href="/en/statistics/tk/yoshi/tk2603.htm">March 2026 Survey</a></td>
    </tr>
    </tbody></table>
    """
    entries = parse_tankan_schedule_html(html)
    assert len(entries) == 1
    assert entries[0].yymm == "2603"


# ──────────────────────────────────────────────────────────────────────────
# schedule_entry_to_records
# ──────────────────────────────────────────────────────────────────────────


def _entry(
    *,
    release: date = date(2026, 4, 1),
    reference: date = date(2026, 3, 1),
    label: str = "March 2026 Survey",
    yymm: str = "2603",
) -> TankanScheduleEntry:
    return TankanScheduleEntry(
        release_date=release,
        reference_date=reference,
        reference_label=label,
        yymm=yymm,
        outline_url=build_outline_url(reference),
    )


def test_schedule_record_uses_0850_jst_convention() -> None:
    # 08:50 JST = 23:50 UTC of the prior day (JST has no DST).
    records = schedule_entry_to_records(_entry(), snapshot_epoch_ms=1_700_000_000)
    assert len(records) == 2
    for raw, event in records:
        assert event.event_time_precision == "datetime"
        assert event.event_time_utc.startswith("2026-03-31T23:50")


def test_schedule_record_emits_two_indicators_per_release() -> None:
    """Each release must generate one event per indicator so a later
    outline-side upgrade can upsert onto both rows independently."""
    records = schedule_entry_to_records(_entry(), snapshot_epoch_ms=1_700_000_000)
    titles = {event.title for _, event in records}
    assert "Tankan Large Manufacturers Index" in titles
    assert "Tankan Large Non-Manufacturers Index" in titles


def test_schedule_record_shape_is_schedule_only() -> None:
    entry = _entry()
    records = schedule_entry_to_records(entry, snapshot_epoch_ms=1_700_000_000)
    for raw, event in records:
        assert raw.provider == PROVIDER == "boj"
        assert event.country_code == "JP"
        # Diffusion index is points-based, not yen-denominated —
        # empty currency matches Conference Board / U Michigan / ISM
        # and keeps list_calendar_items currency filters honest.
        assert event.currency == ""
        assert event.source == "Bank of Japan"
        # Schedule source_url points at the per-release outline page,
        # not the yoshi index. When the value-side sweep re-seeds
        # schedule, the upsert rewrites source_url; keeping this URL
        # aligned with the value-side write prevents a historical
        # row's provenance from flipping back to the index page.
        assert event.source_url == entry.outline_url
        # Schedule-only — no value fields populated.
        assert event.actual is None
        assert event.previous is None
        assert event.forecast is None


def test_schedule_source_url_survives_value_side_seeded_resweep(
    store: SQLiteEngineStore,
) -> None:
    """Codex P2 round 2: every frequent sweep re-runs the schedule
    scrape to seed just-published releases. If the schedule write
    uses the yoshi index URL and the value write uses the outline
    URL, the source_url flips on every cron. Both sides must share
    the per-release outline URL so historical rows don't lose their
    canonical provenance."""
    html = _yoshi_fixture()
    with store._connection(commit=True) as conn:
        fetch_boj_tankan_calendar(
            conn, dry_run=False, html_fetcher=lambda: html,
            snapshot_epoch_ms=1_700_000_000,
        )
    # Fill actual for the March 2026 row through the outline path.
    value = parse_outline_html(
        _outline_fixture("tk2603.htm"),
        reference_date=date(2026, 3, 1),
    )
    out = outline_value_to_records(
        value,
        snapshot_epoch_ms=1_700_000_001,
        event_time_utc="2026-03-31T23:50:00+00:00",
    )
    with store._connection(commit=True) as conn:
        store_raw(conn, [r for r, _ in out])
        project_events(conn, [e for _, e in out])

    # Re-seed schedule (simulates the next cron sweep).
    with store._connection(commit=True) as conn:
        fetch_boj_tankan_calendar(
            conn, dry_run=False, html_fetcher=lambda: html,
            snapshot_epoch_ms=1_700_000_002,
        )

    # Every row for March 2026 must still carry the outline URL,
    # not the yoshi index URL.
    with store._connection(commit=False) as conn:
        rows = conn.execute(
            "SELECT source_url FROM cal_econ_event "
            "WHERE provider=? AND reference_date=?",
            (PROVIDER, "2026-03-01"),
        ).fetchall()
    expected = build_outline_url(date(2026, 3, 1))
    for (source_url,) in rows:
        assert source_url == expected, source_url


def test_schedule_record_provider_event_ids_are_indicator_distinct() -> None:
    """Large Mfg and Large Non-Mfg must get different ids even though
    they share the reference date — otherwise the outline upgrade
    would overwrite one indicator with the other."""
    records = schedule_entry_to_records(_entry(), snapshot_epoch_ms=1_700_000_000)
    ids = {event.provider_event_id for _, event in records}
    assert len(ids) == 2


def test_schedule_and_outline_ids_align_per_indicator() -> None:
    """Schedule-side id and outline-side id must match for the same
    ``(indicator, reference_date)`` so the value upsert lands on the
    existing schedule row rather than duplicating."""
    sched = {
        event.title: event.provider_event_id
        for _, event in schedule_entry_to_records(
            _entry(), snapshot_epoch_ms=1_700_000_000,
        )
    }
    value = OutlineValue(
        reference_date=date(2026, 3, 1),
        release_date=date(2026, 4, 1),
        large_mfg=SectorDI(
            sector="manufacturing",
            current=17, previous=16,
            forecast_prior=15, forecast_next=14,
        ),
        large_nonmfg=SectorDI(
            sector="nonmanufacturing",
            current=36, previous=36,
            forecast_prior=31, forecast_next=29,
        ),
    )
    outline_ids = {
        event.title: event.provider_event_id
        for _, event in outline_value_to_records(
            value,
            snapshot_epoch_ms=1_700_000_000,
            release_date=date(2026, 4, 1),
        )
    }
    assert sched == outline_ids


def test_schedule_content_hash_changes_when_release_date_slips() -> None:
    payload_a = {
        "release_date":   "2026-04-01",
        "reference_date": "2026-03-01",
        "event_time_utc": "2026-03-31T23:50:00+00:00",
    }
    payload_b = {**payload_a, "release_date": "2026-04-02"}
    assert _schedule_hash(payload_a) != _schedule_hash(payload_b)


# ──────────────────────────────────────────────────────────────────────────
# Outline parser
# ──────────────────────────────────────────────────────────────────────────


def test_outline_parser_extracts_current_quarter_di() -> None:
    value = parse_outline_html(
        _outline_fixture("tk2603.htm"),
        reference_date=date(2026, 3, 1),
    )
    # Large Enterprises, Manufacturing DI for March 2026 Survey = 17.
    assert value.large_mfg.current == 17
    assert value.large_mfg.previous == 16
    # Forecast made in the previous survey (Dec 2025) for this
    # quarter sits in the parens cell of row 1 — BoJ published (15).
    assert value.large_mfg.forecast_prior == 15
    # Large Non-Manufacturing DI for March 2026 Survey = 36.
    assert value.large_nonmfg.current == 36
    assert value.large_nonmfg.previous == 36
    assert value.large_nonmfg.forecast_prior == 31


def test_outline_parser_handles_previous_surveys() -> None:
    """The Large-Enterprises table shape must be stable across
    captured fixtures; a DOM change in one fixture but not another
    would break the parser silently without this check."""
    for name, ref, exp_mfg, exp_nonmfg in [
        ("tk2512.htm", date(2025, 12, 1), 15, 34),
        ("tk2503.htm", date(2025, 3, 1),  12, 35),
    ]:
        value = parse_outline_html(_outline_fixture(name), reference_date=ref)
        assert value.large_mfg.current == exp_mfg, name
        assert value.large_nonmfg.current == exp_nonmfg, name


def test_outline_parser_disambiguates_large_enterprises_section() -> None:
    """The outline page uses ``<h3>Large Enterprises</h3>`` under
    several ``<h2>`` sections (Business Conditions, Sales, Current
    Profits, …). The parser must pick the Business-Conditions table,
    not the later Sales table whose columns carry YoY growth rates."""
    value = parse_outline_html(
        _outline_fixture("tk2603.htm"),
        reference_date=date(2026, 3, 1),
    )
    # Business-Conditions Manufacturing = 17; Sales Manufacturing
    # FY 2025 number is ~1-2 percent — orders of magnitude different.
    assert value.large_mfg.current == 17


def test_outline_parser_raises_on_missing_large_enterprises_block() -> None:
    html = """
    <html><body>
      <h2>Number of Sample Enterprises</h2>
      <p>no business-conditions block on this page</p>
    </body></html>
    """
    with pytest.raises(TankanOutlineParseError):
        parse_outline_html(html, reference_date=date(2026, 3, 1))


def test_outline_parser_raises_on_unparseable_di_cells() -> None:
    # DOM drift where the DI cells are replaced with prose — the
    # parser must fail loud rather than stamping None onto a row.
    html = """
    <h2> Business Conditions</h2>
    <h3>Large Enterprises</h3>
    <table><tbody>
      <tr>
        <th rowspan="2">Manufacturing</th>
        <td>&nbsp;</td><td>TBD</td><td>&nbsp;</td><td>&nbsp;</td><td>&nbsp;</td>
      </tr>
      <tr>
        <td>pending</td><td>pending</td><td>-</td><td>-</td><td>-</td>
      </tr>
      <tr>
        <th rowspan="2">Nonmanufacturing</th>
        <td>&nbsp;</td><td>&nbsp;</td><td>&nbsp;</td><td>&nbsp;</td><td>&nbsp;</td>
      </tr>
      <tr>
        <td>pending</td><td>pending</td><td>-</td><td>-</td><td>-</td>
      </tr>
    </tbody></table>
    """
    with pytest.raises(TankanOutlineParseError):
        parse_outline_html(html, reference_date=date(2026, 3, 1))


# ──────────────────────────────────────────────────────────────────────────
# outline_value_to_records
# ──────────────────────────────────────────────────────────────────────────


def test_outline_record_fills_actual_previous_forecast() -> None:
    value = parse_outline_html(
        _outline_fixture("tk2603.htm"),
        reference_date=date(2026, 3, 1),
    )
    records = outline_value_to_records(
        value,
        snapshot_epoch_ms=1_700_000_000,
        release_date=date(2026, 4, 1),
    )
    by_title = {event.title: event for _, event in records}
    mfg = by_title["Tankan Large Manufacturers Index"]
    assert mfg.actual == "17"
    assert mfg.previous == "16"
    assert mfg.forecast == "15"
    assert mfg.source_url == build_outline_url(date(2026, 3, 1))
    # Diffusion-index rows must not carry a currency on the value side
    # either — a JPY stamp would slip into downstream currency filters.
    assert mfg.currency == ""


def test_outline_record_accepts_event_time_override() -> None:
    """Value-side auto-discovery passes the schedule-side
    ``event_time_utc`` through verbatim so the upsert preserves the
    canonical publish stamp when the outline page has no
    release-time block of its own."""
    value = parse_outline_html(
        _outline_fixture("tk2603.htm"),
        reference_date=date(2026, 3, 1),
    )
    override = "2026-03-31T23:50:00+00:00"
    records = outline_value_to_records(
        value,
        snapshot_epoch_ms=1_700_000_000,
        event_time_utc=override,
    )
    for _, event in records:
        assert event.event_time_utc == override


def test_outline_record_falls_back_when_release_date_omitted() -> None:
    """The projector must still emit a sensible datetime if the
    caller skips ``release_date`` — the fallback is the first day
    of the month following the reference quarter, which matches
    Tankan's historical release pattern."""
    value = parse_outline_html(
        _outline_fixture("tk2512.htm"),
        reference_date=date(2025, 12, 1),
    )
    records = outline_value_to_records(
        value,
        snapshot_epoch_ms=1_700_000_000,
    )
    # December survey fallback: reference 2025-12-01 → release
    # 2026-01-01 → event_time 2025-12-31T23:50:00+00:00.
    for _, event in records:
        assert event.event_time_utc.startswith("2025-12-31T23:50")


def test_outline_content_hash_changes_on_di_revision() -> None:
    """Raw revision model must capture DI-value corrections. Rate
    alone isn't enough — we also hash on event_time_utc so a later
    schedule-side re-stamp creates a new audit row."""
    payload_a = {
        "indicator": "TANKAN_LARGE_MFG",
        "current": 17, "previous": 16, "forecast_prior": 15,
        "event_time_utc": "2026-03-31T23:50:00+00:00",
    }
    payload_b = {**payload_a, "current": 18}
    assert _outline_hash(payload_a) != _outline_hash(payload_b)


# ──────────────────────────────────────────────────────────────────────────
# Projector
# ──────────────────────────────────────────────────────────────────────────


def test_store_raw_is_idempotent(store: SQLiteEngineStore) -> None:
    records = schedule_entry_to_records(_entry(), snapshot_epoch_ms=1_700_000_000)
    raws = [r for r, _ in records]
    with store._connection(commit=True) as conn:
        first = store_raw(conn, raws)
        second = store_raw(conn, raws)
    assert first == 2
    assert second == 0


def test_outline_upserts_actual_onto_schedule_rows(
    store: SQLiteEngineStore,
) -> None:
    # First write the two schedule rows for March 2026 with actual=NULL.
    sched = schedule_entry_to_records(_entry(), snapshot_epoch_ms=1_700_000_000)
    with store._connection(commit=True) as conn:
        store_raw(conn, [r for r, _ in sched])
        project_schedule_events(conn, [e for _, e in sched])

    # Outline scrape upserts DI values onto the same rows.
    value = parse_outline_html(
        _outline_fixture("tk2603.htm"),
        reference_date=date(2026, 3, 1),
    )
    out = outline_value_to_records(
        value,
        snapshot_epoch_ms=1_700_000_001,
        event_time_utc="2026-03-31T23:50:00+00:00",
    )
    with store._connection(commit=True) as conn:
        store_raw(conn, [r for r, _ in out])
        changed = project_events(conn, [e for _, e in out])
    assert changed == 2

    with store._connection(commit=False) as conn:
        rows = conn.execute(
            "SELECT title, actual, previous, forecast, event_time_precision "
            "FROM cal_econ_event WHERE provider=? AND reference_date=? "
            "ORDER BY title",
            (PROVIDER, "2026-03-01"),
        ).fetchall()
    assert len(rows) == 2
    by_title = {r[0]: r for r in rows}
    assert by_title["Tankan Large Manufacturers Index"][1:4] == ("17", "16", "15")
    assert by_title["Tankan Large Non-Manufacturers Index"][1:4] == ("36", "36", "31")
    for r in rows:
        assert r[4] == "datetime"


# ──────────────────────────────────────────────────────────────────────────
# Fetcher
# ──────────────────────────────────────────────────────────────────────────


def test_fetch_dry_run_returns_indicator_plan(store: SQLiteEngineStore) -> None:
    with store._connection(commit=False) as conn:
        summary = fetch_boj_tankan_calendar(conn, dry_run=True)
    assert summary.dry_run is True
    assert summary.indicators_planned == list(ALL_INDICATORS)
    assert summary.releases_parsed == 0


def test_fetch_projects_fixture_into_events(store: SQLiteEngineStore) -> None:
    html = _yoshi_fixture()

    with store._connection(commit=True) as conn:
        summary = fetch_boj_tankan_calendar(
            conn, dry_run=False, html_fetcher=lambda: html,
            snapshot_epoch_ms=1_700_000_000,
        )
    assert summary.dry_run is False
    assert summary.releases_parsed == 12
    # Two rows per release: Large Mfg + Large Non-Mfg.
    assert summary.rows_raw_inserted == 24
    assert summary.events_upserted == 24
    with store._connection(commit=False) as conn:
        rows = conn.execute(
            "SELECT COUNT(*) FROM cal_econ_event "
            "WHERE provider=? AND title LIKE 'Tankan Large %'",
            (PROVIDER,),
        ).fetchone()[0]
    assert rows == 24


def test_fetch_raises_when_parse_yields_zero_releases(
    store: SQLiteEngineStore,
) -> None:
    with store._connection(commit=True) as conn:
        with pytest.raises(TankanScheduleParseError):
            fetch_boj_tankan_calendar(
                conn, dry_run=False,
                html_fetcher=lambda: "<html><body>Access Denied</body></html>",
                snapshot_epoch_ms=1_700_000_000,
            )


def test_fetch_values_discovers_pending_rows(store: SQLiteEngineStore) -> None:
    """Schedule write first, then auto-discovery in dry-run mode must
    return the discovered references without hitting any outline page."""
    html = _yoshi_fixture()
    with store._connection(commit=True) as conn:
        fetch_boj_tankan_calendar(
            conn, dry_run=False, html_fetcher=lambda: html,
            snapshot_epoch_ms=1_700_000_000,
        )

    # Snapshot far in the future so every schedule row counts as past.
    far_future_ms = 4_000_000_000_000
    with store._connection(commit=False) as conn:
        summary = fetch_boj_tankan_outlines(
            conn, dry_run=True, snapshot_epoch_ms=far_future_ms,
        )
    # Twelve releases × two indicators = 24 rows, but auto-discovery
    # deduplicates to release-level — one outline fetch per release.
    assert summary.releases_planned == 12


def test_fetch_values_respects_release_buffer(store: SQLiteEngineStore) -> None:
    """Auto-discovery must not queue a release whose scheduled event
    time is less than 1h in the past. Parallels the BoJ MPM 1h
    buffer so a cron sweep that fires on release day before the
    outline page is live doesn't trip the circuit breaker."""
    html = _yoshi_fixture()
    with store._connection(commit=True) as conn:
        fetch_boj_tankan_calendar(
            conn, dry_run=False, html_fetcher=lambda: html,
            snapshot_epoch_ms=1_700_000_000,
        )

    # March 2026 Survey publishes 2026-04-01 08:50 JST
    # = 2026-03-31T23:50:00+00:00.
    # "Just after 08:50 JST" poll: as_of = 2026-04-01T00:30:00+00:00
    # = 09:30 JST (40 min post-release, < 1h buffer).
    too_early = int(datetime(
        2026, 4, 1, 0, 30, tzinfo=timezone.utc,
    ).timestamp() * 1000)
    with store._connection(commit=False) as conn:
        early_summary = fetch_boj_tankan_outlines(
            conn, dry_run=True, snapshot_epoch_ms=too_early,
        )
    # Every release is still inside the 1h buffer window or older —
    # the Apr 2026 row is blocked, the earlier rows (already well
    # past 1h) remain.
    assert early_summary.releases_planned == 11

    # "1h+ after release" poll: as_of = 2026-04-01T01:01:00+00:00
    # = 10:01 JST, past the 1h buffer — Apr 2026 row admitted.
    safe = int(datetime(
        2026, 4, 1, 1, 1, tzinfo=timezone.utc,
    ).timestamp() * 1000)
    with store._connection(commit=False) as conn:
        safe_summary = fetch_boj_tankan_outlines(
            conn, dry_run=True, snapshot_epoch_ms=safe,
        )
    assert safe_summary.releases_planned == 12


def test_fetch_values_passes_event_time_through_to_upsert(
    store: SQLiteEngineStore,
) -> None:
    """End-to-end: schedule write → outline fetch → upsert must
    preserve the schedule-side event_time on the final row."""
    html = _yoshi_fixture()
    with store._connection(commit=True) as conn:
        fetch_boj_tankan_calendar(
            conn, dry_run=False, html_fetcher=lambda: html,
            snapshot_epoch_ms=1_700_000_000,
        )

    fixture_map = {
        date(2026, 3, 1):  _outline_fixture("tk2603.htm"),
        date(2025, 12, 1): _outline_fixture("tk2512.htm"),
        date(2025, 3, 1):  _outline_fixture("tk2503.htm"),
    }

    def _local_fetcher(ref: date) -> str:
        return fixture_map.get(ref, _outline_fixture("tk2603.htm"))

    far_future_ms = 4_000_000_000_000
    with store._connection(commit=True) as conn:
        summary = fetch_boj_tankan_outlines(
            conn, dry_run=False,
            snapshot_epoch_ms=far_future_ms,
            reference_dates=[date(2026, 3, 1)],
            html_fetcher=_local_fetcher,
        )
    assert summary.releases_fetched == 1
    assert summary.events_upserted == 2
    with store._connection(commit=False) as conn:
        rows = conn.execute(
            "SELECT title, actual, event_time_utc FROM cal_econ_event "
            "WHERE provider=? AND reference_date=? ORDER BY title",
            (PROVIDER, "2026-03-01"),
        ).fetchall()
    by_title = {r[0]: r for r in rows}
    assert by_title["Tankan Large Manufacturers Index"][1] == "17"
    assert by_title["Tankan Large Manufacturers Index"][2].startswith(
        "2026-03-31T23:50"
    )
    assert by_title["Tankan Large Non-Manufacturers Index"][1] == "36"


def test_fetch_values_preserves_schedule_event_time_on_manual_replay(
    store: SQLiteEngineStore,
) -> None:
    """Codex P2 round 1: December survey releases mid-month (Dec 15)
    rather than on the first of the following month. A manual
    ``reference_dates`` replay must carry the schedule-side
    ``event_time_utc`` through so the upsert preserves the stored
    row — otherwise the fallback writer would stamp Jan 1, shifting
    the date-ordered row on every replay."""
    html = _yoshi_fixture()
    with store._connection(commit=True) as conn:
        fetch_boj_tankan_calendar(
            conn, dry_run=False, html_fetcher=lambda: html,
            snapshot_epoch_ms=1_700_000_000,
        )
    # Baseline: the schedule write stamped Dec 2025 row at
    # 2025-12-14T23:50:00+00:00 (Dec 15 JST 08:50 → prior-day UTC).
    with store._connection(commit=False) as conn:
        stored = conn.execute(
            "SELECT event_time_utc FROM cal_econ_event "
            "WHERE provider=? AND reference_date=? LIMIT 1",
            (PROVIDER, "2025-12-01"),
        ).fetchone()
    assert stored is not None
    assert stored[0].startswith("2025-12-14T23:50")

    fixture_map = {date(2025, 12, 1): _outline_fixture("tk2512.htm")}

    def _local_fetcher(ref: date) -> str:
        return fixture_map[ref]

    with store._connection(commit=True) as conn:
        fetch_boj_tankan_outlines(
            conn, dry_run=False,
            snapshot_epoch_ms=1_700_000_001,
            reference_dates=[date(2025, 12, 1)],
            html_fetcher=_local_fetcher,
        )
    with store._connection(commit=False) as conn:
        rows = conn.execute(
            "SELECT event_time_utc FROM cal_econ_event "
            "WHERE provider=? AND reference_date=? "
            "ORDER BY title",
            (PROVIDER, "2025-12-01"),
        ).fetchall()
    # Both indicators must keep the schedule-side Dec 14 UTC stamp —
    # if the override is dropped, the fallback path would resolve to
    # 2026-01-01 and shift the rows by 17 days.
    for (event_time,) in rows:
        assert event_time.startswith("2025-12-14T23:50"), event_time


def test_fetch_values_manual_replay_against_unseeded_reference_falls_back(
    store: SQLiteEngineStore,
) -> None:
    """An ad-hoc replay for a reference that was never seeded
    (schedule side hasn't run) must still project a plausible
    datetime via the release-day fallback rather than raising."""
    fixture_map = {date(2026, 3, 1): _outline_fixture("tk2603.htm")}

    def _local_fetcher(ref: date) -> str:
        return fixture_map[ref]

    with store._connection(commit=True) as conn:
        summary = fetch_boj_tankan_outlines(
            conn, dry_run=False,
            snapshot_epoch_ms=1_700_000_000,
            reference_dates=[date(2026, 3, 1)],
            html_fetcher=_local_fetcher,
        )
    assert summary.releases_fetched == 1
    assert summary.events_upserted == 2
    with store._connection(commit=False) as conn:
        rows = conn.execute(
            "SELECT event_time_utc FROM cal_econ_event "
            "WHERE provider=? AND reference_date=?",
            (PROVIDER, "2026-03-01"),
        ).fetchall()
    # Fallback release day is first-of-next-month (2026-04-01) at
    # 08:50 JST → 2026-03-31T23:50 UTC.
    for (event_time,) in rows:
        assert event_time.startswith("2026-03-31T23:50"), event_time


def test_scheduler_value_side_seeds_schedule_before_discovery(
    store: SQLiteEngineStore,
) -> None:
    """Codex P1 round 1: yoshi/index.htm is past-only, so the frequent
    value-side sweep must re-run the schedule scrape before
    auto-discovery — otherwise a newly-published release (whose row
    isn't in ``cal_econ_event`` yet) stays invisible until the next
    daily refresh."""
    from ingestion.calendar.scheduler import sweep_value_side

    calls: list[str] = []

    def _fake_schedule(conn, dry_run):
        calls.append("schedule")
        # Write a fake schedule row exposing the Tankan title the
        # auto-discovery path keys off, with actual=NULL and an
        # event_time already well in the past.
        conn.execute(
            "INSERT INTO cal_econ_event "
            "(provider, provider_event_id, event_time_utc, event_time_precision, "
            " reference_date, reference_label, country_code, indicator_id, "
            " category, title, importance, currency, unit, "
            " actual, previous, revised, forecast, consensus_forecast, "
            " ticker, source, source_url, content_hash, "
            " last_update_epoch_ms, observed_at_epoch_ms, created_at, updated_at) "
            "VALUES ('boj', 'synthetic', '2020-01-01T00:00:00+00:00', 'datetime', "
            "        '2020-01-01', 'Stub Survey', 'JP', NULL, "
            "        'Business Survey', 'Tankan Large Manufacturers Index', "
            "        'high', 'JPY', 'points', "
            "        NULL, NULL, NULL, NULL, NULL, "
            "        '', 'Bank of Japan', 'https://example.test/', 'h', "
            "        NULL, 1, '2020-01-01', '2020-01-01')"
        )

    def _fake_outlines(conn, dry_run):
        calls.append("outlines")
        from ingestion.calendar.boj_tankan_api.fetcher import (
            OutlineValuesRunSummary,
            _discover_pending_references,
        )
        as_of = "2030-01-01T00:00:00+00:00"
        discovered = _discover_pending_references(conn, as_of_utc_iso=as_of)
        summary = OutlineValuesRunSummary(
            indicators_planned=[], dry_run=dry_run,
            releases_planned=len(discovered),
        )
        return summary

    import ingestion.calendar.scheduler as scheduler_module
    monkey_schedule = scheduler_module.fetch_boj_tankan_calendar
    monkey_outlines = scheduler_module.fetch_boj_tankan_outlines
    scheduler_module.fetch_boj_tankan_calendar = _fake_schedule
    scheduler_module.fetch_boj_tankan_outlines = _fake_outlines
    try:
        sweep_value_side(
            store.get_connection,
            dry_run=False,
            connectors=["boj-tankan-values"],
        )
    finally:
        scheduler_module.fetch_boj_tankan_calendar = monkey_schedule
        scheduler_module.fetch_boj_tankan_outlines = monkey_outlines

    # Schedule seed must run first so auto-discovery can see the
    # just-written row.
    assert calls == ["schedule", "outlines"]


def test_total_outage_detection_covers_releases_counters() -> None:
    """Codex P2 round 2: the circuit breaker only tripped on
    ``meetings_planned`` / ``meetings_fetched``. Tankan names the
    counters ``releases_*``, so a run where every outline URL 404s
    would flag ok=False but bypass the 15-minute cool-down. The
    detection must recognise either pair."""
    from ingestion.calendar.scheduler import _summary_is_total_outage
    from ingestion.calendar.boj_tankan_api.fetcher import OutlineValuesRunSummary

    every_outline_404 = OutlineValuesRunSummary(
        indicators_planned=list(ALL_INDICATORS),
        dry_run=False,
        releases_planned=3,
        releases_fetched=0,
        fetch_failures=[("2026-03-01", "404"), ("2025-12-01", "404")],
    )
    assert _summary_is_total_outage(every_outline_404) is True

    # Partial run where some outlines landed must not trip the breaker.
    partial = OutlineValuesRunSummary(
        indicators_planned=list(ALL_INDICATORS),
        dry_run=False,
        releases_planned=3,
        releases_fetched=2,
        fetch_failures=[("2025-12-01", "503")],
    )
    assert _summary_is_total_outage(partial) is False


def test_service_op_dry_run_returns_plan(store: SQLiteEngineStore) -> None:
    from macro_data.service import LocalMacroDataService
    svc = LocalMacroDataService(store=store)
    result = svc.invoke("calendar_econ_fetch_boj_tankan", {"dry_run": True})
    assert result["dry_run"] is True
    assert result["stopped_reason"] == "dry_run"
    assert result["indicators_planned"] == list(ALL_INDICATORS)


def test_service_op_values_dry_run_returns_plan(store: SQLiteEngineStore) -> None:
    from macro_data.service import LocalMacroDataService
    svc = LocalMacroDataService(store=store)
    result = svc.invoke(
        "calendar_econ_fetch_boj_tankan_values", {"dry_run": True},
    )
    assert result["dry_run"] is True
    assert result["stopped_reason"] == "dry_run"
    assert result["indicators_planned"] == list(ALL_INDICATORS)

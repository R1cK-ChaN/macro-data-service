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


def test_registry_ships_p5c_whitelist() -> None:
    assert set(INDICATOR_REGISTRY.keys()) == {
        "CPI",
        "PPI",
        "INDUSTRIAL_PRODUCTION",
        "FIXED_ASSET_INVESTMENT",
        "RETAIL_SALES",
        "MANUFACTURING_PMI",
        "NON_MANUFACTURING_PMI",
        "GDP",
    }
    for spec in INDICATOR_REGISTRY.values():
        assert spec.country_code == "CN"
        assert spec.importance in {"low", "medium", "high"}
        assert spec.label_fragment and spec.label_fragment == spec.label_fragment.lower()
    # The two PMI specs share a label_fragment so the scraper emits
    # both events on every PMI release date.
    assert (
        INDICATOR_REGISTRY["MANUFACTURING_PMI"].label_fragment
        == INDICATOR_REGISTRY["NON_MANUFACTURING_PMI"].label_fragment
    )
    # GDP is the one quarterly-cadence spec — shares the "National
    # Economic Performance" row with monthly roll-ups, filtered down
    # to {1, 4, 7, 10}.
    gdp = INDICATOR_REGISTRY["GDP"]
    assert gdp.reference_cadence == "quarterly"
    assert gdp.publishing_months == frozenset({1, 4, 7, 10})
    assert gdp.label_fragment == "national economic performance"


# ──────────────────────────────────────────────────────────────────────────
# parse_nbs_calendar_html
# ──────────────────────────────────────────────────────────────────────────


def test_parse_2026_calendar_extracts_whitelist_releases() -> None:
    entries = parse_nbs_calendar_html(_fixture_html("nbs_2026.html"))
    # CPI + PPI fire every month (12 each), Industrial Production /
    # Fixed Asset Investment / Retail Sales skip Feb (11 each due to
    # the Spring Festival combine-into-March adjustment). PMI's Feb
    # release gets pushed into March with Note5, so the March cell
    # carries two dates (``"4/Wed Note5 31/Tue"``) — the parser emits
    # both, so Manufacturing PMI and Non-Manufacturing PMI each land
    # 12 entries for the year (the delayed Feb release + the regular
    # Mar 31 release across 11 months).
    by_indicator: dict[str, int] = {}
    for entry in entries:
        by_indicator[entry.indicator] = by_indicator.get(entry.indicator, 0) + 1
    assert by_indicator == {
        "CPI": 12,
        "PPI": 12,
        "INDUSTRIAL_PRODUCTION": 11,
        "FIXED_ASSET_INVESTMENT": 11,
        "RETAIL_SALES": 11,
        "MANUFACTURING_PMI": 12,
        "NON_MANUFACTURING_PMI": 12,
        # GDP rides on the "National Economic Performance" row, which
        # lists 11 release dates (every month except Feb) — but the
        # quarterly-month filter keeps only Jan / Apr / Jul / Oct.
        "GDP": 4,
    }
    for entry in entries:
        assert entry.year == 2026
        assert entry.release_time_local in {"9:30", "10:00"}


def test_parse_gdp_fires_only_on_quarterly_months() -> None:
    """The "National Economic Performance" row publishes on 11 months
    (every month except Feb), but only Jan / Apr / Jul / Oct carry
    the quarterly GDP release — the other seven are monthly roll-ups
    the NBS 2026 calendar note 3 describes as a separate cadence.
    The GDP spec's ``publishing_months`` filter keeps this from
    over-emitting."""
    entries = parse_nbs_calendar_html(_fixture_html("nbs_2026.html"))
    gdp_months = sorted(e.month for e in entries if e.indicator == "GDP")
    assert gdp_months == [1, 4, 7, 10]


def test_parse_gdp_projects_end_of_prior_quarter_reference() -> None:
    """GDP carries ``reference_cadence="quarterly"`` so the projector
    anchors each event on the end-of-prior-quarter date. Jan release
    → prior-year Q4; Apr / Jul / Oct → the current year's Q1 / Q2 /
    Q3. The ``reference_label`` switches from ``"YYYY-MM"`` to
    ``"YYYY-QN"`` so downstream parity buckets compare against TE's
    quarterly convention cleanly."""
    entries = parse_nbs_calendar_html(_fixture_html("nbs_2026.html"))
    gdp_entries = sorted(
        (e for e in entries if e.indicator == "GDP"),
        key=lambda e: e.month,
    )
    from ingestion.calendar.nbs_api import release_entry_to_records

    projected = [
        release_entry_to_records(e, snapshot_epoch_ms=0)[1]
        for e in gdp_entries
    ]
    assert [(p.reference_date, p.reference_label) for p in projected] == [
        ("2025-12-31", "2025-Q4"),   # Jan 2026 release → Q4 2025
        ("2026-03-31", "2026-Q1"),   # Apr 2026 release → Q1 2026
        ("2026-06-30", "2026-Q2"),   # Jul 2026 release → Q2 2026
        ("2026-09-30", "2026-Q3"),   # Oct 2026 release → Q3 2026
    ]


def test_china_gdp_canonicalizes_to_gdp_alias() -> None:
    """Codex P2 on 2026-04-22 — the parity harness canonicalizes every
    ``cal_econ_event.title`` before bucketing; without this alias the
    four NBS GDP rows would land in their own ``"china gdp"`` bucket
    instead of joining TE's ``"GDP"`` rows for the same reference
    quarter. Lock the alias down here so a future drop from the
    shared table raises a loud regression instead of a silent
    parity-harness miss."""
    from ingestion.calendar._official_shared import canonicalize_indicator

    assert canonicalize_indicator("China GDP") == "GDP"
    # The NBS spec's ``title`` is the input the projector writes into
    # ``cal_econ_event.title`` and the parity harness reads back.
    assert canonicalize_indicator(INDICATOR_REGISTRY["GDP"].title) == "GDP"


def test_every_nbs_registry_title_canonicalizes_to_shared_token() -> None:
    """Every NBS indicator's ``title`` prefixes ``"China "`` for display;
    the parity harness canonicalizes each row's title before
    bucketing. If any title lacks an alias, the NBS rows for that
    indicator would bucket separately from TE's rows and surface as
    spurious official-only gaps. Walk the registry end-to-end so the
    invariant stays locked as new indicators land — adding a new NBS
    spec without a matching alias fails here, not silently in the
    parity report weeks later."""
    from ingestion.calendar._official_shared import canonicalize_indicator

    expected_tokens = {
        "CPI":                    "CPI",
        "PPI":                    "PPI",
        "INDUSTRIAL_PRODUCTION":  "INDUSTRIAL_PRODUCTION",
        "FIXED_ASSET_INVESTMENT": "FIXED_ASSET_INVESTMENT",
        "RETAIL_SALES":           "RETAIL_SALES",
        "MANUFACTURING_PMI":      "MFG_PMI",
        "NON_MANUFACTURING_PMI":  "NON_MFG_PMI",
        "GDP":                    "GDP",
    }
    assert set(expected_tokens) == set(INDICATOR_REGISTRY)
    for key, expected in expected_tokens.items():
        title = INDICATOR_REGISTRY[key].title
        canonical = canonicalize_indicator(title)
        assert canonical == expected, (
            f"NBS spec {key!r} title {title!r} canonicalizes to "
            f"{canonical!r} — expected {expected!r}. Add a "
            f"'{title.lower()}': '{expected}' entry to _ALIASES in "
            "ingestion.calendar._official_shared.canonicalize."
        )


def test_parse_monthly_indicators_unchanged_by_gdp_addition() -> None:
    """CPI / PPI and the other monthly indicators still use
    release-month-first reference dates and ``"YYYY-MM"`` labels —
    the GDP quarterly branch is gated on the spec's
    ``reference_cadence``, so monthly specs land exactly as before."""
    entries = parse_nbs_calendar_html(_fixture_html("nbs_2026.html"))
    january_cpi = next(
        e for e in entries if e.indicator == "CPI" and e.month == 1
    )
    from ingestion.calendar.nbs_api import release_entry_to_records

    _, event = release_entry_to_records(january_cpi, snapshot_epoch_ms=0)
    assert event.reference_date == "2026-01-01"
    assert event.reference_label == "2026-01"


def test_parse_pmi_march_cell_emits_both_spring_festival_and_regular_dates() -> None:
    entries = parse_nbs_calendar_html(_fixture_html("nbs_2026.html"))
    march_mpmi = sorted(
        (e.day for e in entries if e.month == 3 and e.indicator == "MANUFACTURING_PMI")
    )
    # NBS 2026 March PMI cell: "4/Wed Note5 31/Tue" — Feb's release
    # pushed forward to Mar 4 plus the regular Mar 31 release.
    assert march_mpmi == [4, 31]


def test_parse_cpi_carries_day_and_weekday() -> None:
    entries = parse_nbs_calendar_html(_fixture_html("nbs_2026.html"))
    january_cpi = next(
        e for e in entries if e.month == 1 and e.indicator == "CPI"
    )
    # NBS 2026 CPI: 9 January 2026 (Friday).
    assert january_cpi.day == 9
    assert january_cpi.weekday_label.lower().startswith("fri")


def test_parse_pmi_row_emits_both_manufacturing_and_non_manufacturing() -> None:
    entries = parse_nbs_calendar_html(_fixture_html("nbs_2026.html"))
    january_pmi = [
        e for e in entries
        if e.month == 1
        and e.indicator in {"MANUFACTURING_PMI", "NON_MANUFACTURING_PMI"}
    ]
    assert {e.indicator for e in january_pmi} == {
        "MANUFACTURING_PMI", "NON_MANUFACTURING_PMI",
    }
    # Both entries land on the same release day.
    assert {e.day for e in january_pmi} == {january_pmi[0].day}


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
            _entry(indicator="NOT_A_REAL_INDICATOR"),
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
    # 12 CPI + 12 PPI + 11 Industrial Production + 11 Fixed Asset
    # Investment + 11 Retail Sales + 12×2 PMI + 4 GDP = 85 entries.
    # PMI lands 12 per spec because the March cell carries both the
    # Spring-Festival-delayed Feb release and the regular Mar 31
    # release. GDP lands 4 entries from the quarterly-month filter on
    # the "National Economic Performance" row.
    expected_entries = 85
    assert captured_url == [FIXTURE_CALENDAR_URL]
    assert summary.entries_parsed == expected_entries
    assert summary.rows_raw_inserted == expected_entries
    assert summary.events_upserted == expected_entries
    with store._connection(commit=False) as conn:
        rows = conn.execute(
            "SELECT COUNT(*) FROM cal_econ_event WHERE provider=?", (PROVIDER,),
        ).fetchone()[0]
    assert rows == expected_entries


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
    assert result["indicators_planned"] == list(INDICATOR_REGISTRY.keys())


def test_service_op_execute_requires_calendar_url(
    store: SQLiteEngineStore,
) -> None:
    from macro_data.service import LocalMacroDataService

    svc = LocalMacroDataService(store=store)
    result = svc.invoke("calendar_econ_fetch_nbs", {"dry_run": False})
    assert "error" in result

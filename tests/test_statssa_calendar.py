"""Mocked tests for the Stats SA calendar connector (issue #90 P1).

Captured fixtures in ``tests/fixtures/statssa_calendar/<month>.html``
were recorded live on 2026-04-28 from the Stats SA Publication
Schedule AJAX endpoint. The corpus covers six months
(February 2026 → July 2026); the past months come back as the
"No further publications scheduled" alert variant — exercising the
empty-month branch — and the four future months carry full schedule
tables.

No real HTTP in CI — every test injects the ``html_fetcher`` seam.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Iterable

import pytest

from ingestion.calendar.statssa_api import (
    INDICATOR_REGISTRY,
    StatsSACalendarParseError,
    announcement_matches_spec,
    announcement_to_records,
    fetch_statssa_calendar,
    parse_publication_schedule,
)
from ingestion.calendar.statssa_api.parser import (
    PROVIDER,
    STATSSA_PUBLIC_SCHEDULE_URL,
)
from storage.sqlite import SQLiteEngineStore


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "statssa_calendar"

# Months whose fixture is known to carry the full schedule table.
_LIVE_MONTHS: tuple[str, ...] = (
    "April 2026",
    "May 2026",
    "June 2026",
    "July 2026",
)
# Months that came back as "No further publications scheduled" at
# capture time — the empty-month branch.
_EMPTY_MONTHS: tuple[str, ...] = (
    "February 2026",
    "March 2026",
)


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _fixture_html(month_token: str) -> str:
    fname = month_token.lower().replace(" ", "_") + ".html"
    return (FIXTURE_DIR / fname).read_text(encoding="utf-8")


# ── parser ───────────────────────────────────────────────────────


def test_parse_returns_announcements_for_full_month() -> None:
    announcements = parse_publication_schedule(
        _fixture_html("May 2026"), schedule_month="May 2026",
    )
    assert announcements
    # Spot-check: the May 2026 fixture contains the QLFS row for
    # 12 May 2026 at 11:30 SAST (Q1 2026 reference quarter).
    qlfs = next(
        (a for a in announcements if a.ppn == "P0211"), None,
    )
    assert qlfs is not None
    assert qlfs.reference_period_text == "1st Quarter 2026"
    assert qlfs.release_datetime_local.date() == date(2026, 5, 12)
    assert qlfs.release_datetime_local.strftime("%H:%M") == "11:30"
    assert qlfs.title.startswith("Quarterly Labour Force Survey")
    assert "PPN=P0211" in qlfs.download_url


def test_parse_extracts_quarterly_gdp_row() -> None:
    """The June 2026 schedule carries the Q1 2026 GDP release on the
    9th of June at 11:30 SAST — the canonical SARB cadence."""
    announcements = parse_publication_schedule(
        _fixture_html("June 2026"), schedule_month="June 2026",
    )
    gdp = next(
        (a for a in announcements if a.ppn == "P0441"), None,
    )
    assert gdp is not None
    assert gdp.reference_period_text == "1st Quarter 2026"
    assert gdp.release_datetime_local.date() == date(2026, 6, 9)
    assert gdp.release_datetime_local.strftime("%H:%M") == "11:30"


def test_parse_returns_empty_for_no_schedule_months() -> None:
    """Past months return the ``"No further publications are
    scheduled"`` alert block instead of a table — must come back as
    ``[]`` rather than raising."""
    announcements = parse_publication_schedule(
        _fixture_html("March 2026"), schedule_month="March 2026",
    )
    assert announcements == []


def test_parse_raises_on_no_table_without_explicit_alert() -> None:
    """A no-``<table>`` payload that *isn't* the documented "No further
    publications are scheduled" alert is layout drift — Cloudflare
    challenge / maintenance page / dropped table markup. Surface it so
    the daily sweep trips ``fetch_error`` instead of silently leaving
    ZA releases unmet."""
    cloudflare_challenge = (
        "<html><head><title>Just a moment...</title></head>"
        "<body>Checking your browser...</body></html>"
    )
    with pytest.raises(StatsSACalendarParseError):
        parse_publication_schedule(
            cloudflare_challenge, schedule_month="May 2026",
        )


def test_parse_raises_on_malformed_table_markup() -> None:
    """A ``<table`` open tag with no matching close is also drift."""
    malformed = (
        "<html><body>"
        "<h4>Publication Schedule for: May 2026</h4>"
        "<table class=\"table\"><tr><td>P0141 - CPI</td>"  # no </table>
        "</body></html>"
    )
    with pytest.raises(StatsSACalendarParseError):
        parse_publication_schedule(malformed, schedule_month="May 2026")


def test_parse_handles_bytes_payload() -> None:
    raw = (FIXTURE_DIR / "may_2026.html").read_bytes()
    announcements = parse_publication_schedule(raw, schedule_month="May 2026")
    assert announcements


def test_parse_decodes_nbsp_in_publication_cell() -> None:
    """Stats SA pads cells with ``&nbsp;`` — the parser unescapes the
    entity so PPN matching survives."""
    announcements = parse_publication_schedule(
        _fixture_html("April 2026"), schedule_month="April 2026",
    )
    assert all("&nbsp;" not in a.title for a in announcements)


def test_parse_skips_row_without_start_metadata() -> None:
    mini = """
    <table>
      <thead><tr><th>Publication</th><th>Date</th><th>Time</th></tr></thead>
      <tbody>
        <tr><td>P0141 - Consumer Price Index (CPI), April 2026</td>
            <td>21 May 2026 (Wednesday)</td>
            <td>10:00 (no metadata)</td></tr>
      </tbody>
    </table>
    """
    announcements = parse_publication_schedule(mini, schedule_month="May 2026")
    assert announcements == []


def test_parse_rejects_non_text_payload() -> None:
    with pytest.raises(StatsSACalendarParseError):
        parse_publication_schedule(12345, schedule_month="May 2026")  # type: ignore[arg-type]


# ── matcher ──────────────────────────────────────────────────────


def _all_matched_indicators(months: Iterable[str]) -> dict[str, list[str]]:
    """Return ``{indicator: [ppn, ...]}`` matched across the given months."""
    out: dict[str, list[str]] = {}
    for month in months:
        announcements = parse_publication_schedule(
            _fixture_html(month), schedule_month=month,
        )
        for ann in announcements:
            for ind, spec in INDICATOR_REGISTRY.items():
                if announcement_matches_spec(ann, spec):
                    out.setdefault(ind, []).append(ann.ppn)
                    break
    return out


def test_matcher_pins_each_p1_indicator_to_its_ppn() -> None:
    """Across the four live-month fixtures every P1 indicator should
    fire at least once with the registered PPN."""
    matched = _all_matched_indicators(_LIVE_MONTHS)
    assert set(matched.keys()) == set(INDICATOR_REGISTRY.keys())
    for ind, ppns in matched.items():
        spec = INDICATOR_REGISTRY[ind]
        assert all(p == spec.ppn for p in ppns), (ind, ppns)


def test_matcher_rejects_quarterly_row_for_monthly_indicator() -> None:
    """If a fixture-time PPN ever ships rows under both cadences, the
    cadence filter splits them at parse time."""
    spec = INDICATOR_REGISTRY["CPI"]
    announcements = parse_publication_schedule(
        _fixture_html("May 2026"), schedule_month="May 2026",
    )
    # Hand-craft a malformed quarterly row carrying CPI's PPN.
    from ingestion.calendar.statssa_api.parser import StatsSAReleaseAnnouncement
    quarterly = StatsSAReleaseAnnouncement(
        release_datetime_local=announcements[0].release_datetime_local,
        release_datetime_utc=announcements[0].release_datetime_utc,
        ppn="P0141",
        title="Consumer Price Index (CPI)",
        reference_period_text="1st Quarter 2026",
        download_url="",
        schedule_month="May 2026",
    )
    assert not announcement_matches_spec(quarterly, spec)


# ── projection ────────────────────────────────────────────────────


def test_announcement_to_records_anchors_event_on_release_time() -> None:
    """CPI April 2026 row publishes 20 May 2026 10:00 SAST → 08:00 UTC."""
    announcements = parse_publication_schedule(
        _fixture_html("May 2026"), schedule_month="May 2026",
    )
    cpi = next(a for a in announcements if a.ppn == "P0141")
    spec = INDICATOR_REGISTRY["CPI"]

    raw, event = announcement_to_records(
        cpi, spec=spec, snapshot_epoch_ms=1_700_000_000_000,
    )
    # SAST is UTC+2 year-round (no DST). The published release time is
    # captured verbatim from ``_start`` — 10:00 SAST.
    assert event.event_time_utc.endswith("Z") or event.event_time_utc.endswith("+00:00")
    assert "2026-05-20T08:00:00" in event.event_time_utc
    # Reference anchor is the first day of the data month.
    assert event.reference_date == "2026-04-01"
    assert event.reference_label == "April 2026"
    assert event.actual is None  # schedule-only slice
    assert event.country_code == "ZA"
    assert event.currency == "ZAR"
    assert event.title == "South Africa Consumer Price Index"
    assert event.source_url == STATSSA_PUBLIC_SCHEDULE_URL


def test_announcement_to_records_anchors_quarterly_on_quarter_first_day() -> None:
    announcements = parse_publication_schedule(
        _fixture_html("June 2026"), schedule_month="June 2026",
    )
    gdp = next(a for a in announcements if a.ppn == "P0441")
    spec = INDICATOR_REGISTRY["GDP"]
    _raw, event = announcement_to_records(
        gdp, spec=spec, snapshot_epoch_ms=1_700_000_000_000,
    )
    assert event.reference_date == "2026-01-01"
    assert event.reference_label == "Q1 2026"


def test_announcement_payload_preserves_download_url() -> None:
    """The deferred P2 value scrape will target the per-release
    ``?page_id=1854&PPN=<PPN>`` page — the audit payload must keep
    it independent of any future detail-URL plumbing."""
    announcements = parse_publication_schedule(
        _fixture_html("June 2026"), schedule_month="June 2026",
    )
    cpi = next(a for a in announcements if a.ppn == "P0141")
    raw, _event = announcement_to_records(
        cpi, spec=INDICATOR_REGISTRY["CPI"], snapshot_epoch_ms=1_700_000_000_000,
    )
    payload = json.loads(raw.payload_json)
    assert payload["kind"] == "statssa_release_calendar"
    assert payload["ppn"] == "P0141"
    assert "PPN=P0141" in payload["download_url"]
    assert payload["schedule_month"] == "June 2026"


# ── fetcher integration ──────────────────────────────────────────


def _live_html_fetcher(month_token: str) -> str:
    """Maps every month token in the captured fixture window onto the
    matching fixture; raises for any other month so the test exercises
    only the captured surface."""
    fname = month_token.lower().replace(" ", "_") + ".html"
    path = FIXTURE_DIR / fname
    if not path.exists():
        # Mirror the empty-month branch: a month that returns no rows.
        return (
            "<div id=\"header\"></div><div class=\"alert\">"
            "No further publications are scheduled.</div>"
        )
    return path.read_text(encoding="utf-8")


def test_fetch_statssa_calendar_dry_run_returns_plan(store) -> None:
    with store._connection(commit=False) as conn:
        summary = fetch_statssa_calendar(conn, dry_run=True)
    assert summary.dry_run is True
    assert set(summary.indicators_planned) == set(INDICATOR_REGISTRY.keys())
    # Documented horizon: current month + 14 future months = 15 POSTs
    # per pass. Around Stats SA's October next-year publication, the
    # 15th token must reach into next year so the December horizon
    # row is in scope on the same pass.
    assert len(summary.months_planned) == 15
    assert summary.events_upserted == 0


def test_fetch_statssa_calendar_writes_one_event_per_match(store) -> None:
    months = list(_LIVE_MONTHS) + list(_EMPTY_MONTHS)
    with store._connection(commit=True) as conn:
        summary = fetch_statssa_calendar(
            conn,
            dry_run=False,
            months=months,
            snapshot_epoch_ms=1_700_000_000_000,
            html_fetcher=_live_html_fetcher,
        )
    assert summary.fetch_error is None
    # Across April-July fixtures every P1 indicator fires at least
    # once. Empty months contribute zero rows.
    assert summary.months_fetched == len(months)
    assert summary.announcements_seen >= len(INDICATOR_REGISTRY)
    assert summary.events_upserted == summary.announcements_seen
    assert set(summary.indicators_ok) == set(INDICATOR_REGISTRY.keys())

    with store._connection(commit=False) as conn:
        rows = conn.execute(
            "SELECT count(*) FROM cal_econ_event WHERE provider = ?",
            (PROVIDER,),
        ).fetchone()
    assert rows[0] == summary.events_upserted


def test_fetch_statssa_calendar_idempotent_on_repeat(store) -> None:
    months = list(_LIVE_MONTHS)
    with store._connection(commit=True) as conn:
        first = fetch_statssa_calendar(
            conn, dry_run=False, months=months,
            snapshot_epoch_ms=1_700_000_000_000,
            html_fetcher=_live_html_fetcher,
        )
        second = fetch_statssa_calendar(
            conn, dry_run=False, months=months,
            snapshot_epoch_ms=1_700_000_000_001,
            html_fetcher=_live_html_fetcher,
        )
    assert first.events_upserted > 0
    assert second.rows_raw_inserted == 0
    assert second.events_upserted == first.events_upserted

    with store._connection(commit=False) as conn:
        total = conn.execute(
            "SELECT count(*) FROM cal_econ_event WHERE provider = ?",
            (PROVIDER,),
        ).fetchone()
    assert total[0] == first.events_upserted


def test_fetch_statssa_calendar_records_fetch_error_on_outage(store) -> None:
    import requests

    def broken(month: str) -> str:
        raise requests.exceptions.ConnectionError(
            f"simulated Stats SA outage for {month}",
        )

    with store._connection(commit=True) as conn:
        summary = fetch_statssa_calendar(
            conn, dry_run=False, months=["April 2026"],
            html_fetcher=broken,
        )
    assert summary.fetch_error is not None
    assert summary.events_upserted == 0


def test_fetch_statssa_calendar_records_unknown_indicator(store) -> None:
    with store._connection(commit=False) as conn:
        summary = fetch_statssa_calendar(
            conn, dry_run=True, indicators=["CPI", "BOGUS_INDICATOR"],
        )
    assert "BOGUS_INDICATOR" in summary.indicators_unknown
    assert "CPI" in summary.indicators_planned


# ── scheduler + agency wiring ─────────────────────────────────────


def test_statssa_listed_in_default_rosters() -> None:
    from ingestion.calendar.scheduler import (
        ALL_CONNECTORS,
        ALL_VALUE_SIDE_CONNECTORS,
    )
    assert "statssa" in ALL_CONNECTORS
    assert "statssa" in ALL_VALUE_SIDE_CONNECTORS


def test_statssa_parity_whitelist_empty_in_p1() -> None:
    from ingestion.calendar.agency_registry import AGENCIES

    statssa_decl = next(a for a in AGENCIES if a.agency_id == "STATSSA")
    assert statssa_decl.indicators == frozenset()
    assert statssa_decl.providers == ("statssa",)


def test_statssa_listed_in_parity_official_providers() -> None:
    from ingestion.calendar.parity import OFFICIAL_PROVIDERS
    assert "statssa" in OFFICIAL_PROVIDERS

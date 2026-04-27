"""Mocked tests for the ABS calendar connector (issue #53 P1).

The captured fixture
``tests/fixtures/abs_release_calendar/latest_releases.html`` was
recorded live on 2026-04-27 and includes the Labour Force March
2026 release (the only P1 indicator that fell inside the page's
~14-day rolling window on capture day). Synthetic HTML cards
exercise the CPI / GDP slug parsers without a live capture for
those indicators.

No real HTTP in CI — every test injects the ``html_fetcher`` seam.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ingestion.calendar.abs_api import (
    ABSCalendarParseError,
    ABSReleaseAnnouncement,
    INDICATOR_REGISTRY,
    announcement_matches_spec,
    announcement_to_records,
    fetch_abs_calendar,
    parse_release_calendar,
)
from ingestion.calendar.abs_api.parser import PROVIDER
from storage.sqlite import SQLiteEngineStore


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "abs_release_calendar"


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _latest_html() -> str:
    return (FIXTURE_DIR / "latest_releases.html").read_text(encoding="utf-8")


# Synthetic ABS release cards covering CPI and GDP. The shape mirrors
# what the live latest-releases page renders — indented for readability
# but the parser regex is whitespace-tolerant.
_SYNTHETIC_RELEASES_HTML = """
<html><body>
<div class="views-row"><div class="views-field-field-rs-release-date"><span> <time datetime="2026-04-29T01:30:00Z" class="datetime">Wednesday 29 April 2026</time></span></div><div class="views-field views-field-nothing"><h3> <a href="/statistics/economy/price-indexes-and-inflation/consumer-price-index-australia/feb-2026"> Consumer Price Index, Australia, Feb 2026 </a></h3></div><div class="release__subtitle">Consumer Price Index, Australia</div></div>
<div class="views-row"><div class="views-field-field-rs-release-date"><span> <time datetime="2026-03-04T00:30:00Z" class="datetime">Wednesday 4 March 2026</time></span></div><div class="views-field views-field-nothing"><h3> <a href="/statistics/economy/national-accounts/australian-national-accounts-national-income-expenditure-and-product/dec-2025"> National Income, Expenditure and Product, December 2025 </a></h3></div><div class="release__subtitle">Australian National Accounts</div></div>
<div class="views-row"><div class="views-field-field-rs-release-date"><span> <time datetime="2026-04-16T01:30:00Z" class="datetime">Thursday 16 April 2026</time></span></div><div class="views-field views-field-nothing"><h3> <a href="/statistics/labour/employment-and-unemployment/labour-force-australia/mar-2026"> Labour Force, Australia, March 2026 </a></h3></div><div class="release__subtitle"></div></div>
</body></html>
"""


# ── parser ───────────────────────────────────────────────────────


def test_parse_release_calendar_extracts_labour_force_from_real_capture() -> None:
    announcements = parse_release_calendar(_latest_html())
    # Live capture window covered Apr 17–24 2026; Labour Force Mar 2026
    # was the only P1 indicator that fell in the window.
    lf = [
        a for a in announcements
        if a.href.startswith(
            "/statistics/labour/employment-and-unemployment/labour-force-australia/",
        )
    ]
    assert len(lf) == 1
    assert lf[0].period_slug == "mar-2026"
    assert lf[0].release_dt_utc.isoformat() == "2026-04-16T01:30:00+00:00"


def test_parse_release_calendar_skips_labour_force_detailed_variant() -> None:
    """The ``-detailed`` variant ships a week after the headline LF
    release with cross-tabs but doesn't move TE's headline figure.
    The indicator URL prefix excludes it by design."""
    announcements = parse_release_calendar(_latest_html())
    detailed = [
        a for a in announcements
        if "labour-force-australia-detailed" in a.href
    ]
    spec = INDICATOR_REGISTRY["UNEMPLOYMENT_RATE"]
    matched = [a for a in detailed if announcement_matches_spec(a, spec)]
    assert detailed != []  # the row IS in the page
    assert matched == []   # but it does NOT match our LF whitelist


def test_parse_release_calendar_extracts_synthetic_cpi_and_gdp() -> None:
    announcements = parse_release_calendar(_SYNTHETIC_RELEASES_HTML)
    cpi_spec = INDICATOR_REGISTRY["CPI"]
    gdp_spec = INDICATOR_REGISTRY["GDP"]
    cpi_matches = [a for a in announcements if announcement_matches_spec(a, cpi_spec)]
    gdp_matches = [a for a in announcements if announcement_matches_spec(a, gdp_spec)]
    assert len(cpi_matches) == 1
    assert cpi_matches[0].period_slug == "feb-2026"
    assert len(gdp_matches) == 1
    assert gdp_matches[0].period_slug == "dec-2025"


def test_parse_release_calendar_drops_non_period_slugs() -> None:
    """The latest-releases page mixes statistical releases with media-
    centre articles whose href has no ``mon-YYYY`` slug. The parser
    must skip those rather than emit them as bogus calendar rows."""
    html = """
    <div class="views-row"><div class="views-field-field-rs-release-date"><span> <time datetime="2026-04-20T01:30:00Z" class="datetime">x</time></span></div><div class="views-field views-field-nothing"><h3> <a href="/statistics/people/crime-and-justice/legal-assistance/2024-25"> Legal stuff </a></h3></div><div class="release__subtitle">Legal Assistance</div></div>
    <div class="views-row"><div class="views-field-field-rs-release-date"><span> <time datetime="2026-04-29T01:30:00Z" class="datetime">x</time></span></div><div class="views-field views-field-nothing"><h3> <a href="/statistics/economy/price-indexes-and-inflation/consumer-price-index-australia/feb-2026"> CPI </a></h3></div><div class="release__subtitle"></div></div>
    """
    announcements = parse_release_calendar(html)
    assert len(announcements) == 1
    assert announcements[0].period_slug == "feb-2026"


def test_parse_release_calendar_raises_on_zero_rows() -> None:
    with pytest.raises(ABSCalendarParseError, match="zero release rows"):
        parse_release_calendar("<html><body><h1>nothing here</h1></body></html>")


def test_parse_release_calendar_keeps_updated_information_marker() -> None:
    """The ``| <strong>Updated information</strong>`` marker on revised
    rows must not break the time + href extraction."""
    html = """
    <div class="views-row"><div class="views-field-field-rs-release-date"><span> <time datetime="2026-04-23T01:30:00Z" class="datetime">x</time> | <strong>Updated information</strong></span></div><div class="views-field views-field-nothing"><h3> <a href="/statistics/labour/employment-and-unemployment/labour-force-australia/mar-2026"> LF </a></h3></div><div class="release__subtitle"></div></div>
    """
    announcements = parse_release_calendar(html)
    assert len(announcements) == 1
    assert announcements[0].is_updated_information is True


# ── projection ───────────────────────────────────────────────────


def test_announcement_to_records_synthesizes_event_at_aest_window() -> None:
    spec = INDICATOR_REGISTRY["UNEMPLOYMENT_RATE"]
    announcements = parse_release_calendar(_latest_html())
    matched = [a for a in announcements if announcement_matches_spec(a, spec)]
    raw_rec, event_rec = announcement_to_records(
        matched[0], spec=spec, snapshot_epoch_ms=1_800_000_000_000,
    )
    assert event_rec.country_code == "AU"
    assert event_rec.title == "Australia Unemployment Rate"
    assert event_rec.currency == "AUD"
    # Schedule-only slice — no value side wired in P1.
    assert event_rec.actual is None
    assert event_rec.event_time_precision == "datetime"
    # The release timestamp is preserved as-is from <time datetime>:
    # 11:30 AEST = 01:30 UTC for an April 2026 release.
    assert event_rec.event_time_utc.startswith("2026-04-16T01:30:00")
    # Reference date anchors on the *first* day of the LF reference month.
    assert event_rec.reference_date == "2026-03-01"
    assert event_rec.reference_label == "March 2026"
    # provider_event_id stable across re-projection.
    _, event_rec_again = announcement_to_records(
        matched[0], spec=spec, snapshot_epoch_ms=2_000_000_000_000,
    )
    assert event_rec.provider_event_id == event_rec_again.provider_event_id


def test_announcement_to_records_anchors_quarterly_gdp_on_quarter_end() -> None:
    """ABS quarterly slugs use the last month of the quarter
    (``dec-2025`` = Q4 2025); the projector normalises to the
    quarter-end reference date so parity buckets line up with TE's
    quarter-end convention (``"2025-12-31T00:00:00"``) rather than
    splitting on the YYYY-MM bucket prefix used by the parity
    comparator."""
    spec = INDICATOR_REGISTRY["GDP"]
    announcements = parse_release_calendar(_SYNTHETIC_RELEASES_HTML)
    matched = [a for a in announcements if announcement_matches_spec(a, spec)]
    _, event_rec = announcement_to_records(
        matched[0], spec=spec, snapshot_epoch_ms=1_800_000_000_000,
    )
    assert event_rec.reference_date == "2025-12-31"
    assert event_rec.reference_label == "Q4 2025"
    # Source URL absolutises against the ABS host.
    assert event_rec.source_url.startswith(
        "https://www.abs.gov.au/statistics/economy/national-accounts/",
    )


# ── full fetch driver ───────────────────────────────────────────


def test_fetch_abs_calendar_writes_event_per_matched_indicator(
    store: SQLiteEngineStore,
) -> None:
    """The synthetic three-card payload should write three events —
    one per indicator in the registry."""
    def fetcher() -> str:
        return _SYNTHETIC_RELEASES_HTML

    with store._connection(commit=True) as conn:
        summary = fetch_abs_calendar(
            conn,
            dry_run=False,
            html_fetcher=fetcher,
            snapshot_epoch_ms=1_800_000_000_000,
        )
    assert summary.fetch_error is None
    assert summary.announcements_seen == 3
    assert summary.events_upserted == 3
    assert set(summary.indicators_ok) == {"CPI", "UNEMPLOYMENT_RATE", "GDP"}
    assert summary.indicators_empty == []


def test_fetch_abs_calendar_isolates_per_indicator_absence(
    store: SQLiteEngineStore,
) -> None:
    """When the page's rolling window doesn't include CPI / GDP, the
    fetcher must report them on ``indicators_empty`` rather than
    flag a connector-wide failure."""
    with store._connection(commit=True) as conn:
        summary = fetch_abs_calendar(
            conn,
            dry_run=False,
            html_fetcher=_latest_html,
            snapshot_epoch_ms=1_800_000_000_000,
        )
    assert summary.fetch_error is None
    assert "UNEMPLOYMENT_RATE" in summary.indicators_ok
    assert set(summary.indicators_empty) == {"CPI", "GDP"}
    assert summary.events_upserted == 1


def test_fetch_abs_calendar_records_fetch_error_on_outage(
    store: SQLiteEngineStore,
) -> None:
    """A whole-page network failure must mark the run with
    ``fetch_error`` and write zero rows; every planned indicator
    lands on ``series_failed`` so the next sweep re-attempts."""
    def broken() -> str:
        raise RuntimeError("simulated 503 from ABS")

    with store._connection(commit=True) as conn:
        summary = fetch_abs_calendar(
            conn, dry_run=False, html_fetcher=broken,
        )
    assert summary.fetch_error is not None
    assert summary.events_upserted == 0
    failed_keys = {k for k, _ in summary.series_failed}
    assert failed_keys == set(INDICATOR_REGISTRY.keys())


def test_fetch_abs_calendar_records_parse_error_on_layout_drift(
    store: SQLiteEngineStore,
) -> None:
    """A page that returns 200 but parses zero release rows must trip
    the connector's loud-failure path so the operator can triage
    layout drift before the rolling 14-day window flushes the gap
    silently."""
    def empty() -> str:
        return "<html><body><h1>maintenance window</h1></body></html>"

    with store._connection(commit=True) as conn:
        summary = fetch_abs_calendar(
            conn, dry_run=False, html_fetcher=empty,
        )
    assert summary.fetch_error is not None
    assert summary.events_upserted == 0


def test_fetch_abs_calendar_dry_run_returns_plan(
    store: SQLiteEngineStore,
) -> None:
    with store._connection(commit=False) as conn:
        summary = fetch_abs_calendar(conn, dry_run=True)
    assert summary.dry_run is True
    assert set(summary.indicators_planned) == set(INDICATOR_REGISTRY.keys())


def test_fetch_abs_calendar_skips_updated_information_cards(
    store: SQLiteEngineStore,
) -> None:
    """ABS surfaces revised releases as ``Updated information`` cards
    sharing the original release's URL slug. Both cards generate the
    same ``provider_event_id``, so the projector would treat the later
    revision as a full-row update and move ``event_time_utc`` off the
    first-publication anchor. The fetcher drops the revision card so
    parity buckets stay aligned with TE's first-release timestamp."""
    html = """
    <div class="views-row"><div class="views-field-field-rs-release-date"><span> <time datetime="2026-04-23T01:30:00Z" class="datetime">x</time> | <strong>Updated information</strong></span></div><div class="views-field views-field-nothing"><h3> <a href="/statistics/labour/employment-and-unemployment/labour-force-australia/mar-2026"> Updated LF </a></h3></div><div class="release__subtitle"></div></div>
    <div class="views-row"><div class="views-field-field-rs-release-date"><span> <time datetime="2026-04-16T01:30:00Z" class="datetime">x</time></span></div><div class="views-field views-field-nothing"><h3> <a href="/statistics/labour/employment-and-unemployment/labour-force-australia/mar-2026"> First LF </a></h3></div><div class="release__subtitle"></div></div>
    """
    with store._connection(commit=True) as conn:
        summary = fetch_abs_calendar(
            conn,
            dry_run=False,
            html_fetcher=lambda: html,
            snapshot_epoch_ms=1_800_000_000_000,
        )
    assert summary.events_upserted == 1
    assert summary.indicators_ok == ["UNEMPLOYMENT_RATE"]
    with store._connection(commit=False) as conn:
        rows = conn.execute(
            "SELECT event_time_utc FROM cal_econ_event WHERE provider='abs'",
        ).fetchall()
    # Only the first-release timestamp survives — the Updated card never
    # gets projected, so the stored event stays on 2026-04-16.
    assert [r[0] for r in rows] == ["2026-04-16T01:30:00+00:00"]


# ── scheduler + agency wiring ───────────────────────────────────


def test_abs_listed_in_default_rosters() -> None:
    from ingestion.calendar.scheduler import (
        ALL_CONNECTORS,
        ALL_VALUE_SIDE_CONNECTORS,
    )
    assert "abs" in ALL_CONNECTORS
    assert "abs" in ALL_VALUE_SIDE_CONNECTORS


def test_abs_agency_attribution_provider_only_in_p1() -> None:
    """ABS owns provider attribution for AU statistical releases, but
    the parity whitelist stays empty in P1 — schedule-only events
    have ``actual=NULL``, so registering an indicator would trip the
    parity comparator's parse_failed-on-missing-actual path on every
    release. Same deferral pattern as the BoC slice."""
    from ingestion.calendar.agency_registry import (
        agency_for,
        provider_to_agency,
    )
    abs_agency = provider_to_agency("abs")
    assert abs_agency is not None and abs_agency.agency_id == "ABS"
    assert abs_agency.indicators == frozenset()
    assert agency_for("AU", "CPI") is None
    assert agency_for("AU", "UNEMPLOYMENT_RATE") is None
    assert agency_for("AU", "GDP") is None


def test_abs_canonicalize_aliases_resolve_australia_titles() -> None:
    from ingestion.calendar._official_shared import canonicalize_indicator
    assert canonicalize_indicator("Australia Inflation Rate") == "CPI"
    assert canonicalize_indicator("Australian CPI") == "CPI"
    assert canonicalize_indicator("Australia Unemployment Rate") == "UNEMPLOYMENT_RATE"
    assert canonicalize_indicator("Australia GDP Growth Rate") == "GDP"

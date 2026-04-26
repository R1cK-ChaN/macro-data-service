"""Mocked tests for the DOL UI Weekly Claims connector (issue #50).

Fixtures captured live on 2026-04-26 from
``https://www.dol.gov/newsroom/releases/eta?lang=en`` (the listing
page) and ``/newsroom/releases/eta/eta20260423`` (one PDF release)
under ``tests/fixtures/dol_calendar/``. No real HTTP in CI.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from ingestion.calendar.dol_api import (
    DOLListingParseError,
    DOLPressReleaseParseError,
    DOLReleaseEntry,
    INDICATOR_REGISTRY,
    extract_press_release_value,
    fetch_dol_calendar,
    parse_listing_html,
    parse_press_release_pdf,
    value_observation_to_records,
)
from ingestion.calendar.dol_api.parser import PROVIDER
from storage.sqlite import SQLiteEngineStore


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "dol_calendar"


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _listing_html() -> str:
    return (FIXTURE_DIR / "eta_listing.html").read_text(encoding="utf-8")


def _real_pdf_text() -> str:
    """Extract text from the real captured 2026-04-23 release PDF."""
    from io import BytesIO
    from pypdf import PdfReader

    pdf_bytes = (FIXTURE_DIR / "eta20260423.pdf").read_bytes()
    reader = PdfReader(BytesIO(pdf_bytes))
    return "\n".join((p.extract_text() or "") for p in reader.pages)


# ── value extraction ─────────────────────────────────────────────


def test_initial_claims_extracted_from_real_pdf() -> None:
    text = _real_pdf_text()
    spec = INDICATOR_REGISTRY["INITIAL_CLAIMS"]
    assert extract_press_release_value(text, spec) == "214000"


def test_continuing_claims_extracted_from_real_pdf() -> None:
    text = _real_pdf_text()
    spec = INDICATOR_REGISTRY["CONTINUING_CLAIMS"]
    assert extract_press_release_value(text, spec) == "1821000"


def test_extract_raises_when_pattern_misses() -> None:
    spec = INDICATOR_REGISTRY["INITIAL_CLAIMS"]
    with pytest.raises(DOLPressReleaseParseError, match="layout drift"):
        extract_press_release_value("totally unrelated text", spec)


def test_extract_handles_uncommaed_counts_without_truncation() -> None:
    """If PDF text-extraction drops thousands separators, the regex
    must capture the whole number rather than the leading three
    digits — otherwise actuals would be off by 3+ orders of
    magnitude."""
    spec = INDICATOR_REGISTRY["INITIAL_CLAIMS"]
    text = "Initial Claims (SA) 214000 208000 +6000"
    assert extract_press_release_value(text, spec) == "214000"


def test_parse_press_release_pdf_anchors_reference_on_week_ending_saturday() -> None:
    spec = INDICATOR_REGISTRY["INITIAL_CLAIMS"]
    obs = parse_press_release_pdf(
        _real_pdf_text(),
        spec=spec,
        release_date=date(2026, 4, 23),     # Thursday
        source_url="https://example/release",
    )
    # Initial Claims week ending = release - 5 days = Saturday April 18.
    assert obs.reference_date == "2026-04-18"
    assert obs.value == "214000"
    assert obs.indicator == "INITIAL_CLAIMS"


def test_continuing_claims_reference_lags_initial_by_one_week() -> None:
    spec = INDICATOR_REGISTRY["CONTINUING_CLAIMS"]
    obs = parse_press_release_pdf(
        _real_pdf_text(),
        spec=spec,
        release_date=date(2026, 4, 23),     # Thursday
        source_url="https://example/release",
    )
    # Continuing Claims week ending = release - 12 days = Saturday April 11.
    assert obs.reference_date == "2026-04-11"
    assert obs.value == "1821000"


def test_holiday_shifted_release_uses_narrative_week_ending(
) -> None:
    """When a federal holiday shifts publication off Thursday, the
    days-back offset would land on the wrong Saturday. The PDF
    narrative carries the real week-ending date — parse it directly
    so ``reference_date`` (and the parity bucket key) stay correct."""
    # Synthesize a release where DOL publishes Wednesday but
    # references the same week as a normal Thursday release.
    text = (
        "In the week ending April 18, the advance figure for "
        "seasonally adjusted initial claims was 214,000."
        " "
        "Initial Claims (SA) 214,000 208,000 +6,000 218,000 224,000"
    )
    spec = INDICATOR_REGISTRY["INITIAL_CLAIMS"]
    obs = parse_press_release_pdf(
        text, spec=spec,
        release_date=date(2026, 4, 22),   # Wednesday holiday-shift
        source_url="https://example/release",
    )
    # 22 - 5 = 17 (Friday) — wrong. Narrative says April 18 (Saturday).
    assert obs.reference_date == "2026-04-18"


# ── listing parse ────────────────────────────────────────────────


def test_listing_parses_ui_claims_rows_and_decodes_release_dates() -> None:
    entries = parse_listing_html(_listing_html())
    assert len(entries) >= 5
    dates = [e.release_date for e in entries]
    assert date(2026, 4, 23) in dates
    assert date(2026, 4, 16) in dates
    # Newest-first ordering.
    assert dates == sorted(dates, reverse=True)


def test_listing_parse_raises_when_no_ui_claims_rows() -> None:
    with pytest.raises(DOLListingParseError, match="zero UI Claims"):
        parse_listing_html(
            "<html><body><a href='/foo'><h3><span>Other report</span></h3></a></body></html>",
        )


# ── full fetch driver ────────────────────────────────────────────


def test_fetch_dol_calendar_writes_both_indicators_per_release(
    store: SQLiteEngineStore,
) -> None:
    text = _real_pdf_text()
    html = _listing_html()

    def listing_fetcher() -> str:
        return html

    fetched_urls: list[str] = []

    def pdf_text_fetcher(url: str) -> str:
        fetched_urls.append(url)
        # Use the captured 2026-04-23 PDF for every release URL — the
        # fixture lookbook only covers one release, but the test
        # exercises the per-release loop.
        return text

    with store._connection(commit=True) as conn:
        summary = fetch_dol_calendar(
            conn,
            dry_run=False,
            listing_fetcher=listing_fetcher,
            pdf_text_fetcher=pdf_text_fetcher,
            today=date(2026, 4, 30),
            lookback_days=28,    # capture only the 4 most recent Thursdays
            snapshot_epoch_ms=1_800_000_000_000,
        )

    assert summary.fetch_error is None
    assert summary.releases_fetched == 4   # Apr 23 / 16 / 9 / 2
    # Two indicators per release × 4 releases = 8 events.
    assert summary.observations_seen == 8
    assert summary.events_upserted == 8
    assert set(summary.indicators_ok) == {"INITIAL_CLAIMS", "CONTINUING_CLAIMS"}
    assert summary.releases_failed == []


def test_fetch_dol_calendar_isolates_per_release_failure(
    store: SQLiteEngineStore,
) -> None:
    """One release returning a 404 must not roll back the others."""
    text = _real_pdf_text()
    html = _listing_html()

    def pdf_text_fetcher(url: str) -> str:
        if "20260416" in url:
            raise RuntimeError("simulated 404")
        return text

    with store._connection(commit=True) as conn:
        summary = fetch_dol_calendar(
            conn,
            dry_run=False,
            listing_fetcher=lambda: html,
            pdf_text_fetcher=pdf_text_fetcher,
            today=date(2026, 4, 30),
            lookback_days=28,
        )

    failed_keys = {k for k, _ in summary.releases_failed}
    assert "2026-04-16" in failed_keys
    # The other 3 releases (Apr 23 / 9 / 2) still produced 6 events.
    assert summary.events_upserted == 6


def test_fetch_dol_calendar_dry_run_returns_plan(
    store: SQLiteEngineStore,
) -> None:
    with store._connection(commit=False) as conn:
        summary = fetch_dol_calendar(conn, dry_run=True)
    assert summary.dry_run is True
    assert set(summary.indicators_planned) == set(INDICATOR_REGISTRY.keys())


def test_fetch_dol_calendar_listing_outage_records_fetch_error(
    store: SQLiteEngineStore,
) -> None:
    def listing_fetcher() -> str:
        raise DOLListingParseError("zero UI Claims rows")

    with store._connection(commit=True) as conn:
        summary = fetch_dol_calendar(
            conn,
            dry_run=False,
            listing_fetcher=listing_fetcher,
            pdf_text_fetcher=lambda url: "",
            today=date(2026, 4, 30),
        )
    assert summary.fetch_error is not None
    assert summary.observations_seen == 0


# ── scheduler + agency wiring ───────────────────────────────────


def test_dol_listed_in_value_side_connectors() -> None:
    from ingestion.calendar.scheduler import (
        ALL_VALUE_SIDE_CONNECTORS,
        ALL_CONNECTORS,
        _VALUE_SIDE_DUE_ROW_FILTERS,
    )
    assert "dol" in ALL_CONNECTORS
    assert "dol" in ALL_VALUE_SIDE_CONNECTORS
    # DOL writes rows post-publication, so it falls through to the
    # hourly baseline rather than carrying a burst predicate
    # (matches the ECB / EIA pattern documented in scheduler.py).
    assert "dol" not in _VALUE_SIDE_DUE_ROW_FILTERS


def test_dol_total_outage_trips_breaker(
    store: SQLiteEngineStore,
) -> None:
    """When the listing parses entries but every PDF GET fails, the
    scheduler must classify the run as a total outage so the breaker
    cools the connector instead of hammering Akamai every sweep."""
    from ingestion.calendar.scheduler import _summary_is_total_outage
    from ingestion.calendar.dol_api.fetcher import FetchRunSummary
    summary = FetchRunSummary(
        listing_entries=4,
        releases_failed=[("2026-04-23", "403"), ("2026-04-16", "403")],
        observations_seen=0,
    )
    assert _summary_is_total_outage(summary) is True


def test_dol_partial_failure_does_not_trip_breaker() -> None:
    """One failed Thursday with the others still landing values must
    keep the breaker counter clean — the source is reachable."""
    from ingestion.calendar.scheduler import _summary_is_total_outage
    from ingestion.calendar.dol_api.fetcher import FetchRunSummary
    summary = FetchRunSummary(
        listing_entries=4,
        releases_failed=[("2026-04-16", "403")],
        observations_seen=6,
    )
    assert _summary_is_total_outage(summary) is False


def test_dol_agency_attribution_added_without_parity_whitelist() -> None:
    from ingestion.calendar.agency_registry import provider_to_agency
    dol = provider_to_agency("dol")
    assert dol is not None and dol.agency_id == "DOL"
    # Parity whitelist is empty until canonicalize aliases + the
    # K-suffix alignment spec land — see the DOL row's docstring.
    assert dol.indicators == frozenset()

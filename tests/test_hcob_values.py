"""Mocked tests for the HCOB / S&P Global value-side connector (issue #23)."""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from ingestion.calendar.hcob_api import (
    INDICATOR_REGISTRY,
    extract_press_release_value,
    fetch_hcob_calendar,
    parse_press_release_pdf,
    resolve_press_release_link,
    schedule_hcob_calendar,
)
from ingestion.calendar.hcob_api.parser import HCOBPressReleaseParseError, PROVIDER
from ingestion.calendar.hcob_api.schedule import HCOBScheduleParseError
from macro_data.service import LocalMacroDataService
from storage.sqlite import SQLiteEngineStore


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _fixture_text(*parts: str) -> str:
    return (Path(__file__).parent / "fixtures" / Path(*parts)).read_text()


def _flash_text() -> str:
    return _fixture_text("hcob_calendar", "press_releases", "flash_apr2026.txt")


def _manufacturing_text() -> str:
    return _fixture_text(
        "hcob_calendar", "press_releases", "manufacturing_mar2026.txt"
    )


def _services_text() -> str:
    return _fixture_text(
        "hcob_calendar", "press_releases", "services_mar2026.txt"
    )


def _listing_html() -> str:
    return _fixture_text("hcob_calendar", "press_releases", "listing.html")


# ── value extraction (per series) ───────────────────────────────────


def test_flash_pdf_yields_three_distinct_values() -> None:
    text = _flash_text()
    mfg = INDICATOR_REGISTRY["HCOB_FLASH_MANUFACTURING_PMI"]
    svc = INDICATOR_REGISTRY["HCOB_FLASH_SERVICES_PMI"]
    comp = INDICATOR_REGISTRY["HCOB_FLASH_COMPOSITE_PMI"]
    assert extract_press_release_value(text, mfg) == "51.2"
    assert extract_press_release_value(text, svc) == "46.9"
    assert extract_press_release_value(text, comp) == "48.3"


def test_final_manufacturing_pdf_yields_headline() -> None:
    spec = INDICATOR_REGISTRY["HCOB_MANUFACTURING_PMI"]
    assert extract_press_release_value(_manufacturing_text(), spec) == "52.2"


def test_final_services_pdf_tolerates_typo_in_came_at_phrase() -> None:
    # The 2026-04-07 PDF actually reads "came it at 50.9" (typo). The
    # parser tolerates a short filler word between "came" and "at" so
    # the extractor still pulls 50.9.
    spec = INDICATOR_REGISTRY["HCOB_SERVICES_PMI"]
    assert extract_press_release_value(_services_text(), spec) == "50.9"


def test_extract_press_release_value_raises_when_pattern_misses() -> None:
    spec = INDICATOR_REGISTRY["HCOB_FLASH_MANUFACTURING_PMI"]
    with pytest.raises(HCOBPressReleaseParseError, match="headline value not found"):
        extract_press_release_value("totally unrelated text", spec)


def test_parse_press_release_pdf_packs_full_observation() -> None:
    spec = INDICATOR_REGISTRY["HCOB_FLASH_MANUFACTURING_PMI"]
    obs = parse_press_release_pdf(
        _flash_text(),
        spec=spec,
        reference_date="2026-05-01",
        reference_label="May 2026",
        event_time_utc="2026-04-23T07:30:00+00:00",
        source_url="https://example/pr",
    )
    assert obs.value == "51.2"
    assert obs.series_id == "HCOB_FLASH_MANUFACTURING_PMI"
    assert obs.reference_date == "2026-05-01"
    assert obs.release_title == spec.title
    assert obs.observed_at_epoch_ms > 0
    assert obs.raw["text"]  # truncated body excerpt for raw audit


# ── listing → press-release URL resolution ───────────────────────────


def test_resolve_press_release_link_picks_english_skipping_deutsch() -> None:
    html = _listing_html()
    flash = resolve_press_release_link(
        html,
        release_date=date(2026, 4, 23),
        expected_listing_match="s&p global flash germany pmi",
    )
    # Each English release has a "(Deutsch)" sibling on the same date;
    # the resolver must skip it deterministically.
    assert "PressRelease/" in flash.source_url
    assert "Deutsch" not in flash.title


def test_resolve_press_release_link_distinguishes_release_dates() -> None:
    html = _listing_html()
    mfg = resolve_press_release_link(
        html,
        release_date=date(2026, 4, 1),
        expected_listing_match="s&p global germany manufacturing pmi",
    )
    svc = resolve_press_release_link(
        html,
        release_date=date(2026, 4, 7),
        expected_listing_match="s&p global germany services pmi",
    )
    assert mfg.source_url != svc.source_url


def test_resolve_press_release_link_raises_when_no_row_matches() -> None:
    html = _listing_html()
    with pytest.raises(HCOBScheduleParseError, match="press release not found"):
        resolve_press_release_link(
            html,
            release_date=date(2099, 1, 1),
            expected_listing_match="s&p global flash germany pmi",
        )


# ── full driver: schedule → value sweep ──────────────────────────────


def _seed_schedule(store: SQLiteEngineStore, *, today: date) -> None:
    """Project the April-2026 schedule fixture so ``cal_econ_event`` carries
    the rows the value-side sweep then resolves against the press-release
    listing fixture (April 1 Mfg, April 7 Svc, April 23 Flash trio)."""
    with store.get_connection() as conn:
        schedule_hcob_calendar(
            conn,
            start_date="2026-04-01",
            end_date="2026-04-30",
            dry_run=False,
            html_fetcher=lambda: _fixture_text(
                "hcob_calendar", "release_dates_april2026.html"
            ),
            today=today,
            snapshot_epoch_ms=1_800_000_000_000,
        )


def test_fetch_value_sweep_upserts_actuals_for_all_pending_rows(
    store: SQLiteEngineStore,
) -> None:
    # Today=2026-04-30 → all 5 April rows resolve in-year (no rollover).
    # today < April 1 so the schedule resolver picks 2026 for every
    # April row (post-today dates roll to next year).
    _seed_schedule(store, today=date(2026, 3, 30))

    pdf_lookup: dict[str, int] = {}

    def listing_fetcher() -> str:
        return _listing_html()

    def pdf_text_fetcher(url: str) -> str:
        pdf_lookup[url] = pdf_lookup.get(url, 0) + 1
        if "444f6be3f701474398dfab659db4eda1" in url:
            return _flash_text()
        if "7bc61f70eb31435c9a90a3e5a5bc9698" in url:
            return _manufacturing_text()
        if "147fafe8c40c4d278ff93f040cb14177" in url:
            return _services_text()
        raise AssertionError(f"unexpected PDF url: {url}")

    with store.get_connection() as conn:
        # Sweep at a "now" past every release in the fixture.
        now = datetime(2026, 4, 30, 23, tzinfo=timezone.utc)
        summary = fetch_hcob_calendar(
            conn,
            dry_run=False,
            listing_fetcher=listing_fetcher,
            pdf_text_fetcher=pdf_text_fetcher,
            now_utc=now,
            snapshot_epoch_ms=1_800_000_001_000,
        )
        actuals = conn.execute(
            "SELECT title, actual FROM cal_econ_event "
            "WHERE provider = ? AND actual IS NOT NULL "
            "ORDER BY title",
            (PROVIDER,),
        ).fetchall()

    assert summary.dry_run is False
    assert summary.pending_releases == 5
    assert summary.observations_seen == 5
    assert summary.events_upserted == 5
    assert len(summary.series_failed) == 0

    # Each flash series resolves to the same PDF; the cache must elide
    # the duplicate downloads (one call per distinct URL, not per row).
    flash_url = next(
        u for u in pdf_lookup if u.endswith("444f6be3f701474398dfab659db4eda1")
    )
    assert pdf_lookup[flash_url] == 1

    by_title = {row["title"]: row["actual"] for row in actuals}
    assert by_title["Germany HCOB Flash Manufacturing PMI"] == "51.2"
    assert by_title["Germany HCOB Flash Services PMI"] == "46.9"
    assert by_title["Germany HCOB Flash Composite PMI"] == "48.3"
    assert by_title["Germany HCOB Manufacturing PMI"] == "52.2"
    assert by_title["Germany HCOB Services PMI"] == "50.9"


def test_fetch_value_sweep_skips_when_no_rows_pending(
    store: SQLiteEngineStore,
) -> None:
    # No schedule seeded → no rows for the sweep to work on.
    with store.get_connection() as conn:
        summary = fetch_hcob_calendar(
            conn,
            dry_run=False,
            listing_fetcher=lambda: _listing_html(),
            pdf_text_fetcher=lambda url: "",
            now_utc=datetime(2026, 5, 31, tzinfo=timezone.utc),
        )
    assert summary.pending_releases == 0
    assert summary.events_upserted == 0
    assert set(summary.series_empty) == set(INDICATOR_REGISTRY)


def test_fetch_value_sweep_isolates_per_series_failures(
    store: SQLiteEngineStore,
) -> None:
    # today < April 1 so the schedule resolver picks 2026 for every
    # April row (post-today dates roll to next year).
    _seed_schedule(store, today=date(2026, 3, 30))

    def pdf_text_fetcher(url: str) -> str:
        # Manufacturing PDF returns garbage; flash + services succeed.
        if "7bc61f70eb31435c9a90a3e5a5bc9698" in url:
            return "garbled PDF text with no recognisable phrasing"
        if "444f6be3f701474398dfab659db4eda1" in url:
            return _flash_text()
        if "147fafe8c40c4d278ff93f040cb14177" in url:
            return _services_text()
        raise AssertionError(f"unexpected url: {url}")

    with store.get_connection() as conn:
        summary = fetch_hcob_calendar(
            conn,
            dry_run=False,
            listing_fetcher=lambda: _listing_html(),
            pdf_text_fetcher=pdf_text_fetcher,
            now_utc=datetime(2026, 4, 30, 23, tzinfo=timezone.utc),
            snapshot_epoch_ms=1_800_000_001_000,
        )

    # Manufacturing fails per-series, flash trio + services succeed.
    failed_ids = {sid for sid, _ in summary.series_failed}
    assert "HCOB_MANUFACTURING_PMI" in failed_ids
    assert {"HCOB_FLASH_MANUFACTURING_PMI",
            "HCOB_FLASH_SERVICES_PMI",
            "HCOB_FLASH_COMPOSITE_PMI",
            "HCOB_SERVICES_PMI"}.issubset(set(summary.series_ok))


def test_value_record_provider_event_id_matches_schedule_row(
    store: SQLiteEngineStore,
) -> None:
    # today < April 1 so the schedule resolver picks 2026 for every
    # April row (post-today dates roll to next year).
    _seed_schedule(store, today=date(2026, 3, 30))

    with store.get_connection() as conn:
        before = {
            (row["title"], row["reference_date"]): row["provider_event_id"]
            for row in conn.execute(
                "SELECT title, reference_date, provider_event_id FROM cal_econ_event "
                "WHERE provider = ?",
                (PROVIDER,),
            ).fetchall()
        }
        fetch_hcob_calendar(
            conn,
            dry_run=False,
            listing_fetcher=lambda: _listing_html(),
            pdf_text_fetcher=lambda url: (
                _flash_text() if "444f6be3" in url
                else _manufacturing_text() if "7bc61f70" in url
                else _services_text()
            ),
            now_utc=datetime(2026, 4, 30, 23, tzinfo=timezone.utc),
            snapshot_epoch_ms=1_800_000_001_000,
        )
        after = {
            (row["title"], row["reference_date"]): row["provider_event_id"]
            for row in conn.execute(
                "SELECT title, reference_date, provider_event_id FROM cal_econ_event "
                "WHERE provider = ?",
                (PROVIDER,),
            ).fetchall()
        }

    # Provider event ids must be invariant across the schedule → value
    # round-trip — the value sweep upserts onto the same row instead
    # of inserting a duplicate.
    assert before == after


# ── service op + scheduler wiring ────────────────────────────────────


def test_service_dry_run_returns_full_plan(store: SQLiteEngineStore) -> None:
    svc = LocalMacroDataService(store=store)
    plan = svc.invoke("calendar_econ_fetch_hcob", {"dry_run": True})
    assert set(plan["series_planned"]) == set(INDICATOR_REGISTRY)
    assert plan["stopped_reason"] == "dry_run"


def test_hcob_listed_in_value_side_connectors() -> None:
    from ingestion.calendar.scheduler import ALL_VALUE_SIDE_CONNECTORS
    assert "hcob" in ALL_VALUE_SIDE_CONNECTORS

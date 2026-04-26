"""Mocked tests for the NBS value-side connector (issue #49).

Fixture HTML lives in ``tests/fixtures/nbs_values/`` — a captured
press-release listing plus per-indicator article fixtures pulled
from ``stats.gov.cn`` on 2026-04-26. No real HTTP in CI.

Covers:

- :func:`extract_press_release_value` — five YoY indicators
  (CPI / PPI / Industrial Production / Fixed Asset Investment /
  Retail Sales). Decline phrasing applies the negative sign;
  pattern-miss raises a loud :class:`NBSValueParseError`.
- :func:`parse_press_listing_html` + :func:`resolve_release_url` —
  listing rows resolve to ``(release_date, title, url)`` triples
  and the resolver matches on the registered fragment.
- :func:`fetch_nbs_values` — auto-discovers ``actual IS NULL`` rows
  past their scheduled time, looks them up on the listing,
  fetches the article, parses the value, upserts via the shared
  ``provider_event_id`` so the schedule row gets ``actual``.
- Service op ``calendar_econ_fetch_nbs_values`` — dry-run plan +
  execute-mode wiring against the same fixtures.
- Scheduler wiring — ``nbs-values`` is in
  ``ALL_VALUE_SIDE_CONNECTORS`` and carries a burst predicate.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from ingestion.calendar.nbs_api import (
    INDICATOR_REGISTRY,
    NBSPressListingParseError,
    NBSValueParseError,
    extract_press_release_value,
    fetch_nbs_calendar,
    fetch_nbs_values,
    parse_press_listing_html,
    resolve_release_url,
    value_observation_to_records,
    parse_press_release_html as nbs_parse_press_release_html,
)
from ingestion.calendar.nbs_api.parser import PROVIDER, release_entry_to_records
from ingestion.calendar.nbs_api.scraper import parse_nbs_calendar_html
from ingestion.calendar.nbs_api.projector import project_events, store_raw
from storage.sqlite import SQLiteEngineStore


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "nbs_values"


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _fixture(name: str) -> str:
    return (FIXTURE_DIR / name).read_text(encoding="utf-8")


def _press_release(name: str) -> str:
    return (FIXTURE_DIR / "press_releases" / name).read_text(encoding="utf-8")


# ──────────────────────────────────────────────────────────────────────────
# Per-indicator narrative parsing
# ──────────────────────────────────────────────────────────────────────────


def test_cpi_yoy_extracted_from_narrative() -> None:
    spec = INDICATOR_REGISTRY["CPI"]
    assert extract_press_release_value(_press_release("cpi_mar2026.html"), spec) == "1.0"


def test_ppi_picks_current_month_value_not_previous_month() -> None:
    # PPI narrative typically frames the current month against the
    # previous month: "turned from a 0.9% year-on-year decline ... to
    # a 0.5% increase". The parser must return the *current* month's
    # value (0.5), not the previous-month figure (0.9).
    spec = INDICATOR_REGISTRY["PPI"]
    assert extract_press_release_value(_press_release("ppi_mar2026.html"), spec) == "0.5"


def test_industrial_production_yoy_extracted() -> None:
    spec = INDICATOR_REGISTRY["INDUSTRIAL_PRODUCTION"]
    assert extract_press_release_value(_press_release("ip_mar2026.html"), spec) == "5.7"


def test_fixed_asset_investment_ytd_extracted() -> None:
    spec = INDICATOR_REGISTRY["FIXED_ASSET_INVESTMENT"]
    assert extract_press_release_value(_press_release("fai_q1_2026.html"), spec) == "1.7"


def test_retail_sales_yoy_extracted() -> None:
    spec = INDICATOR_REGISTRY["RETAIL_SALES"]
    assert extract_press_release_value(_press_release("retail_q1_2026.html"), spec) == "1.7"


def test_decline_phrasing_signs_value_negative() -> None:
    # Synthesize a CPI narrative that uses "decreased by" so the
    # direction map flips the sign — the press release *can* publish
    # a negative YoY value, and the parser must not silently drop the
    # sign.
    html = (
        "<html><body><div class='TRS_Editor'>"
        "<p>In April 2024, China's Consumer Price Index (CPI) "
        "decreased by 0.5% year on year.</p></div></body></html>"
    )
    assert extract_press_release_value(html, INDICATOR_REGISTRY["CPI"]) == "-0.5"


def test_pattern_miss_raises_loudly() -> None:
    with pytest.raises(NBSValueParseError, match="phrasing drift"):
        extract_press_release_value(
            "<html><body>Totally unrelated text — no CPI here.</body></html>",
            INDICATOR_REGISTRY["CPI"],
        )


def test_no_value_pattern_for_pmi_raises_keyerror() -> None:
    # PMI / GDP are schedule-only in this slice; trying to parse them
    # via the value parser is a developer error, not silent.
    with pytest.raises(KeyError, match="MANUFACTURING_PMI"):
        extract_press_release_value("", INDICATOR_REGISTRY["MANUFACTURING_PMI"])


# ──────────────────────────────────────────────────────────────────────────
# Listing parse + URL resolution
# ──────────────────────────────────────────────────────────────────────────


def test_listing_parses_release_rows_and_skips_unrelated_anchors() -> None:
    entries = parse_press_listing_html(_fixture("listing.html"))
    titles = [e.title for e in entries]
    assert "Consumer Price Index in March 2026" in titles
    assert "Industrial Producer Price Indexes in March 2026" in titles
    # Navigation anchors without a YYYY-MM-DD date are dropped.
    assert all("About Us" not in t for t in titles)


def test_listing_zero_entries_raises() -> None:
    with pytest.raises(NBSPressListingParseError, match="zero entries"):
        parse_press_listing_html("<html><body><p>no releases</p></body></html>")


def test_resolve_release_url_matches_release_date_and_fragment() -> None:
    entries = parse_press_listing_html(_fixture("listing.html"))
    cpi_entry = resolve_release_url(
        entries,
        release_date=date(2026, 4, 13),
        listing_title_fragment="consumer price index in",
    )
    assert cpi_entry is not None
    assert cpi_entry.url.endswith("t20260413_1963288.html")


def test_resolve_release_url_returns_none_when_listing_lacks_match() -> None:
    entries = parse_press_listing_html(_fixture("listing.html"))
    # Date is in the future — listing doesn't carry that release yet.
    miss = resolve_release_url(
        entries,
        release_date=date(2099, 1, 1),
        listing_title_fragment="consumer price index in",
    )
    assert miss is None


# ──────────────────────────────────────────────────────────────────────────
# fetch_nbs_values driver — schedule seed → press-release sweep
# ──────────────────────────────────────────────────────────────────────────


def _seed_schedule_for_april_2026(store: SQLiteEngineStore) -> None:
    """Write 5 NBS schedule rows mirroring the value-side test indicators.

    Bypasses the yearly-calendar HTML fixture so the test is decoupled
    from the schedule-side parser. Each row carries a release_date /
    event_time_utc that the listing fixture's release dates match.
    """
    snapshot = 1_800_000_000_000
    raw_records = []
    event_records = []
    plan = [
        ("CPI",                     2026, 4, 13),
        ("PPI",                     2026, 4, 13),
        ("INDUSTRIAL_PRODUCTION",   2026, 4, 17),
        ("FIXED_ASSET_INVESTMENT",  2026, 4, 17),
        ("RETAIL_SALES",            2026, 4, 17),
    ]
    for indicator, year, month, day in plan:
        entry = release_entry_to_records.__wrapped__ if False else None  # noqa
        from ingestion.calendar.nbs_api.parser import NBSReleaseEntry
        e = NBSReleaseEntry(
            year=year, month=month, day=day,
            release_time_local="9:30",
            indicator=indicator,
            weekday_label="Mon",
            date_cell=f"{day}/Mon",
        )
        raw, ev = release_entry_to_records(
            e, snapshot_epoch_ms=snapshot,
            calendar_url="https://example/nbs/cal",
        )
        raw_records.append(raw)
        event_records.append(ev)

    with store._connection(commit=True) as conn:
        store_raw(conn, raw_records)
        project_events(conn, event_records)


def test_fetch_nbs_values_fills_actuals_for_pending_rows(
    store: SQLiteEngineStore,
) -> None:
    _seed_schedule_for_april_2026(store)

    listing_html = _fixture("listing.html")
    article_lookup = {
        "1963288.html": _press_release("cpi_mar2026.html"),
        "1963289.html": _press_release("ppi_mar2026.html"),
        "1963356.html": _press_release("ip_mar2026.html"),
        "1963355.html": _press_release("fai_q1_2026.html"),
        "1963351.html": _press_release("retail_q1_2026.html"),
    }
    fetched_urls: list[str] = []

    def article_fetcher(url: str) -> str:
        fetched_urls.append(url)
        for tail, html in article_lookup.items():
            if tail in url:
                return html
        raise AssertionError(f"unexpected article url: {url}")

    now = datetime(2026, 4, 30, 23, tzinfo=timezone.utc)
    with store._connection(commit=True) as conn:
        summary = fetch_nbs_values(
            conn,
            dry_run=False,
            listing_fetcher=lambda: listing_html,
            article_fetcher=article_fetcher,
            now_utc=now,
            snapshot_epoch_ms=1_800_000_001_000,
        )

    assert summary.dry_run is False
    assert summary.pending_releases == 5
    assert summary.observations_seen == 5
    assert summary.events_upserted == 5
    assert summary.listing_misses == 0
    assert set(summary.series_ok) == {
        "CPI", "PPI", "INDUSTRIAL_PRODUCTION",
        "FIXED_ASSET_INVESTMENT", "RETAIL_SALES",
    }
    assert summary.series_failed == []

    with store._connection(commit=False) as conn:
        actuals = {
            row[0]: row[1]
            for row in conn.execute(
                "SELECT title, actual FROM cal_econ_event "
                "WHERE provider = ? AND actual IS NOT NULL ORDER BY title",
                (PROVIDER,),
            ).fetchall()
        }

    assert actuals == {
        "China Consumer Price Index":     "1.0",
        "China Fixed Asset Investment":   "1.7",
        "China Industrial Production":    "5.7",
        "China Producer Price Index":     "0.5",
        "China Retail Sales":             "1.7",
    }


def test_fetch_nbs_values_no_pending_rows_returns_clean(
    store: SQLiteEngineStore,
) -> None:
    # No schedule seeded → nothing to fill. The driver must short-
    # circuit before touching the listing fetcher and report every
    # value-side indicator under ``series_empty``.
    def listing_fetcher() -> str:
        raise AssertionError("listing fetch should not run when no rows pending")

    with store._connection(commit=True) as conn:
        summary = fetch_nbs_values(
            conn,
            dry_run=False,
            listing_fetcher=listing_fetcher,
            article_fetcher=lambda url: "",
            now_utc=datetime(2026, 4, 30, tzinfo=timezone.utc),
        )

    assert summary.pending_releases == 0
    assert summary.observations_seen == 0
    assert summary.events_upserted == 0
    assert set(summary.series_empty) == {
        "CPI", "PPI", "INDUSTRIAL_PRODUCTION",
        "FIXED_ASSET_INVESTMENT", "RETAIL_SALES",
    }


def test_fetch_nbs_values_listing_miss_does_not_fail_connector(
    store: SQLiteEngineStore,
) -> None:
    # Seed only the CPI row. Build a listing fixture without a CPI row
    # for that date so the resolver returns ``None``. The connector
    # must report ``listing_misses=1`` but not crash, and not flag the
    # indicator as failed (the listing miss is "not yet published",
    # not "broken upstream").
    _seed_schedule_for_april_2026(store)
    listing_html = (
        "<html><body><ul>"
        "<li><a href='./202604/t20260417_1963356.html'>"
        "Industrial Production Operation in March 2026</a> 2026-04-17</li>"
        "</ul></body></html>"
    )

    with store._connection(commit=True) as conn:
        summary = fetch_nbs_values(
            conn,
            dry_run=False,
            listing_fetcher=lambda: listing_html,
            article_fetcher=lambda url: _press_release("ip_mar2026.html"),
            now_utc=datetime(2026, 4, 30, tzinfo=timezone.utc),
            snapshot_epoch_ms=1_800_000_002_000,
        )

    # IP resolves; CPI / PPI / FAI / Retail miss the listing.
    assert summary.listing_misses == 4
    assert summary.observations_seen == 1
    assert "INDUSTRIAL_PRODUCTION" in summary.series_ok


def test_fetch_nbs_values_dry_run_returns_plan(
    store: SQLiteEngineStore,
) -> None:
    with store._connection(commit=False) as conn:
        summary = fetch_nbs_values(conn, dry_run=True)
    assert summary.dry_run is True
    assert set(summary.indicators_planned) == {
        "CPI", "PPI", "INDUSTRIAL_PRODUCTION",
        "FIXED_ASSET_INVESTMENT", "RETAIL_SALES",
    }


# ──────────────────────────────────────────────────────────────────────────
# Provider event id stability: schedule → value upsert lands on same row
# ──────────────────────────────────────────────────────────────────────────


def test_value_upsert_lands_on_same_provider_event_id(
    store: SQLiteEngineStore,
) -> None:
    _seed_schedule_for_april_2026(store)
    with store._connection(commit=False) as conn:
        before = {
            row[0]: row[1]
            for row in conn.execute(
                "SELECT title, provider_event_id FROM cal_econ_event "
                "WHERE provider = ?",
                (PROVIDER,),
            ).fetchall()
        }
    listing_html = _fixture("listing.html")
    article_lookup = {
        "1963288": _press_release("cpi_mar2026.html"),
        "1963289": _press_release("ppi_mar2026.html"),
        "1963356": _press_release("ip_mar2026.html"),
        "1963355": _press_release("fai_q1_2026.html"),
        "1963351": _press_release("retail_q1_2026.html"),
    }

    def article_fetcher(url: str) -> str:
        for tail, html in article_lookup.items():
            if tail in url:
                return html
        raise AssertionError(url)

    with store._connection(commit=True) as conn:
        fetch_nbs_values(
            conn,
            dry_run=False,
            listing_fetcher=lambda: listing_html,
            article_fetcher=article_fetcher,
            now_utc=datetime(2026, 4, 30, tzinfo=timezone.utc),
        )
    with store._connection(commit=False) as conn:
        after = {
            row[0]: row[1]
            for row in conn.execute(
                "SELECT title, provider_event_id FROM cal_econ_event "
                "WHERE provider = ?",
                (PROVIDER,),
            ).fetchall()
        }
    # The value upsert must reuse the schedule row's id rather than
    # inserting a duplicate — same set of titles, same ids, just with
    # ``actual`` filled.
    assert before == after


# ──────────────────────────────────────────────────────────────────────────
# Service op + scheduler wiring
# ──────────────────────────────────────────────────────────────────────────


def test_service_op_dry_run_returns_value_side_plan(
    store: SQLiteEngineStore,
) -> None:
    from macro_data.service import LocalMacroDataService
    svc = LocalMacroDataService(store=store)
    plan = svc.invoke("calendar_econ_fetch_nbs_values", {"dry_run": True})
    assert plan["dry_run"] is True
    assert plan["stopped_reason"] == "dry_run"
    assert set(plan["indicators_planned"]) == {
        "CPI", "PPI", "INDUSTRIAL_PRODUCTION",
        "FIXED_ASSET_INVESTMENT", "RETAIL_SALES",
    }


def test_nbs_values_listed_in_value_side_connectors() -> None:
    from ingestion.calendar.scheduler import (
        ALL_VALUE_SIDE_CONNECTORS,
        _VALUE_SIDE_DUE_ROW_FILTERS,
    )
    assert "nbs-values" in ALL_VALUE_SIDE_CONNECTORS
    predicate = _VALUE_SIDE_DUE_ROW_FILTERS["nbs-values"]
    # Predicate must scope to provider='nbs' and the five value-side
    # titles (PMI / GDP excluded — schedule-only in this slice).
    assert "provider = 'nbs'" in predicate
    assert "China Consumer Price Index" in predicate
    assert "China Manufacturing PMI" not in predicate


def test_nbs_remains_out_of_agency_registry_until_bucket_alignment() -> None:
    # NBS schedule rows still anchor reference_date on the release
    # month while TE anchors on the data month — registering NBS
    # without aligning the bucket would generate false parity alerts.
    # The follow-up slice fixes the reference anchor and then re-adds
    # the agency declaration.
    from ingestion.calendar.agency_registry import provider_to_agency
    assert provider_to_agency("nbs") is None


def test_fetch_nbs_values_isolates_per_article_failure(
    store: SQLiteEngineStore,
) -> None:
    """One article's 404 / parse error must not roll back values
    parsed for other due rows in the same sweep."""
    _seed_schedule_for_april_2026(store)
    listing_html = _fixture("listing.html")
    article_lookup = {
        "1963288": _press_release("cpi_mar2026.html"),
        # PPI article intentionally omitted — fetcher will raise.
        "1963356": _press_release("ip_mar2026.html"),
        "1963355": _press_release("fai_q1_2026.html"),
        "1963351": _press_release("retail_q1_2026.html"),
    }

    def article_fetcher(url: str) -> str:
        for tail, html in article_lookup.items():
            if tail in url:
                return html
        raise RuntimeError(f"simulated 404 for {url}")

    with store._connection(commit=True) as conn:
        summary = fetch_nbs_values(
            conn,
            dry_run=False,
            listing_fetcher=lambda: listing_html,
            article_fetcher=article_fetcher,
            now_utc=datetime(2026, 4, 30, tzinfo=timezone.utc),
            snapshot_epoch_ms=1_800_000_001_000,
        )

    # PPI failure surfaces in series_failed; the other 4 still land.
    assert summary.observations_seen == 4
    assert summary.events_upserted == 4
    failed = {ind for ind, _ in summary.series_failed}
    assert "PPI" in failed
    assert {"CPI", "INDUSTRIAL_PRODUCTION",
            "FIXED_ASSET_INVESTMENT", "RETAIL_SALES"} <= set(summary.series_ok)


def test_fetch_nbs_values_clamps_lookback_to_recent_window(
    store: SQLiteEngineStore,
) -> None:
    """Older schedule rows whose press-release page has fallen off the
    listing must roll out of the auto-discovery query — otherwise every
    sweep would inflate ``listing_misses`` and burn a listing fetch on
    rows it can never fulfil."""
    # Seed a row whose event_time is ~75 days before "now" — well past
    # the 30-day lookback window.
    snapshot = 1_800_000_000_000
    from ingestion.calendar.nbs_api.parser import NBSReleaseEntry
    e = NBSReleaseEntry(
        year=2026, month=1, day=14,
        release_time_local="9:30",
        indicator="CPI",
        weekday_label="Tue",
        date_cell="14/Tue",
    )
    raw, ev = release_entry_to_records(
        e, snapshot_epoch_ms=snapshot,
        calendar_url="https://example/nbs/cal",
    )
    with store._connection(commit=True) as conn:
        store_raw(conn, [raw])
        project_events(conn, [ev])

    listing_calls = {"n": 0}

    def listing_fetcher() -> str:
        listing_calls["n"] += 1
        return _fixture("listing.html")

    with store._connection(commit=True) as conn:
        summary = fetch_nbs_values(
            conn,
            dry_run=False,
            listing_fetcher=listing_fetcher,
            article_fetcher=lambda url: _press_release("cpi_mar2026.html"),
            now_utc=datetime(2026, 4, 30, tzinfo=timezone.utc),
        )

    # Row falls outside the 14-day lookback → counted as nothing
    # pending → no listing fetch issued.
    assert summary.pending_releases == 0
    assert listing_calls["n"] == 0


def test_schedule_refresh_preserves_value_source_url(
    store: SQLiteEngineStore,
) -> None:
    """After the value sweep writes the press-release URL to
    ``source_url``, a daily schedule re-scrape must not rewrite it
    back to the calendar URL."""
    _seed_schedule_for_april_2026(store)
    listing_html = _fixture("listing.html")
    article_lookup = {
        "1963288": _press_release("cpi_mar2026.html"),
        "1963289": _press_release("ppi_mar2026.html"),
        "1963356": _press_release("ip_mar2026.html"),
        "1963355": _press_release("fai_q1_2026.html"),
        "1963351": _press_release("retail_q1_2026.html"),
    }

    def article_fetcher(url: str) -> str:
        for tail, html in article_lookup.items():
            if tail in url:
                return html
        raise AssertionError(url)

    with store._connection(commit=True) as conn:
        fetch_nbs_values(
            conn,
            dry_run=False,
            listing_fetcher=lambda: listing_html,
            article_fetcher=article_fetcher,
            now_utc=datetime(2026, 4, 30, tzinfo=timezone.utc),
            snapshot_epoch_ms=1_800_000_001_000,
        )
    with store._connection(commit=False) as conn:
        cpi_url_after_value = conn.execute(
            "SELECT source_url FROM cal_econ_event "
            "WHERE provider = 'nbs' AND title = 'China Consumer Price Index'",
        ).fetchone()[0]
    assert "PressRelease" in cpi_url_after_value
    assert "1963288" in cpi_url_after_value

    # Re-run schedule scrape at a later snapshot — would otherwise
    # rewrite source_url to the calendar URL via the schedule-side
    # upsert.
    _seed_schedule_for_april_2026(store)

    with store._connection(commit=False) as conn:
        cpi_url_after_schedule = conn.execute(
            "SELECT source_url FROM cal_econ_event "
            "WHERE provider = 'nbs' AND title = 'China Consumer Price Index'",
        ).fetchone()[0]
    assert cpi_url_after_schedule == cpi_url_after_value


def test_nbs_schedule_refresh_preserves_value_actual(
    store: SQLiteEngineStore,
) -> None:
    """Re-running the schedule scrape must not blank ``actual`` that the
    value-side sweep filled — covered by switching the schedule writer
    to ``project_schedule_events``."""
    _seed_schedule_for_april_2026(store)
    listing_html = _fixture("listing.html")
    article_lookup = {
        "1963288": _press_release("cpi_mar2026.html"),
        "1963289": _press_release("ppi_mar2026.html"),
        "1963356": _press_release("ip_mar2026.html"),
        "1963355": _press_release("fai_q1_2026.html"),
        "1963351": _press_release("retail_q1_2026.html"),
    }

    def article_fetcher(url: str) -> str:
        for tail, html in article_lookup.items():
            if tail in url:
                return html
        raise AssertionError(url)

    # 1) Value sweep fills ``actual``.
    with store._connection(commit=True) as conn:
        fetch_nbs_values(
            conn,
            dry_run=False,
            listing_fetcher=lambda: listing_html,
            article_fetcher=article_fetcher,
            now_utc=datetime(2026, 4, 30, tzinfo=timezone.utc),
            snapshot_epoch_ms=1_800_000_001_000,
        )

    # 2) Schedule scrape re-runs at a *later* snapshot — without the
    #    schedule-only projector path the full upsert would write
    #    ``actual=NULL`` and mark observed_at fresher, clobbering the
    #    value-side write.
    _seed_schedule_for_april_2026(store)

    with store._connection(commit=False) as conn:
        actuals = {
            row[0]: row[1]
            for row in conn.execute(
                "SELECT title, actual FROM cal_econ_event "
                "WHERE provider = ? AND actual IS NOT NULL",
                (PROVIDER,),
            ).fetchall()
        }
    assert "China Consumer Price Index" in actuals
    assert actuals["China Consumer Price Index"] == "1.0"
    # All five value-side titles still carry their actual.
    assert len(actuals) == 5

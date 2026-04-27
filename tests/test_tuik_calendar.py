"""Mocked tests for the TÜİK calendar connector (issue #86 P1).

The captured fixtures
``tests/fixtures/tuik_release_calendar/{2025,2026}.json`` were
recorded live on 2026-04-27 from
``www.tuik.gov.tr/Kurumsal/GetYillikHaberBulteniListesi``. They carry
the full Turkish national release calendar for both years
(~4000 rows each, of which ~390 are TÜİK-owned).

No real HTTP in CI — every test injects the ``json_fetcher`` seam.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from ingestion.calendar.tuik_api import (
    INDICATOR_REGISTRY,
    TUIKCalendarParseError,
    TUIK_RESPONSIBLE_CODE,
    announcement_matches_spec,
    announcement_to_records,
    fetch_tuik_calendar,
    parse_release_calendar,
)
from ingestion.calendar.tuik_api.parser import PROVIDER
from storage.sqlite import SQLiteEngineStore


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "tuik_release_calendar"


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _calendar_json(year: int) -> str:
    return (FIXTURE_DIR / f"{year}.json").read_text(encoding="utf-8")


# ── parser ───────────────────────────────────────────────────────


def test_parse_release_calendar_extracts_only_tuik_owned_rows() -> None:
    announcements = parse_release_calendar(
        _calendar_json(2026), schedule_year=2026,
    )
    # 2026 fixture contains ~4000 rows across all Turkish agencies;
    # the TÜİK-only filter keeps ~390 of them.
    assert 300 < len(announcements) < 500
    # Spot-check by re-loading and confirming the upstream cardinality
    raw = json.loads(_calendar_json(2026))
    total = (
        len(raw["yayindaOlanlarList"]) + len(raw["yayindaOlmayanlarList"])
    )
    assert total > 4 * len(announcements)


def test_parse_release_calendar_localizes_naive_gtarih_to_istanbul() -> None:
    announcements = parse_release_calendar(
        _calendar_json(2026), schedule_year=2026,
    )
    # ``gTarih`` is naive Istanbul wall-clock; the parser must emit
    # UTC-anchored timestamps. Spot-check one.
    cpi_march = next(
        a for a in announcements
        if a.title == "Tüketici Fiyat Endeksi (TÜFE)"
        and a.reference_period_text == "Mart 2026"
    )
    # 2026-04-03 10:00 Europe/Istanbul == 07:00 UTC.
    assert cpi_march.release_datetime_utc.isoformat() == "2026-04-03T07:00:00+00:00"


def test_parse_release_calendar_finds_known_p1_indicator_titles() -> None:
    announcements = parse_release_calendar(
        _calendar_json(2026), schedule_year=2026,
    )
    titles = {a.title for a in announcements}
    assert "Tüketici Fiyat Endeksi (TÜFE)" in titles
    assert "Yurt İçi Üretici Fiyat Endeksi" in titles
    assert "Sanayi Üretim Endeksi" in titles
    assert "İşgücü İstatistikleri" in titles
    assert "Dönemsel Gayrisafi Yurt İçi Hasıla" in titles
    assert "Dış Ticaret İstatistikleri" in titles


def test_parse_release_calendar_returns_empty_for_unpublished_year() -> None:
    """An unpublished future year returns empty arrays (TÜİK posts
    next year's calendar from December onward; before that the JSON
    returns ``{[], []}`` with HTTP 200). Empty arrays are *legitimate*
    — the parser must not raise, otherwise the rolling next-year
    fetch would store a fetch_error and trip the breaker daily for
    most of the year."""
    empty = json.dumps({
        "yayindaOlanlarList":   [],
        "yayindaOlmayanlarList": [],
    })
    assert parse_release_calendar(empty, schedule_year=2099) == []


def test_parse_release_calendar_rejects_missing_arrays() -> None:
    """A response shape without the expected array keys IS a layout
    drift signal (distinct from empty arrays) — must surface loudly."""
    malformed = json.dumps({"foo": "bar"})
    with pytest.raises(TUIKCalendarParseError):
        parse_release_calendar(malformed, schedule_year=2026)


def test_fetch_tuik_calendar_does_not_treat_empty_year_as_outage(store) -> None:
    """A current-year fetch landing rows + a next-year fetch returning
    empty arrays must NOT set ``fetch_error`` — the scheduler reads
    that field as a breaker condition."""
    payloads = {
        2026: _calendar_json(2026),
        2027: '{"yayindaOlanlarList": [], "yayindaOlmayanlarList": []}',
    }
    with store._connection(commit=True) as conn:
        summary = fetch_tuik_calendar(
            conn,
            years=[2026, 2027],
            dry_run=False,
            snapshot_epoch_ms=1_700_000_000_000,
            json_fetcher=lambda y: payloads[y],
        )
    assert summary.fetch_error is None
    assert summary.years_fetched == 2
    assert summary.events_upserted == 64  # all 64 land from the 2026 fixture


def test_parse_release_calendar_skips_non_tuik_rows() -> None:
    payload = {
        "yayindaOlanlarList": [
            {
                "sorumluKisaAd": "TCMB",
                "adi":            "Gösterge Niteliğindeki Merkez Bankası Kurları",
                "gTarih":         "2026-04-27T15:30:00",
                "donemi":         "27-04-2026",
                "id":             61479,
                "link":           "https://www.tcmb.gov.tr/...",
            },
            {
                "sorumluKisaAd": "BDDK",
                "adi":            "Bankacılık Sektörü",
                "gTarih":         "2026-04-30T10:00:00",
                "donemi":         "Mart 2026",
                "id":             88888,
                "link":           "",
            },
            {
                "sorumluKisaAd": TUIK_RESPONSIBLE_CODE,
                "adi":            "Tüketici Fiyat Endeksi (TÜFE)",
                "gTarih":         "2026-05-04T10:00:00",
                "donemi":         "Nisan 2026",
                "id":             99999,
                "link":           "",
            },
        ],
        "yayindaOlmayanlarList": [],
    }
    announcements = parse_release_calendar(
        json.dumps(payload), schedule_year=2026,
    )
    assert len(announcements) == 1
    assert announcements[0].title == "Tüketici Fiyat Endeksi (TÜFE)"


# ── matching + projection ────────────────────────────────────────


def test_isgucu_monthly_matches_but_annual_rolled_up_does_not() -> None:
    """``İşgücü İstatistikleri`` is shared between monthly + annual.

    Filtering by frequency in :func:`announcement_matches_spec`
    keeps only the monthly variant under ``UNEMPLOYMENT_RATE`` —
    without it the annual rollup (``donemi='2025'``) would fall
    through the monthly fallback and collide on the reference key.
    """
    announcements = parse_release_calendar(
        _calendar_json(2026), schedule_year=2026,
    )
    spec = INDICATOR_REGISTRY["UNEMPLOYMENT_RATE"]
    matched = [a for a in announcements if announcement_matches_spec(a, spec)]
    # Twelve monthly releases + zero annual rollups = clean cardinality.
    assert len(matched) == 12
    assert all(a.reference_period_text != "2025" for a in matched)


def test_quarterly_matcher_rejects_monthly_donemi() -> None:
    """A row whose ``adi`` is the GDP prefix but ``donemi`` is monthly
    would be a real upstream anomaly. The frequency filter rejects it."""
    payload = {
        "yayindaOlanlarList": [
            {
                "sorumluKisaAd": TUIK_RESPONSIBLE_CODE,
                "adi":            "Dönemsel Gayrisafi Yurt İçi Hasıla",
                "gTarih":         "2026-06-01T10:00:00",
                "donemi":         "Mart 2026",  # monthly text on a quarterly title
                "id":             1,
                "link":           "",
            },
        ],
        "yayindaOlmayanlarList": [],
    }
    announcements = parse_release_calendar(
        json.dumps(payload), schedule_year=2026,
    )
    spec = INDICATOR_REGISTRY["GDP"]
    matched = [a for a in announcements if announcement_matches_spec(a, spec)]
    assert matched == []


def test_announcement_to_records_anchors_on_reference_month() -> None:
    spec = INDICATOR_REGISTRY["CPI"]
    announcements = parse_release_calendar(
        _calendar_json(2026), schedule_year=2026,
    )
    cpi_feb = next(
        a for a in announcements
        if a.title == "Tüketici Fiyat Endeksi (TÜFE)"
        and a.reference_period_text == "Şubat 2026"
    )
    raw, event = announcement_to_records(
        cpi_feb, spec=spec, snapshot_epoch_ms=1_700_000_000_000,
    )
    assert event.reference_date == "2026-02-01"
    assert event.reference_label == "February 2026"
    assert event.actual is None  # schedule-only slice
    assert event.country_code == "TR"
    assert event.currency == "TRY"
    assert event.source == "Türkiye İstatistik Kurumu"
    assert event.event_time_utc == "2026-03-03T07:00:00Z"


def test_quarterly_gdp_anchors_on_quarter_first_day() -> None:
    spec = INDICATOR_REGISTRY["GDP"]
    announcements = parse_release_calendar(
        _calendar_json(2026), schedule_year=2026,
    )
    gdp_q4 = next(
        a for a in announcements
        if a.title.startswith("Dönemsel Gayrisafi Yurt İçi Hasıla")
        and a.reference_period_text.startswith("IV")
    )
    raw, event = announcement_to_records(
        gdp_q4, spec=spec, snapshot_epoch_ms=1_700_000_000_000,
    )
    assert event.reference_date == "2025-10-01"
    assert event.reference_label == "Q4 2025"


def test_announcement_to_records_includes_bulletin_id_in_payload() -> None:
    spec = INDICATOR_REGISTRY["CPI"]
    announcements = parse_release_calendar(
        _calendar_json(2026), schedule_year=2026,
    )
    cpi_march = next(
        a for a in announcements
        if a.title == "Tüketici Fiyat Endeksi (TÜFE)"
        and a.reference_period_text == "Mart 2026"
    )
    raw, _event = announcement_to_records(
        cpi_march, spec=spec, snapshot_epoch_ms=1_700_000_000_000,
    )
    payload = json.loads(raw.payload_json)
    assert payload["bulletin_id"] == 58295
    assert payload["kind"] == "tuik_release_calendar"
    assert payload["reference_period"] == "Mart 2026"


# ── fetcher integration ──────────────────────────────────────────


def test_fetch_tuik_calendar_dry_run_returns_plan_only(store) -> None:
    with store._connection(commit=False) as conn:
        summary = fetch_tuik_calendar(
            conn, years=[2026], dry_run=True,
        )
    assert summary.dry_run is True
    assert summary.years_planned == [2026]
    assert summary.years_fetched == 0
    assert summary.events_upserted == 0
    assert set(summary.indicators_planned) == set(INDICATOR_REGISTRY)


def test_fetch_tuik_calendar_writes_idempotent_rows(store) -> None:
    payloads = {2026: _calendar_json(2026)}
    with store._connection(commit=True) as conn:
        summary = fetch_tuik_calendar(
            conn,
            years=[2026],
            dry_run=False,
            snapshot_epoch_ms=1_700_000_000_000,
            json_fetcher=lambda y: payloads[y],
        )

    # 12 monthly × 5 indicators + 4 quarterly GDP = 64 events from the
    # 2026 fixture (verified during live probe).
    assert summary.events_upserted == 64
    assert summary.rows_raw_inserted == 64
    assert sorted(summary.indicators_ok) == sorted(INDICATOR_REGISTRY)

    # Re-run with the same snapshot — raw rows are silently ignored
    # (INSERT OR IGNORE on the natural PK), event upsert count
    # matches the first pass (the projector reports rows touched, not
    # rows changed; the natural-key collision keeps event cardinality
    # constant), and total event-table cardinality is unchanged.
    with store._connection(commit=True) as conn:
        rerun = fetch_tuik_calendar(
            conn,
            years=[2026],
            dry_run=False,
            snapshot_epoch_ms=1_700_000_000_001,
            json_fetcher=lambda y: payloads[y],
        )
    assert rerun.rows_raw_inserted == 0
    assert rerun.events_upserted == summary.events_upserted

    with store._connection(commit=False) as conn:
        total = conn.execute(
            "SELECT count(*) FROM cal_econ_event WHERE provider = ?",
            (PROVIDER,),
        ).fetchone()
    assert total[0] == summary.events_upserted


def test_fetch_tuik_calendar_propagates_fetch_error_summary(store) -> None:
    def failing(year: int) -> str:
        raise RuntimeError(f"upstream HTTP 503 for {year}")

    with store._connection(commit=True) as conn:
        summary = fetch_tuik_calendar(
            conn,
            years=[2026],
            dry_run=False,
            snapshot_epoch_ms=1_700_000_000_000,
            json_fetcher=failing,
        )
    assert summary.years_fetched == 0
    assert summary.events_upserted == 0
    assert summary.fetch_error is not None
    assert "503" in summary.fetch_error


def test_fetch_tuik_calendar_writes_provider_event_id(store) -> None:
    payloads = {2026: _calendar_json(2026)}
    with store._connection(commit=True) as conn:
        fetch_tuik_calendar(
            conn,
            years=[2026],
            dry_run=False,
            snapshot_epoch_ms=1_700_000_000_000,
            json_fetcher=lambda y: payloads[y],
        )

    with store._connection(commit=False) as conn:
        rows = conn.execute(
            """
            SELECT provider, country_code, title, reference_date
            FROM cal_econ_event
            WHERE provider = ?
            ORDER BY reference_date DESC
            """,
            (PROVIDER,),
        ).fetchall()
    assert rows
    countries = {r["country_code"] for r in rows}
    assert countries == {"TR"}

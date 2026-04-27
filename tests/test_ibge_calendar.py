"""Mocked tests for the IBGE calendar connector (issue #84 P1).

The captured fixtures
``tests/fixtures/ibge_release_calendar/release_2026_03.html`` and
``release_2026_04.html`` were recorded live on 2026-04-27 from
``ibge.gov.br/calendario/mensal.html``. They carry the IBGE March 2026
release calendar (which contains the quarterly GDP release) and the
April 2026 monthly release calendar (which contains IPCA, IPCA-15,
PIM-PF, and PNAD-Contínua).

No real HTTP in CI — every test injects the ``html_fetcher`` seam.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from ingestion.calendar.ibge_api import (
    INDICATOR_REGISTRY,
    IBGECalendarParseError,
    announcement_to_records,
    fetch_ibge_calendar,
    parse_release_calendar,
)
from ingestion.calendar.ibge_api.parser import PROVIDER
from storage.sqlite import SQLiteEngineStore


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "ibge_release_calendar"


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _calendar_html(year: int, month: int) -> str:
    return (FIXTURE_DIR / f"release_{year}_{month:02d}.html").read_text(encoding="utf-8")


def _two_month_fetcher(year: int, month: int) -> str:
    return _calendar_html(year, month)


# ── parser ───────────────────────────────────────────────────────


def test_parse_release_calendar_extracts_data_divulgacao_timestamps() -> None:
    announcements = parse_release_calendar(
        _calendar_html(2026, 4),
        schedule_year=2026,
        schedule_month=4,
    )
    assert announcements
    # Every parsed row carries a UTC datetime — the release_datetime_utc
    # field is normalised away from the page's local UTC-3 offset.
    for a in announcements:
        assert a.release_datetime_utc.utcoffset().total_seconds() == 0


def test_parse_release_calendar_finds_known_p1_indicator_titles() -> None:
    announcements = parse_release_calendar(
        _calendar_html(2026, 4),
        schedule_year=2026,
        schedule_month=4,
    )
    titles = {a.title for a in announcements}
    # Each headline P1 indicator's product surface name should appear
    # in the April 2026 fixture (IPCA + IPCA-15 + PIM-PF + PNAD-CM).
    assert any(
        "Índice Nacional de Preços ao Consumidor Amplo" in t
        and "15" not in t for t in titles
    )
    assert any(
        "Índice Nacional de Preços ao Consumidor Amplo 15" in t for t in titles
    )
    assert any(
        "Pesquisa Industrial Mensal: Produção Física" in t for t in titles
    )
    assert any(
        "Pesquisa Nacional por Amostra de Domicílios Contínua Mensal" in t for t in titles
    )


def test_parse_release_calendar_extracts_reference_period() -> None:
    announcements = parse_release_calendar(
        _calendar_html(2026, 4),
        schedule_year=2026,
        schedule_month=4,
    )
    ipca_15 = next(
        a for a in announcements
        if a.title == "Índice Nacional de Preços ao Consumidor Amplo 15"
    )
    assert ipca_15.reference_period_text == "4/2026"
    pim_pf = next(
        a for a in announcements
        if a.title == "Pesquisa Industrial Mensal: Produção Física - Brasil"
    )
    assert pim_pf.reference_period_text == "2/2026"


def test_parse_release_calendar_finds_quarterly_gdp_in_march_fixture() -> None:
    """The March 2026 fixture publishes the quarterly Sistema de Contas
    Nacionais (PIB / GDP) release on 2026-03-03 09:00 BRT. IBGE writes
    the quarterly reference period as a month range — ``"10/2025 a
    12/2025"`` for Q4 2025."""
    announcements = parse_release_calendar(
        _calendar_html(2026, 3),
        schedule_year=2026,
        schedule_month=3,
    )
    pib = next(
        a for a in announcements
        if a.produto_id == "9300"
    )
    assert pib.reference_period_text == "10/2025 a 12/2025"
    assert pib.release_datetime_utc.year == 2026
    assert pib.release_datetime_utc.month == 3


def test_parse_release_calendar_raises_on_zero_rows() -> None:
    with pytest.raises(IBGECalendarParseError, match="zero event rows"):
        parse_release_calendar(
            "<html><body><p>maintenance</p></body></html>",
            schedule_year=2030,
            schedule_month=1,
        )


# ── projection ───────────────────────────────────────────────────


def test_announcement_to_records_anchors_ipca_at_brt_window() -> None:
    """IPCA 09:00 BRT = 12:00 UTC year-round (Brazil dropped DST in 2019)."""
    announcements = parse_release_calendar(
        _calendar_html(2026, 4),
        schedule_year=2026,
        schedule_month=4,
    )
    ipca = next(
        a for a in announcements
        if a.title == "Índice Nacional de Preços ao Consumidor Amplo"
    )
    spec = INDICATOR_REGISTRY["CPI"]
    raw_rec, event_rec = announcement_to_records(
        ipca, spec=spec, snapshot_epoch_ms=1_800_000_000_000,
    )
    assert event_rec.country_code == "BR"
    assert event_rec.title == "Brazil Consumer Price Index"
    assert event_rec.currency == "BRL"
    assert event_rec.actual is None  # schedule-only slice
    assert event_rec.event_time_precision == "datetime"
    # Apr 10 09:00 BRT (UTC-3) = Apr 10 12:00 UTC.
    assert event_rec.event_time_utc.startswith("2026-04-10T12:00:00")
    assert event_rec.reference_date == "2026-03-01"
    assert event_rec.reference_label == "March 2026"


def test_announcement_to_records_quarterly_gdp_anchors_on_quarter() -> None:
    """Quarterly PIB release on 2026-03-03 covers Q4 2025 — reference_date
    must land on 2025-10-01 (start of Q4 2025), label ``"Q4 2025"``."""
    announcements = parse_release_calendar(
        _calendar_html(2026, 3),
        schedule_year=2026,
        schedule_month=3,
    )
    pib = next(
        a for a in announcements
        if a.title == "Sistema de Contas Nacionais Trimestrais"
    )
    spec = INDICATOR_REGISTRY["GDP"]
    _, event_rec = announcement_to_records(
        pib, spec=spec, snapshot_epoch_ms=1_800_000_000_000,
    )
    assert event_rec.reference_date == "2025-10-01"
    assert event_rec.reference_label == "Q4 2025"
    # Mar 03 09:00 BRT (UTC-3) = Mar 03 12:00 UTC.
    assert event_rec.event_time_utc.startswith("2026-03-03T12:00:00")
    assert event_rec.country_code == "BR"


def test_announcement_to_records_provider_event_id_stable_across_runs() -> None:
    announcements = parse_release_calendar(
        _calendar_html(2026, 4),
        schedule_year=2026,
        schedule_month=4,
    )
    ipca = next(
        a for a in announcements
        if a.title == "Índice Nacional de Preços ao Consumidor Amplo"
    )
    spec = INDICATOR_REGISTRY["CPI"]
    _, event_rec_a = announcement_to_records(
        ipca, spec=spec, snapshot_epoch_ms=1_800_000_000_000,
    )
    _, event_rec_b = announcement_to_records(
        ipca, spec=spec, snapshot_epoch_ms=2_000_000_000_000,
    )
    assert event_rec_a.provider_event_id == event_rec_b.provider_event_id


# ── full fetch driver ───────────────────────────────────────────


def test_fetch_ibge_calendar_writes_events_for_p1_indicators(
    store: SQLiteEngineStore,
) -> None:
    """Fetching the two captured months should land at least the four
    headline monthly indicators (IPCA / IPCA-15 / PIM-PF / PNAD-CM)
    plus the quarterly PIB release."""
    months = [(2026, 3), (2026, 4)]
    with store._connection(commit=True) as conn:
        summary = fetch_ibge_calendar(
            conn,
            dry_run=False,
            months=months,
            html_fetcher=_two_month_fetcher,
            snapshot_epoch_ms=1_800_000_000_000,
        )
    assert summary.fetch_error is None
    assert summary.months_fetched == 2
    assert "CPI" in summary.indicators_ok
    assert "IPCA_15" in summary.indicators_ok
    assert "INDUSTRIAL_PRODUCTION" in summary.indicators_ok
    assert "UNEMPLOYMENT_RATE" in summary.indicators_ok
    assert "GDP" in summary.indicators_ok
    # Each month yields one row per matched indicator; the two fixtures
    # together carry at least 6 events (5 distinct indicators, one or
    # two months each).
    assert summary.events_upserted >= 6


def test_fetch_ibge_calendar_does_not_double_count_ipca_when_ipca_15_matches(
    store: SQLiteEngineStore,
) -> None:
    """IPCA's title substring is a prefix of IPCA-15's. The fetcher's
    matcher must attribute each row to exactly one indicator; the
    IPCA-15 row must NOT also surface as a CPI event."""
    with store._connection(commit=True) as conn:
        summary = fetch_ibge_calendar(
            conn,
            dry_run=False,
            months=[(2026, 4)],
            html_fetcher=_two_month_fetcher,
            snapshot_epoch_ms=1_800_000_000_000,
        )
        rows = conn.execute(
            "SELECT title FROM cal_econ_event WHERE provider=? ORDER BY title",
            (PROVIDER,),
        ).fetchall()
    titles = [r[0] for r in rows]
    cpi_count = titles.count("Brazil Consumer Price Index")
    ipca_15_count = titles.count("Brazil IPCA-15 Mid-month CPI")
    # Exactly one of each in April 2026.
    assert cpi_count == 1
    assert ipca_15_count == 1
    assert summary.events_upserted == len(rows)


def test_fetch_ibge_calendar_records_fetch_error_on_outage(
    store: SQLiteEngineStore,
) -> None:
    def broken(year: int, month: int) -> str:
        raise RuntimeError("simulated 503 from IBGE")

    with store._connection(commit=True) as conn:
        summary = fetch_ibge_calendar(
            conn,
            dry_run=False,
            months=[(2026, 4)],
            html_fetcher=broken,
        )
    assert summary.fetch_error is not None
    assert summary.events_upserted == 0


def test_fetch_ibge_calendar_records_parse_error_on_empty_page(
    store: SQLiteEngineStore,
) -> None:
    def empty(year: int, month: int) -> str:
        return "<html><body><h1>maintenance window</h1></body></html>"

    with store._connection(commit=True) as conn:
        summary = fetch_ibge_calendar(
            conn,
            dry_run=False,
            months=[(2026, 4)],
            html_fetcher=empty,
        )
    assert summary.fetch_error is not None
    assert summary.events_upserted == 0


def test_fetch_ibge_calendar_dry_run_returns_plan(
    store: SQLiteEngineStore,
) -> None:
    with store._connection(commit=False) as conn:
        summary = fetch_ibge_calendar(conn, dry_run=True)
    assert summary.dry_run is True
    assert set(summary.indicators_planned) == set(INDICATOR_REGISTRY)


def test_fetch_ibge_calendar_idempotent_on_repeat(
    store: SQLiteEngineStore,
) -> None:
    """The provider_event_id is stable per (indicator, reference period)
    so a second sweep over the same calendar writes zero new events."""
    months = [(2026, 3), (2026, 4)]
    with store._connection(commit=True) as conn:
        first = fetch_ibge_calendar(
            conn, dry_run=False, months=months,
            html_fetcher=_two_month_fetcher,
            snapshot_epoch_ms=1_800_000_000_000,
        )
        second = fetch_ibge_calendar(
            conn, dry_run=False, months=months,
            html_fetcher=_two_month_fetcher,
            snapshot_epoch_ms=1_800_000_000_001,
        )
    assert first.events_upserted >= 6
    assert first.events_upserted == second.events_upserted
    with store._connection(commit=False) as conn:
        rows = conn.execute(
            "SELECT count(*) FROM cal_econ_event WHERE provider=?", (PROVIDER,),
        ).fetchone()
    assert rows[0] == first.events_upserted


# ── scheduler + agency wiring ───────────────────────────────────


def test_ibge_listed_in_default_rosters() -> None:
    from ingestion.calendar.scheduler import (
        ALL_CONNECTORS,
        ALL_VALUE_SIDE_CONNECTORS,
    )
    assert "ibge" in ALL_CONNECTORS
    assert "ibge" in ALL_VALUE_SIDE_CONNECTORS


def test_ibge_agency_attribution_provider_only_in_p1() -> None:
    """IBGE owns provider attribution for BR macro indicators, but the
    P1 slice ships schedule-only events (``actual=NULL``); wiring
    ``(BR, …)`` into the parity whitelist would trip the
    parse_failed-on-missing-actual path on every release. P2 adds the
    per-release detail-page scrape for the value side."""
    from ingestion.calendar.agency_registry import (
        agency_for,
        provider_to_agency,
    )
    ibge_agency = provider_to_agency("ibge")
    assert ibge_agency is not None and ibge_agency.agency_id == "IBGE"
    assert ibge_agency.indicators == frozenset()
    assert agency_for("BR", "CPI") is None
    assert agency_for("BR", "INDUSTRIAL_PRODUCTION") is None
    assert agency_for("BR", "UNEMPLOYMENT_RATE") is None
    assert agency_for("BR", "GDP") is None
    assert agency_for("BR", "IPCA_15") is None


def test_ibge_canonicalize_aliases_resolve_release_titles() -> None:
    from ingestion.calendar._official_shared import canonicalize_indicator
    assert canonicalize_indicator(
        "Brazil Consumer Price Index",
    ) == "CPI"
    assert canonicalize_indicator(
        "Brazil IPCA-15 Mid-month CPI",
    ) == "IPCA_15"
    assert canonicalize_indicator(
        "Brazil Industrial Production",
    ) == "INDUSTRIAL_PRODUCTION"
    assert canonicalize_indicator(
        "Brazil Unemployment Rate",
    ) == "UNEMPLOYMENT_RATE"
    assert canonicalize_indicator(
        "Brazil GDP",
    ) == "GDP"

"""Mocked tests for the INEGI calendar connector (issue #88 P1).

Fixtures captured live on 2026-04-28 from
``inegi.org.mx/app/api/saladeprensa/api/saladeprensa/ObtenerFechasTabla/v3``
— the per-program 2026 release schedule for INPC / PIB / IMAI / IGAE
/ ENOE / Balanza Comercial. No real HTTP in CI — every test injects
the ``json_fetcher`` seam.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from ingestion.calendar.inegi_api import (
    INDICATOR_REGISTRY,
    INEGICalendarParseError,
    INEGIReleaseAnnouncement,
    announcement_matches_spec,
    announcement_to_records,
    fetch_inegi_calendar,
    parse_release_calendar,
)
from ingestion.calendar.inegi_api.parser import (
    INEGI_CALENDAR_PUBLIC_URL,
    PROVIDER,
)
from storage.sqlite import SQLiteEngineStore


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "inegi_release_calendar"


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _program_payload(pid: str) -> list[dict]:
    return json.loads(
        (FIXTURE_DIR / f"program_{pid}_2026.json").read_text(encoding="utf-8"),
    )


def _all_programs_payload() -> list[dict]:
    return json.loads(
        (FIXTURE_DIR / "all_programs_2026Q2Q3.json").read_text(encoding="utf-8"),
    )


# ── parser ───────────────────────────────────────────────────────


def test_parse_release_calendar_returns_rows_for_inpc_2026() -> None:
    rows = parse_release_calendar(
        _program_payload("2353"),
        fetched_pid="2353",
        schedule_year=2026,
    )
    assert rows
    # Every row is INPC under idPrograma 2353; January is the earliest
    # captured.
    assert min(r.fecha for r in rows) <= date(2026, 1, 24)
    assert all(r.fetched_pid == "2353" for r in rows)


def test_parse_release_calendar_skips_rows_missing_fecha_or_programa() -> None:
    """Rows without a ``fecha`` or ``programa`` are dropped silently."""
    payload = [
        {"fecha": "01/04/2026", "programa": "Headline", "idNoticia": "1"},
        {"fecha": "", "programa": "No fecha"},
        {"programa": "No fecha key"},
        {"fecha": "02/04/2026", "programa": ""},
        {"fecha": "03/04/2026", "programa": "Another", "idNoticia": "2"},
    ]
    rows = parse_release_calendar(
        payload, fetched_pid="2353", schedule_year=2026,
    )
    assert {r.programa for r in rows} == {"Headline", "Another"}


def test_parse_release_calendar_returns_empty_on_empty_array() -> None:
    """Empty arrays are legitimate (no scheduled releases in window).

    INEGI's API returns ``[]`` for an idPrograma with no releases in the
    requested window, and the daily fetcher must treat that as a no-op
    rather than a fetch_error / circuit-breaker trip.
    """
    rows = parse_release_calendar([], fetched_pid="0", schedule_year=2026)
    assert rows == []


def test_parse_release_calendar_raises_on_non_array_payload() -> None:
    """A JSON object instead of an array (or an object string-encoded
    as JSON) is a layout-drift signal we want loud."""
    with pytest.raises(INEGICalendarParseError):
        parse_release_calendar(
            {"foo": "bar"},  # type: ignore[arg-type]
            fetched_pid="0", schedule_year=2026,
        )
    with pytest.raises(INEGICalendarParseError, match="not a JSON array"):
        parse_release_calendar(
            '{"foo": "bar"}', fetched_pid="0", schedule_year=2026,
        )


def test_parse_release_calendar_preserves_pdf_url_when_url_consulta_blank() -> None:
    """For published INEGI rows where ``urlConsulta`` is empty and
    ``comunicadoEsUrlPdf`` is populated, the parser must preserve the
    boletín PDF on the announcement so the audit payload doesn't drop
    the per-release citation that the deferred P2 value scrape needs.
    """
    payload = [
        {
            "fecha": "08/01/2026",
            "programa": "Índice Nacional de Precios al Consumidor (INPC).",
            "periodo": "Diciembre de 2025",
            "idNoticia": "10001",
            "urlConsulta": "",
            "comunicadoEsUrlPdf": "/saladeprensa/boletines/2026/inpc/inpc_2q2026_01.pdf",
        },
        {
            "fecha": "09/01/2026",
            "programa": "Índice Nacional de Precios al Consumidor (INPC).",
            "periodo": "Diciembre de 2025",
            "idNoticia": "10002",
            "urlConsulta": "https://www.inegi.org.mx/temas/inpc/",
            "comunicadoEsUrlPdf": "/saladeprensa/boletines/2026/inpc/inpc_2q2026_02.pdf",
        },
    ]
    rows = parse_release_calendar(
        payload, fetched_pid="2353", schedule_year=2026,
    )
    by_id = {r.id_noticia: r for r in rows}
    # Row with blank urlConsulta falls back to the resolved PDF URL.
    assert by_id["10001"].pdf_url == (
        "https://www.inegi.org.mx/saladeprensa/boletines/"
        "2026/inpc/inpc_2q2026_01.pdf"
    )
    assert by_id["10001"].detail_url == by_id["10001"].pdf_url
    # Row with both — detail_url stays urlConsulta but pdf_url remains
    # populated for the deferred value scrape.
    assert by_id["10002"].detail_url == "https://www.inegi.org.mx/temas/inpc/"
    assert by_id["10002"].pdf_url == (
        "https://www.inegi.org.mx/saladeprensa/boletines/"
        "2026/inpc/inpc_2q2026_02.pdf"
    )


def test_parse_release_calendar_raises_on_unparseable_payload() -> None:
    with pytest.raises(INEGICalendarParseError, match="parseable JSON"):
        parse_release_calendar(
            "not json", fetched_pid="0", schedule_year=2026,
        )


# ── matcher ──────────────────────────────────────────────────────


def test_inpc_monthly_split_from_quincenal_via_cadence_filter() -> None:
    """INPC monthly (CPI) and quincenal mid-month (INPC_15) share
    idPrograma 2353. The cadence filter splits them: ``periodo`` matching
    ``"Marzo de 2026"`` lands under CPI; ``"Primera quincena de Marzo"``
    lands under INPC_15.
    """
    rows = parse_release_calendar(
        _program_payload("2353"),
        fetched_pid="2353",
        schedule_year=2026,
    )
    cpi_spec = INDICATOR_REGISTRY["CPI"]
    inpc15_spec = INDICATOR_REGISTRY["INPC_15"]
    cpi_rows = [r for r in rows if announcement_matches_spec(r, cpi_spec)]
    inpc15_rows = [r for r in rows if announcement_matches_spec(r, inpc15_spec)]

    # Both subsets should be non-empty; their reference periods should
    # be mutually exclusive.
    assert cpi_rows
    assert inpc15_rows
    cpi_periodos = {r.reference_period_text for r in cpi_rows}
    inpc15_periodos = {r.reference_period_text for r in inpc15_rows}
    assert cpi_periodos.isdisjoint(inpc15_periodos)
    # Sanity: the quincenal subset's text always starts with "Primera
    # quincena", and the monthly subset never does.
    assert all(
        p.lower().startswith("primera quincena")
        for p in inpc15_periodos
    )
    assert not any(
        p.lower().startswith("primera quincena")
        for p in cpi_periodos
    )


def test_enoe_monthly_split_from_quarterly_via_cadence_filter() -> None:
    """ENOE publishes both a monthly headline (``"Enero de 2024"``) and
    a quarterly bulletin (``"Cuarto trimestre de 2023"``) under the same
    idPrograma 2303. The cadence filter pins UNEMPLOYMENT_RATE to the
    monthly variant.
    """
    rows = parse_release_calendar(
        _program_payload("2303"),
        fetched_pid="2303",
        schedule_year=2026,
    )
    spec = INDICATOR_REGISTRY["UNEMPLOYMENT_RATE"]
    matched = [r for r in rows if announcement_matches_spec(r, spec)]
    assert matched
    # No row in the matched set has a ``trimestre`` reference period.
    assert not any(
        "trimestre" in r.reference_period_text.lower()
        for r in matched
    )


def test_gdp_real_split_from_nominal_via_programa_substring() -> None:
    """PIBT publishes two same-day boletines per quarter — Precios
    Constantes (real / volume) and Precios Corrientes (nominal). Both
    share idPrograma 2648 and the same ``periodo``; without the
    ``programa_includes=("Precios Constantes",)`` filter the two would
    collide on the GDP reference key and the second upsert would
    overwrite the first.
    """
    rows = parse_release_calendar(
        _program_payload("2648"),
        fetched_pid="2648",
        schedule_year=2026,
    )
    spec = INDICATOR_REGISTRY["GDP"]
    matched = [r for r in rows if announcement_matches_spec(r, spec)]
    assert matched
    assert all("precios constantes" in r.programa.lower() for r in matched)
    assert not any("precios corrientes" in r.programa.lower() for r in matched)
    # Each quarter shows up exactly once after the filter (compared to
    # twice in the raw ``rows`` list).
    quarters = [r.reference_period_text for r in matched]
    assert len(quarters) == len(set(quarters))


def test_trade_balance_advance_split_via_programa_substring() -> None:
    """Balanza Comercial publishes both ``"Información oportuna"``
    (advance) and ``"Cifras revisadas"`` (revised) under the same
    idPrograma 2355. The advance variant is the headline; the
    ``programa_includes`` filter pins TRADE_BALANCE to it.
    """
    rows = parse_release_calendar(
        _program_payload("2355"),
        fetched_pid="2355",
        schedule_year=2026,
    )
    spec = INDICATOR_REGISTRY["TRADE_BALANCE"]
    matched = [r for r in rows if announcement_matches_spec(r, spec)]
    assert matched
    assert all(
        "información oportuna" in r.programa.lower()
        for r in matched
    )
    # Revised variant is excluded.
    assert not any(
        "cifras revisadas" in r.programa.lower()
        for r in matched
    )


def test_announcement_matches_spec_rejects_wrong_pid() -> None:
    spec = INDICATOR_REGISTRY["CPI"]
    announcement = INEGIReleaseAnnouncement(
        fecha=date(2026, 4, 9),
        id_fecha_publicacion="1",
        id_noticia="1",
        programa="Índice Nacional de Precios al Consumidor (INPC).",
        reference_period_text="Marzo de 2026",
        subtitulo="",
        detail_url="",
        pdf_url="",
        fetched_pid="9999",  # wrong pid
        schedule_year=2026,
    )
    assert not announcement_matches_spec(announcement, spec)


# ── projection ───────────────────────────────────────────────────


def test_announcement_to_records_anchors_event_at_06_local_post_dst() -> None:
    """Mexico abolished federal DST on 30 October 2022; post-2022
    America/Mexico_City sits at UTC−6 year-round. 06:00 local → 12:00 UTC.
    """
    spec = INDICATOR_REGISTRY["CPI"]
    announcement = INEGIReleaseAnnouncement(
        fecha=date(2026, 4, 9),
        id_fecha_publicacion="abc123",
        id_noticia="10911",
        programa="Índice Nacional de Precios al Consumidor (INPC).",
        reference_period_text="Marzo de 2026",
        subtitulo="Inflación subió a 4.0%",
        detail_url="https://www.inegi.org.mx/temas/inpc/",
        pdf_url="https://www.inegi.org.mx/saladeprensa/boletines/2026/inpc/inpc_2q2026_04.pdf",
        fetched_pid="2353",
        schedule_year=2026,
    )
    raw, event = announcement_to_records(
        announcement, spec=spec, snapshot_epoch_ms=1_800_000_000_000,
    )
    assert event.event_time_utc == "2026-04-09T12:00:00Z"
    assert event.country_code == "MX"
    assert event.currency == "MXN"
    assert event.title == "Mexico Consumer Price Index"
    # Released 9 April 2026; reference period text "Marzo de 2026" →
    # March 2026 data anchors on 2026-03-01.
    assert event.reference_date == "2026-03-01"
    assert event.reference_label == "March 2026"
    assert event.actual is None  # schedule-only
    assert event.previous is None
    assert event.source_url == INEGI_CALENDAR_PUBLIC_URL
    payload = json.loads(raw.payload_json)
    assert payload["kind"] == "inegi_release_calendar"
    assert payload["id_fecha_publicacion"] == "abc123"
    assert payload["id_noticia"] == "10911"


def test_announcement_to_records_handles_pre_2022_dst_window() -> None:
    """Mexico observed DST until 30 October 2022 (UTC−5 summer). A May
    2018 row falls inside that window; ZoneInfo for America/Mexico_City
    must resolve the correct historical offset.
    """
    spec = INDICATOR_REGISTRY["CPI"]
    announcement = INEGIReleaseAnnouncement(
        fecha=date(2018, 5, 9),
        id_fecha_publicacion="x",
        id_noticia="x",
        programa="Índice Nacional de Precios al Consumidor (INPC).",
        reference_period_text="Abril de 2018",
        subtitulo="",
        detail_url="",
        pdf_url="",
        fetched_pid="2353",
        schedule_year=2018,
    )
    _, event = announcement_to_records(
        announcement, spec=spec, snapshot_epoch_ms=1_800_000_000_000,
    )
    # 06:00 CDT (UTC-5) = 11:00 UTC.
    assert event.event_time_utc == "2018-05-09T11:00:00Z"


def test_announcement_to_records_quarterly_reference_anchors_on_quarter() -> None:
    spec = INDICATOR_REGISTRY["GDP"]
    announcement = INEGIReleaseAnnouncement(
        fecha=date(2026, 5, 22),
        id_fecha_publicacion="1",
        id_noticia="1",
        programa="Producto Interno Bruto Trimestral (PIBT). Año base 2018.",
        reference_period_text="Primer trimestre de 2026",
        subtitulo="",
        detail_url="",
        pdf_url="",
        fetched_pid="2648",
        schedule_year=2026,
    )
    _, event = announcement_to_records(
        announcement, spec=spec, snapshot_epoch_ms=1_800_000_000_000,
    )
    assert event.reference_date == "2026-01-01"
    assert event.reference_label == "Q1 2026"


def test_announcement_to_records_quincenal_infers_year_from_fecha() -> None:
    """``"Primera quincena de Enero"`` carries no explicit year — the
    parser infers it from the publication date (the quincenal preview
    is always published in the same month as the reference period).
    """
    spec = INDICATOR_REGISTRY["INPC_15"]
    announcement = INEGIReleaseAnnouncement(
        fecha=date(2026, 1, 23),
        id_fecha_publicacion="1",
        id_noticia="1",
        programa="Índice Nacional de Precios al Consumidor (INPC).",
        reference_period_text="Primera quincena de Enero",
        subtitulo="",
        detail_url="",
        pdf_url="",
        fetched_pid="2353",
        schedule_year=2026,
    )
    _, event = announcement_to_records(
        announcement, spec=spec, snapshot_epoch_ms=1_800_000_000_000,
    )
    assert event.reference_date == "2026-01-01"
    assert event.reference_label.startswith("H1 ")


def test_announcement_to_records_stable_provider_event_id_across_snapshots() -> None:
    spec = INDICATOR_REGISTRY["CPI"]
    announcement = INEGIReleaseAnnouncement(
        fecha=date(2026, 4, 9),
        id_fecha_publicacion="abc",
        id_noticia="abc",
        programa="Índice Nacional de Precios al Consumidor (INPC).",
        reference_period_text="Marzo de 2026",
        subtitulo="",
        detail_url="",
        pdf_url="",
        fetched_pid="2353",
        schedule_year=2026,
    )
    _, ev1 = announcement_to_records(
        announcement, spec=spec, snapshot_epoch_ms=1_700_000_000_000,
    )
    _, ev2 = announcement_to_records(
        announcement, spec=spec, snapshot_epoch_ms=1_900_000_000_000,
    )
    assert ev1.provider_event_id == ev2.provider_event_id


# ── full fetch driver ───────────────────────────────────────────


def test_fetch_inegi_calendar_dry_run_returns_plan(store) -> None:
    with store._connection(commit=False) as conn:
        summary = fetch_inegi_calendar(conn, dry_run=True)
    assert summary.dry_run is True
    assert set(summary.indicators_planned) == set(INDICATOR_REGISTRY.keys())
    # CPI and INPC_15 both ride on idPrograma 2353; the de-duplicated
    # plan retains only the unique pids.
    assert len(set(summary.pids_planned)) == len(summary.pids_planned)
    assert "2353" in summary.pids_planned


def test_fetch_inegi_calendar_writes_events_for_each_program(store) -> None:
    pid_to_payload = {
        pid: _program_payload(pid)
        for pid in {
            spec.tematica_ids[0]
            for spec in INDICATOR_REGISTRY.values()
        }
    }

    def fetcher(pid, fd, fh):
        return pid_to_payload.get(pid, [])

    with store._connection(commit=True) as conn:
        summary = fetch_inegi_calendar(
            conn,
            dry_run=False,
            snapshot_epoch_ms=1_700_000_000_000,
            json_fetcher=fetcher,
        )
    assert summary.fetch_error is None
    assert summary.events_upserted > 0
    # All six P1 indicators land at least one row.
    assert set(summary.indicators_ok) == set(INDICATOR_REGISTRY.keys())

    with store._connection(commit=False) as conn:
        rows = conn.execute(
            "SELECT count(*) FROM cal_econ_event WHERE provider = ?",
            (PROVIDER,),
        ).fetchone()
    assert rows[0] == summary.events_upserted


def test_fetch_inegi_calendar_idempotent_on_repeat(store) -> None:
    pid_to_payload = {
        pid: _program_payload(pid)
        for pid in {
            spec.tematica_ids[0]
            for spec in INDICATOR_REGISTRY.values()
        }
    }
    fetcher = lambda pid, fd, fh: pid_to_payload.get(pid, [])

    with store._connection(commit=True) as conn:
        first = fetch_inegi_calendar(
            conn, dry_run=False,
            snapshot_epoch_ms=1_700_000_000_000,
            json_fetcher=fetcher,
        )
        second = fetch_inegi_calendar(
            conn, dry_run=False,
            snapshot_epoch_ms=1_700_000_000_001,
            json_fetcher=fetcher,
        )
    assert first.events_upserted > 0
    # Raw rows collapse to zero on the second pass; cardinality stays
    # the same.
    assert second.rows_raw_inserted == 0
    assert second.events_upserted == first.events_upserted


def test_fetch_inegi_calendar_records_fetch_error_on_outage(store) -> None:
    import requests

    def broken(pid, fd, fh):
        raise requests.exceptions.ConnectionError("simulated INEGI outage")

    with store._connection(commit=True) as conn:
        summary = fetch_inegi_calendar(
            conn, dry_run=False, json_fetcher=broken,
        )
    assert summary.fetch_error is not None
    assert summary.events_upserted == 0


def test_fetch_inegi_calendar_records_parse_error_on_object_payload(store) -> None:
    """An ``Obtener...`` JSON response that returns a dict instead of an
    array is a layout-drift signal — surface it as a fetch_error rather
    than silently ingesting zero rows.
    """
    def odd(pid, fd, fh):
        return {"oops": "wrong shape"}  # type: ignore[return-value]

    with store._connection(commit=True) as conn:
        summary = fetch_inegi_calendar(
            conn, dry_run=False, json_fetcher=odd,
        )
    assert summary.fetch_error is not None
    assert summary.events_upserted == 0


# ── scheduler + agency wiring ───────────────────────────────────


def test_inegi_listed_in_default_rosters() -> None:
    from ingestion.calendar.scheduler import (
        ALL_CONNECTORS,
        ALL_VALUE_SIDE_CONNECTORS,
    )
    assert "inegi" in ALL_CONNECTORS
    assert "inegi" in ALL_VALUE_SIDE_CONNECTORS


def test_inegi_parity_whitelist_empty_in_p1() -> None:
    """INEGI stays out of the parity whitelist in P1 — same deferral
    pattern as the IBGE / KOSTAT / MoSPI / TÜİK schedule-only slices.
    P2 fills ``actual`` from the per-release boletín scrape and then
    ``(MX, ...)`` pairs join."""
    from ingestion.calendar.agency_registry import AGENCIES

    inegi_decl = next(a for a in AGENCIES if a.agency_id == "INEGI")
    assert inegi_decl.indicators == frozenset()


def test_inegi_canonicalize_aliases_resolve_titles() -> None:
    from ingestion.calendar._official_shared import canonicalize_indicator
    assert canonicalize_indicator("INPC") == "CPI"
    assert canonicalize_indicator("Mexico Inflation Rate") == "CPI"
    assert canonicalize_indicator("Mexico INPC Mid-month CPI") == "INPC_15"
    assert canonicalize_indicator("Mexico GDP") == "GDP"
    assert canonicalize_indicator("Mexico Industrial Production") == "INDUSTRIAL_PRODUCTION"
    assert canonicalize_indicator("ENOE") == "UNEMPLOYMENT_RATE"
    assert canonicalize_indicator(
        "Balanza Comercial de Mercancías de México",
    ) == "TRADE_BALANCE"


def test_mexico_country_alias_resolves() -> None:
    from storage.queries.calendar import _calendar_country_code
    assert _calendar_country_code("Mexico") == "MX"
    assert _calendar_country_code("MEX") == "MX"
    assert _calendar_country_code("México") == "MX"

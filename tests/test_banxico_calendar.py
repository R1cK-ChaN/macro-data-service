"""Mocked tests for the Banxico calendar connector (issue #88 P1).

Fixture captured live on 2026-04-28 from
``banxico.org.mx/publicaciones-y-prensa/anuncios-de-las-decisiones-de-politica-monetaria/anuncios-politica-monetaria-t.html``
— the full Junta de Gobierno decision history. The page returns
ISO-8859-1 text. No real HTTP in CI — every test injects the
``html_fetcher`` seam.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from ingestion.calendar.banxico_api import (
    INDICATOR_REGISTRY,
    BanxicoDecisionsParseError,
    BanxicoRateDecision,
    decision_to_records,
    fetch_banxico_calendar,
    parse_decisions_history,
)
from ingestion.calendar.banxico_api.parser import (
    BANXICO_DECISIONS_URL,
    PROVIDER,
)
from storage.sqlite import SQLiteEngineStore


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "banxico_rate"


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _decisions_html() -> str:
    # Banxico's public page is served as ISO-8859-1; preserve that on
    # disk-read so the parser sees the same byte-for-byte surface as
    # production.
    return (FIXTURE_DIR / "anuncios_politica_monetaria.html").read_text(
        encoding="iso-8859-1",
    )


# ── parser ───────────────────────────────────────────────────────


def test_parse_decisions_history_returns_decisions_most_recent_first() -> None:
    decisions = parse_decisions_history(_decisions_html())
    assert decisions
    for prev, curr in zip(decisions, decisions[1:]):
        assert prev.announcement_date >= curr.announcement_date


def test_parse_decisions_history_anchors_seed_at_2008() -> None:
    """The modern Tasa Objetivo regime began on 21 January 2008. Pre-
    2008 ``"corto"`` rows are filtered out at parse time, so the oldest
    row in the parsed list is the earliest tasa-objetivo decision on
    the page."""
    decisions = parse_decisions_history(_decisions_html())
    earliest = min(d.announcement_date for d in decisions)
    assert earliest.year == 2008


def test_parse_decisions_history_recovers_latest_known_rate() -> None:
    """The 2026-03-26 announcement was a 25 bps cut from 7.00% to 6.75%
    (verified at fixture-capture time, 2026-04-28)."""
    decisions = parse_decisions_history(_decisions_html())
    latest = decisions[0]
    assert latest.announcement_date == date(2026, 3, 26)
    assert latest.movement == "cut"
    assert latest.bps_change == 25
    assert latest.rate == "6.75"
    assert latest.previous_rate == "7.00"


def test_parse_decisions_history_hold_carries_absolute_rate_inline() -> None:
    """Hold rows give the absolute rate explicitly in the link text
    (``"se mantiene sin cambio en X.XX por ciento"``)."""
    decisions = parse_decisions_history(_decisions_html())
    feb_2026_hold = next(
        d for d in decisions if d.announcement_date == date(2026, 2, 5)
    )
    assert feb_2026_hold.movement == "hold"
    assert feb_2026_hold.bps_change == 0
    assert feb_2026_hold.rate == "7.00"


def test_parse_decisions_history_seed_anchor_validates_zero_disagreements() -> None:
    """Walk forward from the oldest decision; every hold row must agree
    with the running cumulative rate. Zero disagreements across the
    156-row tasa-objetivo corpus at fixture-capture time."""
    decisions = parse_decisions_history(_decisions_html())
    chrono = sorted(decisions, key=lambda d: d.announcement_date)
    for i in range(1, len(chrono)):
        prev, curr = chrono[i - 1], chrono[i]
        # ``previous_rate`` is set from the prior decision's running
        # rate; mismatches would mean the cumulative walk drifted.
        assert curr.previous_rate == prev.rate, (
            f"cumulative drift at {curr.announcement_date}: "
            f"prev={prev.rate} curr.prev={curr.previous_rate}"
        )


def test_parse_decisions_history_captures_pdf_url_after_br() -> None:
    """Banxico writes ``<br/>`` immediately before the PDF anchor in
    every link-text cell. A naïve alternation that ends the description
    scan at whichever marker comes first lands on the ``<br/>`` and
    silently leaves the PDF group empty for every decision in the
    corpus. Verify the PDF anchor is captured for every parsed row.
    """
    decisions = parse_decisions_history(_decisions_html())
    assert decisions
    assert all(d.pdf_url for d in decisions), (
        "expected every parsed decision to carry a pdf_url"
    )
    # Spot-check: the latest decision (2026-03-26) maps to the cut PDF
    # observed at fixture-capture time.
    assert decisions[0].pdf_url.endswith(".pdf")
    assert decisions[0].pdf_url.startswith(
        "/publicaciones-y-prensa/anuncios-de-las-decisiones-de-politica-monetaria/",
    )


def test_decision_to_records_resolves_full_pdf_url_in_payload() -> None:
    """``decision_to_records`` rewrites the relative ``pdf_url`` to a
    fully-qualified URL on the audit payload so an operator's link
    works without needing the connector to know about the base URL.
    """
    import json as _json
    decision = BanxicoRateDecision(
        announcement_date=date(2026, 3, 26),
        rate="6.75",
        previous_rate="7.00",
        movement="cut",
        bps_change=25,
        description="...disminuye en 25 puntos base",
        pdf_url="/publicaciones-y-prensa/anuncios-de-las-decisiones-de-politica-monetaria/{ABC}.pdf",
    )
    raw, _event = decision_to_records(
        decision, snapshot_epoch_ms=1_800_000_000_000,
    )
    payload = _json.loads(raw.payload_json)
    assert payload["pdf_url"].startswith("https://www.banxico.org.mx/")
    assert payload["pdf_url"].endswith(".pdf")


def test_parse_decisions_history_filters_pre_2008_corto_rows() -> None:
    """The pre-2008 ``"corto"`` instrument rows share the same HTML
    table shape as Tasa Objetivo rows. The parser pins on the
    ``"tasa objetivo"`` substring so the cumulative walk doesn't get
    poisoned by the older instrument's units (millones de pesos)."""
    decisions = parse_decisions_history(_decisions_html())
    # No row before 2008 should land in the parsed output.
    assert all(d.announcement_date.year >= 2008 for d in decisions)


def test_parse_decisions_history_classifies_phrasing_variants() -> None:
    """Spanish phrasing for changes overlaps four variants:
    ``aumenta``/``incrementa`` (hike), ``disminuye``/``reduce`` (cut)."""
    mini = (
        '<TR>'
        '<TD tag="[current].bm:referenceDate" class=bmdateview '
        'aria-label="01 de Enero 2024">'
        '<SPAN aria-hidden=true>01/01/24</span></TD>'
        '<TD tag="[current].bm:linkText" class=bmtextview>'
        'El objetivo para la Tasa de Interés Interbancaria a 1 día '
        '(tasa objetivo) se mantiene sin cambio en 11.25 por ciento'
        '<br/><A HREF="/x.pdf">Texto completo</A></TD></TR>'
        '<TR>'
        '<TD tag="[current].bm:referenceDate" class=bmdateview '
        'aria-label="08 de Febrero 2024">'
        '<SPAN aria-hidden=true>08/02/24</span></TD>'
        '<TD tag="[current].bm:linkText" class=bmtextview>'
        'El objetivo para la Tasa de Interés Interbancaria a 1 día '
        '(tasa objetivo) disminuye en 25 puntos base'
        '<br/><A HREF="/y.pdf">Texto completo</A></TD></TR>'
        '<TR>'
        '<TD tag="[current].bm:referenceDate" class=bmdateview '
        'aria-label="14 de Marzo 2024">'
        '<SPAN aria-hidden=true>14/03/24</span></TD>'
        '<TD tag="[current].bm:linkText" class=bmtextview>'
        'El objetivo para la Tasa de Interés Interbancaria a 1 día '
        '(tasa objetivo) se incrementa en 50 puntos base'
        '<br/><A HREF="/z.pdf">Texto completo</A></TD></TR>'
    )
    decisions = parse_decisions_history(mini)
    by_date = {d.announcement_date: d for d in decisions}
    assert by_date[date(2024, 1, 1)].movement == "hold"
    assert by_date[date(2024, 1, 1)].rate == "11.25"
    assert by_date[date(2024, 2, 8)].movement == "cut"
    assert by_date[date(2024, 2, 8)].bps_change == 25
    assert by_date[date(2024, 2, 8)].rate == "11.00"
    assert by_date[date(2024, 3, 14)].movement == "hike"
    assert by_date[date(2024, 3, 14)].bps_change == 50
    assert by_date[date(2024, 3, 14)].rate == "11.50"


def test_parse_decisions_history_raises_on_blank_page() -> None:
    with pytest.raises(BanxicoDecisionsParseError, match="layout drift"):
        parse_decisions_history(
            "<html><body><p>maintenance window</p></body></html>",
        )


def test_parse_decisions_history_raises_when_change_lacks_seed() -> None:
    """A change-only row at the head of the chronological order with
    no prior hold to seed the cumulative walk is a layout-drift signal
    we want loud."""
    mini = (
        '<TR>'
        '<TD tag="[current].bm:referenceDate" class=bmdateview '
        'aria-label="01 de Enero 2024">'
        '<SPAN aria-hidden=true>01/01/24</span></TD>'
        '<TD tag="[current].bm:linkText" class=bmtextview>'
        'El objetivo para la Tasa de Interés Interbancaria a 1 día '
        '(tasa objetivo) disminuye en 25 puntos base'
        '<br/><A HREF="/y.pdf">Texto completo</A></TD></TR>'
    )
    with pytest.raises(BanxicoDecisionsParseError, match="cannot seed"):
        parse_decisions_history(mini)


# ── projection ───────────────────────────────────────────────────


def test_decision_to_records_anchors_event_at_13_local_post_dst() -> None:
    """Mexico abolished federal DST on 30 October 2022; post-2022
    America/Mexico_City sits at UTC−6 year-round. 13:00 local → 19:00 UTC.
    """
    decision = BanxicoRateDecision(
        announcement_date=date(2026, 3, 26),
        rate="6.75",
        previous_rate="7.00",
        movement="cut",
        bps_change=25,
        description="...disminuye en 25 puntos base",
        pdf_url="/publicaciones-y-prensa/.../{UUID}.pdf",
    )
    raw, event = decision_to_records(
        decision, snapshot_epoch_ms=1_800_000_000_000,
    )
    assert event.event_time_utc.startswith("2026-03-26T19:00:00")
    assert event.country_code == "MX"
    assert event.currency == "MXN"
    assert event.title == "Banxico Interest Rate Decision"
    assert event.actual == "6.75"
    assert event.previous == "7.00"
    assert event.reference_date == "2026-03-26"
    assert event.source_url == BANXICO_DECISIONS_URL
    payload = json.loads(raw.payload_json)
    assert payload["kind"] == "banxico_rate_decision"
    assert payload["movement"] == "cut"
    assert payload["bps_change"] == 25


def test_decision_to_records_handles_pre_2022_dst_window() -> None:
    """Mexico observed DST until 30 October 2022 (UTC−5 summer). A
    May 2018 decision falls inside that window; ZoneInfo for
    America/Mexico_City must resolve the correct historical offset.
    """
    decision = BanxicoRateDecision(
        announcement_date=date(2018, 5, 17),
        rate="7.50",
        previous_rate="7.50",
        movement="hold",
        bps_change=0,
        description="...se mantiene sin cambio en 7.50 por ciento",
        pdf_url="/x.pdf",
    )
    _, event = decision_to_records(
        decision, snapshot_epoch_ms=1_800_000_000_000,
    )
    # 13:00 CDT (UTC-5) = 18:00 UTC.
    assert event.event_time_utc.startswith("2018-05-17T18:00:00")


def test_decision_to_records_emits_hold_with_actual_equal_previous() -> None:
    """Hold decisions ship as events with ``actual == previous`` (the
    rate did not change). The parity whitelist depends on every Junta
    de Gobierno announcement being projected the same way.
    """
    decision = BanxicoRateDecision(
        announcement_date=date(2026, 2, 5),
        rate="7.00",
        previous_rate="7.00",
        movement="hold",
        bps_change=0,
        description="...se mantiene sin cambio en 7.00 por ciento",
        pdf_url="/x.pdf",
    )
    _, event = decision_to_records(
        decision, snapshot_epoch_ms=1_800_000_000_000,
    )
    assert event.actual == "7.00"
    assert event.previous == "7.00"


# ── full fetch driver ───────────────────────────────────────────


def test_fetch_banxico_calendar_writes_one_event_per_decision(
    store: SQLiteEngineStore,
) -> None:
    payload = _decisions_html()
    with store._connection(commit=True) as conn:
        summary = fetch_banxico_calendar(
            conn,
            dry_run=False,
            html_fetcher=lambda: payload,
            snapshot_epoch_ms=1_800_000_000_000,
        )
    assert summary.fetch_error is None
    # Captured fixture carries 156 tasa-objetivo decisions (Feb 2008
    # through Mar 2026).
    assert summary.decisions_parsed == 156
    assert summary.events_upserted == 156

    with store._connection(commit=False) as conn:
        rows = conn.execute(
            "SELECT count(*) FROM cal_econ_event WHERE provider = ?",
            (PROVIDER,),
        ).fetchone()
    assert rows[0] == 156


def test_fetch_banxico_calendar_idempotent_on_repeat(
    store: SQLiteEngineStore,
) -> None:
    payload = _decisions_html()
    with store._connection(commit=True) as conn:
        first = fetch_banxico_calendar(
            conn, dry_run=False,
            snapshot_epoch_ms=1_700_000_000_000,
            html_fetcher=lambda: payload,
        )
        second = fetch_banxico_calendar(
            conn, dry_run=False,
            snapshot_epoch_ms=1_700_000_000_001,
            html_fetcher=lambda: payload,
        )
    assert first.events_upserted == 156
    assert second.rows_raw_inserted == 0
    assert second.events_upserted == first.events_upserted


def test_fetch_banxico_calendar_dry_run_returns_plan(
    store: SQLiteEngineStore,
) -> None:
    with store._connection(commit=False) as conn:
        summary = fetch_banxico_calendar(conn, dry_run=True)
    assert summary.dry_run is True
    assert summary.indicators_planned == ["BANXICO_RATE"]


def test_fetch_banxico_calendar_records_fetch_error_on_outage(
    store: SQLiteEngineStore,
) -> None:
    def broken() -> str:
        raise BanxicoDecisionsParseError("layout drift")

    with store._connection(commit=True) as conn:
        summary = fetch_banxico_calendar(
            conn, dry_run=False, html_fetcher=broken,
        )
    assert summary.fetch_error is not None
    assert summary.events_upserted == 0


# ── scheduler + agency wiring ───────────────────────────────────


def test_banxico_listed_in_default_rosters() -> None:
    from ingestion.calendar.scheduler import (
        ALL_CONNECTORS,
        ALL_VALUE_SIDE_CONNECTORS,
    )
    assert "banxico" in ALL_CONNECTORS
    assert "banxico" in ALL_VALUE_SIDE_CONNECTORS


def test_banxico_agency_attribution_includes_banxico_rate() -> None:
    """Banxico owns ``(MX, BANXICO_RATE)`` in the parity whitelist —
    the cumulative-walk projector ships value-bearing rows on day one
    (BCB-style coverage).
    """
    from ingestion.calendar.agency_registry import (
        agency_for,
        provider_to_agency,
    )
    bx = provider_to_agency("banxico")
    assert bx is not None and bx.agency_id == "BANXICO"
    assert agency_for("MX", "BANXICO_RATE") is bx


def test_banxico_canonicalize_aliases_resolve_rate_titles() -> None:
    from ingestion.calendar._official_shared import canonicalize_indicator
    assert canonicalize_indicator(
        "Banxico Interest Rate Decision",
    ) == "BANXICO_RATE"
    assert canonicalize_indicator(
        "Banco de México Interest Rate Decision",
    ) == "BANXICO_RATE"
    assert canonicalize_indicator(
        "Mexico Interest Rate Decision",
    ) == "BANXICO_RATE"
    assert canonicalize_indicator("Tasa Objetivo") == "BANXICO_RATE"
    assert canonicalize_indicator(
        "Tasa de Interés Interbancaria a 1 día",
    ) == "BANXICO_RATE"

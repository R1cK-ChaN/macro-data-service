"""Mocked tests for the BCB calendar connector (issue #84 P1).

Fixture captured live on 2026-04-27 from
``https://www.bcb.gov.br/api/servico/sitebcb/historicotaxasjuros`` —
the full Copom decision history (every meeting since #1, June 1996;
285 decisions at capture time). No real HTTP in CI — every test
injects the ``json_fetcher`` seam.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from ingestion.calendar.bcb_api import (
    INDICATOR_REGISTRY,
    BCBCopomParseError,
    BCBRateDecision,
    decision_to_records,
    fetch_bcb_calendar,
    parse_copom_history,
)
from ingestion.calendar.bcb_api.parser import PROVIDER
from storage.sqlite import SQLiteEngineStore


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "bcb_copom"


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


def _copom_json() -> str:
    return (FIXTURE_DIR / "historicotaxasjuros.json").read_text(encoding="utf-8")


# ── parser ───────────────────────────────────────────────────────


def test_parse_copom_history_returns_recent_decisions_first() -> None:
    decisions = parse_copom_history(_copom_json())
    # Live capture covers every Copom decision back to 26 June 1996
    # (~285 rows). Assert the most-recent few against published BCB
    # history. The JSON encodes ``MetaSelic`` as a number (Python
    # float), so an integer rate prints as ``"15.0"`` after str()
    # — the parity comparator strips trailing zeros so the format
    # divergence with TE / Reuters is tolerated downstream.
    assert decisions[0].announcement_date == date(2026, 3, 18)
    assert decisions[0].rate == "14.75"
    assert decisions[0].previous_rate == "15.0"
    assert decisions[0].effective_date == date(2026, 3, 19)
    assert decisions[0].meeting_number == 277


def test_parse_copom_history_carries_holds_inline() -> None:
    """Two meetings in late 2025 / early 2026 held the rate at 15.0; a
    hold row carries ``rate == previous_rate`` (no separate flag)."""
    decisions = parse_copom_history(_copom_json())
    holds = [d for d in decisions if d.previous_rate == d.rate]
    assert holds, "expected at least one hold row in the captured history"


def test_parse_copom_history_derives_previous_rate_from_chronology() -> None:
    decisions = parse_copom_history(_copom_json())
    oldest = decisions[-1]
    assert oldest.announcement_date == date(1996, 6, 26)
    # First decision in the page has no predecessor.
    assert oldest.previous_rate is None
    assert oldest.meeting_number == 1


def test_parse_copom_history_raises_on_missing_conteudo() -> None:
    with pytest.raises(BCBCopomParseError, match="missing 'conteudo'"):
        parse_copom_history('{"foo": []}')


def test_parse_copom_history_raises_on_unparseable_payload() -> None:
    with pytest.raises(BCBCopomParseError, match="parseable JSON"):
        parse_copom_history("not json at all")


def test_parse_copom_history_raises_on_zero_decisions() -> None:
    with pytest.raises(BCBCopomParseError, match="zero decisions"):
        parse_copom_history('{"conteudo": []}')


def test_parse_copom_history_skips_malformed_rows() -> None:
    """A row with an unparseable rate must not nuke the whole list — skip
    and keep walking. Mirrors the RBA / BoC parser's defensive shape."""
    payload = (
        '{"conteudo": ['
        '  {"NumeroReuniaoCopom": 1, "DataReuniaoCopom": "2026-01-29T03:00:00Z",'
        '   "MetaSelic": "garbage"},'
        '  {"NumeroReuniaoCopom": 2, "DataReuniaoCopom": "2026-03-18T03:00:00Z",'
        '   "MetaSelic": 14.75}'
        ']}'
    )
    decisions = parse_copom_history(payload)
    assert {d.rate for d in decisions} == {"14.75"}


# ── projection ───────────────────────────────────────────────────


def test_decision_to_records_anchors_event_at_brt_window() -> None:
    decision = BCBRateDecision(
        meeting_number=277,
        announcement_date=date(2026, 3, 18),
        effective_date=date(2026, 3, 19),
        end_date=None,
        rate="14.75",
        previous_rate="15.00",
        bias="n/a",
        extraordinary=False,
        monocratic_president=False,
    )
    raw_rec, event_rec = decision_to_records(
        decision, snapshot_epoch_ms=1_800_000_000_000,
    )
    assert event_rec.country_code == "BR"
    assert event_rec.actual == "14.75"
    assert event_rec.previous == "15.00"
    assert event_rec.title == "BCB Interest Rate Decision"
    assert event_rec.currency == "BRL"
    # Mar 18 18:30 BRT (UTC-3) = Mar 18 21:30 UTC. Brazil dropped DST
    # in 2019 so post-2019 rows always land at +3h offset.
    assert event_rec.event_time_utc.startswith("2026-03-18T21:30:00")
    assert event_rec.event_time_precision == "datetime"
    assert event_rec.reference_date == "2026-03-18"
    # provider_event_id stable across re-projection.
    _, event_rec_again = decision_to_records(
        decision, snapshot_epoch_ms=2_000_000_000_000,
    )
    assert event_rec.provider_event_id == event_rec_again.provider_event_id


def test_decision_to_records_handles_pre_dst_abolition_meeting() -> None:
    """A 2017 January meeting sits inside the pre-2019 DST window when
    Brazil observed UTC-2 in summer. ZoneInfo for ``America/Sao_Paulo``
    must resolve the correct offset for the meeting date."""
    decision = BCBRateDecision(
        meeting_number=204,
        announcement_date=date(2017, 1, 11),  # BRST = UTC-2 in Jan 2017
        effective_date=date(2017, 1, 12),
        end_date=date(2017, 2, 22),
        rate="13.00",
        previous_rate="13.75",
        bias="n/a",
        extraordinary=False,
        monocratic_president=False,
    )
    _, event_rec = decision_to_records(
        decision, snapshot_epoch_ms=1_800_000_000_000,
    )
    # Jan 11 18:30 BRST (UTC-2) = Jan 11 20:30 UTC.
    assert event_rec.event_time_utc.startswith("2017-01-11T20:30:00")


def test_decision_to_records_emits_hold_with_actual_equal_previous() -> None:
    """Hold decisions ship as events with ``actual == previous`` and
    no special flag — the parity whitelist depends on every Copom
    announcement being projected the same way."""
    decision = BCBRateDecision(
        meeting_number=275,
        announcement_date=date(2025, 12, 10),
        effective_date=date(2025, 12, 11),
        end_date=date(2026, 1, 28),
        rate="15.00",
        previous_rate="15.00",
        bias="n/a",
        extraordinary=False,
        monocratic_president=False,
    )
    raw_rec, event_rec = decision_to_records(
        decision, snapshot_epoch_ms=1_800_000_000_000,
    )
    assert event_rec.actual == "15.00"
    assert event_rec.previous == "15.00"
    # Audit payload preserves both dates so downstream consumers can see
    # the announce-vs-effective-vs-end split if needed.
    import json as _json
    payload = _json.loads(raw_rec.payload_json)
    assert payload["announcement_date"] == "2025-12-10"
    assert payload["effective_date"] == "2025-12-11"
    assert payload["end_date"] == "2026-01-28"


# ── full fetch driver ───────────────────────────────────────────


def test_fetch_bcb_calendar_writes_one_event_per_decision(
    store: SQLiteEngineStore,
) -> None:
    payload = _copom_json()
    with store._connection(commit=True) as conn:
        summary = fetch_bcb_calendar(
            conn,
            dry_run=False,
            json_fetcher=lambda: payload,
            snapshot_epoch_ms=1_800_000_000_000,
        )
    assert summary.fetch_error is None
    # The captured history contains ~285 decisions (back to 1996).
    assert summary.decisions_parsed > 200
    assert summary.events_upserted == summary.decisions_parsed


def test_fetch_bcb_calendar_dry_run_returns_plan(
    store: SQLiteEngineStore,
) -> None:
    with store._connection(commit=False) as conn:
        summary = fetch_bcb_calendar(conn, dry_run=True)
    assert summary.dry_run is True
    assert summary.indicators_planned == ["BCB_RATE"]


def test_fetch_bcb_calendar_records_fetch_error_on_outage(
    store: SQLiteEngineStore,
) -> None:
    def broken() -> str:
        raise BCBCopomParseError("zero decisions")

    with store._connection(commit=True) as conn:
        summary = fetch_bcb_calendar(
            conn, dry_run=False, json_fetcher=broken,
        )
    assert summary.fetch_error is not None
    assert summary.events_upserted == 0


# ── scheduler + agency wiring ───────────────────────────────────


def test_bcb_listed_in_default_rosters() -> None:
    from ingestion.calendar.scheduler import (
        ALL_CONNECTORS,
        ALL_VALUE_SIDE_CONNECTORS,
    )
    assert "bcb" in ALL_CONNECTORS
    assert "bcb" in ALL_VALUE_SIDE_CONNECTORS


def test_bcb_agency_attribution_includes_bcb_rate() -> None:
    """BCB owns ``(BR, BCB_RATE)`` in the parity whitelist — the Copom
    history JSON exposes target-Selic + announcement date inline (RBA-
    style coverage), so the daily comparator can rely on matched rows
    for every Copom announcement."""
    from ingestion.calendar.agency_registry import (
        agency_for,
        provider_to_agency,
    )
    bcb = provider_to_agency("bcb")
    assert bcb is not None and bcb.agency_id == "BCB"
    assert agency_for("BR", "BCB_RATE") is bcb


def test_bcb_canonicalize_aliases_resolve_rate_titles() -> None:
    from ingestion.calendar._official_shared import canonicalize_indicator
    assert canonicalize_indicator("BCB Interest Rate Decision") == "BCB_RATE"
    assert canonicalize_indicator(
        "Banco Central do Brasil Interest Rate Decision",
    ) == "BCB_RATE"
    assert canonicalize_indicator("Brazil Interest Rate Decision") == "BCB_RATE"
    assert canonicalize_indicator("Selic Rate") == "BCB_RATE"
    assert canonicalize_indicator("Selic Target Rate") == "BCB_RATE"

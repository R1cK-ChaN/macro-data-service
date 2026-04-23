"""Scaffold tests for issue #9 P0 — official-source calendar shared utilities
and the `cal_provider` seeding they ride on.

Covers three things, no real HTTP:

- :mod:`ingestion.calendar._official_shared.canonicalize` — alias hits
  and identity-fallback for unknown labels.
- :mod:`ingestion.calendar._official_shared.release_time` — 24h/12h
  shapes, DST resolution, explicit-abbreviation override.
- :mod:`ingestion.calendar._official_shared.event_id` — determinism
  and sensitivity to each component.

Plus the end-to-end smoke test: a synthetic BLS row written with a
sha256 event-id round-trips through ``v_calendar_item`` and the new
official provider rows show the precedence=100 tier.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from ingestion.calendar._official_shared import (
    TIMEZONE_ALIASES,
    canonicalize_indicator,
    parse_scheduled_release_time,
    synthesize_event_id,
)
from storage.sqlite import SQLiteEngineStore


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteEngineStore:
    return SQLiteEngineStore(db_path=tmp_path / "engine.db")


# ──────────────────────────────────────────────────────────────────────────
# canonicalize_indicator
# ──────────────────────────────────────────────────────────────────────────


def test_canonicalize_basic_aliases() -> None:
    assert canonicalize_indicator("Consumer Price Index") == "CPI"
    assert canonicalize_indicator("CPI") == "CPI"
    assert canonicalize_indicator("cpi") == "CPI"
    assert canonicalize_indicator("Gross Domestic Product") == "GDP"
    assert canonicalize_indicator("Employment Situation") == "NFP"
    assert canonicalize_indicator("Nonfarm Payrolls") == "NFP"


def test_canonicalize_strips_parens_and_modifiers() -> None:
    """Same indicator family under different upstream labels → same token."""
    assert canonicalize_indicator("Consumer Price Index (CPI)") == "CPI"
    assert canonicalize_indicator("CPI YoY") == "CPI"
    assert canonicalize_indicator("CPI MoM") == "CPI"
    assert canonicalize_indicator("CPI YoY SA") == "CPI"
    assert canonicalize_indicator("GDP Advance") == "GDP"
    assert canonicalize_indicator("GDP Prelim") == "GDP"
    assert canonicalize_indicator("Michigan Consumer Sentiment Prel") == "MICHIGAN_SENTIMENT"


def test_canonicalize_collapses_whitespace() -> None:
    assert canonicalize_indicator("  Consumer   Price    Index  ") == "CPI"


def test_canonicalize_unknown_falls_through_normalized() -> None:
    """Unknown label returns the normalized form — not an alias, not empty."""
    assert canonicalize_indicator("Some New Release 2027") == "some new release 2027"
    assert canonicalize_indicator("") == ""
    assert canonicalize_indicator("  ") == ""


def test_canonicalize_central_bank_aliases() -> None:
    assert canonicalize_indicator("FOMC Rate Decision") == "FOMC_RATE"
    assert canonicalize_indicator("Interest Rate Decision") == "FOMC_RATE"
    assert canonicalize_indicator("Main Refinancing Operations Rate") == "ECB_MRO"
    assert canonicalize_indicator("Deposit Facility Rate") == "ECB_DFR"
    assert canonicalize_indicator("Summary of Economic Projections") == "FED_SEP"


# ──────────────────────────────────────────────────────────────────────────
# parse_scheduled_release_time
# ──────────────────────────────────────────────────────────────────────────


def test_release_time_bls_morning_et_winter() -> None:
    """BLS CPI at 8:30 AM ET in January = 13:30 UTC (EST = UTC-5)."""
    result = parse_scheduled_release_time(
        date(2026, 1, 14),
        "8:30 AM ET",
    )
    assert result.utc == datetime(2026, 1, 14, 13, 30, tzinfo=timezone.utc)
    assert result.local_tz == "America/New_York"


def test_release_time_bls_morning_et_summer_respects_dst() -> None:
    """Same 8:30 AM ET release in July = 12:30 UTC (EDT = UTC-4)."""
    result = parse_scheduled_release_time(
        date(2026, 7, 15),
        "8:30 AM ET",
    )
    assert result.utc == datetime(2026, 7, 15, 12, 30, tzinfo=timezone.utc)


def test_release_time_ecb_cet_winter() -> None:
    """ECB MRO rate decision at 13:45 CET in January = 12:45 UTC."""
    result = parse_scheduled_release_time(
        date(2026, 1, 22),
        "13:45 CET",
    )
    assert result.utc == datetime(2026, 1, 22, 12, 45, tzinfo=timezone.utc)


def test_release_time_ecb_cest_summer_respects_dst() -> None:
    """Same zone in July = 11:45 UTC under CEST."""
    result = parse_scheduled_release_time(
        date(2026, 7, 23),
        "13:45 CET",
    )
    assert result.utc == datetime(2026, 7, 23, 11, 45, tzinfo=timezone.utc)


def test_release_time_nbs_default_tz_for_bare_time() -> None:
    """NBS publishes bare ``09:30`` on the release calendar — caller
    passes ``default_tz='Asia/Shanghai'``."""
    result = parse_scheduled_release_time(
        date(2026, 3, 10),
        "09:30",
        default_tz="Asia/Shanghai",
    )
    assert result.utc == datetime(2026, 3, 10, 1, 30, tzinfo=timezone.utc)
    assert result.local_tz == "Asia/Shanghai"


def test_release_time_explicit_offset_overrides_default() -> None:
    result = parse_scheduled_release_time(
        date(2026, 3, 10),
        "08:30+02:00",
        default_tz="America/New_York",
    )
    assert result.utc == datetime(2026, 3, 10, 6, 30, tzinfo=timezone.utc)


def test_release_time_rejects_empty_input() -> None:
    with pytest.raises(ValueError):
        parse_scheduled_release_time(date(2026, 1, 1), "")


def test_release_time_rejects_unknown_abbrev() -> None:
    with pytest.raises(ValueError):
        parse_scheduled_release_time(date(2026, 1, 1), "08:30 XYZ")


def test_release_time_rejects_bare_without_default() -> None:
    with pytest.raises(ValueError):
        parse_scheduled_release_time(date(2026, 1, 1), "08:30")


def test_release_time_bare_am_pm_with_default_tz() -> None:
    """Codex P2 — the 12-hour shape ``"8:30 AM"`` with a connector-supplied
    ``default_tz`` must not treat ``AM`` as a timezone abbreviation."""
    result = parse_scheduled_release_time(
        date(2026, 1, 14),
        "8:30 AM",
        default_tz="America/New_York",
    )
    assert result.utc == datetime(2026, 1, 14, 13, 30, tzinfo=timezone.utc)
    assert result.local_tz == "America/New_York"

    pm_result = parse_scheduled_release_time(
        date(2026, 1, 14),
        "2:00 PM",
        default_tz="America/New_York",
    )
    assert pm_result.utc == datetime(2026, 1, 14, 19, 0, tzinfo=timezone.utc)


def test_timezone_aliases_covers_expected_abbreviations() -> None:
    """Regression guard — the five P1-P5 connectors rely on these."""
    for expected in ("ET", "EST", "EDT", "CET", "CEST", "CST", "GMT", "UTC"):
        assert expected in TIMEZONE_ALIASES


# ──────────────────────────────────────────────────────────────────────────
# synthesize_event_id
# ──────────────────────────────────────────────────────────────────────────


def test_event_id_is_deterministic() -> None:
    a = synthesize_event_id("bls", "US", "CPI", "2026-04-10T12:30:00+00:00")
    b = synthesize_event_id("bls", "US", "CPI", "2026-04-10T12:30:00+00:00")
    assert a == b
    assert len(a) == 64  # sha256 hex


def test_event_id_normalizes_case() -> None:
    """Provider lowercased, country uppercased — so ``BLS``/``bls`` and
    ``us``/``US`` all land on the same id."""
    canonical = synthesize_event_id("bls", "US", "CPI", "2026-04-10T12:30:00+00:00")
    assert synthesize_event_id("BLS", "us", "CPI", "2026-04-10T12:30:00+00:00") == canonical


def test_event_id_sensitive_to_each_component() -> None:
    base = synthesize_event_id("bls", "US", "CPI", "2026-04-10T12:30:00+00:00")
    assert synthesize_event_id("bea", "US", "CPI", "2026-04-10T12:30:00+00:00") != base
    assert synthesize_event_id("bls", "EU", "CPI", "2026-04-10T12:30:00+00:00") != base
    assert synthesize_event_id("bls", "US", "GDP", "2026-04-10T12:30:00+00:00") != base
    assert synthesize_event_id("bls", "US", "CPI", "2026-04-10T13:30:00+00:00") != base


# ──────────────────────────────────────────────────────────────────────────
# End-to-end smoke: synthetic BLS row → v_calendar_item
# ──────────────────────────────────────────────────────────────────────────


def test_official_provider_row_surfaces_in_unified_view(
    store: SQLiteEngineStore,
) -> None:
    """Insert a synthetic BLS event via the shared-utility event-id
    scheme; confirm it surfaces through ``v_calendar_item`` with
    ``provider='bls'``."""
    release_time = parse_scheduled_release_time(
        date(2026, 5, 13), "8:30 AM ET",
    ).utc
    event_id = synthesize_event_id(
        "bls", "US", canonicalize_indicator("Consumer Price Index"),
        release_time.isoformat(),
    )

    with store._connection(commit=True) as c:
        c.execute(
            """
            INSERT INTO cal_econ_event (
                provider, provider_event_id, event_time_utc, event_time_precision,
                country_code, title, importance, content_hash,
                observed_at_epoch_ms, created_at, updated_at
            ) VALUES (
                'bls', ?, ?, 'datetime',
                'US', 'CPI YoY', 'high', 'h-smoke', 0, '2026-05-01', '2026-05-01'
            )
            """,
            (event_id, release_time.isoformat()),
        )

    with store._connection(commit=False) as c:
        row = c.execute(
            "SELECT provider, country, title, importance "
            "FROM v_calendar_item WHERE provider_event_id = ?",
            (event_id,),
        ).fetchone()
    assert tuple(row) == ("bls", "US", "CPI YoY", "high")


def test_official_providers_have_higher_precedence_than_te(
    store: SQLiteEngineStore,
) -> None:
    """Official-source providers sit at 100, TE at 10. ``v_calendar_item``
    itself is ``UNION ALL`` today — precedence is the ranking signal the
    P6 parity harness will read to pick the official row when both
    sources carry the same event."""
    with store._connection(commit=False) as c:
        rows = dict(
            c.execute(
                "SELECT provider_id, precedence FROM cal_provider"
            ).fetchall()
        )
    for provider in (
        "bls", "bea", "census", "ism", "umich",
        "federal-reserve", "ecb", "nbs",
    ):
        assert rows[provider] == 100, f"{provider} precedence"
    assert rows["tradingeconomics"] == 10
    assert rows["eodhd"] == 10

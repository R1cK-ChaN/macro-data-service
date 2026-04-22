"""Tests for the Fed live-probe scaffold (issue #9 P4b-infra +
P4b-live-follow-up).

No real HTTP. Exercises the probe planner, runner dispatch, and the
per-surface probe paths (FOMC calendar HTML + release calendar JSON).
Fixtures come from :mod:`tests.fixtures.fed_fomc_calendar` and
:mod:`tests.fixtures.fed_releasedates` — the same captures the
connector's own scaffold tests use.

Parallel in shape to :mod:`tests.test_bls_live_probe_scaffold` /
:mod:`tests.test_bea_live_probe_scaffold` /
:mod:`tests.test_ecb_live_probe_scaffold`. Fed has no auth, so there's
no ``auth_missing`` branch; the probe consumes HTML / JSON locally,
so the diff-field-level coverage present for API probes is not
meaningful here — instead we test the parse-count / sample / dry-project
surfaces.
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import date
from pathlib import Path

import pytest

from ingestion.calendar.fed_api import FedReleaseEntry, FomcMeetingEntry


REPO_ROOT = Path(__file__).resolve().parent.parent
FOMC_FIXTURE_DIR = REPO_ROOT / "tests" / "fixtures" / "fed_fomc_calendar"
RELEASEDATES_FIXTURE_DIR = REPO_ROOT / "tests" / "fixtures" / "fed_releasedates"


def _load_validator_module():
    module_name = "validate_calendar_acquisition"
    if module_name in sys.modules:
        return sys.modules[module_name]
    spec = importlib.util.spec_from_file_location(
        module_name,
        REPO_ROOT / "scripts" / "validate_calendar_acquisition.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def validator():
    return _load_validator_module()


def _fomc_fixture_html() -> str:
    return (FOMC_FIXTURE_DIR / "fomc_2026.html").read_text(encoding="utf-8")


def _releasedates_fixture_json() -> str:
    return (RELEASEDATES_FIXTURE_DIR / "calendar.json").read_text(
        encoding="utf-8",
    )


# ──────────────────────────────────────────────────────────────────────────
# plan_fed_probes
# ──────────────────────────────────────────────────────────────────────────


def test_planner_ships_two_probes(validator) -> None:
    probes = validator.plan_fed_probes()
    assert len(probes) == 2
    sources = {p.source for p in probes}
    assert sources == {"fomc_calendar", "releasedates"}


def test_planner_names_are_unique(validator) -> None:
    probes = validator.plan_fed_probes()
    names = [p.name for p in probes]
    assert len(names) == len(set(names))


def test_planner_fomc_probe_carries_calendar_url(validator) -> None:
    from ingestion.calendar.fed_api import FOMC_CALENDAR_URL

    probes = {p.source: p for p in validator.plan_fed_probes()}
    assert probes["fomc_calendar"].url == FOMC_CALENDAR_URL


def test_planner_releasedates_probe_carries_json_feed_url(validator) -> None:
    from ingestion.calendar.fed_api import FED_CALENDAR_JSON_URL

    probes = {p.source: p for p in validator.plan_fed_probes()}
    assert probes["releasedates"].url == FED_CALENDAR_JSON_URL


# ──────────────────────────────────────────────────────────────────────────
# _try_project_fomc_entry + _try_project_release_entry
# ──────────────────────────────────────────────────────────────────────────


def test_try_project_fomc_entry_succeeds(validator) -> None:
    entry = FomcMeetingEntry(
        year=2026,
        month_name="January",
        date_cell="27-28",
        closing_date=date(2026, 1, 28),
        has_sep=False,
    )
    ok, msg = validator._try_project_fomc_entry(entry)
    assert ok is True
    assert "indicator=FOMC_RATE" in msg
    assert "2026-01-28" in msg


def test_try_project_release_entry_succeeds(validator) -> None:
    entry = FedReleaseEntry(
        series_id="BEIGE_BOOK",
        release_title="Beige Book",
        release_date="2026-01-21",
        release_time_local="2:00 PM",
        event_time_utc="2026-01-21T19:00:00+00:00",
    )
    ok, msg = validator._try_project_release_entry(entry)
    assert ok is True
    assert "indicator=BEIGE_BOOK" in msg


# ──────────────────────────────────────────────────────────────────────────
# run_fed_probe — FOMC calendar branch
# ──────────────────────────────────────────────────────────────────────────


def _fomc_probe(validator):
    return next(
        p for p in validator.plan_fed_probes()
        if p.source == "fomc_calendar"
    )


def test_fomc_runner_reports_http_error(validator) -> None:
    probe = _fomc_probe(validator)
    result = validator.run_fed_probe(
        probe,
        fomc_fetcher=lambda: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    assert result.status == "http_error"
    assert any("RuntimeError" in n for n in result.notes)


def test_fomc_runner_handles_happy_path(validator) -> None:
    """Real fixture HTML through the production parser + projector."""
    probe = _fomc_probe(validator)
    result = validator.run_fed_probe(
        probe, fomc_fetcher=_fomc_fixture_html,
    )
    assert result.status == "ok"
    # 2026 fixture — 8 scheduled FOMC meetings.
    assert result.row_count >= 1
    assert result.sample_row is not None
    assert "closing_date" in result.sample_row
    assert result.parse_attempts >= 1
    assert result.parse_successes == result.parse_attempts
    # Year-span note surfaces.
    assert any("year span" in n for n in result.notes)


def test_fomc_runner_flags_empty_parse_as_http_error(validator) -> None:
    """``parse_fomc_calendar_html`` returns ``[]`` on HTML with no
    meeting panels. ``fetch_fed_calendar`` raises on that exact path
    (200 interstitial or DOM drift), so the probe mirrors the loud-
    fail and lands as ``http_error`` rather than a green ``ok`` —
    otherwise the report's summary could claim the acquisition layer
    matches expectations while production would refuse."""
    empty_html = "<html><body>No meetings panel here.</body></html>"
    probe = _fomc_probe(validator)
    result = validator.run_fed_probe(
        probe, fomc_fetcher=lambda: empty_html,
    )
    assert result.status == "http_error"
    assert result.row_count == 0
    assert any("zero FOMC meetings parsed" in n for n in result.notes)


def test_releasedates_runner_flags_whitelist_miss_as_http_error(validator) -> None:
    """``parse_fed_calendar_json`` raises ``FedCalendarJsonParseError``
    when no whitelist fragment matches — the calendar feed never
    legitimately publishes zero whitelist matches (weekly H.4.1 / H.8
    dominate the rolling window), so the raise is the upstream-drift
    signal. The probe's general exception branch converts it into
    ``http_error`` with the raise message preserved."""
    json_with_no_matching_titles = (
        '{"events":[{"title":"Speech -- Governor Jane Doe",'
        '"time":"2:30 p.m.","month":"2026-01","days":"9","type":"Speeches"}]}'
    )
    probe = _releasedates_probe(validator)
    result = validator.run_fed_probe(
        probe, releasedates_fetcher=lambda: json_with_no_matching_titles,
    )
    assert result.status == "http_error"
    assert result.row_count == 0
    assert any("FedCalendarJsonParseError" in n for n in result.notes)


# ──────────────────────────────────────────────────────────────────────────
# run_fed_probe — releasedates branch
# ──────────────────────────────────────────────────────────────────────────


def _releasedates_probe(validator):
    return next(
        p for p in validator.plan_fed_probes()
        if p.source == "releasedates"
    )


def test_releasedates_runner_reports_http_error(validator) -> None:
    probe = _releasedates_probe(validator)
    result = validator.run_fed_probe(
        probe,
        releasedates_fetcher=lambda: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    assert result.status == "http_error"
    assert any("RuntimeError" in n for n in result.notes)


def test_releasedates_runner_handles_happy_path(validator) -> None:
    probe = _releasedates_probe(validator)
    result = validator.run_fed_probe(
        probe, releasedates_fetcher=_releasedates_fixture_json,
    )
    assert result.status == "ok"
    assert result.row_count >= 1
    assert result.sample_row is not None
    assert result.sample_row["series_id"] in {
        "BEIGE_BOOK", "FED_H41", "FED_H8",
    }
    assert result.parse_attempts >= 1
    assert result.parse_successes == result.parse_attempts
    # by-indicator enum tally surfaces.
    assert "series_id" in result.enum_counters
    assert any("entries by indicator" in n for n in result.notes)


# ──────────────────────────────────────────────────────────────────────────
# run_fed_probe — dispatch guards
# ──────────────────────────────────────────────────────────────────────────


def test_runner_raises_on_unknown_source(validator) -> None:
    probe = validator.FedProbe(
        name="bogus",
        source="nonexistent_surface",
        url="https://example.com",
        description="test",
    )
    with pytest.raises(ValueError, match="unknown Fed probe source"):
        validator.run_fed_probe(probe)


# ──────────────────────────────────────────────────────────────────────────
# CLI dispatch — Fed now wired, only nbs remains
# ──────────────────────────────────────────────────────────────────────────


def test_fed_no_longer_unwired(validator) -> None:
    assert "fed" not in (
        validator._OFFICIAL_PROVIDERS
        - validator._OFFICIAL_PROVIDERS_WITH_PROBES
    )


def test_no_providers_remain_unwired(validator) -> None:
    """P5c closed out the last stub — every official provider now has
    a live probe scaffold."""
    unwired = (
        validator._OFFICIAL_PROVIDERS
        - validator._OFFICIAL_PROVIDERS_WITH_PROBES
    )
    assert unwired == frozenset()


def test_fed_dry_run_prints_plan(
    validator, capsys: pytest.CaptureFixture,
) -> None:
    rc = validator.main(["--provider", "fed"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "DRY RUN (fed)" in out
    assert "fomc_calendar" in out
    assert "fed_releasedates" in out

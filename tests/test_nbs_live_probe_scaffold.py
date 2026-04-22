"""Tests for the NBS live-probe scaffold (issue #9 P5c-infra).

No real HTTP. Uses the existing NBS fixtures
(``tests/fixtures/nbs_calendar/index.html`` and ``nbs_2026.html``) to
exercise the end-to-end probe path via the ``index_fetcher`` +
``html_fetcher`` seams.

Parallel in shape to :mod:`tests.test_fed_live_probe_scaffold` — NBS
is HTML-scrape-only, no auth. The probe runner's general exception
branch is the single unified failure surface (index timeout, article
timeout, parse raise all land as ``http_error``); the NBS upstream is
flagged on the issue body as the highest-risk source so this
tolerance shape is intentional.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

from ingestion.calendar.nbs_api import NBSReleaseEntry


REPO_ROOT = Path(__file__).resolve().parent.parent
FIXTURE_DIR = REPO_ROOT / "tests" / "fixtures" / "nbs_calendar"


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


def _fixture(name: str) -> str:
    return (FIXTURE_DIR / name).read_text(encoding="utf-8")


def _index_fetcher(*, session=None, timeout=30.0, retries=2):
    return _fixture("index.html")


def _article_fetcher(url: str, *, session=None, timeout=30.0):
    return _fixture("nbs_2026.html")


# ──────────────────────────────────────────────────────────────────────────
# plan_nbs_probes
# ──────────────────────────────────────────────────────────────────────────


def test_planner_ships_one_probe(validator) -> None:
    probes = validator.plan_nbs_probes()
    assert len(probes) == 1
    assert probes[0].year == validator.datetime.now(
        validator.timezone.utc,
    ).year


def test_planner_probe_name_embeds_year(validator) -> None:
    probe = validator.plan_nbs_probes()[0]
    assert str(probe.year) in probe.name


# ──────────────────────────────────────────────────────────────────────────
# _try_project_nbs_entry
# ──────────────────────────────────────────────────────────────────────────


def test_try_project_nbs_entry_succeeds(validator) -> None:
    entry = NBSReleaseEntry(
        year=2026,
        month=1,
        day=9,
        release_time_local="9:30",
        indicator="CPI",
        weekday_label="Fri",
        date_cell="9/Fri",
    )
    ok, msg = validator._try_project_nbs_entry(entry)
    assert ok is True
    assert "indicator=CPI" in msg
    assert "2026-01-09" in msg


# ──────────────────────────────────────────────────────────────────────────
# run_nbs_probe — happy path + empty/exception paths
# ──────────────────────────────────────────────────────────────────────────


def test_runner_happy_path(validator) -> None:
    """Index + article fixtures drive the full probe flow; the 2026
    fixture carries entries across every whitelisted indicator. CPI +
    PPI fire every month (12 each), Industrial Production / Fixed
    Asset Investment / Retail Sales skip Feb (11 each), and the PMI
    row emits two entries per date (Manufacturing + Non-Manufacturing,
    22 total) — 79 rows in all."""
    probe = validator.NBSProbe(
        name="nbs_yearly_calendar_2026",
        year=2026,
        description="fixture probe",
    )
    result = validator.run_nbs_probe(
        probe,
        index_fetcher=_index_fetcher,
        html_fetcher=_article_fetcher,
    )
    assert result.status == "ok"
    # 12 CPI + 12 PPI + 11 Industrial Production + 11 Fixed Asset
    # Investment + 11 Retail Sales + 12×2 PMI + 4 GDP = 85 entries
    # from the fixture.
    assert result.row_count == 85
    # Sample is newest-first — December.
    assert result.sample_row["month"] == 12
    assert result.parse_attempts == 10  # capped at 10 per existing pattern
    assert result.parse_successes == 10
    assert "indicator" in result.enum_counters
    assert result.enum_counters["indicator"]["CPI"] == 12
    assert result.enum_counters["indicator"]["PPI"] == 12
    # PMI lands 12 per spec because the March cell carries both the
    # Spring-Festival-delayed Feb release and the regular Mar 31 date.
    assert result.enum_counters["indicator"]["MANUFACTURING_PMI"] == 12
    assert result.enum_counters["indicator"]["NON_MANUFACTURING_PMI"] == 12
    assert any("entries by indicator" in n for n in result.notes)


def test_runner_reports_index_timeout_as_http_error(validator) -> None:
    """Index-fetch failures must land cleanly as ``http_error`` — NBS
    is documented as the highest-risk upstream, so the probe card
    reports transient failures without killing a multi-probe run."""
    def timeout_index(*, session=None, timeout=30.0, retries=2):
        raise TimeoutError("connect timeout")

    probe = validator.plan_nbs_probes()[0]
    result = validator.run_nbs_probe(
        probe,
        index_fetcher=timeout_index,
        html_fetcher=_article_fetcher,
    )
    assert result.status == "http_error"
    assert any("TimeoutError" in n for n in result.notes)
    # The NBS-specific "transient failures expected" context note
    # also lands so the operator knows not to trigger an alarm on
    # the first failure.
    assert any("highest-risk source" in n for n in result.notes)


def test_runner_reports_article_fetch_error_as_http_error(validator) -> None:
    """Article-fetch errors follow the same http_error shape."""
    def timeout_article(url, *, session=None, timeout=30.0):
        raise ConnectionError("connection reset")

    probe = validator.plan_nbs_probes()[0]
    result = validator.run_nbs_probe(
        probe,
        index_fetcher=_index_fetcher,
        html_fetcher=timeout_article,
    )
    assert result.status == "http_error"
    assert any("ConnectionError" in n for n in result.notes)


def test_runner_flags_empty_parse_as_http_error(validator) -> None:
    """Similar to FOMC: an empty parse must go red to mirror the
    production fetcher's raise. Seeding empty article HTML reaches
    the post-exception zero-entries guard."""
    def empty_article(url, *, session=None, timeout=30.0):
        return "<html><body><h1>No calendar here</h1></body></html>"

    probe = validator.plan_nbs_probes()[0]
    result = validator.run_nbs_probe(
        probe,
        index_fetcher=_index_fetcher,
        html_fetcher=empty_article,
    )
    # ``parse_nbs_calendar_html`` raises on zero-entries input, so the
    # general exception branch carries it into http_error — either
    # route lands as http_error per P4b Codex lesson.
    assert result.status == "http_error"


# ──────────────────────────────────────────────────────────────────────────
# CLI dispatch — every official provider now wired
# ──────────────────────────────────────────────────────────────────────────


def test_no_official_providers_remain_unwired(validator) -> None:
    unwired = (
        validator._OFFICIAL_PROVIDERS
        - validator._OFFICIAL_PROVIDERS_WITH_PROBES
    )
    assert unwired == frozenset()


def test_nbs_dry_run_prints_plan(
    validator, capsys: pytest.CaptureFixture,
) -> None:
    rc = validator.main(["--provider", "nbs"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "DRY RUN (nbs)" in out
    assert "nbs_yearly_calendar_" in out
    assert "index-page fetch" in out

"""Tests for the data_quality_daily script (issue #102 P3).

Stubs out the live SQLite store, validation engine, and shadow_runner
digest so the script's findings-assembly logic is exercised in
isolation.
"""

from __future__ import annotations

import datetime as dt
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "src"))
if str(REPO_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "scripts"))

import data_quality_daily as script  # noqa: E402


def _patched_build(tmp_path: Path, *, reports, digest):
    """Yield context managers that intercept the heavy dependencies."""
    seeded_store = type("FakeStore", (), {"seed_concept_map": lambda self: None})()
    fake_engine = type(
        "FakeEngine",
        (),
        {"validate_all_concepts": staticmethod(lambda store: reports)},
    )()
    fake_validation_store = object()

    cm = patch.multiple(
        script,
        SQLiteEngineStore=lambda **kwargs: seeded_store,
        ValidationStore=lambda *args, **kwargs: fake_validation_store,
        ValidationEngine=lambda *args, **kwargs: fake_engine,
    )
    return cm, digest


def test_build_report_flags_empty_validation_reports(tmp_path: Path) -> None:
    """Issue #102 P3 codex round 1: an empty concept_map must not pass
    the filer's clean gate. The script must surface a blocking finding."""

    digest = {
        "concepts_covered": 0, "concepts_total": 0,
        "coverage_pct": 0, "confirmed_24h": 0, "error_sources": [],
    }
    cm, _ = _patched_build(tmp_path, reports=[], digest=digest)
    fake_log_dir = tmp_path / "logs"
    fake_log_dir.mkdir()

    with cm:
        with patch("shadow_runner.compute_digest", return_value=digest), \
             patch.object(script, "_scan_log_for_secrets", return_value=None):
            report, summary = script._build_report(
                target_date=dt.date(2026, 5, 1),
                engine_db=tmp_path / "engine.db",
            )

    assert summary["concept_reports"] == 0
    assert any(
        f.kind == "zero_validation_reports" and f.severity == "error"
        for f in report.findings
    )
    assert not report.clean


def test_build_report_clean_when_all_pass(tmp_path: Path) -> None:
    from ingestion.validation._types import ValidationReport

    passing = ValidationReport(
        source="concept:CPI_US",
        run_id="r1",
        timestamp="2026-05-01T00:00:00+00:00",
        checks=(),
    )
    digest = {
        "concepts_covered": 88, "concepts_total": 88, "coverage_pct": 100.0,
        "confirmed_24h": 88, "error_sources": [],
    }
    cm, _ = _patched_build(tmp_path, reports=[passing], digest=digest)
    log_dir = tmp_path / "logs"
    log_dir.mkdir()

    with cm:
        with patch("shadow_runner.compute_digest", return_value=digest), \
             patch.object(script, "_scan_log_for_secrets", return_value=None):
            report, summary = script._build_report(
                target_date=dt.date(2026, 5, 1),
                engine_db=tmp_path / "engine.db",
            )

    assert summary["concept_failures"] == 0
    assert report.clean
    assert report.findings == []

"""Tests for the macro data-quality auto-filer (issue #102 P3)."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from ingestion.calendar.parity_filer import GhRunner
from ingestion.quality.data_quality_filer import (
    CLEAN_STREAK_TO_CLOSE,
    DATA_QUALITY_LABEL,
    DataQualityFinding,
    DataQualityReport,
    coverage_drop_from_digest,
    file_data_quality_report,
    findings_from_concept_reports,
    secret_leak_from_text,
)
from ingestion.validation._types import (
    CheckResult,
    ValidationLayer,
    ValidationReport,
    ValidationSeverity,
)


class FakeRunner(GhRunner):
    """In-memory ``gh`` runner that records intent, never spawns subprocesses."""

    def __init__(self) -> None:
        super().__init__(dry_run=False)
        self.next_issue_number = 5000
        self._open_numbers: list[int] = []
        self.creates: list[dict] = []
        self.comments: list[dict] = []
        self.closes: list[dict] = []

    def _run(self, *args: str) -> str:  # type: ignore[override]
        if args[:2] == ("issue", "list"):
            return json.dumps([{"number": n} for n in self._open_numbers])
        if args[:2] == ("issue", "create"):
            number = self.next_issue_number
            self.next_issue_number += 1
            title = args[args.index("--title") + 1]
            body = args[args.index("--body") + 1]
            self.creates.append({"number": number, "title": title, "body": body})
            self._open_numbers.append(number)
            return f"https://github.com/example/repo/issues/{number}\n"
        if args[:2] == ("issue", "comment"):
            number = int(args[2])
            body = args[args.index("--body") + 1]
            self.comments.append({"number": number, "body": body})
            return ""
        if args[:2] == ("issue", "close"):
            number = int(args[2])
            comment = args[args.index("--comment") + 1] if "--comment" in args else None
            self.closes.append({"number": number, "comment": comment})
            if number in self._open_numbers:
                self._open_numbers.remove(number)
            return ""
        return ""


# ---------------------------------------------------------------------------
# Detector helpers
# ---------------------------------------------------------------------------


def _failing_concept_report() -> ValidationReport:
    failing = CheckResult(
        check_name="concept_source_coverage",
        layer=ValidationLayer.CONCEPT,
        passed=False,
        severity=ValidationSeverity.ERROR,
        message="FEDWATCH_US: 0/1 sources have data",
        source="FEDWATCH_US",
        timestamp="2026-05-01T00:00:00+00:00",
    )
    info = CheckResult(
        check_name="concept_exists",
        layer=ValidationLayer.CONCEPT,
        passed=True,
        severity=ValidationSeverity.INFO,
        message="ok",
        source="FEDWATCH_US",
        timestamp="2026-05-01T00:00:00+00:00",
    )
    return ValidationReport(
        source="concept:FEDWATCH_US",
        run_id="rep-001",
        timestamp="2026-05-01T00:00:00+00:00",
        checks=(info, failing),
    )


def _passing_concept_report() -> ValidationReport:
    return ValidationReport(
        source="concept:CPI_US",
        run_id="rep-002",
        timestamp="2026-05-01T00:00:00+00:00",
        checks=(),
    )


def test_findings_from_concept_reports_collects_errors() -> None:
    findings = findings_from_concept_reports([_failing_concept_report(), _passing_concept_report()])
    assert len(findings) == 1
    f = findings[0]
    assert f.kind == "concept_failure"
    assert f.severity == "error"
    assert f.concept_id == "FEDWATCH_US"
    assert f.run_id == "rep-001"
    assert "FEDWATCH_US" in f.detail


def test_findings_from_concept_reports_includes_warnings() -> None:
    warning = CheckResult(
        check_name="cross_source_level",
        layer=ValidationLayer.CROSS_SOURCE,
        passed=False,
        severity=ValidationSeverity.WARNING,
        message="CPI_EU level mismatch 98%",
        source="CPI_EU",
        timestamp="2026-05-01T00:00:00+00:00",
    )
    report = ValidationReport(
        source="concept:CPI_EU",
        run_id="rep-003",
        timestamp="2026-05-01T00:00:00+00:00",
        checks=(warning,),
    )
    findings = findings_from_concept_reports([report])
    assert len(findings) == 1
    assert findings[0].severity == "warning"
    assert findings[0].kind == "concept_warning"


def test_coverage_drop_emits_finding_when_partial() -> None:
    digest = {"concepts_covered": 86, "concepts_total": 88, "error_sources": ["fred_daily"]}
    finding = coverage_drop_from_digest(digest)
    assert finding is not None
    assert finding.kind == "coverage_drop"
    assert "86/88" in finding.detail
    assert "fred_daily" in finding.detail


def test_coverage_drop_returns_none_when_clean() -> None:
    digest = {"concepts_covered": 88, "concepts_total": 88, "error_sources": []}
    assert coverage_drop_from_digest(digest) is None


def test_secret_leak_from_text_detects_api_key() -> None:
    text = "GET /fred?api_key=DEADBEEF&series=T5YIE"
    finding = secret_leak_from_text(text, source_label="shadow.log")
    assert finding is not None
    assert "shadow.log" in finding.detail
    assert "DEADBEEF" not in finding.detail


def test_secret_leak_from_text_clean_returns_none() -> None:
    assert secret_leak_from_text("nothing sensitive here") is None


# ---------------------------------------------------------------------------
# Filer driver
# ---------------------------------------------------------------------------


def _findings_report(*findings: DataQualityFinding) -> DataQualityReport:
    return DataQualityReport(
        target_date=date(2026, 5, 1),
        findings=list(findings),
        digest_summary={"concepts_covered": 86, "concepts_total": 88},
    )


def _failure_finding() -> DataQualityFinding:
    return DataQualityFinding(
        kind="concept_failure",
        severity="error",
        detail="FEDWATCH_US: 0/1 sources have data",
        concept_id="FEDWATCH_US",
        run_id="rep-001",
    )


def test_filer_creates_issue_on_first_failure(tmp_path: Path) -> None:
    runner = FakeRunner()
    state_path = tmp_path / "state.json"

    action = file_data_quality_report(
        report=_findings_report(_failure_finding()),
        runner=runner,
        state_path=state_path,
    )

    assert len(action.created) == 1
    number, title = action.created[0]
    assert "Macro data-quality" in title
    assert "2026-05-01" in title
    body = runner.creates[0]["body"]
    assert "FEDWATCH_US" in body
    assert "86/88" in body
    state = json.loads(state_path.read_text())
    assert state["open_issue"] == number
    assert state["clean_streak"] == 0


def test_filer_comments_when_open_issue_exists(tmp_path: Path) -> None:
    runner = FakeRunner()
    state_path = tmp_path / "state.json"
    runner._open_numbers.append(7777)

    action = file_data_quality_report(
        report=_findings_report(_failure_finding()),
        runner=runner,
        state_path=state_path,
    )

    assert action.commented == [7777]
    assert not runner.creates
    assert "Daily data-quality run" in runner.comments[0]["body"]
    assert "FEDWATCH_US" in runner.comments[0]["body"]


def test_filer_clean_run_bumps_streak_until_close(tmp_path: Path) -> None:
    runner = FakeRunner()
    state_path = tmp_path / "state.json"

    # Day 0 — create issue.
    file_data_quality_report(
        report=_findings_report(_failure_finding()),
        runner=runner,
        state_path=state_path,
    )
    assert runner.creates
    open_number = runner.creates[0]["number"]

    # Days 1..(N-1) clean — bump streak only.
    for day in range(1, CLEAN_STREAK_TO_CLOSE):
        clean_report = DataQualityReport(target_date=date(2026, 5, 1 + day))
        action = file_data_quality_report(
            report=clean_report,
            runner=runner,
            state_path=state_path,
        )
        assert action.skipped_clean
        assert not action.closed

    # Final clean run hits threshold and closes.
    final_action = file_data_quality_report(
        report=DataQualityReport(target_date=date(2026, 5, 1 + CLEAN_STREAK_TO_CLOSE)),
        runner=runner,
        state_path=state_path,
    )
    assert final_action.closed == [open_number]
    state = json.loads(state_path.read_text())
    assert state["open_issue"] is None
    assert state["clean_streak"] == 0


def test_filer_redacts_secrets_in_body(tmp_path: Path) -> None:
    runner = FakeRunner()
    state_path = tmp_path / "state.json"

    poisoned_finding = DataQualityFinding(
        kind="concept_failure",
        severity="error",
        detail="error fetching url=https://api.example.com/x?api_key=DEADBEEF&z=1",
        concept_id="FEDWATCH_US",
        run_id="rep-leak",
    )
    digest = {
        "concepts_covered": 86,
        "concepts_total": 88,
        "error_sources": ["fred_daily?api_key=DEADBEEF"],
    }
    report = DataQualityReport(
        target_date=date(2026, 5, 1),
        findings=[poisoned_finding],
        digest_summary=digest,
    )

    file_data_quality_report(report=report, runner=runner, state_path=state_path)

    body = runner.creates[0]["body"]
    assert "DEADBEEF" not in body
    assert "api_key=***" in body


def test_critical_severity_blocks_clean_classification() -> None:
    """A finding tagged ``critical`` (the documented vocabulary) must
    keep the report from looking clean. Without this guard the filer's
    auto-close path would advance through critical regressions."""

    critical_finding = DataQualityFinding(
        kind="zero_validation_reports",
        severity="critical",
        detail="validate_all_concepts returned 0 reports",
    )
    report = DataQualityReport(
        target_date=date(2026, 5, 1),
        findings=[critical_finding],
    )
    assert not report.clean
    assert critical_finding in report.error_findings


def test_critical_finding_creates_issue(tmp_path: Path) -> None:
    """End-to-end: a critical finding must drive a real ``gh issue create``."""

    runner = FakeRunner()
    state_path = tmp_path / "state.json"

    critical_finding = DataQualityFinding(
        kind="zero_validation_reports",
        severity="critical",
        detail="validate_all_concepts returned 0 reports",
    )
    report = DataQualityReport(
        target_date=date(2026, 5, 1),
        findings=[critical_finding],
    )
    action = file_data_quality_report(
        report=report, runner=runner, state_path=state_path,
    )
    assert len(action.created) == 1


def test_filer_re_failure_after_close_creates_fresh_issue(tmp_path: Path) -> None:
    runner = FakeRunner()
    state_path = tmp_path / "state.json"

    file_data_quality_report(
        report=_findings_report(_failure_finding()),
        runner=runner,
        state_path=state_path,
    )
    first_number = runner.creates[0]["number"]

    # Drive clean streak to threshold.
    for day in range(1, CLEAN_STREAK_TO_CLOSE + 1):
        file_data_quality_report(
            report=DataQualityReport(target_date=date(2026, 5, 1 + day)),
            runner=runner,
            state_path=state_path,
        )
    assert runner.closes and runner.closes[0]["number"] == first_number

    # New failure after close.
    action = file_data_quality_report(
        report=DataQualityReport(
            target_date=date(2026, 5, 20),
            findings=[_failure_finding()],
        ),
        runner=runner,
        state_path=state_path,
    )
    assert len(action.created) == 1
    assert action.created[0][0] != first_number

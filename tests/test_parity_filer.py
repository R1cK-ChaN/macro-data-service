"""Tests for the parity gh-issue filer (issue #22 P4).

Substitutes a fake ``GhRunner`` so no ``gh`` binary is required and no
network calls are made.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from ingestion.calendar.parity_daily import (
    AgencyReport,
    MissingRelease,
    ValueMismatch,
)
from ingestion.calendar.parity_filer import (
    CLEAN_STREAK_TO_CLOSE,
    GhRunner,
    file_infra_failure,
    file_reports,
)


class FakeRunner(GhRunner):
    """Captures intended actions; never spawns gh."""

    def __init__(self) -> None:
        super().__init__(dry_run=False)
        self.next_issue_number = 1000
        self.open_by_label: dict[tuple[str, str], int] = {}
        self.creates: list[dict] = []
        self.comments: list[dict] = []
        self.closes: list[dict] = []

    def _key(self, agency_id: str) -> tuple[str, str]:
        return ("parity-drift", f"agency:{agency_id}")

    def list_open_issue(self, *, agency_id: str) -> int | None:
        return self.open_by_label.get(self._key(agency_id))

    def create_issue(
        self, *, title: str, body: str, agency_id: str,
        extra_labels=(),
    ) -> int | None:
        number = self.next_issue_number
        self.next_issue_number += 1
        self.open_by_label[self._key(agency_id)] = number
        self.creates.append({
            "title": title, "body": body, "agency_id": agency_id,
            "number": number,
        })
        return number

    def comment_issue(self, *, number: int, body: str) -> None:
        self.comments.append({"number": number, "body": body})

    def close_issue(self, *, number: int, comment: str | None = None) -> None:
        self.closes.append({"number": number, "comment": comment})
        for k, v in list(self.open_by_label.items()):
            if v == number:
                del self.open_by_label[k]


def _bls_report_with_anomaly(target: str = "2026-04-24") -> AgencyReport:
    report = AgencyReport(
        agency_id="BLS", label="US Bureau of Labor Statistics",
        target_date=target,
    )
    report.mismatches.append(ValueMismatch(
        country="US", canonical="NFP", reference_date="2026-03-31",
        te_event_id="te-3", agency_event_id="bls-3",
        te_actual="200", agency_actual="195",
        parse_failed=False, likely_alignment_bug=False,
        note="TE=200 != BLS=195",
    ))
    return report


def test_creates_issue_on_first_anomaly(tmp_path: Path) -> None:
    runner = FakeRunner()
    state = tmp_path / "state.json"
    log = file_reports(
        reports=[_bls_report_with_anomaly()],
        target_date=date(2026, 4, 24),
        runner=runner,
        state_path=state,
    )
    assert len(runner.creates) == 1
    assert runner.creates[0]["agency_id"] == "BLS"
    assert ("BLS", "Parity drift — US Bureau of Labor Statistics (2026-04-24)") in [
        (a, t) for a, t in log.created
    ]
    persisted = json.loads(state.read_text())
    assert persisted["BLS"]["clean_streak"] == 0
    assert persisted["BLS"]["open_issue"] == 1000


def test_appends_comment_when_open_issue_exists(tmp_path: Path) -> None:
    runner = FakeRunner()
    runner.open_by_label[runner._key("BLS")] = 42
    state = tmp_path / "state.json"
    file_reports(
        reports=[_bls_report_with_anomaly()],
        target_date=date(2026, 4, 24),
        runner=runner,
        state_path=state,
    )
    assert runner.creates == []
    assert len(runner.comments) == 1
    assert runner.comments[0]["number"] == 42


def test_clean_run_bumps_streak_and_closes_after_seven(tmp_path: Path) -> None:
    runner = FakeRunner()
    runner.open_by_label[runner._key("BLS")] = 99
    state = tmp_path / "state.json"
    # Pre-seed state to streak=6 so the next clean run hits the close
    # threshold.
    state.write_text(json.dumps({
        "BLS": {"clean_streak": 6, "open_issue": 99},
    }))

    log = file_reports(
        reports=[],
        target_date=date(2026, 4, 24),
        runner=runner,
        state_path=state,
    )
    assert ("BLS", 99) in log.closed
    persisted = json.loads(state.read_text())
    assert persisted["BLS"]["open_issue"] is None
    assert persisted["BLS"]["clean_streak"] == 0


def test_clean_run_below_threshold_only_increments(tmp_path: Path) -> None:
    runner = FakeRunner()
    runner.open_by_label[runner._key("BLS")] = 99
    state = tmp_path / "state.json"
    state.write_text(json.dumps({
        "BLS": {"clean_streak": 3, "open_issue": 99},
    }))

    log = file_reports(
        reports=[],
        target_date=date(2026, 4, 24),
        runner=runner,
        state_path=state,
    )
    assert log.closed == []
    persisted = json.loads(state.read_text())
    assert persisted["BLS"]["clean_streak"] == 4
    assert persisted["BLS"]["open_issue"] == 99


def test_re_anomaly_after_close_creates_fresh_issue(tmp_path: Path) -> None:
    runner = FakeRunner()
    state = tmp_path / "state.json"
    # Streak hit threshold previously; issue closed; state reset.
    state.write_text(json.dumps({
        "BLS": {"clean_streak": 0, "open_issue": None},
    }))
    log = file_reports(
        reports=[_bls_report_with_anomaly()],
        target_date=date(2026, 4, 24),
        runner=runner,
        state_path=state,
    )
    assert len(runner.creates) == 1


def test_operator_close_suppresses_refile_until_clean(tmp_path: Path) -> None:
    """If the operator closes the issue (streak < threshold), the next
    anomaly run must NOT recreate it. The suppression clears once a
    clean run lands."""
    runner = FakeRunner()
    state = tmp_path / "state.json"
    # State remembers issue 77 open; gh shows nothing open (operator
    # closed manually after streak=2 — well below the 7-day auto-close
    # threshold).
    state.write_text(json.dumps({
        "BLS": {"clean_streak": 2, "open_issue": 77},
    }))

    log1 = file_reports(
        reports=[_bls_report_with_anomaly("2026-04-24")],
        target_date=date(2026, 4, 24),
        runner=runner,
        state_path=state,
    )
    # Suppressed: no creates, no comments.
    assert runner.creates == []
    assert runner.comments == []
    assert log1.suppressed == ["BLS"]
    persisted = json.loads(state.read_text())
    assert persisted["BLS"]["suppressed_by_operator"] is True

    # Clean day breaks the suppression.
    log2 = file_reports(
        reports=[],
        target_date=date(2026, 4, 25),
        runner=runner,
        state_path=state,
    )
    persisted = json.loads(state.read_text())
    assert "suppressed_by_operator" not in persisted["BLS"]

    # Next anomaly day after the clean break creates fresh.
    log3 = file_reports(
        reports=[_bls_report_with_anomaly("2026-04-26")],
        target_date=date(2026, 4, 26),
        runner=runner,
        state_path=state,
    )
    assert len(runner.creates) == 1
    assert log3.created[0][0] == "BLS"


def test_auto_close_then_re_anomaly_creates_fresh_issue(tmp_path: Path) -> None:
    """Auto-close path (clean streak ≥ 7) is distinct from the
    operator-close path: re-anomaly opens a fresh issue immediately,
    no suppression."""
    runner = FakeRunner()
    state = tmp_path / "state.json"
    # State left after a 7-day clean run that auto-closed issue 50.
    state.write_text(json.dumps({
        "BLS": {"clean_streak": 0, "open_issue": None},
    }))
    log = file_reports(
        reports=[_bls_report_with_anomaly("2026-04-24")],
        target_date=date(2026, 4, 24),
        runner=runner,
        state_path=state,
    )
    assert len(runner.creates) == 1
    assert log.suppressed == []


def test_dry_run_real_runner_emits_no_subprocess(tmp_path: Path) -> None:
    runner = GhRunner(dry_run=True, gh_path="/nonexistent/gh")
    state = tmp_path / "state.json"
    log = file_reports(
        reports=[_bls_report_with_anomaly()],
        target_date=date(2026, 4, 24),
        runner=runner,
        state_path=state,
    )
    # In dry-run mode list/create/comment/close all return ""/None and
    # never spawn gh; the run must complete without raising.
    assert log.created == [
        ("BLS", "Parity drift — US Bureau of Labor Statistics (2026-04-24)"),
    ]


def test_infra_failure_creates_or_comments(tmp_path: Path) -> None:
    runner = FakeRunner()
    n1 = file_infra_failure(
        summary="job failed",
        target_date=date(2026, 4, 24),
        runner=runner,
    )
    assert n1 is not None and len(runner.creates) == 1

    n2 = file_infra_failure(
        summary="job failed again",
        target_date=date(2026, 4, 25),
        runner=runner,
    )
    assert n2 == n1  # same open issue → comment, not new
    assert len(runner.comments) == 1

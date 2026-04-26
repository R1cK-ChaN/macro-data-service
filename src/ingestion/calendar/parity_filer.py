"""GitHub issue filer for the daily parity tripwire (issue #22 P4).

Wraps ``gh issue list/create/comment/close``. One open issue per agency,
identified by the ``parity-drift`` + ``agency:<id>`` label pair. New
anomalies on a day with an open issue append a comment; clean days
increment the agency's clean streak; 7 consecutive clean days close
the issue.

Lifecycle invariants:

* Re-anomaly after auto-close opens a *new* issue, never re-opens the
  closed one. The clean-streak file resets on close; the next anomaly
  finds no open issue and falls into the create branch.
* The clean-streak state lives at ``.macro-data/parity_state.json`` so
  a manually-closed issue (operator triages and decides "false alarm,
  closing") doesn't immediately reopen the next morning — the streak
  only ticks up on confirmed clean comparator runs.

``--dry-run`` mode prints intended actions and never spawns ``gh``.
"""

from __future__ import annotations

import json
import logging
import shutil
import subprocess
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Iterable, Sequence

from .agency_registry import AGENCIES
from .parity_daily import AgencyReport

logger = logging.getLogger(__name__)

PARITY_DRIFT_LABEL = "parity-drift"
INFRA_AGENCY_LABEL = "agency:infra"
CLEAN_STREAK_TO_CLOSE = 7


@dataclass
class FilerActionLog:
    """Mutable record of what the filer did this run.

    Useful for the daily log output and for tests that want to assert
    on actions without running the real ``gh`` binary.
    """

    created: list[tuple[str, str]] = field(default_factory=list)  # (agency_id, title)
    commented: list[tuple[str, int]] = field(default_factory=list)  # (agency_id, issue_number)
    closed: list[tuple[str, int]] = field(default_factory=list)
    skipped_no_anomaly: list[str] = field(default_factory=list)
    suppressed: list[str] = field(default_factory=list)  # operator-closed, no refile
    lag_only: list[tuple[str, int]] = field(default_factory=list)  # (agency_id, lag_count)


# ---------------------------------------------------------------------------
# Default state path
# ---------------------------------------------------------------------------


def default_state_path(root: Path | None = None) -> Path:
    base = root or Path.cwd()
    return base / ".macro-data" / "parity_state.json"


def _load_state(path: Path) -> dict[str, dict]:
    if not path.is_file():
        return {}
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        logger.warning("parity_state.json unreadable at %s; starting fresh", path)
        return {}


def _save_state(path: Path, state: dict[str, dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, sort_keys=True, indent=2))


# ---------------------------------------------------------------------------
# Anomaly markdown
# ---------------------------------------------------------------------------


def _format_report_body(report: AgencyReport) -> str:
    lines: list[str] = []
    lines.append(f"# Parity drift — {report.label} ({report.agency_id})")
    lines.append("")
    lines.append(f"Comparison date: `{report.target_date}`")
    lines.append(
        f"Anomalies: **{len(report.missing)} missing**, "
        f"**{len(report.mismatches)} value mismatches**",
    )
    lines.append("")
    if report.missing:
        lines.append("## Missing releases (TE has actual, agency does not)")
        lines.append("")
        lines.append("| Country | Indicator | Reference | TE actual | TE event |")
        lines.append("|---------|-----------|-----------|-----------|----------|")
        for m in report.missing:
            lines.append(
                f"| {m.country} | {m.canonical} | {m.reference_date or '—'} "
                f"| `{m.te_actual}` | `{m.te_event_id}` |",
            )
        lines.append("")
    if report.mismatches:
        lines.append("## Value mismatches (first-release ≠ TE)")
        lines.append("")
        lines.append(
            "| Country | Indicator | Reference | TE actual | "
            "Agency actual | Note |",
        )
        lines.append(
            "|---------|-----------|-----------|-----------|"
            "---------------|------|",
        )
        for m in report.mismatches:
            lines.append(
                f"| {m.country} | {m.canonical} | {m.reference_date or '—'} "
                f"| `{m.te_actual}` | "
                f"`{m.agency_actual if m.agency_actual is not None else '—'}` "
                f"| {m.note} |",
            )
        lines.append("")
    return "\n".join(lines) + "\n"


def _format_comment_body(report: AgencyReport) -> str:
    return (
        f"### Daily anomaly — `{report.target_date}`\n\n"
        + _format_report_body(report).split("\n", 2)[2]
    )


# ---------------------------------------------------------------------------
# gh CLI runner
# ---------------------------------------------------------------------------


class GhRunner:
    """Thin wrapper over the ``gh`` binary.

    Construct once per filer run; methods raise :class:`RuntimeError`
    when ``gh`` exits non-zero. The infra self-monitor in P5 catches
    raises and converts them to ``agency:infra`` issues after two
    consecutive failures.
    """

    def __init__(self, *, dry_run: bool = False, gh_path: str | None = None) -> None:
        self.dry_run = dry_run
        self._gh = gh_path or shutil.which("gh") or "gh"

    def list_open_issue(self, *, agency_id: str) -> int | None:
        """Return the issue number for the open ``parity-drift`` +
        ``agency:<id>`` issue, or ``None`` if no such issue exists."""
        out = self._run(
            "issue", "list",
            "--label", PARITY_DRIFT_LABEL,
            "--label", _agency_label(agency_id),
            "--state", "open",
            "--json", "number",
            "--limit", "5",
        )
        if not out:
            return None
        try:
            data = json.loads(out)
        except json.JSONDecodeError:
            return None
        if not data:
            return None
        # If multiple are open (shouldn't happen but operator may have
        # filed manually), pick the most recent — others are operator-
        # tracked and we leave them alone.
        return max(int(entry["number"]) for entry in data if "number" in entry)

    def create_issue(
        self, *, title: str, body: str, agency_id: str,
        extra_labels: Sequence[str] = (),
    ) -> int | None:
        labels = [PARITY_DRIFT_LABEL, _agency_label(agency_id), *extra_labels]
        out = self._run(
            "issue", "create",
            "--title", title,
            "--body", body,
            *_label_args(labels),
        )
        return _parse_issue_url(out)

    def comment_issue(self, *, number: int, body: str) -> None:
        self._run(
            "issue", "comment", str(number),
            "--body", body,
        )

    def close_issue(self, *, number: int, comment: str | None = None) -> None:
        args = ["issue", "close", str(number)]
        if comment:
            args.extend(["--comment", comment])
        self._run(*args)

    def _run(self, *args: str) -> str:
        cmd = [self._gh, *args]
        if self.dry_run:
            logger.info("dry_run: gh %s", " ".join(args))
            return ""
        logger.info("gh %s", " ".join(args))
        try:
            result = subprocess.run(
                cmd, check=True, capture_output=True, text=True, timeout=60,
            )
        except FileNotFoundError as exc:
            raise RuntimeError(f"gh binary not found: {exc}") from exc
        except subprocess.CalledProcessError as exc:
            raise RuntimeError(
                f"gh {' '.join(args)} failed (rc={exc.returncode}): {exc.stderr}",
            ) from exc
        return result.stdout.strip()


def _agency_label(agency_id: str) -> str:
    return f"agency:{agency_id}"


def _label_args(labels: Iterable[str]) -> list[str]:
    out: list[str] = []
    for label in labels:
        out.extend(["--label", label])
    return out


def _parse_issue_url(stdout: str) -> int | None:
    """Pull the issue number out of ``gh issue create``'s URL output."""
    for line in stdout.splitlines():
        if "/issues/" in line:
            tail = line.rsplit("/issues/", 1)[-1].strip()
            if tail.isdigit():
                return int(tail)
    return None


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


def file_reports(
    *,
    reports: Sequence[AgencyReport],
    target_date: date,
    runner: GhRunner,
    state_path: Path | None = None,
) -> FilerActionLog:
    """Reconcile ``reports`` against the open-issues set.

    For each agency:

    * Anomaly + open issue → comment + reset clean streak.
    * Anomaly + no open issue, no operator-suppression → create + reset
      clean streak.
    * Anomaly + no open issue, operator-suppression active → record
      under ``suppressed`` and skip filing. Suppression is set when
      ``state.open_issue`` points at an issue that is no longer open
      on GitHub *and* ``state.clean_streak`` was below
      :data:`CLEAN_STREAK_TO_CLOSE` (i.e. the close did not come from
      our auto-close path). It clears on the next clean comparator
      run, or when the operator deletes the suppression marker by
      hand. Without this guard the filer would keep reopening an
      issue the operator just triaged closed as a false alarm.
    * No anomaly + open issue → bump clean streak; close on
      :data:`CLEAN_STREAK_TO_CLOSE`. Clears any suppression.
    * No anomaly + no open issue → bump streak, clear suppression.

    Agencies absent from ``reports`` are treated as clean (the
    comparator drops clean reports). The state file is updated even on
    dry-run so successive dry-runs can rehearse the streak progression.
    """
    iso = target_date.isoformat()
    state_path = state_path or default_state_path()
    state = _load_state(state_path)
    log = FilerActionLog()

    reports_by_agency = {r.agency_id: r for r in reports}

    for agency in AGENCIES:
        agency_state = state.setdefault(agency.agency_id, {})
        report = reports_by_agency.get(agency.agency_id)

        # Detect operator-closed transitions before anything else: if
        # state remembers an open issue but GitHub does not, the
        # operator either closed it manually or it was auto-closed by a
        # previous run. Distinguish by ``clean_streak``.
        live_issue = runner.list_open_issue(agency_id=agency.agency_id)
        remembered = agency_state.get("open_issue")
        if remembered is not None and live_issue is None:
            prior_streak = int(agency_state.get("clean_streak", 0))
            if prior_streak < CLEAN_STREAK_TO_CLOSE:
                # Operator closed by hand. Latch suppression until a
                # clean run breaks it.
                agency_state["suppressed_by_operator"] = True
                agency_state["suppressed_issue"] = remembered
            agency_state["open_issue"] = None

        if report is not None and report.clean and report.lag_after_parity:
            # Lag-only path: vintages landed after parity_run_time.
            # Per #38 these are not real drift — skip filing entirely
            # and leave the clean-streak counter untouched (neither
            # bumped nor reset) so the agency neither auto-closes nor
            # opens a fresh issue from a sweep timing artifact.
            log.lag_only.append(
                (agency.agency_id, len(report.lag_after_parity)),
            )
            continue

        if report is not None and not report.clean:
            # Anomaly path.
            agency_state["clean_streak"] = 0
            agency_state["last_anomaly_date"] = iso
            if live_issue is not None:
                runner.comment_issue(
                    number=live_issue, body=_format_comment_body(report),
                )
                log.commented.append((agency.agency_id, live_issue))
                agency_state["open_issue"] = live_issue
                continue
            if agency_state.get("suppressed_by_operator"):
                log.suppressed.append(agency.agency_id)
                continue
            title = f"Parity drift — {agency.label} ({iso})"
            body = _format_report_body(report)
            created = runner.create_issue(
                title=title, body=body, agency_id=agency.agency_id,
            )
            log.created.append((agency.agency_id, title))
            if created is not None:
                agency_state["open_issue"] = created
            continue

        # Clean-day path.
        log.skipped_no_anomaly.append(agency.agency_id)
        streak = int(agency_state.get("clean_streak", 0)) + 1
        agency_state["clean_streak"] = streak
        agency_state["last_clean_date"] = iso
        # A clean run vindicates the operator's suppression — clear so
        # the next anomaly opens a fresh issue normally.
        if agency_state.pop("suppressed_by_operator", None):
            agency_state.pop("suppressed_issue", None)

        if live_issue is None:
            continue
        if streak >= CLEAN_STREAK_TO_CLOSE:
            runner.close_issue(
                number=int(live_issue),
                comment=(
                    f"Auto-closing: {streak} consecutive clean parity runs "
                    f"through {iso}."
                ),
            )
            log.closed.append((agency.agency_id, int(live_issue)))
            agency_state["open_issue"] = None
            agency_state["clean_streak"] = 0

    _save_state(state_path, state)
    return log


def file_infra_failure(
    *,
    summary: str,
    target_date: date,
    runner: GhRunner,
    extra_body: str = "",
) -> int | None:
    """File an ``agency:infra`` issue when the daily job has self-reported
    a 2-strike failure (P5).

    Idempotent within a single day — if an open infra issue already
    exists, append a comment instead of opening a duplicate.
    """
    iso = target_date.isoformat()
    title = f"Parity job failure — {iso}"
    body = (
        f"# Parity daily-job failure\n\nDate: `{iso}`\n\n"
        f"Summary: {summary}\n\n{extra_body}"
    )
    existing = runner.list_open_issue(agency_id="infra")
    if existing is not None:
        runner.comment_issue(number=existing, body=body)
        return existing
    return runner.create_issue(
        title=title, body=body, agency_id="infra",
    )


__all__ = [
    "CLEAN_STREAK_TO_CLOSE",
    "FilerActionLog",
    "GhRunner",
    "INFRA_AGENCY_LABEL",
    "PARITY_DRIFT_LABEL",
    "default_state_path",
    "file_infra_failure",
    "file_reports",
]

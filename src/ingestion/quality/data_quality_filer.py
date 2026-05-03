"""Macro data-quality auto-filer (issue #102).

The TE calendar parity tripwire (``ingestion/calendar/parity_filer.py``)
only acts on calendar-style ``AgencyReport`` anomalies. Everything else
that signals upstream rot — concept-validation hard failures, shadow
digest coverage drops, ``validate`` runs that produced zero checks,
provider keys leaking into persisted logs — has no single sink today.

This module fills that gap with a separate filer keyed on the
``data-quality`` label so the two pipelines never collide:

* One open ``data-quality`` issue at a time.
* Daily comments append the latest run when an issue is already open.
* :data:`CLEAN_STREAK_TO_CLOSE` consecutive clean runs auto-close.
* Issue / comment bodies are redacted via
  :func:`ingestion._shared.redaction.redact_secrets` before they hit
  GitHub, mirroring the on-disk log guarantee from P1.

The :class:`GhRunner` from ``parity_filer`` is reused as-is (the only
parity-specific surface there is the agency_id label, which we replace
with ``"data-quality"``).
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Sequence

from ingestion._shared.redaction import redact_secrets
from ingestion.calendar.parity_filer import (
    GhRunner,
    _label_args,
    _parse_issue_url,
)

logger = logging.getLogger(__name__)

DATA_QUALITY_LABEL = "data-quality"
CLEAN_STREAK_TO_CLOSE = 7
DEFAULT_REPRO_COMMANDS = (
    "PYTHONPATH=src python3 -m macro_data.cli validate "
    "--db-path .macro-data/engine.db --json",
    "PYTHONPATH=src python3 -c "
    "\"from ingestion.quality.shadow_digest import compute_digest; import json; "
    "print(json.dumps(compute_digest('.macro-data/engine.db'), indent=2))\"",
)


# ---------------------------------------------------------------------------
# Findings + report
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DataQualityFinding:
    """One concrete macro data-quality failure detected on a run.

    ``kind`` identifies the trigger family (matches the four signals
    listed in the issue thread). ``severity`` keeps the same vocabulary
    as :class:`ingestion.validation._types.ValidationSeverity` so the
    filer's clean / error gating mirrors the underlying check.
    """

    kind: str
    detail: str
    severity: str = "error"
    concept_id: str = ""
    series_id: str = ""
    source: str = ""
    run_id: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "severity": self.severity,
            "detail": self.detail,
            "concept_id": self.concept_id,
            "series_id": self.series_id,
            "source": self.source,
            "run_id": self.run_id,
        }


_BLOCKING_SEVERITIES = ("error", "critical")


@dataclass
class DataQualityReport:
    """Aggregate of findings from one filer cycle."""

    target_date: date
    findings: list[DataQualityFinding] = field(default_factory=list)
    digest_summary: dict[str, Any] = field(default_factory=dict)
    repro_commands: tuple[str, ...] = DEFAULT_REPRO_COMMANDS

    @property
    def clean(self) -> bool:
        return not any(f.severity in _BLOCKING_SEVERITIES for f in self.findings)

    @property
    def error_findings(self) -> list[DataQualityFinding]:
        return [f for f in self.findings if f.severity in _BLOCKING_SEVERITIES]

    @property
    def warning_findings(self) -> list[DataQualityFinding]:
        return [f for f in self.findings if f.severity == "warning"]


# ---------------------------------------------------------------------------
# Detectors
# ---------------------------------------------------------------------------


def findings_from_concept_reports(reports: Iterable[Any]) -> list[DataQualityFinding]:
    """Map concept-level :class:`ValidationReport`s to findings.

    Pulls every error- or critical-severity check from each report and
    one finding per failed check. Reports that pass cleanly contribute
    nothing.
    """
    out: list[DataQualityFinding] = []
    for report in reports:
        if getattr(report, "passed", True) and not _has_warning_checks(report):
            continue
        run_id = getattr(report, "run_id", "")
        for check in getattr(report, "checks", ()):  # CheckResult
            if check.passed:
                continue
            severity = (
                check.severity.value
                if hasattr(check.severity, "value")
                else str(check.severity)
            )
            if severity not in ("error", "critical", "warning"):
                continue
            kind = (
                "concept_warning"
                if severity == "warning"
                else "concept_failure"
            )
            out.append(
                DataQualityFinding(
                    kind=kind,
                    severity="warning" if severity == "warning" else "error",
                    detail=redact_secrets(check.message),
                    concept_id=check.source,
                    series_id=getattr(check, "series_id", "") or "",
                    run_id=run_id,
                ),
            )
    return out


def _has_warning_checks(report: Any) -> bool:
    for check in getattr(report, "checks", ()):
        severity = (
            check.severity.value
            if hasattr(check.severity, "value")
            else str(check.severity)
        )
        if severity == "warning" and not check.passed:
            return True
    return False


def coverage_drop_from_digest(digest: dict[str, Any]) -> DataQualityFinding | None:
    """Emit a finding when shadow digest reports incomplete coverage.

    The threshold is strict: any ``concepts_covered < concepts_total``
    or any non-empty ``error_sources`` flips this to a finding. We
    don't try to be clever about transient gaps — that's what the
    clean-streak counter on the filer is for.
    """
    covered = int(digest.get("concepts_covered", 0))
    total = int(digest.get("concepts_total", 0))
    error_sources = list(digest.get("error_sources", []) or [])
    if total > 0 and covered >= total and not error_sources:
        return None
    parts: list[str] = []
    if total > 0 and covered < total:
        parts.append(f"coverage {covered}/{total}")
    if error_sources:
        # Pre-redact in case any source name carries a stray query string.
        parts.append(
            "error_sources=["
            + ", ".join(redact_secrets(s) for s in error_sources)
            + "]"
        )
    if not parts:
        return None
    return DataQualityFinding(
        kind="coverage_drop",
        severity="error",
        detail="shadow digest: " + "; ".join(parts),
    )


def secret_leak_from_text(text: str, *, source_label: str = "") -> DataQualityFinding | None:
    """Emit a finding when ``text`` still contains a known secret.

    Use when scanning persisted logs to confirm the redaction guard
    held. ``source_label`` gets included verbatim in the detail so the
    operator knows which file to inspect.
    """
    if not text:
        return None
    redacted = redact_secrets(text)
    if redacted == text:
        return None
    label = source_label or "log"
    return DataQualityFinding(
        kind="secret_leak",
        severity="error",
        detail=f"{label}: provider secret detected (redacted in this issue)",
    )


# ---------------------------------------------------------------------------
# Body formatting
# ---------------------------------------------------------------------------


def _format_findings_table(findings: Sequence[DataQualityFinding], title: str) -> list[str]:
    if not findings:
        return []
    lines: list[str] = []
    lines.append(f"## {title}")
    lines.append("")
    lines.append("| Kind | Concept | Series | Detail | Run ID |")
    lines.append("|------|---------|--------|--------|--------|")
    for f in findings:
        lines.append(
            f"| {f.kind} | {f.concept_id or '—'} | {f.series_id or '—'} "
            f"| {redact_secrets(f.detail)} | `{f.run_id or '—'}` |",
        )
    lines.append("")
    return lines


def _format_digest_section(digest: dict[str, Any]) -> list[str]:
    if not digest:
        return []
    lines: list[str] = ["## Shadow digest"]
    lines.append("")
    if "concepts_covered" in digest and "concepts_total" in digest:
        coverage_pct = digest.get("coverage_pct")
        coverage_str = (
            f"{digest['concepts_covered']}/{digest['concepts_total']}"
            + (f" ({coverage_pct}%)" if coverage_pct is not None else "")
        )
        lines.append(f"- Coverage: {coverage_str}")
    if "confirmed_24h" in digest:
        lines.append(f"- Confirmed (24h): {digest['confirmed_24h']}")
    error_sources = digest.get("error_sources")
    if error_sources:
        lines.append(
            "- Error sources: "
            + ", ".join(f"`{redact_secrets(s)}`" for s in error_sources),
        )
    if "cycle" in digest:
        lines.append(f"- Cycle: {digest['cycle']}")
    if "timestamp" in digest:
        lines.append(f"- Timestamp: `{digest['timestamp']}`")
    lines.append("")
    return lines


def _format_repro_section(commands: Sequence[str]) -> list[str]:
    if not commands:
        return []
    lines = ["## Repro", "", "```"]
    lines.extend(redact_secrets(cmd) for cmd in commands)
    lines.append("```")
    lines.append("")
    return lines


def _format_report_body(report: DataQualityReport) -> str:
    lines: list[str] = []
    iso = report.target_date.isoformat()
    lines.append(f"# Macro data-quality — {iso}")
    lines.append("")
    lines.append(
        f"Findings: **{len(report.error_findings)} errors**, "
        f"**{len(report.warning_findings)} warnings**",
    )
    lines.append("")
    lines.extend(_format_findings_table(report.error_findings, "Errors"))
    lines.extend(_format_findings_table(report.warning_findings, "Warnings"))
    lines.extend(_format_digest_section(report.digest_summary))
    lines.extend(_format_repro_section(report.repro_commands))
    return "\n".join(lines).rstrip() + "\n"


def _format_comment_body(report: DataQualityReport) -> str:
    iso = report.target_date.isoformat()
    body = _format_report_body(report)
    # Strip the H1 header from the issue body so the comment leads with
    # a date stamp instead of a duplicate title.
    _, _, rest = body.partition("\n")
    return f"### Daily data-quality run — `{iso}`\n\n{rest}"


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------


def default_state_path(root: Path | None = None) -> Path:
    base = root or Path.cwd()
    return base / ".macro-data" / "data_quality_state.json"


def _load_state(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        logger.warning("data_quality_state.json unreadable at %s; starting fresh", path)
        return {}


def _save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, sort_keys=True, indent=2))


# ---------------------------------------------------------------------------
# GhRunner extensions
# ---------------------------------------------------------------------------


def _list_open_data_quality_issue(runner: GhRunner) -> int | None:
    """List the single open ``data-quality`` issue, regardless of whether
    other label filters were applied.

    ``GhRunner.list_open_issue(agency_id=...)`` is parity-specific (it
    appends ``parity-drift`` + ``agency:<id>``). We need a different
    label tuple, so we route through the same private ``_run`` so dry-
    run handling stays consistent.
    """
    out = runner._run(  # type: ignore[attr-defined]
        "issue", "list",
        "--label", DATA_QUALITY_LABEL,
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
    return max(int(entry["number"]) for entry in data if "number" in entry)


def _create_data_quality_issue(
    runner: GhRunner, *, title: str, body: str,
) -> int | None:
    out = runner._run(  # type: ignore[attr-defined]
        "issue", "create",
        "--title", title,
        "--body", body,
        *_label_args([DATA_QUALITY_LABEL]),
    )
    return _parse_issue_url(out)


# ---------------------------------------------------------------------------
# Action log
# ---------------------------------------------------------------------------


@dataclass
class DataQualityFilerAction:
    """Mirror of ``parity_filer.FilerActionLog`` for the data-quality path."""

    created: list[tuple[int, str]] = field(default_factory=list)  # (number, title)
    commented: list[int] = field(default_factory=list)
    closed: list[int] = field(default_factory=list)
    skipped_clean: bool = False


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


def file_data_quality_report(
    *,
    report: DataQualityReport,
    runner: GhRunner,
    state_path: Path | None = None,
) -> DataQualityFilerAction:
    """Reconcile ``report`` against the open ``data-quality`` issue.

    Behavior matches the parity filer's lifecycle invariants:

    * Findings + open issue → comment + reset clean streak.
    * Findings + no open issue → create + reset clean streak.
    * Clean run + open issue → bump streak; close on
      :data:`CLEAN_STREAK_TO_CLOSE`.
    * Clean run + no open issue → bump streak only.
    """
    iso = report.target_date.isoformat()
    state_path = state_path or default_state_path()
    state = _load_state(state_path)
    action = DataQualityFilerAction()

    live_issue = _list_open_data_quality_issue(runner)
    state.setdefault("clean_streak", 0)

    if not report.clean:
        state["clean_streak"] = 0
        state["last_failure_date"] = iso
        if live_issue is not None:
            runner.comment_issue(
                number=live_issue, body=_format_comment_body(report),
            )
            action.commented.append(live_issue)
            state["open_issue"] = live_issue
            _save_state(state_path, state)
            return action
        title = f"Macro data-quality — {iso}"
        body = _format_report_body(report)
        created = _create_data_quality_issue(runner, title=title, body=body)
        if created is not None:
            action.created.append((created, title))
            state["open_issue"] = created
        _save_state(state_path, state)
        return action

    # Clean run.
    streak = int(state.get("clean_streak", 0)) + 1
    state["clean_streak"] = streak
    state["last_clean_date"] = iso
    action.skipped_clean = True

    if live_issue is None:
        _save_state(state_path, state)
        return action
    if streak >= CLEAN_STREAK_TO_CLOSE:
        runner.close_issue(
            number=int(live_issue),
            comment=(
                f"Auto-closing: {streak} consecutive clean macro data-"
                f"quality runs through {iso}."
            ),
        )
        action.closed.append(int(live_issue))
        state["open_issue"] = None
        state["clean_streak"] = 0
    _save_state(state_path, state)
    return action


__all__ = [
    "CLEAN_STREAK_TO_CLOSE",
    "DATA_QUALITY_LABEL",
    "DEFAULT_REPRO_COMMANDS",
    "DataQualityFilerAction",
    "DataQualityFinding",
    "DataQualityReport",
    "coverage_drop_from_digest",
    "default_state_path",
    "file_data_quality_report",
    "findings_from_concept_reports",
    "secret_leak_from_text",
]

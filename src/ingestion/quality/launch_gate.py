"""Production data-quality launch gate.

The gate converts existing production-readiness signals into one JSON-safe
decision: green, degraded, or blocked. It also persists a global validation
report under the ``data_quality`` source so the public manifest can expose the
latest gate quality status.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import json
import logging
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable

from ingestion._shared.redaction import redact_secrets
from ingestion.calendar.parity_filer import GhRunner
from ingestion.quality.data_quality_filer import (
    DataQualityFinding,
    DataQualityFilerAction,
    DataQualityReport,
    coverage_drop_from_digest,
    default_state_path,
    file_data_quality_report,
    findings_from_concept_reports,
    secret_leak_from_text,
)
from ingestion.validation import ValidationEngine, ValidationStore
from ingestion.validation._types import (
    CheckResult,
    ValidationLayer,
    ValidationReport,
    ValidationSeverity,
)
from storage import SQLiteEngineStore, default_engine_db_path

logger = logging.getLogger(__name__)

LAUNCH_GATE_LOG_FILENAME = "launch_gate.log"
LAUNCH_GATE_QUALITY_SOURCE = "data_quality"
_SECRET_SCAN_LOGS = ("shadow.log", "daily_digest.jsonl", "data_quality.log")


@dataclass(frozen=True)
class LaunchGateCheck:
    name: str
    status: str
    severity: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status,
            "severity": self.severity,
            "message": redact_secrets(self.message),
            "details": _redact_value(self.details),
        }


@dataclass
class LaunchGateResult:
    status: str
    generated_at: str
    target_date: dt.date
    checks: list[LaunchGateCheck]
    findings: list[DataQualityFinding]
    concept_report_count: int
    concept_failed_count: int
    concept_zero_check_count: int
    digest_summary: dict[str, Any]
    manifest: dict[str, Any]
    filer_action: DataQualityFilerAction | None = None
    log_path: Path | None = None
    state_path: Path | None = None

    @property
    def blocked(self) -> bool:
        return self.status == "blocked"

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "status": self.status,
            "generated_at": self.generated_at,
            "target_date": self.target_date.isoformat(),
            "summary": {
                "checks": len(self.checks),
                "failed_checks": sum(1 for c in self.checks if c.status == "fail"),
                "warning_checks": sum(1 for c in self.checks if c.status == "warn"),
                "concept_reports": self.concept_report_count,
                "concept_failures": self.concept_failed_count,
                "zero_check_reports": self.concept_zero_check_count,
                "findings": len(self.findings),
            },
            "checks": [c.to_dict() for c in self.checks],
            "findings": _redact_value([f.to_dict() for f in self.findings]),
            "digest": _redact_value(self.digest_summary),
            "manifest": _redact_value(self.manifest),
        }
        if self.filer_action is not None:
            payload["filer"] = {
                "created": [number for number, _ in self.filer_action.created],
                "commented": list(self.filer_action.commented),
                "closed": list(self.filer_action.closed),
                "skipped_clean": self.filer_action.skipped_clean,
            }
        if self.log_path is not None:
            payload["log_path"] = str(self.log_path)
        if self.state_path is not None:
            payload["state_path"] = str(self.state_path)
        return payload


def default_launch_gate_log_path(engine_db: Path | None = None) -> Path:
    db_path = engine_db or default_engine_db_path()
    return db_path.parent / "logs" / LAUNCH_GATE_LOG_FILENAME


def default_launch_gate_state_path(engine_db: Path | None = None) -> Path:
    db_path = engine_db or default_engine_db_path()
    root = db_path.parent.parent if db_path.parent.name == ".macro-data" else db_path.parent
    return default_state_path(root)


def scan_file_tail_for_secrets(
    log_path: Path,
    *,
    source_label: str | None = None,
    tail_bytes: int = 256 * 1024,
) -> DataQualityFinding | None:
    if not log_path.is_file():
        return None
    try:
        with log_path.open("rb") as fh:
            fh.seek(0, 2)
            size = fh.tell()
            fh.seek(max(0, size - tail_bytes))
            sample = fh.read().decode("utf-8", errors="replace")
    except OSError:
        return None
    return secret_leak_from_text(sample, source_label=source_label or log_path.name)


def append_launch_gate_log(log_path: Path, payload: dict[str, Any]) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(_redact_value(payload), sort_keys=True) + "\n")


def run_launch_gate(
    *,
    engine_db: Path | None = None,
    target_date: dt.date | None = None,
    market_bars: dict[str, Any] | None = None,
    state_path: Path | None = None,
    log_path: Path | None = None,
    dry_run: bool = False,
    update_issue: bool = True,
    runner: GhRunner | None = None,
    digest_loader: Callable[[str], dict[str, Any]] | None = None,
    secret_log_names: Iterable[str] = _SECRET_SCAN_LOGS,
) -> LaunchGateResult:
    """Run the launch gate and return a JSON-serializable result."""

    started_at = dt.datetime.now(dt.timezone.utc)
    target = target_date or started_at.date()
    db_path = engine_db or default_engine_db_path()
    resolved_log_path = log_path or default_launch_gate_log_path(db_path)
    resolved_state_path = state_path or default_launch_gate_state_path(db_path)
    checks: list[LaunchGateCheck] = []
    findings: list[DataQualityFinding] = []
    digest_summary: dict[str, Any] = {}
    manifest: dict[str, Any] = {}
    reports: list[ValidationReport] = []

    store = SQLiteEngineStore(db_path=db_path)
    validation_store = ValidationStore(str(db_path))
    try:
        reports = _run_concept_validation(
            store=store,
            validation_store=validation_store,
            checks=checks,
            findings=findings,
        )
        digest_summary = _run_digest_check(
            db_path=db_path,
            digest_loader=digest_loader,
            checks=checks,
            findings=findings,
        )
        _run_secret_scan(
            log_dir=db_path.parent / "logs",
            log_names=tuple(secret_log_names),
            checks=checks,
            findings=findings,
        )
        manifest = _run_manifest_checks(
            store=store,
            market_bars=market_bars or {},
            checks=checks,
            findings=findings,
        )

        generated_at = dt.datetime.now(dt.timezone.utc).isoformat()
        result = _build_result(
            generated_at=generated_at,
            target_date=target,
            checks=checks,
            findings=findings,
            reports=reports,
            digest_summary=digest_summary,
            manifest=manifest,
            log_path=resolved_log_path,
            state_path=resolved_state_path,
        )

        try:
            _save_launch_gate_quality_report(validation_store, result)
            manifest = store.get_data_manifest(market_bars=market_bars or {})
            result.manifest = manifest
        except Exception as exc:
            msg = f"launch gate quality report write failed: {exc!r}"
            checks.append(
                LaunchGateCheck(
                    name="manifest_quality_status",
                    status="fail",
                    severity="error",
                    message=redact_secrets(msg),
                )
            )
            findings.append(
                DataQualityFinding(
                    kind="manifest_quality_status",
                    severity="error",
                    detail=redact_secrets(msg),
                )
            )
            result = _build_result(
                generated_at=generated_at,
                target_date=target,
                checks=checks,
                findings=findings,
                reports=reports,
                digest_summary=digest_summary,
                manifest=manifest,
                log_path=resolved_log_path,
                state_path=resolved_state_path,
            )

        if update_issue:
            data_quality_report = DataQualityReport(
                target_date=target,
                findings=list(findings),
                digest_summary=digest_summary,
                repro_commands=(
                    "PYTHONPATH=src macro-data-service launch-gate "
                    f"--db-path {db_path} --json --dry-run",
                ),
            )
            result.filer_action = file_data_quality_report(
                report=data_quality_report,
                runner=runner or GhRunner(dry_run=dry_run),
                state_path=resolved_state_path,
            )

        append_launch_gate_log(resolved_log_path, result.to_dict())
        return result
    finally:
        validation_store.close()


def _run_concept_validation(
    *,
    store: SQLiteEngineStore,
    validation_store: ValidationStore,
    checks: list[LaunchGateCheck],
    findings: list[DataQualityFinding],
) -> list[ValidationReport]:
    try:
        store.seed_concept_map()
        engine = ValidationEngine(validation_store)
        reports = engine.validate_all_concepts(store)
    except Exception as exc:
        msg = f"validate_all_concepts raised {exc!r}"
        checks.append(
            LaunchGateCheck(
                name="concept_validation",
                status="fail",
                severity="error",
                message=redact_secrets(msg),
            )
        )
        findings.append(
            DataQualityFinding(
                kind="concept_validation_error",
                severity="error",
                detail=redact_secrets(msg),
            )
        )
        return []

    if not reports:
        checks.append(
            LaunchGateCheck(
                name="concept_validation_reports",
                status="fail",
                severity="error",
                message="validate_all_concepts returned 0 reports",
            )
        )
        findings.append(
            DataQualityFinding(
                kind="zero_validation_reports",
                severity="error",
                detail=(
                    "validate_all_concepts returned 0 reports; "
                    "concept_map is empty or unseeded"
                ),
            )
        )
    else:
        checks.append(
            LaunchGateCheck(
                name="concept_validation_reports",
                status="pass",
                severity="info",
                message=f"validate_all_concepts returned {len(reports)} reports",
                details={"report_count": len(reports)},
            )
        )

    zero_check_reports = [r.source for r in reports if len(r.checks) == 0]
    if zero_check_reports:
        detail = ", ".join(zero_check_reports[:12])
        extra = len(zero_check_reports) - 12
        if extra > 0:
            detail += f", +{extra} more"
        checks.append(
            LaunchGateCheck(
                name="zero_check_validation",
                status="fail",
                severity="error",
                message=(
                    f"{len(zero_check_reports)} validation reports contained 0 checks"
                ),
                details={"sources": zero_check_reports},
            )
        )
        findings.append(
            DataQualityFinding(
                kind="zero_check_validation",
                severity="error",
                detail=f"validation reports with 0 checks: {detail}",
            )
        )
    else:
        checks.append(
            LaunchGateCheck(
                name="zero_check_validation",
                status="pass",
                severity="info",
                message="all validation reports contained checks",
            )
        )

    concept_findings = findings_from_concept_reports(reports)
    findings.extend(concept_findings)
    error_count = sum(1 for f in concept_findings if f.severity in ("error", "critical"))
    warning_count = sum(1 for f in concept_findings if f.severity == "warning")
    if error_count:
        checks.append(
            LaunchGateCheck(
                name="concept_hard_failures",
                status="fail",
                severity="error",
                message=f"{error_count} concept hard failures detected",
                details={"warning_findings": warning_count},
            )
        )
    elif warning_count:
        checks.append(
            LaunchGateCheck(
                name="concept_hard_failures",
                status="warn",
                severity="warning",
                message=f"{warning_count} concept warnings detected",
            )
        )
    else:
        checks.append(
            LaunchGateCheck(
                name="concept_hard_failures",
                status="pass",
                severity="info",
                message="concept validation has no hard failures",
            )
        )
    return reports


def _run_digest_check(
    *,
    db_path: Path,
    digest_loader: Callable[[str], dict[str, Any]] | None,
    checks: list[LaunchGateCheck],
    findings: list[DataQualityFinding],
) -> dict[str, Any]:
    try:
        if digest_loader is None:
            from ingestion.quality.shadow_digest import compute_digest

            digest_loader = compute_digest
        digest = digest_loader(str(db_path))
        digest = {"timestamp": dt.datetime.now(dt.timezone.utc).isoformat(), **digest}
    except Exception as exc:
        msg = f"shadow digest failed: {exc!r}"
        checks.append(
            LaunchGateCheck(
                name="shadow_digest",
                status="fail",
                severity="error",
                message=redact_secrets(msg),
            )
        )
        findings.append(
            DataQualityFinding(
                kind="shadow_digest_error",
                severity="error",
                detail=redact_secrets(msg),
            )
        )
        return {}

    drop = coverage_drop_from_digest(digest)
    if drop is not None:
        findings.append(drop)
        checks.append(
            LaunchGateCheck(
                name="shadow_digest_coverage",
                status="fail",
                severity="error",
                message=drop.detail,
                details={
                    "concepts_covered": digest.get("concepts_covered"),
                    "concepts_total": digest.get("concepts_total"),
                    "error_sources": digest.get("error_sources", []),
                },
            )
        )
    else:
        checks.append(
            LaunchGateCheck(
                name="shadow_digest_coverage",
                status="pass",
                severity="info",
                message="shadow digest coverage is complete",
                details={
                    "concepts_covered": digest.get("concepts_covered"),
                    "concepts_total": digest.get("concepts_total"),
                },
            )
        )
    return {
        k: digest.get(k)
        for k in (
            "timestamp",
            "cycle",
            "concepts_covered",
            "concepts_total",
            "coverage_pct",
            "confirmed_24h",
            "error_sources",
        )
        if digest.get(k) is not None
    }


def _run_secret_scan(
    *,
    log_dir: Path,
    log_names: tuple[str, ...],
    checks: list[LaunchGateCheck],
    findings: list[DataQualityFinding],
) -> None:
    leaks: list[DataQualityFinding] = []
    scanned = 0
    for log_name in log_names:
        log_path = log_dir / log_name
        if log_path.is_file():
            scanned += 1
        leak = scan_file_tail_for_secrets(log_path, source_label=log_name)
        if leak is not None:
            leaks.append(leak)
    findings.extend(leaks)
    if leaks:
        checks.append(
            LaunchGateCheck(
                name="secret_leak_scan",
                status="fail",
                severity="error",
                message=f"{len(leaks)} secret leak findings detected",
                details={"scanned_files": scanned, "log_names": list(log_names)},
            )
        )
    else:
        checks.append(
            LaunchGateCheck(
                name="secret_leak_scan",
                status="pass",
                severity="info",
                message="secret leak scan passed",
                details={"scanned_files": scanned, "log_names": list(log_names)},
            )
        )


def _run_manifest_checks(
    *,
    store: SQLiteEngineStore,
    market_bars: dict[str, Any],
    checks: list[LaunchGateCheck],
    findings: list[DataQualityFinding],
) -> dict[str, Any]:
    try:
        manifest = store.get_data_manifest(market_bars=market_bars)
    except Exception as exc:
        msg = f"manifest inventory failed: {exc!r}"
        checks.append(
            LaunchGateCheck(
                name="manifest_inventory",
                status="fail",
                severity="error",
                message=redact_secrets(msg),
            )
        )
        findings.append(
            DataQualityFinding(
                kind="manifest_inventory_error",
                severity="error",
                detail=redact_secrets(msg),
            )
        )
        return {}

    blocking: list[DataQualityFinding] = []
    for row in manifest.get("datasets", []):
        dataset = str(row.get("dataset") or "")
        launch_state = str(row.get("launch_state") or "")
        status = str(row.get("status") or "")
        row_count = int(row.get("row_count") or 0)

        if launch_state == "available" and row_count <= 0:
            blocking.append(
                DataQualityFinding(
                    kind="dataset_inventory_empty",
                    severity="error",
                    detail=(
                        f"{dataset}: launch_state=available has empty inventory"
                    ),
                )
            )
        if row_count <= 0 and status != "empty":
            blocking.append(
                DataQualityFinding(
                    kind="manifest_empty_contract",
                    severity="error",
                    detail=(
                        f"{dataset}: empty inventory is marked status={status!r}"
                    ),
                )
            )
        if row_count <= 0 and launch_state != "empty":
            blocking.append(
                DataQualityFinding(
                    kind="manifest_empty_contract",
                    severity="error",
                    detail=(
                        f"{dataset}: empty inventory is marked "
                        f"launch_state={launch_state!r}"
                    ),
                )
            )

    findings.extend(blocking)
    if blocking:
        checks.append(
            LaunchGateCheck(
                name="manifest_inventory",
                status="fail",
                severity="error",
                message=f"{len(blocking)} manifest inventory blockers detected",
            )
        )
    else:
        checks.append(
            LaunchGateCheck(
                name="manifest_inventory",
                status="pass",
                severity="info",
                message="manifest inventory contract passed",
            )
        )
    return manifest


def _build_result(
    *,
    generated_at: str,
    target_date: dt.date,
    checks: list[LaunchGateCheck],
    findings: list[DataQualityFinding],
    reports: list[ValidationReport],
    digest_summary: dict[str, Any],
    manifest: dict[str, Any],
    log_path: Path,
    state_path: Path,
) -> LaunchGateResult:
    status = _status_from_checks(checks)
    return LaunchGateResult(
        status=status,
        generated_at=generated_at,
        target_date=target_date,
        checks=list(checks),
        findings=list(findings),
        concept_report_count=len(reports),
        concept_failed_count=sum(1 for r in reports if not r.passed),
        concept_zero_check_count=sum(1 for r in reports if len(r.checks) == 0),
        digest_summary=dict(digest_summary),
        manifest=dict(manifest),
        log_path=log_path,
        state_path=state_path,
    )


def _status_from_checks(checks: list[LaunchGateCheck]) -> str:
    if any(c.status == "fail" for c in checks):
        return "blocked"
    if any(c.status == "warn" for c in checks):
        return "degraded"
    return "green"


def _save_launch_gate_quality_report(
    validation_store: ValidationStore,
    result: LaunchGateResult,
) -> None:
    checks = tuple(
        _check_result_from_gate_check(c, result.generated_at)
        for c in result.checks
    )
    report = ValidationReport(
        source=LAUNCH_GATE_QUALITY_SOURCE,
        run_id=f"launch-gate-{uuid.uuid4().hex[:12]}",
        timestamp=result.generated_at,
        checks=checks,
    )
    validation_store.save_report(report.to_dict())
    validation_store.save_check_results([dataclasses.asdict(c) for c in checks])


def _check_result_from_gate_check(
    check: LaunchGateCheck,
    timestamp: str,
) -> CheckResult:
    if check.status == "fail":
        severity = ValidationSeverity.ERROR
        passed = False
    elif check.status == "warn":
        severity = ValidationSeverity.WARNING
        passed = False
    else:
        severity = ValidationSeverity.INFO
        passed = True
    return CheckResult(
        check_name=check.name,
        layer=ValidationLayer.LAUNCH,
        passed=passed,
        severity=severity,
        message=redact_secrets(check.message),
        source=LAUNCH_GATE_QUALITY_SOURCE,
        timestamp=timestamp,
        details=_redact_value(check.details),
    )


def _redact_value(value: Any) -> Any:
    if isinstance(value, str):
        return redact_secrets(value)
    if isinstance(value, dict):
        return {str(k): _redact_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_redact_value(v) for v in value]
    if isinstance(value, tuple):
        return [_redact_value(v) for v in value]
    return value


__all__ = [
    "LAUNCH_GATE_LOG_FILENAME",
    "LAUNCH_GATE_QUALITY_SOURCE",
    "LaunchGateCheck",
    "LaunchGateResult",
    "append_launch_gate_log",
    "default_launch_gate_log_path",
    "default_launch_gate_state_path",
    "run_launch_gate",
    "scan_file_tail_for_secrets",
]

from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class ValidationSeverity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class ValidationLayer(str, Enum):
    SCHEMA = "schema"
    CATALOG = "catalog"
    SERIES = "series"
    ANOMALY = "anomaly"
    CROSS_SOURCE = "cross_source"
    DATA_DIFF = "data_diff"
    CONCEPT = "concept"


@dataclass(frozen=True)
class CheckResult:
    """Result of a single validation check."""

    check_name: str
    layer: ValidationLayer
    passed: bool
    severity: ValidationSeverity
    message: str
    source: str = ""
    series_id: str = ""
    details: dict[str, Any] = field(default_factory=dict)
    timestamp: str = ""


@dataclass(frozen=True)
class ValidationReport:
    """Aggregate report from one validation run."""

    source: str
    run_id: str
    timestamp: str
    checks: tuple[CheckResult, ...] = ()
    duration_ms: int = 0

    @property
    def passed(self) -> bool:
        return all(
            c.passed
            for c in self.checks
            if c.severity in (ValidationSeverity.ERROR, ValidationSeverity.CRITICAL)
        )

    @property
    def error_count(self) -> int:
        return sum(
            1
            for c in self.checks
            if not c.passed
            and c.severity in (ValidationSeverity.ERROR, ValidationSeverity.CRITICAL)
        )

    @property
    def warning_count(self) -> int:
        return sum(
            1
            for c in self.checks
            if not c.passed and c.severity == ValidationSeverity.WARNING
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "run_id": self.run_id,
            "timestamp": self.timestamp,
            "passed": self.passed,
            "error_count": self.error_count,
            "warning_count": self.warning_count,
            "total_checks": len(self.checks),
            "duration_ms": self.duration_ms,
            "checks": [dataclasses.asdict(c) for c in self.checks],
        }

    def format_text(self) -> str:
        lines: list[str] = []
        lines.append(f"Validation Report: {self.source}")
        lines.append(f"  Run ID:    {self.run_id}")
        lines.append(f"  Timestamp: {self.timestamp}")
        lines.append(f"  Status:    {'PASS' if self.passed else 'FAIL'}")
        lines.append(f"  Checks:    {len(self.checks)} total, {self.error_count} errors, {self.warning_count} warnings")
        lines.append(f"  Duration:  {self.duration_ms}ms")
        if self.checks:
            lines.append("")
            lines.append(f"  {'Check':<35} {'Layer':<14} {'Severity':<10} {'Status':<6} Message")
            lines.append(f"  {'─' * 35} {'─' * 14} {'─' * 10} {'─' * 6} {'─' * 40}")
            for c in self.checks:
                status = "PASS" if c.passed else "FAIL"
                lines.append(
                    f"  {c.check_name:<35} {c.layer.value:<14} {c.severity.value:<10} {status:<6} {c.message}"
                )
        return "\n".join(lines)


@dataclass(frozen=True)
class SchemaFingerprint:
    """Captures the structure of an API response for drift detection."""

    source: str
    endpoint: str
    field_names: tuple[str, ...]
    field_types: dict[str, str]
    sample_hash: str
    captured_at: str


@dataclass(frozen=True)
class SeriesProfile:
    """Statistical profile of a time series for anomaly baseline."""

    series_id: str
    source: str
    observation_count: int
    date_min: str
    date_max: str
    value_mean: float
    value_std: float
    value_min: float
    value_max: float
    missing_rate: float
    captured_at: str

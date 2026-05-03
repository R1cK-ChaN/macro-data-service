"""Data-quality auto-filing surface (issue #102)."""

from ingestion.quality.data_quality_filer import (
    DATA_QUALITY_LABEL,
    DataQualityFinding,
    DataQualityReport,
    coverage_drop_from_digest,
    file_data_quality_report,
    findings_from_concept_reports,
    secret_leak_from_text,
)
from ingestion.quality.launch_gate import (
    LAUNCH_GATE_QUALITY_SOURCE,
    LaunchGateResult,
    run_launch_gate,
)

__all__ = [
    "DATA_QUALITY_LABEL",
    "DataQualityFinding",
    "DataQualityReport",
    "LAUNCH_GATE_QUALITY_SOURCE",
    "LaunchGateResult",
    "coverage_drop_from_digest",
    "file_data_quality_report",
    "findings_from_concept_reports",
    "run_launch_gate",
    "secret_leak_from_text",
]

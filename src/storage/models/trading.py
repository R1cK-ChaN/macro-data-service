"""Storage records — trade signals, decision log, position/performance, trading artifacts.

Extracted out of src/storage/sqlite.py as part of issue #58 Tier 2.1A —
pure mechanical split, no behavior change. The records are re-exported by
storage.sqlite for backwards compatibility, so existing
``from storage.sqlite import XRecord`` consumers keep working.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class TradeSignalRecord:
    signal_id: int
    signal_type: str
    title: str
    summary: str
    rationale_markdown: str
    signal: dict[str, Any]
    confidence: float
    created_at: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DecisionLogRecord:
    decision_id: int
    decision_type: str
    title: str
    summary: str
    rationale_markdown: str
    research_artifact_id: int | None
    signal_id: int | None
    created_at: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PositionStateRecord:
    symbol: str
    exposure: float
    direction: str
    thesis: str
    updated_at: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PerformanceRecord:
    record_id: int
    metric_name: str
    metric_value: float
    period_label: str
    created_at: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TradingArtifactRecord:
    artifact_id: int
    artifact_type: str
    title: str
    summary: str
    rationale_markdown: str
    research_artifact_id: int
    decision_log_id: int | None
    signal: dict[str, Any]
    confidence: float
    created_at: str
    tags: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

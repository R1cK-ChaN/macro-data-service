"""Data models for macro RAG retrieval."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any


class MacroMode(str, enum.Enum):
    RESEARCH = "RESEARCH"
    BRIEFING = "BRIEFING"
    QA = "QA"
    REGIME = "REGIME"


@dataclass
class MacroCandidate:
    chunk_id: str
    text: str
    source_type: str  # news_article | calendar_event | central_bank_comm | ...
    source_id: str  # FK to SQLite
    section_path: str
    content_type: str  # article | speech | statement | minutes | data_release | ...
    country: str
    indicator_group: str
    impact_level: str
    data_source: str
    updated_at: str
    content_hash: str
    doc_id: str
    chunk_index: int
    chunk_total: int
    scores: dict[str, float | None] = field(default_factory=dict)


@dataclass(frozen=True)
class MacroEvidence:
    chunk_id: str
    text: str
    source_type: str
    source_id: str
    section_path: str
    content_type: str
    country: str
    indicator_group: str
    impact_level: str
    data_source: str
    updated_at: str
    scores: dict[str, float | None] = field(default_factory=dict)
    meta: dict[str, Any] | None = None


@dataclass
class MacroEvidenceBundle:
    news: list[MacroEvidence] = field(default_factory=list)
    fed_comms: list[MacroEvidence] = field(default_factory=list)
    indicators: list[MacroEvidence] = field(default_factory=list)
    events: list[MacroEvidence] = field(default_factory=list)
    research: list[MacroEvidence] = field(default_factory=list)

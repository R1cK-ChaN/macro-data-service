"""Content-type-aware chunking for macro data from SQLite."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List

from .text_utils import content_hash


@dataclass
class RawChunk:
    """Intermediate chunk before embedding."""

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
    content_hash_val: str
    doc_id: str
    chunk_index: int
    chunk_total: int


def _make_chunk_id(source_type: str, source_id: str, chunk_index: int) -> str:
    raw = f"{source_type}:{source_id}:{chunk_index}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def _split_paragraphs(text: str, max_chars: int = 2000) -> List[str]:
    """Split text into paragraph-group chunks respecting max_chars."""
    paragraphs = text.split("\n\n")
    chunks: list[str] = []
    current: list[str] = []
    current_len = 0
    for para in paragraphs:
        para = para.strip()
        if not para:
            continue
        if current_len + len(para) + 2 > max_chars and current:
            chunks.append("\n\n".join(current))
            current = []
            current_len = 0
        current.append(para)
        current_len += len(para) + 2
    if current:
        chunks.append("\n\n".join(current))
    return chunks or [text[:max_chars]] if text.strip() else []


def _split_by_headings(text: str, max_chars: int = 2000) -> List[str]:
    """Split markdown by heading sections."""
    import re

    sections: list[str] = []
    current_lines: list[str] = []
    for line in text.split("\n"):
        if re.match(r"^#{1,4}\s", line) and current_lines:
            sections.append("\n".join(current_lines))
            current_lines = []
        current_lines.append(line)
    if current_lines:
        sections.append("\n".join(current_lines))

    # Merge small sections, split large ones
    chunks: list[str] = []
    for section in sections:
        if len(section) <= max_chars:
            chunks.append(section)
        else:
            chunks.extend(_split_paragraphs(section, max_chars))
    return chunks or [text[:max_chars]] if text.strip() else []


def _infer_indicator_group(indicator: str, category: str) -> str:
    indicator_lower = (indicator or "").lower()
    category_lower = (category or "").lower()
    combined = f"{indicator_lower} {category_lower}"
    if any(k in combined for k in ("cpi", "pce", "inflation", "price")):
        return "inflation"
    if any(k in combined for k in ("nfp", "employment", "payroll", "jobless", "unemployment")):
        return "employment"
    if any(k in combined for k in ("gdp", "growth", "pmi", "ism")):
        return "growth"
    if any(k in combined for k in ("rate", "fomc", "fed fund", "interest", "fedprob", "effr", "obfr", "sofr")):
        return "rates"
    if any(k in combined for k in ("housing", "home", "construction")):
        return "housing"
    if any(k in combined for k in ("retail", "consumer", "spending", "sales")):
        return "consumer"
    if any(k in combined for k in ("trade", "export", "import", "current account")):
        return "trade"
    return category_lower or "other"


# ------------------------------------------------------------------
# Chunkers per source type
# ------------------------------------------------------------------


def chunk_news_article(row: Dict[str, Any]) -> List[RawChunk]:
    """Chunk a news article row from SQLite ``news_articles``."""
    url_hash = str(row.get("url_hash") or "")
    text = str(row.get("content_markdown") or row.get("description") or "")
    if not text.strip():
        return []

    title = str(row.get("title") or "")
    source_id = url_hash
    doc_id = f"news:{url_hash}"
    country = str(row.get("country") or "")
    impact = str(row.get("impact_level") or "medium")
    data_source = str(row.get("source_feed") or "")
    timestamp = row.get("timestamp")
    updated_at = (
        datetime.fromtimestamp(int(timestamp), tz=timezone.utc).isoformat()
        if timestamp
        else ""
    )
    category = str(row.get("finance_category") or "")
    section_path = f"{country} > {category}" if country and category else country or category

    parts = _split_paragraphs(text, max_chars=2000) if len(text) > 2000 else [text]
    chunks: list[RawChunk] = []
    for i, part in enumerate(parts):
        chunk_text = f"{title}\n\n{part}" if i == 0 and title else part
        chunks.append(
            RawChunk(
                chunk_id=_make_chunk_id("news_article", source_id, i),
                text=chunk_text,
                source_type="news_article",
                source_id=source_id,
                section_path=section_path,
                content_type="article",
                country=country,
                indicator_group=_infer_indicator_group("", category),
                impact_level=impact,
                data_source=data_source,
                updated_at=updated_at,
                content_hash_val=content_hash(chunk_text),
                doc_id=doc_id,
                chunk_index=i,
                chunk_total=len(parts),
            )
        )
    return chunks


def chunk_central_bank_comm(row: Dict[str, Any]) -> List[RawChunk]:
    """Chunk a central bank communication from SQLite ``central_bank_comms``."""
    url_hash = str(row.get("url_hash") or "")
    full_text = str(row.get("full_text") or "")
    summary = str(row.get("summary") or "")
    title = str(row.get("title") or "")
    source_id = url_hash
    doc_id = f"fed:{url_hash}"
    speaker = str(row.get("speaker") or "")
    ctype = str(row.get("content_type") or "speech")
    timestamp = row.get("timestamp")
    updated_at = (
        datetime.fromtimestamp(int(timestamp), tz=timezone.utc).isoformat()
        if timestamp
        else ""
    )
    data_source = str(row.get("source") or "fed_rss")
    country = "US"

    chunks: list[RawChunk] = []
    texts: list[str] = []

    # Summary as standalone chunk
    if summary.strip():
        summary_text = f"{title}\nSpeaker: {speaker}\nSummary: {summary}" if speaker else f"{title}\n{summary}"
        texts.append(summary_text)

    # Full text split
    if full_text.strip():
        parts = _split_paragraphs(full_text, max_chars=2000)
        texts.extend(parts)

    if not texts:
        return []

    for i, text in enumerate(texts):
        chunks.append(
            RawChunk(
                chunk_id=_make_chunk_id("central_bank_comm", source_id, i),
                text=text,
                source_type="central_bank_comm",
                source_id=source_id,
                section_path=f"US > monetary_policy > {ctype}",
                content_type=ctype,
                country=country,
                indicator_group="rates",
                impact_level="high",
                data_source=data_source,
                updated_at=updated_at,
                content_hash_val=content_hash(text),
                doc_id=doc_id,
                chunk_index=i,
                chunk_total=len(texts),
            )
        )
    return chunks


def chunk_calendar_event(row: Dict[str, Any]) -> List[RawChunk]:
    """Chunk a calendar event — single chunk per event."""
    source = str(row.get("source") or "")
    event_id = str(row.get("event_id") or "")
    source_id = f"{source}:{event_id}"
    doc_id = f"event:{source_id}"
    country = str(row.get("country") or "")
    indicator = str(row.get("indicator") or "")
    category = str(row.get("category") or "")
    importance = str(row.get("importance") or "medium")
    actual = row.get("actual") or "pending"
    forecast = row.get("forecast") or "N/A"
    previous = row.get("previous") or "N/A"
    timestamp = row.get("timestamp")
    updated_at = (
        datetime.fromtimestamp(int(timestamp), tz=timezone.utc).isoformat()
        if timestamp
        else ""
    )

    text = f"{country} {indicator}: actual {actual}, forecast {forecast}, previous {previous}"
    surprise = row.get("surprise")
    if surprise is not None:
        text += f", surprise {surprise}"

    return [
        RawChunk(
            chunk_id=_make_chunk_id("calendar_event", source_id, 0),
            text=text,
            source_type="calendar_event",
            source_id=source_id,
            section_path=f"{country} > {category} > {indicator}",
            content_type="data_release",
            country=country,
            indicator_group=_infer_indicator_group(indicator, category),
            impact_level=importance,
            data_source=source,
            updated_at=updated_at,
            content_hash_val=content_hash(text),
            doc_id=doc_id,
            chunk_index=0,
            chunk_total=1,
        )
    ]


def chunk_indicator_observations(
    series_id: str, rows: List[Dict[str, Any]]
) -> List[RawChunk]:
    """Chunk a group of indicator observations into a single chunk."""
    if not rows:
        return []
    source = str(rows[0].get("source") or "fred")
    doc_id = f"ind:{series_id}"

    lines: list[str] = [f"Indicator: {series_id}"]
    for r in rows:
        date_val = str(r.get("date") or "")
        value = r.get("value")
        lines.append(f"  {date_val}: {value}")

    text = "\n".join(lines)
    now_iso = datetime.now(timezone.utc).isoformat()

    return [
        RawChunk(
            chunk_id=_make_chunk_id("indicator", series_id, 0),
            text=text,
            source_type="indicator",
            source_id=series_id,
            section_path=f"indicators > {series_id}",
            content_type="data_release",
            country="US",
            indicator_group=_infer_indicator_group(series_id, ""),
            impact_level="medium",
            data_source=source,
            updated_at=now_iso,
            content_hash_val=content_hash(text),
            doc_id=doc_id,
            chunk_index=0,
            chunk_total=1,
        )
    ]


def chunk_research_artifact(row: Dict[str, Any]) -> List[RawChunk]:
    """Chunk a research artifact from SQLite ``research_artifacts``."""
    artifact_id = str(row.get("id") or row.get("artifact_id") or "")
    text = str(row.get("content_markdown") or "")
    if not text.strip():
        return []

    title = str(row.get("title") or "")
    source_id = artifact_id
    doc_id = f"research:{artifact_id}"
    artifact_type = str(row.get("artifact_type") or "research_note")
    summary = str(row.get("summary") or "")
    created_at = str(row.get("created_at") or "")
    tags_raw = row.get("tags")
    tags = tags_raw if isinstance(tags_raw, list) else []

    parts = _split_by_headings(text, max_chars=2000)
    chunks: list[RawChunk] = []
    for i, part in enumerate(parts):
        chunk_text = f"{title}\n\n{part}" if i == 0 and title else part
        chunks.append(
            RawChunk(
                chunk_id=_make_chunk_id("research_artifact", source_id, i),
                text=chunk_text,
                source_type="research_artifact",
                source_id=source_id,
                section_path=f"research > {artifact_type}",
                content_type="flash_commentary",
                country="",
                indicator_group="",
                impact_level="medium",
                data_source="analyst",
                updated_at=created_at,
                content_hash_val=content_hash(chunk_text),
                doc_id=doc_id,
                chunk_index=i,
                chunk_total=len(parts),
            )
        )
    return chunks

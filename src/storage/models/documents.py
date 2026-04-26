"""Storage records — document records + blobs + extras + source / release-family registry.

Extracted out of src/storage/sqlite.py as part of issue #58 Tier 2.1A —
pure mechanical split, no behavior change. The records are re-exported by
storage.sqlite for backwards compatibility, so existing
``from storage.sqlite import XRecord`` consumers keep working.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class DocSourceRecord:
    source_id: str
    source_code: str
    source_name: str
    source_type: str
    country_code: str
    default_language_code: str
    homepage_url: str
    is_active: bool
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class DocReleaseFamilyRecord:
    release_family_id: str
    source_id: str
    release_code: str
    release_name: str
    topic_code: str
    country_code: str
    frequency: str
    default_language_code: str
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class DocumentRecord:
    document_id: str
    release_family_id: str
    source_id: str
    canonical_url: str
    title: str
    subtitle: str
    document_type: str
    mime_type: str
    language_code: str
    country_code: str
    topic_code: str
    published_date: str
    published_at: str
    status: str
    version_no: int
    parent_document_id: str
    hash_sha256: str
    created_at: str
    updated_at: str
    published_precision: str = ""
    published_epoch_ms: int = 0
    created_epoch_ms: int = 0
    updated_epoch_ms: int = 0
    # 17-field LLM extraction surface (information-layer port).
    # All default blank/zero so gov_report / SDMX ingestion paths that
    # never populate them stay valid.
    institution: str = ""
    authors: str = ""
    data_period: str = ""
    market: str = ""
    asset_class: str = ""
    sector: str = ""
    event_type: str = ""
    impact_level: str = ""
    contains_commentary: bool = False
    confidence: float = 0.0
    subject_freetext: str = ""


@dataclass(frozen=True)
class DocumentBlobRecord:
    document_blob_id: str
    document_id: str
    blob_role: str
    storage_path: str
    content_text: str
    content_bytes: bytes | None
    byte_size: int
    encoding: str
    parser_name: str
    parser_version: str
    extracted_at: str


@dataclass(frozen=True)
class DocumentExtraRecord:
    document_id: str
    extra_json: dict[str, Any]

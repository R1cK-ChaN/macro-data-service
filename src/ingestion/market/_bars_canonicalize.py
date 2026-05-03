"""Canonicalization for EODHD bar payload content hashing."""

from __future__ import annotations

import hashlib
import json
from typing import Any


def canonicalize_bars_payload(payload: list[dict[str, Any]]) -> str:
    """Sort by ``date`` and emit sorted-key JSON for hashing.

    EODHD ships arrays keyed by ``date``. Sorting by date plus
    ``sort_keys=True`` gives stable hashes across identical payloads.
    """
    if not isinstance(payload, list):
        return json.dumps(payload, sort_keys=True, ensure_ascii=False)
    cleaned = [row for row in payload if isinstance(row, dict)]
    cleaned.sort(key=lambda r: str(r.get("date") or ""))
    return json.dumps(cleaned, sort_keys=True, ensure_ascii=False)


def bars_content_hash(payload: list[dict[str, Any]]) -> str:
    """sha256 of the canonical JSON string."""
    canonical = canonicalize_bars_payload(payload)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

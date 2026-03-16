"""URL canonicalization and content fingerprinting for news deduplication."""

from __future__ import annotations

import hashlib
import re
from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit

TRACKING_PARAMS = frozenset({
    "utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term",
    "fbclid", "gclid", "msclkid", "ref", "source", "src",
    "ncid", "ocid", "mod", "smid", "smtyp",
    "si", "s", "_ga", "_gl", "mc_cid", "mc_eid",
})


def canonicalize_url(raw_url: str) -> str:
    """Normalize a URL by lowercasing scheme/host, stripping tracking params
    and fragments, sorting remaining query params, and removing trailing slash."""
    parts = urlsplit(raw_url)
    scheme = parts.scheme.lower()
    netloc = parts.netloc.lower()
    path = parts.path

    # Strip fragment
    # Parse query, drop tracking params, sort remaining
    qs = parse_qs(parts.query, keep_blank_values=True)
    cleaned = {k: v for k, v in qs.items() if k.lower() not in TRACKING_PARAMS}
    # Sort keys and values for deterministic output
    sorted_query = urlencode(
        sorted((k, v) for k, vs in sorted(cleaned.items()) for v in vs)
    )

    # Strip trailing slash (unless path is just "/")
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")

    return urlunsplit((scheme, netloc, path, sorted_query, ""))


def normalize_title(title: str) -> str:
    """Lowercase, strip punctuation, collapse whitespace."""
    t = title.lower()
    t = re.sub(r"[^\w\s]", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def content_hash(title: str, timestamp: int) -> str:
    """SHA-256 of normalized title + hour-floored timestamp."""
    norm = normalize_title(title)
    payload = f"{norm}|{timestamp // 3600}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

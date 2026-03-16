"""Shared parsing helpers used by all SDMX providers."""

from __future__ import annotations

import re

_QUARTER_MAP = {"Q1": "01", "Q2": "04", "Q3": "07", "Q4": "10"}


def normalize_date(raw: str) -> str:
    """Normalize SDMX date strings to YYYY-MM-DD.

    Handles common formats across providers:
        ``"2024-01"``   → ``"2024-01-01"``
        ``"2024-Q1"``   → ``"2024-01-01"``
        ``"2024"``      → ``"2024-01-01"``
        ``"2024-01-23"`` → passthrough
    """
    m = re.match(r"^(\d{4})-Q(\d)$", raw)
    if m:
        return f"{m.group(1)}-{_QUARTER_MAP.get('Q' + m.group(2), '01')}-01"
    if re.match(r"^\d{4}-\d{2}$", raw):
        return f"{raw}-01"
    if re.match(r"^\d{4}$", raw):
        return f"{raw}-01-01"
    return raw


def build_decade_chunks(start_year: int, end_year: int) -> list[tuple[str, str]]:
    """Split a year range into decade-sized chunks for time-range queries."""
    chunks: list[tuple[str, str]] = []
    year = start_year
    while year <= end_year:
        chunk_end = min(year + 9, end_year)
        chunks.append((str(year), str(chunk_end)))
        year = chunk_end + 1
    return chunks


def extract_id_from_urn(urn: str) -> str:
    """Extract the artefact ID from an SDMX URN.

    Example: ``"urn:sdmx:...Codelist=BIS:CL_FREQ(1.0)"`` → ``"CL_FREQ"``
    """
    if "=" not in urn:
        return ""
    after_eq = urn.rsplit("=", 1)[-1]
    if ":" in after_eq:
        after_eq = after_eq.split(":", 1)[-1]
    if "(" in after_eq:
        return after_eq.split("(")[0]
    return after_eq

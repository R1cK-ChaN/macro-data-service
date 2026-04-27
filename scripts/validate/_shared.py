"""Shared types for the calendar acquisition validator.

Source-specific planners and runners (TE, EODHD, BLS, BEA, ...) live
in sibling modules under ``scripts/validate/``; this module holds only
the structurally cross-cutting dataclasses they all instantiate.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import Any


@dataclass
class Probe:
    """One planned upstream request.

    ``expected_fields`` differs per probe (TE pointer shape has fewer
    fields than full; each EODHD subtype has its own read set).
    ``params`` + ``rows_key`` + ``subtype`` are EODHD-only; TE probes
    embed everything in ``path`` and ignore these.
    """

    name: str
    path: str
    description: str
    expected_shape: str  # human-readable
    expected_fields: frozenset[str] = frozenset()
    # EODHD-only knobs.
    params: dict[str, Any] = field(default_factory=dict)
    rows_key: str = ""
    subtype: str = ""
    # True for EODHD endpoints whose payload is itself the row list
    # (``/api/div/{TICKER}``); False for envelope-style calendar endpoints
    # that wrap rows under a subtype-specific key.
    top_level_array: bool = False


@dataclass
class RowDiff:
    """Field-level audit of one observed row vs parser expectations."""

    observed_fields: list[str] = field(default_factory=list)
    read_by_parser: list[str] = field(default_factory=list)
    ignored_by_parser: list[str] = field(default_factory=list)
    unknown_observed: list[str] = field(default_factory=list)
    missing_expected: list[str] = field(default_factory=list)
    type_warnings: list[str] = field(default_factory=list)


@dataclass
class ProbeResult:
    probe: Probe
    status: str  # "skipped" | "ok" | "http_error" | "auth_missing"
    request_path: str = ""
    http_elapsed_ms: float = 0.0
    row_count: int = 0
    truncated: bool = False
    sample_row: dict[str, Any] | None = None
    # First N CalendarIds captured from this probe's rows — feeds the
    # calendarid_rehydrate probe so we hit real ids that were just
    # returned by /country/All.
    dynamic_ids_sample: list[str] = field(default_factory=list)
    field_diff: RowDiff | None = None
    parse_attempts: int = 0
    parse_successes: int = 0
    parse_error_samples: list[str] = field(default_factory=list)
    enum_counters: dict[str, Counter] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)

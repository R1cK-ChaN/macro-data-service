"""Dataclasses, value-parsing helpers, and provider constant for the HCOB connector.

Issue #15 P5 shipped the schedule-side. Issue #23 fills in value-side
parsing — the press-release listing at
``/Public/Release/PressReleases?language=en`` returns a public list of
GUID-keyed PDF URLs and ``pypdf`` extracts clean text from each.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    synthesize_event_id,
)

from .indicators import INDICATOR_REGISTRY, HCOBIndicatorSpec

PROVIDER = "hcob"


@dataclass(frozen=True)
class HCOBCalendarRawRecord:
    """One row destined for ``cal_econ_raw``."""

    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class HCOBCalendarEventRecord:
    """One row destined for ``cal_econ_event``."""

    provider: str
    provider_event_id: str
    event_time_utc: str
    event_time_precision: str
    reference_date: str | None
    reference_label: str
    country_code: str
    indicator_id: str | None
    category: str
    title: str
    importance: str | None
    currency: str
    unit: str
    actual: str | None
    previous: str | None
    revised: str | None
    forecast: str | None
    consensus_forecast: str | None
    ticker: str
    source: str
    source_url: str
    content_hash: str
    last_update_epoch_ms: int | None
    observed_at_epoch_ms: int


@dataclass(frozen=True)
class HCOBValueObservation:
    """One headline value parsed from an HCOB / S&P Global press release."""

    series_id: str
    reference_date: str
    reference_label: str
    value: str
    event_time_utc: str
    event_time_precision: str
    source_url: str
    release_title: str
    raw: dict[str, Any]
    observed_at_epoch_ms: int


class HCOBPressReleaseParseError(ValueError):
    """Raised when an HCOB / S&P Global press-release PDF cannot be projected."""


_NUMBER_RE = r"(\d{1,3}(?:[.,]\d+))"

# Per-series patterns extracting the headline index from the PDF text.
# All five releases publish the same numeric form ("XX.X"); the prefix
# phrase disambiguates which series the value belongs to. The flash
# trio's PDF carries all three lines, so each series matches its own
# anchor in a single document.
_VALUE_PATTERNS: dict[str, tuple[re.Pattern[str], ...]] = {
    "HCOB_FLASH_MANUFACTURING_PMI": (
        # "Flash Germany Manufacturing PMI: 51.2 (Mar: 52.2)."
        re.compile(rf"Flash\s+Germany\s+Manufacturing\s+PMI\s*:\s*{_NUMBER_RE}", re.I | re.S),
    ),
    "HCOB_FLASH_SERVICES_PMI": (
        # "Flash Germany Services PMI Business Activity Index: 46.9 (Mar: 50.9)."
        re.compile(
            rf"Flash\s+Germany\s+Services\s+PMI\s+Business\s+Activity\s*Index\s*:\s*{_NUMBER_RE}",
            re.I | re.S,
        ),
    ),
    "HCOB_FLASH_COMPOSITE_PMI": (
        # "Flash Germany PMI Composite Output Index: 48.3 (Mar: 51.9)."
        re.compile(
            rf"Flash\s+Germany\s+PMI\s+Composite\s+Output\s*Index\s*:\s*{_NUMBER_RE}",
            re.I | re.S,
        ),
    ),
    "HCOB_MANUFACTURING_PMI": (
        # "headline ... Germany Manufacturing PMI® ... registered 52.2 in March"
        re.compile(
            rf"headline\s+S&P\s+Global\s+Germany\s+Manufacturing\s+PMI\W[^.]*?registered\s+{_NUMBER_RE}\s+in\s+\w+",
            re.I | re.S,
        ),
        # Fallback: "Germany Manufacturing PMI ... 52.2 (Feb: 50.9)" header line.
        re.compile(
            rf"Germany\s+Manufacturing\s+PMI[^A-Za-z0-9]{{0,40}}{_NUMBER_RE}\s*\(",
            re.I | re.S,
        ),
    ),
    "HCOB_SERVICES_PMI": (
        # "headline Business Activity Index came in at 50.9 in March".
        # The 2026-04-07 PDF actually reads "came it at" — clearly a
        # typo in the upstream copy editor's text. Tolerate any short
        # filler-word run between "came" and "at" so a future "came up
        # at" / "came out at" rephrasing keeps matching.
        re.compile(
            rf"headline\s+Business\s+Activity\s+Index\s+came\s+\w{{1,4}}\s+at\s+{_NUMBER_RE}\s+in\s+\w+",
            re.I | re.S,
        ),
        # Fallback header line: "Germany Services PMI ... 50.9 (Feb: 53.5)".
        re.compile(
            rf"Germany\s+Services\s+PMI[^A-Za-z0-9]{{0,40}}{_NUMBER_RE}\s*\(",
            re.I | re.S,
        ),
    ),
}


def _value_text(raw: str) -> str:
    cleaned = str(raw or "").strip().replace(" ", "").replace(",", ".").replace("+", "")
    try:
        Decimal(cleaned)
    except InvalidOperation as exc:
        raise HCOBPressReleaseParseError(f"invalid HCOB value: {raw!r}") from exc
    return cleaned


def _normalise_pdf_text(text: str) -> str:
    """Collapse the soft-wrapped PDF text so multi-line patterns match."""
    return re.sub(r"\s+", " ", text.replace("\xa0", " ")).strip()


def extract_press_release_value(text: str, spec: HCOBIndicatorSpec) -> str:
    """Pick the headline index out of the PDF body text for ``spec``."""
    patterns = _VALUE_PATTERNS.get(spec.series_id)
    if not patterns:
        raise KeyError(f"no value pattern registered for {spec.series_id!r}")
    haystack = _normalise_pdf_text(text)
    for pattern in patterns:
        match = pattern.search(haystack)
        if match:
            return _value_text(match.group(1))
    raise HCOBPressReleaseParseError(
        f"headline value not found for {spec.series_id}"
    )


def parse_press_release_pdf(
    text: str,
    *,
    spec: HCOBIndicatorSpec,
    reference_date: str,
    reference_label: str,
    event_time_utc: str,
    event_time_precision: str = "datetime",
    source_url: str = "",
) -> HCOBValueObservation:
    """Extract one HCOB / S&P Global headline value from extracted PDF text."""
    value = extract_press_release_value(text, spec)
    observed_at_epoch_ms = int(
        datetime.fromisoformat(event_time_utc).timestamp() * 1000
    )
    body = _normalise_pdf_text(text)
    return HCOBValueObservation(
        series_id=spec.series_id,
        reference_date=reference_date,
        reference_label=reference_label,
        value=value,
        event_time_utc=event_time_utc,
        event_time_precision=event_time_precision,
        source_url=source_url or spec.source_url,
        release_title=spec.title,
        raw={"text": body[:4000]},
        observed_at_epoch_ms=observed_at_epoch_ms,
    )


_HASH_FIELDS: tuple[str, ...] = ("value", "reference_date", "series_id", "source_url")


def _content_hash(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    for field_name in _HASH_FIELDS:
        value = payload.get(field_name)
        parts.append("" if value is None else str(value))
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def parse_observation(
    obs: HCOBValueObservation,
    *,
    snapshot_epoch_ms: int,
    observed_at_epoch_ms: int | None = None,
    spec: HCOBIndicatorSpec | None = None,
) -> tuple[HCOBCalendarRawRecord, HCOBCalendarEventRecord]:
    """Convert one HCOB observation into raw + PIT event records.

    The synthesized ``provider_event_id`` matches the one the
    schedule-side projection produced for the same
    ``(country, canonical(indicator), reference_date)`` tuple, so the
    upsert lands on the existing row instead of inserting a duplicate.
    """
    resolved_spec = spec or INDICATOR_REGISTRY.get(obs.series_id)
    if resolved_spec is None:
        raise KeyError(f"series_id {obs.series_id!r} not in HCOB INDICATOR_REGISTRY")

    payload: dict[str, Any] = {
        "series_id": obs.series_id,
        "reference_date": obs.reference_date,
        "reference_label": obs.reference_label,
        "value": obs.value,
        "source_url": obs.source_url,
        "release_title": obs.release_title,
        "raw": obs.raw,
    }
    content_hash = _content_hash(payload)
    payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    provider_event_id = synthesize_event_id(
        PROVIDER,
        resolved_spec.country_code,
        canonicalize_indicator(resolved_spec.indicator),
        obs.reference_date,
    )
    observed = (
        observed_at_epoch_ms
        if observed_at_epoch_ms is not None
        else snapshot_epoch_ms
    )
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()

    raw_record = HCOBCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = HCOBCalendarEventRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        event_time_utc=obs.event_time_utc,
        event_time_precision=obs.event_time_precision,
        reference_date=obs.reference_date,
        reference_label=obs.reference_label,
        country_code=resolved_spec.country_code,
        indicator_id=None,
        category=resolved_spec.category,
        title=resolved_spec.title,
        importance=resolved_spec.importance,
        currency="",
        unit=resolved_spec.unit,
        actual=obs.value,
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="HCOB / S&P Global",
        source_url=obs.source_url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=observed,
    )
    return raw_record, event_record

"""Headline-value extraction from the DOL UI Weekly Claims PDF.

Each weekly press release (issue #50) carries a table at the
bottom titled ``UNEMPLOYMENT INSURANCE DATA FOR REGULAR STATE
PROGRAMS`` with rows like:

    Initial Claims (SA)        214,000   208,000   +6,000  218,000  224,000
    Insured Unemployment (SA)  1,821,000 1,809,000 +12,000 1,798,000 1,795,000

The headline figure is the first numeric column on each line —
the advance figure for the most recent week. Subsequent columns
are the prior-week revised figure, the change, the prior-prior
revised, and the year-ago comparison.

The parser also captures the narrative phrasings as a fallback:

    "advance figure for seasonally adjusted initial claims was 214,000"
    "advance number for seasonally adjusted insured unemployment …
     was 1,821,000"
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    parse_scheduled_release_time,
    synthesize_event_id,
)

from .indicators import DOLIndicatorSpec, INDICATOR_REGISTRY

PROVIDER = "dol"
DOL_RELEASE_TZ = "America/New_York"
DOL_RELEASE_TIME = "8:30 AM ET"


class DOLPressReleaseParseError(ValueError):
    """Raised when the DOL UI Claims PDF doesn't expose a headline value."""


@dataclass(frozen=True)
class DOLCalendarRawRecord:
    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class DOLCalendarEventRecord:
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
class DOLValueObservation:
    """One headline DOL UI Claims value extracted from a release PDF."""

    indicator: str
    release_date: str        # ISO date of the Thursday release
    reference_date: str      # ISO date of the week-ending Saturday
    reference_label: str     # human-readable ("week ending April 18")
    value: str
    source_url: str
    raw: dict[str, Any]


# Number tokens are either comma-grouped (``"214,000"`` /
# ``"1,821,000"`` — the canonical PDF table shape) or plain digits
# (``"214000"`` — defensively, in case PDF text-extraction drops
# the commas). The whole-token alternation prevents the comma-
# grouped branch from matching only the leading 3 digits of an
# uncommaed count.
_NUM = r"(?:\d{1,3}(?:,\d{3})+|\d{4,})"


# Per-indicator extraction strategy. The PDF table is the most
# stable source — fall back to the narrative if the table parse
# misses (PDF text-extraction can drop spaces between cells).
_TABLE_PATTERNS: dict[str, tuple[re.Pattern[str], ...]] = {
    "INITIAL_CLAIMS": (
        # Table line: "Initial Claims (SA) 214,000 208,000 …"
        re.compile(
            rf"Initial\s+Claims\s*\(\s*SA\s*\)\s+(?P<val>{_NUM})",
            re.IGNORECASE | re.DOTALL,
        ),
        # Narrative fallback.
        re.compile(
            rf"advance\s+figure\s+for\s+seasonally\s+adjusted\s+"
            rf"initial\s+claims\s+was\s+(?P<val>{_NUM})",
            re.IGNORECASE | re.DOTALL,
        ),
    ),
    "CONTINUING_CLAIMS": (
        # Table line: "Insured Unemployment (SA) 1,821,000 …"
        re.compile(
            rf"Insured\s+Unemployment\s*\(\s*SA\s*\)\s+(?P<val>{_NUM})",
            re.IGNORECASE | re.DOTALL,
        ),
        # Narrative fallback — the SA continuing line.
        re.compile(
            rf"advance\s+number\s+for\s+seasonally\s+adjusted\s+"
            rf"insured\s+unemployment[^.]{{0,200}}?was\s+(?P<val>{_NUM})",
            re.IGNORECASE | re.DOTALL,
        ),
    ),
}


def _normalise_pdf_text(text: str) -> str:
    """Collapse whitespace; preserve line structure for the table parser."""
    return re.sub(r"[ \t]+", " ", text.replace("\xa0", " ")).strip()


def extract_press_release_value(
    text: str, spec: DOLIndicatorSpec,
) -> str:
    """Pick the headline ``actual`` out of the press-release PDF text."""
    patterns = _TABLE_PATTERNS.get(spec.indicator)
    if not patterns:
        raise KeyError(
            f"no DOL value pattern registered for {spec.indicator!r}"
        )
    haystack = _normalise_pdf_text(text)
    for pattern in patterns:
        m = pattern.search(haystack)
        if not m:
            continue
        cleaned = m.group("val").replace(",", "")
        try:
            Decimal(cleaned)
        except InvalidOperation as exc:
            raise DOLPressReleaseParseError(
                f"DOL {spec.indicator}: unparseable {m.group('val')!r}",
            ) from exc
        return cleaned
    raise DOLPressReleaseParseError(
        f"DOL {spec.indicator}: no headline value pattern matched the "
        f"press-release PDF — upstream layout drift"
    )


def parse_press_release_pdf(
    text: str,
    *,
    spec: DOLIndicatorSpec,
    release_date: date,
    source_url: str = "",
) -> DOLValueObservation:
    """Build a :class:`DOLValueObservation` from extracted PDF text."""
    value = extract_press_release_value(text, spec)
    reference_date = release_date - _days(spec.reference_days_back)
    reference_label = f"week ending {reference_date.strftime('%B %-d, %Y')}"
    return DOLValueObservation(
        indicator=spec.indicator,
        release_date=release_date.isoformat(),
        reference_date=reference_date.isoformat(),
        reference_label=reference_label,
        value=value,
        source_url=source_url,
        raw={"text": _normalise_pdf_text(text)[:4000]},
    )


def _days(n: int):
    from datetime import timedelta
    return timedelta(days=n)


_HASH_FIELDS: tuple[str, ...] = (
    "indicator", "reference_date", "value", "release_date",
)


def _content_hash(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    for field_name in _HASH_FIELDS:
        v = payload.get(field_name)
        parts.append("" if v is None else str(v))
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def value_observation_to_records(
    obs: DOLValueObservation,
    *,
    snapshot_epoch_ms: int,
    spec: DOLIndicatorSpec | None = None,
) -> tuple[DOLCalendarRawRecord, DOLCalendarEventRecord]:
    """Project one observation onto (raw, event) records.

    ``provider_event_id`` anchors on ``(provider, country, canonical,
    reference_date)`` so a later schedule scrape lands on the same
    row. Reference date is the week-ending Saturday — TE buckets the
    same way, so this is what makes the parity comparator match.
    """
    resolved_spec = spec or INDICATOR_REGISTRY.get(obs.indicator)
    if resolved_spec is None:
        raise KeyError(
            f"indicator {obs.indicator!r} not in DOL INDICATOR_REGISTRY"
        )

    indicator_canonical = canonicalize_indicator(resolved_spec.indicator)
    provider_event_id = synthesize_event_id(
        PROVIDER,
        resolved_spec.country_code,
        indicator_canonical,
        obs.reference_date,
    )

    release_date = date.fromisoformat(obs.release_date)
    scheduled = parse_scheduled_release_time(
        release_date, DOL_RELEASE_TIME, default_tz=DOL_RELEASE_TZ,
    )
    event_time_utc = scheduled.utc.isoformat()

    payload: dict[str, Any] = {
        "kind":            "dol_ui_claims_value",
        "indicator":       resolved_spec.indicator,
        "release_date":    obs.release_date,
        "reference_date":  obs.reference_date,
        "reference_label": obs.reference_label,
        "value":           obs.value,
        "source_url":      obs.source_url,
        "raw":             obs.raw,
    }
    content_hash = _content_hash(payload)
    payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()

    raw_record = DOLCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = DOLCalendarEventRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        event_time_utc=event_time_utc,
        event_time_precision="datetime",
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
        source="US Department of Labor — Employment and Training Administration",
        source_url=obs.source_url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=snapshot_epoch_ms,
    )
    return raw_record, event_record


__all__ = [
    "DOLCalendarEventRecord",
    "DOLCalendarRawRecord",
    "DOLPressReleaseParseError",
    "DOLValueObservation",
    "DOL_RELEASE_TIME",
    "DOL_RELEASE_TZ",
    "PROVIDER",
    "extract_press_release_value",
    "parse_press_release_pdf",
    "value_observation_to_records",
]

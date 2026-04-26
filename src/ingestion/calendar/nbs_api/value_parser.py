"""Headline-value extraction from NBS English press-release HTML.

Issue #49 — value side. Each NBS press-release article carries the
headline figure inside the opening narrative paragraph. The exact
phrasing is indicator-specific but stable across releases:

- CPI: ``"China's Consumer Price Index (CPI) increased by X.X% year on year"``
- PPI: ``"China's producer price index for industrial products (PPI)
   turned from a Y.Y% year-on-year decline … to a X.X% increase"`` —
   plus simpler variants in months without a sign flip.
- Industrial Production: ``"the total value added of industrial
   enterprises above the designated size increased by X.X% year on year"``
- Fixed Asset Investment: ``"the national investment in fixed assets …
   was N billion yuan, a year-on-year increase of X.X%"`` (YTD).
- Retail Sales: ``"the total retail sales of consumer goods reached
   N billion yuan, up by X.X% year on year"``

Each indicator carries a tuple of regex patterns tried in order.
The first match wins. The captured magnitude is signed by the
direction word (``increased`` / ``decreased`` / ``decline``).
A loud :class:`NBSValueParseError` fires when no pattern matches —
better than silently writing a no-op row.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    synthesize_event_id,
)

from .indicators import INDICATOR_REGISTRY, NBSIndicatorSpec
from .parser import (
    NBSCalendarEventRecord,
    NBSCalendarRawRecord,
    PROVIDER,
)


class NBSValueParseError(ValueError):
    """Raised when the press-release HTML cannot be projected to a value."""


@dataclass(frozen=True)
class NBSValueObservation:
    """One headline value parsed from an NBS press-release article."""

    indicator: str
    reference_date: str
    reference_label: str
    value: str
    event_time_utc: str
    event_time_precision: str
    source_url: str
    release_title: str
    raw: dict[str, Any]
    observed_at_epoch_ms: int


@dataclass(frozen=True)
class _ValuePattern:
    """One attempt at extracting the headline value.

    ``regex`` must yield two named groups: ``dir`` (direction word —
    ``increased`` / ``decreased`` / ``increase`` / ``decrease`` /
    ``decline``) and ``val`` (unsigned magnitude). A pattern matching
    only the magnitude (no direction) sets ``signed=False``; the
    parser then trusts the sign baked into the source text.
    """

    regex: re.Pattern[str]
    signed: bool = True


_NUM = r"-?\d{1,3}(?:\.\d{1,3})?"


# Per-indicator patterns. Order matters — first match wins. Add
# fallbacks for known phrasing variants (PPI's "turned from … to a"
# vs. plain "increased by …"). When a regex stops matching upstream,
# the parser raises rather than silently writing a no-op.
_VALUE_PATTERNS: dict[str, tuple[_ValuePattern, ...]] = {
    "CPI": (
        # "China's Consumer Price Index (CPI) increased by 1.0% year on year."
        _ValuePattern(re.compile(
            r"Consumer\s+Price\s+Index\s*(?:\([^)]*\))?\s+"
            r"(?P<dir>increased|decreased)\s+by\s+"
            rf"(?P<val>{_NUM})\s*%\s*year[\s-]on[\s-]year",
            re.IGNORECASE,
        )),
        # Fallback: "the CPI rose/fell by X.X% year on year"
        _ValuePattern(re.compile(
            r"\bCPI\b\s+(?P<dir>rose|fell|increased|decreased)\s+by\s+"
            rf"(?P<val>{_NUM})\s*%\s*year[\s-]on[\s-]year",
            re.IGNORECASE,
        )),
    ),
    "PPI": (
        # "turned from a 0.9% year-on-year decline in the previous month
        #  to a 0.5% increase"
        _ValuePattern(re.compile(
            r"\bPPI\b.{0,300}?to\s+a\s+"
            rf"(?P<val>{_NUM})\s*%\s+(?P<dir>increase|decline|decrease)",
            re.IGNORECASE | re.DOTALL,
        )),
        # Plain "the PPI increased/decreased by X.X% year on year"
        _ValuePattern(re.compile(
            r"\bPPI\b.{0,300}?(?P<dir>increased|decreased|rose|fell)\s+by\s+"
            rf"(?P<val>{_NUM})\s*%\s*year[\s-]on[\s-]year",
            re.IGNORECASE | re.DOTALL,
        )),
    ),
    "INDUSTRIAL_PRODUCTION": (
        # "the total value added of industrial enterprises above the
        #  designated size increased by 5.7% year on year"
        _ValuePattern(re.compile(
            r"value\s+added\s+of\s+industrial\s+enterprises.{0,300}?"
            r"(?P<dir>increased|decreased|rose|fell)\s+by\s+"
            rf"(?P<val>{_NUM})\s*%\s*year[\s-]on[\s-]year",
            re.IGNORECASE | re.DOTALL,
        )),
    ),
    "FIXED_ASSET_INVESTMENT": (
        # "the national investment in fixed assets (excluding rural
        #  households) was 10,270.8 billion yuan, a year-on-year
        #  increase of 1.7%"
        _ValuePattern(re.compile(
            r"investment\s+in\s+fixed\s+assets.{0,400}?"
            r"year[\s-]on[\s-]year\s+(?P<dir>increase|decrease|decline)\s+of\s+"
            rf"(?P<val>{_NUM})\s*%",
            re.IGNORECASE | re.DOTALL,
        )),
    ),
    "RETAIL_SALES": (
        # "the total retail sales of consumer goods reached 4,161.6
        #  billion yuan, up by 1.7% year on year"
        _ValuePattern(re.compile(
            r"total\s+retail\s+sales\s+of\s+consumer\s+goods.{0,400}?"
            r"(?P<dir>up|down|increased|decreased|rose|fell)\s+by\s+"
            rf"(?P<val>{_NUM})\s*%\s*year[\s-]on[\s-]year",
            re.IGNORECASE | re.DOTALL,
        )),
    ),
}

# Words → sign multiplier. Anything not in this map raises — better to
# error loudly than silently flip a sign on an unfamiliar phrasing.
_DIRECTION_SIGN: dict[str, int] = {
    "increased": +1,
    "increase":  +1,
    "rose":      +1,
    "rise":      +1,
    "up":        +1,
    "decreased": -1,
    "decrease":  -1,
    "decline":   -1,
    "declined":  -1,
    "fell":      -1,
    "fall":      -1,
    "down":      -1,
}


def _strip_html(html: str) -> str:
    """Strip tags + collapse whitespace; keep the narrative readable.

    NBS press-release articles wrap each paragraph in ``<p>`` inside a
    ``<div class="TRS_Editor">`` content area. A naive regex strip is
    enough — we only need the narrative text for the value parser, not
    structure.
    """
    text = re.sub(r"<script\b[^>]*>.*?</script>", " ", html, flags=re.I | re.S)
    text = re.sub(r"<style\b[^>]*>.*?</style>",   " ", text, flags=re.I | re.S)
    text = re.sub(r"<[^>]+>", " ", text)
    # Collapse non-breaking spaces and the various Unicode spaces NBS
    # uses inside Chinese-input forms.
    text = text.replace("\xa0", " ").replace("　", " ")
    return re.sub(r"\s+", " ", text).strip()


def _normalize_value(raw: str, sign: int) -> str:
    """Return the signed value as a clean Decimal-formatted string."""
    cleaned = raw.strip().replace(",", "")
    try:
        magnitude = Decimal(cleaned)
    except InvalidOperation as exc:
        raise NBSValueParseError(f"unparseable NBS value: {raw!r}") from exc
    # Numeric magnitudes from the press release are always non-negative
    # (e.g. ``"0.5"``); the direction word carries the sign. Apply it
    # explicitly rather than trusting a stray ``-`` in the source.
    abs_mag = magnitude.copy_abs()
    signed = abs_mag if sign >= 0 else -abs_mag
    # Preserve the upstream printed precision (``"1.0"`` not ``"1"``).
    return format(signed, "f")


def extract_press_release_value(
    html: str,
    spec: NBSIndicatorSpec,
) -> str:
    """Pick the signed headline value out of ``html`` for ``spec``.

    Returns the value as a string (matches the rest of the codebase's
    ``cal_econ_event.actual`` storage shape). Raises
    :class:`NBSValueParseError` when no registered pattern matches.
    """
    patterns = _VALUE_PATTERNS.get(spec.indicator)
    if not patterns:
        raise KeyError(
            f"no NBS value pattern registered for {spec.indicator!r}"
        )
    text = _strip_html(html)
    for pattern in patterns:
        match = pattern.regex.search(text)
        if not match:
            continue
        direction = match.group("dir").lower()
        sign = _DIRECTION_SIGN.get(direction)
        if sign is None:
            raise NBSValueParseError(
                f"NBS {spec.indicator}: unknown direction word "
                f"{direction!r} in matched text — pattern needs update"
            )
        return _normalize_value(match.group("val"), sign)
    raise NBSValueParseError(
        f"NBS {spec.indicator}: no headline value pattern matched the "
        f"press-release narrative — upstream phrasing drift"
    )


def parse_press_release_html(
    html: str,
    *,
    spec: NBSIndicatorSpec,
    reference_date: str,
    reference_label: str,
    event_time_utc: str,
    event_time_precision: str = "datetime",
    source_url: str = "",
    observed_at_epoch_ms: int | None = None,
) -> NBSValueObservation:
    """Build an :class:`NBSValueObservation` from one press-release page."""
    value = extract_press_release_value(html, spec)
    if observed_at_epoch_ms is None:
        observed_at_epoch_ms = int(
            datetime.fromisoformat(event_time_utc).timestamp() * 1000
        )
    body = _strip_html(html)
    return NBSValueObservation(
        indicator=spec.indicator,
        reference_date=reference_date,
        reference_label=reference_label,
        value=value,
        event_time_utc=event_time_utc,
        event_time_precision=event_time_precision,
        source_url=source_url,
        release_title=spec.title,
        raw={"text": body[:4000]},
        observed_at_epoch_ms=observed_at_epoch_ms,
    )


_HASH_FIELDS: tuple[str, ...] = (
    "value", "reference_date", "indicator", "source_url",
)


def _content_hash(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    for field_name in _HASH_FIELDS:
        value = payload.get(field_name)
        parts.append("" if value is None else str(value))
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def value_observation_to_records(
    obs: NBSValueObservation,
    *,
    snapshot_epoch_ms: int,
    observed_at_epoch_ms: int | None = None,
    spec: NBSIndicatorSpec | None = None,
    schedule_release_date: str | None = None,
) -> tuple[NBSCalendarRawRecord, NBSCalendarEventRecord]:
    """Project one value observation onto (raw, event) records.

    The synthesized ``provider_event_id`` matches the schedule-side row's
    id so the upsert lands on the existing row. Schedule rows anchor on
    the ISO release date (``release_date.isoformat()``), so the value
    side reuses the same release-date string — passed in as
    ``schedule_release_date`` because the press-release publication date
    is what schedule-side hashed on.
    """
    resolved_spec = spec or INDICATOR_REGISTRY.get(obs.indicator)
    if resolved_spec is None:
        raise KeyError(
            f"indicator {obs.indicator!r} not in NBS INDICATOR_REGISTRY"
        )

    indicator_canonical = canonicalize_indicator(resolved_spec.indicator)
    # Schedule rows hashed the id on the release-date ISO string. The
    # value side must reuse the same anchor so the upsert merges. When
    # a caller has the schedule row in hand, pass its release-date
    # explicitly; otherwise fall back to ``event_time_utc[:10]`` which
    # is the same date in UTC.
    anchor = schedule_release_date or obs.event_time_utc[:10]
    provider_event_id = synthesize_event_id(
        PROVIDER,
        resolved_spec.country_code,
        indicator_canonical,
        anchor,
    )

    payload: dict[str, Any] = {
        "kind":             "nbs_press_release_value",
        "indicator":        resolved_spec.indicator,
        "value":            obs.value,
        "reference_date":   obs.reference_date,
        "reference_label":  obs.reference_label,
        "event_time_utc":   obs.event_time_utc,
        "release_title":    obs.release_title,
        "source_url":       obs.source_url,
        "raw":              obs.raw,
    }
    content_hash = _content_hash(payload)
    payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)

    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()
    # Mirror BLS / HCOB / Census shape: ``observed_at`` defaults to
    # ``snapshot_epoch_ms`` (the value-fetch wall-clock time) rather
    # than the release time. The shared projector's merge guard
    # ``excluded.observed_at >= stored.observed_at`` would otherwise
    # reject the value upsert when the schedule row was last written
    # at a snapshot time that postdates the release timestamp.
    observed = (
        observed_at_epoch_ms
        if observed_at_epoch_ms is not None
        else snapshot_epoch_ms
    )

    raw_record = NBSCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = NBSCalendarEventRecord(
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
        currency="CNY",
        unit=resolved_spec.unit,
        actual=obs.value,
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="National Bureau of Statistics of China",
        source_url=obs.source_url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=observed,
    )
    return raw_record, event_record


def indicator_for_listing_title(
    title: str,
    *,
    indicators: Iterable[str] | None = None,
) -> str | None:
    """Return the canonical indicator whose listing fragment matches ``title``.

    Used when the value-side fetcher walks the listing top-down before
    knowing which schedule row a given listing entry belongs to. Each
    indicator's ``listing_title_fragment`` must be a unique substring of
    its press-release listing row across the four-indicator coverage
    set; the registry assertion in :mod:`indicators` enforces that.
    """
    needle_lower = _strip_html(title).lower()
    candidates = (
        indicators if indicators is not None else INDICATOR_REGISTRY.keys()
    )
    for indicator in candidates:
        spec = INDICATOR_REGISTRY[indicator]
        if spec.listing_title_fragment is None:
            continue
        if spec.listing_title_fragment in needle_lower:
            return indicator
    return None


__all__ = [
    "NBSValueObservation",
    "NBSValueParseError",
    "extract_press_release_value",
    "indicator_for_listing_title",
    "parse_press_release_html",
    "value_observation_to_records",
]

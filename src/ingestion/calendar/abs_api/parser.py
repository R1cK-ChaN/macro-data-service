"""ABS release calendar HTML → calendar projection.

The latest-releases page at
``abs.gov.au/release-calendar/latest-releases`` lists the ~14 most
recent publications. Each release is a ``<div class="views-row">``
carrying:

- ``<time datetime="YYYY-MM-DDTHH:MM:SSZ">`` — UTC publication time.
  ABS publishes at 11:30am Sydney local; the ``Z`` suffix marks UTC,
  so the wall-clock conversion is implicit (no DST handling needed
  on the parse side — ABS pre-converts).
- ``<h3><a href="/statistics/...">headline</a></h3>`` — the
  release URL whose final slug carries the reference period
  (``feb-2026``, ``dec-2025``).
- ``<div class="release__subtitle">`` — release name (often empty
  on headline rows; the URL slug is the durable handle).

The parser walks every ``views-row`` and emits one
:class:`ABSReleaseAnnouncement` per row, leaving the indicator
matcher to the fetcher (it knows the registry).

Schedule-only slice — values stay ``actual=NULL``. The projector's
``COALESCE`` merge rule on ``reference_date`` lets a future value-
side fetcher fill ``actual`` without clobbering the schedule
anchor.

``provider_event_id`` is the standard
``synthesize_event_id(provider, country, canonical, anchor)``
keyed on the ISO reference period, so the id stays stable across
re-scrapes of the same release.
"""

from __future__ import annotations

import hashlib
import json
import re
from calendar import monthrange
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    synthesize_event_id,
)

from .indicators import ABSIndicatorSpec, INDICATOR_REGISTRY

PROVIDER = "abs"
ABS_RELEASE_TZ = "Australia/Sydney"
ABS_RELEASE_TIME_LOCAL = "11:30"
ABS_BASE_URL = "https://www.abs.gov.au"
ABS_RELEASE_CALENDAR_URL = f"{ABS_BASE_URL}/release-calendar/latest-releases"


class ABSCalendarParseError(ValueError):
    """ABS release-calendar HTML did not expose a parseable release card."""


# Each release card. The regex tolerates whitespace differences and
# the optional ``| <strong>Updated information</strong>`` marker
# that follows the time on revised-release rows.
_RELEASE_CARD_RE = re.compile(
    r'<div class="views-row">'
    r'<div class="views-field-field-rs-release-date"><span>\s*'
    r'<time datetime="(?P<dt>[^"]+)"[^>]*>(?P<dt_text>[^<]*)</time>'
    r'(?:[^<]*<strong>(?P<updated>[^<]+)</strong>)?'
    r'\s*</span></div>'
    r'<div class="views-field views-field-nothing"><h3>\s*'
    r'<a href="(?P<href>/statistics/[^"]+)"[^>]*>\s*(?P<headline>[^<]+?)\s*</a></h3></div>'
    r'<div class="release__subtitle">(?P<subtitle>[^<]*)</div>',
    re.DOTALL,
)

_MONTHS: dict[str, int] = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4,
    "MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8,
    "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}
# ABS quarterly slugs use the last month of the quarter:
# ``mar-`` → Q1 (Jan-Mar), ``jun-`` → Q2, ``sep-`` → Q3, ``dec-`` → Q4.
# The reference_date anchors on the *last* day of the quarter to match
# TE's quarterly convention (``"2025-12-31T00:00:00"`` for Q4 2025) and
# the Eurostat / Destatis quarterly outputs. Parity buckets on the
# ``YYYY-MM`` prefix; using quarter-start would put ABS Q4 2025 at
# ``2025-10`` while TE sits at ``2025-12``, breaking the advertised
# presence match.
_QUARTER_END_MONTHS: tuple[int, ...] = (3, 6, 9, 12)

_SLUG_PERIOD_RE = re.compile(r"^([a-z]{3})-(\d{4})$")


@dataclass(frozen=True)
class ABSReleaseAnnouncement:
    """One release row parsed from the latest-releases page."""

    release_dt_utc: datetime  # publication time in UTC (from <time datetime>)
    href: str                 # ``/statistics/...`` path with trailing period slug
    headline: str             # link text (e.g., "Consumer Price Index, Australia")
    subtitle: str             # release__subtitle text (often empty for headline rows)
    period_slug: str          # final URL segment (``"feb-2026"``)
    is_updated_information: bool  # row carried the ``Updated information`` marker


@dataclass(frozen=True)
class ABSCalendarRawRecord:
    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class ABSCalendarEventRecord:
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


def parse_release_calendar(
    html: str | bytes,
) -> list[ABSReleaseAnnouncement]:
    """Walk the latest-releases page for release announcements.

    Returns one :class:`ABSReleaseAnnouncement` per ``views-row``
    matched. Rows without a parseable ``mon-YYYY`` slug are skipped
    silently — non-statistical media releases / generic articles
    appear in the same listing and don't carry a calendar period.
    Raises :class:`ABSCalendarParseError` when zero rows parse —
    layout drift or an empty-page response.
    """
    if isinstance(html, (bytes, bytearray)):
        html = html.decode("utf-8", errors="replace")

    announcements: list[ABSReleaseAnnouncement] = []
    for match in _RELEASE_CARD_RE.finditer(html):
        dt_raw = match.group("dt").strip()
        href = match.group("href").strip()
        headline = match.group("headline").strip()
        subtitle = match.group("subtitle").strip()
        updated = match.group("updated")
        try:
            release_dt = _parse_iso_utc(dt_raw)
        except ValueError:
            continue
        # Slug = final non-empty segment after stripping query string.
        path = href.split("?", 1)[0].rstrip("/")
        period_slug = path.rsplit("/", 1)[-1]
        if not _SLUG_PERIOD_RE.match(period_slug):
            # Non-period slug (e.g., ``2024-25`` annual financial-year,
            # ``latest-release``). Drop — out of P1 scope.
            continue
        announcements.append(ABSReleaseAnnouncement(
            release_dt_utc=release_dt,
            href=href,
            headline=headline,
            subtitle=subtitle,
            period_slug=period_slug,
            is_updated_information=bool(updated and "Updated" in updated),
        ))

    if not announcements:
        raise ABSCalendarParseError(
            "ABS release-calendar HTML parsed zero release rows — DOM/API drift",
        )
    return announcements


def _parse_iso_utc(value: str) -> datetime:
    cleaned = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(cleaned)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _slug_to_reference(
    slug: str, frequency: str,
) -> tuple[date, str]:
    """Convert ``"feb-2026"`` to ``(reference_date, label)``.

    Monthly: first day of the named month.
    Quarterly: ABS slugs use the *last* month of the quarter; the
    parser anchors ``reference_date`` on the first day of the quarter
    to match the StatCan / ONS convention, and the label uses
    ``"Qn YYYY"``.
    """
    match = _SLUG_PERIOD_RE.match(slug)
    if not match:
        raise ABSCalendarParseError(f"unparseable period slug: {slug!r}")
    month_token = match.group(1).upper()
    year = int(match.group(2))
    month = _MONTHS.get(month_token)
    if month is None:
        raise ABSCalendarParseError(f"unknown month token in slug: {slug!r}")

    if frequency == "monthly":
        ref = date(year, month, 1)
        return ref, ref.strftime("%B %Y")
    if frequency == "quarterly":
        if month not in _QUARTER_END_MONTHS:
            raise ABSCalendarParseError(
                f"quarterly slug {slug!r} does not end on a quarter boundary",
            )
        # Quarter-end day matches TE's convention so parity buckets line up.
        last_day = monthrange(year, month)[1]
        ref = date(year, month, last_day)
        quarter = (month - 1) // 3 + 1
        return ref, f"Q{quarter} {year}"
    raise ABSCalendarParseError(f"unsupported frequency: {frequency!r}")


def announcement_matches_spec(
    announcement: ABSReleaseAnnouncement,
    spec: ABSIndicatorSpec,
) -> bool:
    """True when the release URL slug starts with the spec prefix."""
    return announcement.href.startswith(spec.url_path_prefix)


_HASH_FIELDS: tuple[str, ...] = (
    "indicator", "reference_date", "release_dt_utc",
)


def _content_hash(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    for field_name in _HASH_FIELDS:
        v = payload.get(field_name)
        parts.append("" if v is None else str(v))
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def announcement_to_records(
    announcement: ABSReleaseAnnouncement,
    *,
    spec: ABSIndicatorSpec,
    snapshot_epoch_ms: int,
) -> tuple[ABSCalendarRawRecord, ABSCalendarEventRecord]:
    """Project a matched announcement onto (raw, event) records."""
    reference_date, reference_label = _slug_to_reference(
        announcement.period_slug, spec.frequency,
    )
    indicator_canonical = canonicalize_indicator(spec.indicator)
    provider_event_id = synthesize_event_id(
        PROVIDER,
        spec.country_code,
        indicator_canonical,
        reference_date.isoformat(),
    )
    event_time_utc = announcement.release_dt_utc.isoformat()
    source_url = f"{ABS_BASE_URL}{announcement.href}"

    payload: dict[str, Any] = {
        "kind":             "abs_release_calendar",
        "indicator":        spec.indicator,
        "release_dt_utc":   event_time_utc,
        "reference_date":   reference_date.isoformat(),
        "reference_label":  reference_label,
        "headline":         announcement.headline,
        "subtitle":         announcement.subtitle,
        "period_slug":      announcement.period_slug,
        "href":             announcement.href,
        "source_url":       source_url,
        "is_updated":       announcement.is_updated_information,
    }
    content_hash = _content_hash(payload)
    payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()

    raw_record = ABSCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = ABSCalendarEventRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        event_time_utc=event_time_utc,
        event_time_precision="datetime",
        reference_date=reference_date.isoformat(),
        reference_label=reference_label,
        country_code=spec.country_code,
        indicator_id=None,
        category=spec.category,
        title=spec.title,
        importance=spec.importance,
        currency="AUD",
        unit=spec.unit,
        actual=None,
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="Australian Bureau of Statistics",
        source_url=source_url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=snapshot_epoch_ms,
    )
    return raw_record, event_record


__all__ = [
    "ABS_BASE_URL",
    "ABS_RELEASE_CALENDAR_URL",
    "ABS_RELEASE_TIME_LOCAL",
    "ABS_RELEASE_TZ",
    "ABSCalendarEventRecord",
    "ABSCalendarParseError",
    "ABSCalendarRawRecord",
    "ABSReleaseAnnouncement",
    "PROVIDER",
    "announcement_matches_spec",
    "announcement_to_records",
    "parse_release_calendar",
]

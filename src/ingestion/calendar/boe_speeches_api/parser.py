"""Bank of England speeches sitemap → calendar projection.

The BoE publishes its full speech archive at
``bankofengland.co.uk/sitemap/speeches`` as nested ``<ul>`` blocks
grouped by year, then month (where available). Each leaf
``<li class="list-links__item">`` carries an
``<a class="list-links__link" href="/speech/...">`` whose URL path
encodes the year and (since 2021) the month. Speeches predating 2021
use the legacy ``/speech/<YYYY>/<slug>`` shape with no month
segment; current-format speeches use ``/speech/<YYYY>/<month>/<slug>``.

The parser extracts only the month-precision (current-format)
entries — older entries land at year precision only and aren't
useful as anchored calendar events. Day-of-month precision lives on
each individual speech page; per-speech HTTP fan-out is deferred to
P2 if downstream needs it.

Schedule-only slice — values stay ``actual=NULL``. Anchored at the
first day of the month with ``event_time_precision='date'`` so the
schema's date-range queries treat the row as a one-day event with
month-level resolution.

``provider_event_id`` keys on
``synthesize_event_id(provider, country, canonical, anchor)`` with
``anchor = f"{year}-{month:02d}:{slug}"``. The slug is unique within
a year/month bucket on the BoE site, so the anchor stays stable
across re-scrapes.
"""

from __future__ import annotations

import hashlib
import html as html_lib
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    synthesize_event_id,
)

from .indicators import INDICATOR_REGISTRY, BoeSpeechesIndicatorSpec

PROVIDER = "boe-speeches"
BOE_SPEECHES_BASE_URL = "https://www.bankofengland.co.uk"
BOE_SPEECHES_SITEMAP_URL = BOE_SPEECHES_BASE_URL + "/sitemap/speeches"


class BoeSpeechesSitemapParseError(ValueError):
    """BoE speeches sitemap did not expose a parseable list."""


_MONTH_TOKENS: dict[str, int] = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}

# Match a current-format ``<a class="list-links__link" href="…">…</a>``
# block. The ``href`` regex requires year/month/slug so legacy
# 2-segment slugs that lack the month don't get picked up.
_LINK_RE = re.compile(
    r'<a\s+href="(?P<href>https://www\.bankofengland\.co\.uk/speech/'
    r'(?P<year>\d{4})/(?P<month>[a-z]+)/(?P<slug>[^"/]+))"'
    r'\s*class="list-links__link"\s*>'
    r'(?P<text>.*?)</a>',
    re.IGNORECASE | re.DOTALL,
)

# Speaker extraction patterns. Each matches one common BoE title
# convention. The first match wins; titles that don't match any
# pattern keep ``speaker=None`` and surface the whole title as the
# event title.
_SPEAKER_PATTERNS: tuple[tuple[re.Pattern[str], int], ...] = (
    # ``<title> - speech by <speaker>`` / ``− speech by`` (en-dash)
    (re.compile(
        r"^(?P<title>.*?)\s*[-−–]\s*(?:speech|remarks|keynote|address|"
        r"keynote\s+speech|keynote\s+address|lecture|panel\s+remarks)\s+"
        r"(?:by|delivered\s+by|given\s+by)\s+(?P<speaker>.+?)\s*$",
        re.IGNORECASE,
    ), 0),
    # ``<speaker>: <description>`` (Charlotte Gerken / Sarah Breeden)
    (re.compile(
        r"^(?P<speaker>[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})\s*:\s*(?P<title>.+?)\s*$",
    ), 0),
    # ``Speech by <speaker> [at <event>]`` (older Edward George style)
    (re.compile(
        r"^(?:speech|remarks|keynote|address|lecture)\s+by\s+"
        r"(?P<speaker>[A-Z][a-z]+(?:\s+[A-Z]\.?)?(?:\s+[A-Z][a-z]+){1,2})"
        r"(?:\s+(?:at|for|to)\s+(?P<title>.+))?\s*$",
        re.IGNORECASE,
    ), 0),
)


_TAG_RE = re.compile(r"<[^>]+>")


def _strip_tags(text: str) -> str:
    return _TAG_RE.sub("", text)


def _clean_text(text: str) -> str:
    return html_lib.unescape(_strip_tags(text)).replace("\xa0", " ").strip()


def _split_speaker(raw_title: str) -> tuple[str, str | None]:
    """Return ``(title, speaker)`` from a BoE link's display text.

    Tries the registered patterns in order; first hit wins. If none
    match, returns the whole text as the title and ``speaker=None``."""
    cleaned = " ".join(raw_title.split())
    for pattern, _ in _SPEAKER_PATTERNS:
        m = pattern.match(cleaned)
        if m is None:
            continue
        speaker = (m.group("speaker") or "").strip()
        title = (m.group("title") or "").strip()
        if speaker and title:
            return title, speaker
        if speaker and not title:
            # Speech-by-without-event shape — keep the speech-by line
            # as the title so the row stays human-readable.
            return cleaned, speaker
    return cleaned, None


@dataclass(frozen=True)
class BoeSpeech:
    """One parsed speech entry from the BoE sitemap."""

    delivery_date: date      # always the 1st of the listing month
    year: int
    month: int
    slug: str
    url: str
    title: str
    speaker: str | None


@dataclass(frozen=True)
class BoeSpeechesRawRecord:
    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class BoeSpeechesEventRecord:
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


def parse_speeches_sitemap(html: str | bytes) -> list[BoeSpeech]:
    """Walk the BoE speeches sitemap and yield one entry per match.

    Returns the speeches ordered by delivery date ascending. Raises
    :class:`BoeSpeechesSitemapParseError` when the page exposes zero
    current-format speech links — drift signal.
    """
    if isinstance(html, (bytes, bytearray)):
        html = html.decode("utf-8", errors="replace")

    rows: list[BoeSpeech] = []
    seen: set[tuple[int, int, str]] = set()
    for match in _LINK_RE.finditer(html):
        try:
            year = int(match.group("year"))
        except ValueError:
            continue
        month_token = match.group("month").lower()
        month = _MONTH_TOKENS.get(month_token)
        if month is None:
            continue
        slug = match.group("slug")
        key = (year, month, slug)
        if key in seen:
            # The sitemap renders a left-rail "Speech" group that
            # repeats some entries; collapse on key so duplicates
            # don't double-count.
            continue
        seen.add(key)
        try:
            delivery_date = date(year, month, 1)
        except ValueError:
            continue
        href = match.group("href")
        text = _clean_text(match.group("text"))
        if not text:
            continue
        title, speaker = _split_speaker(text)
        rows.append(BoeSpeech(
            delivery_date=delivery_date,
            year=year,
            month=month,
            slug=slug,
            url=href,
            title=title,
            speaker=speaker,
        ))

    if not rows:
        raise BoeSpeechesSitemapParseError(
            "BoE speeches sitemap parsed zero current-format entries — "
            "DOM/API drift",
        )

    rows.sort(key=lambda s: (s.delivery_date, s.slug))
    return rows


_HASH_FIELDS: tuple[str, ...] = (
    "year", "month", "slug", "title", "speaker",
)


def _content_hash(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    for field_name in _HASH_FIELDS:
        v = payload.get(field_name)
        parts.append("" if v is None else str(v))
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def speech_to_records(
    speech: BoeSpeech,
    *,
    snapshot_epoch_ms: int,
    spec: BoeSpeechesIndicatorSpec | None = None,
) -> tuple[BoeSpeechesRawRecord, BoeSpeechesEventRecord]:
    """Project a :class:`BoeSpeech` to (raw, event) records."""
    resolved_spec = spec or INDICATOR_REGISTRY["BOE_SPEECHES"]

    indicator_canonical = canonicalize_indicator(resolved_spec.indicator)
    anchor = f"{speech.year}-{speech.month:02d}:{speech.slug}"
    provider_event_id = synthesize_event_id(
        PROVIDER,
        resolved_spec.country_code,
        indicator_canonical,
        anchor,
    )

    event_time_utc = datetime(
        speech.year, speech.month, 1, tzinfo=timezone.utc,
    ).isoformat()

    if speech.speaker:
        display_title = f"{resolved_spec.title} — {speech.speaker}: {speech.title}"
    else:
        display_title = f"{resolved_spec.title}: {speech.title}"

    reference_label = speech.delivery_date.strftime("%B %Y")
    payload: dict[str, Any] = {
        "kind":          "boe_speech",
        "year":          speech.year,
        "month":         speech.month,
        "slug":          speech.slug,
        "title":         speech.title,
        "speaker":       speech.speaker,
        "source_url":    speech.url,
    }
    content_hash = _content_hash(payload)
    payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()

    raw_record = BoeSpeechesRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = BoeSpeechesEventRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        event_time_utc=event_time_utc,
        event_time_precision="date",
        reference_date=speech.delivery_date.isoformat(),
        reference_label=reference_label,
        country_code=resolved_spec.country_code,
        indicator_id=None,
        category=resolved_spec.category,
        title=display_title,
        importance=resolved_spec.importance,
        currency="GBP",
        unit=resolved_spec.unit,
        actual=None,
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="Bank of England",
        source_url=speech.url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=snapshot_epoch_ms,
    )
    return raw_record, event_record


__all__ = [
    "PROVIDER",
    "BOE_SPEECHES_BASE_URL",
    "BOE_SPEECHES_SITEMAP_URL",
    "BoeSpeech",
    "BoeSpeechesEventRecord",
    "BoeSpeechesRawRecord",
    "BoeSpeechesSitemapParseError",
    "parse_speeches_sitemap",
    "speech_to_records",
]

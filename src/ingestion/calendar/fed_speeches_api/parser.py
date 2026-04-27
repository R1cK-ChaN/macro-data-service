"""Federal Reserve speeches archive HTML → calendar projection.

The Fed exposes one speeches archive page per calendar year at
``federalreserve.gov/newsevents/speech/<YYYY>-speeches.htm``. Each
speech is rendered as a ``<div class="row">`` carrying a
``<div class="col-... eventlist__time"><time>M/D/YYYY</time></div>``
column and a ``<div class="col-... eventlist__event">`` column whose
inner ``<p><a href="/newsevents/speech/<slug>.htm"><em>Title</em></a>
</p>`` link carries the speech URL plus title. A ``<p class=
"news__speaker">Role Speaker Name</p>`` paragraph follows, then a
free-text ``<p>`` with the venue. Rows for upcoming events that
haven't yet been delivered carry only the ``Watch Live`` paragraph
and skip the speaker / venue entries until the transcript is
posted.

Schedule-only slice — values stay ``actual=NULL``. Speeches don't
have a value to fill; they serve as event anchors for downstream
research and impact analysis (mirrors the BOK / RBI schedule-only
deferral pattern).

``provider_event_id`` keys on the standard
``synthesize_event_id(provider, country, canonical, anchor)`` with the
speech URL slug as the anchor (the slug encodes speaker last name +
date + suffix and is unique per speech), so the id stays stable
across re-scrapes. Title revisions on the live page (rare — the Fed
typically only adjusts the title between the initial Watch-Live
posting and the final transcript) upsert through the merge rule;
the row's anchor is unchanged.
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

from .indicators import INDICATOR_REGISTRY, FedSpeechesIndicatorSpec

PROVIDER = "fed-speeches"
FED_SPEECHES_BASE_URL = "https://www.federalreserve.gov"
FED_SPEECHES_URL_TEMPLATE = (
    FED_SPEECHES_BASE_URL + "/newsevents/speech/{year}-speeches.htm"
)


class FedSpeechesArchiveParseError(ValueError):
    """Fed speeches archive page did not expose a parseable list."""


# Whole-row matcher. The page renders one ``<div class="row">…</div>``
# per speech inside the year's listing. Anchoring on
# ``eventlist__time`` inside the row block keeps the regex stable
# across the unrelated ``<div class="row">`` blocks the page also
# carries in its header / footer.
_ROW_RE = re.compile(
    r'<div class="row">\s*'
    r'<div class="col-[^"]*eventlist__time">\s*'
    r'<time>(?P<date>\d{1,2}/\d{1,2}/\d{4})</time>\s*'
    r'</div>\s*'
    r'<div class="col-[^"]*eventlist__event">'
    r'(?P<body>.*?)'
    r'</div>\s*</div>',
    re.IGNORECASE | re.DOTALL,
)


# Speech link inside a row. The Fed wraps the title in ``<em>`` so we
# strip that wrapper before storing.
_LINK_RE = re.compile(
    r'<a\s+href="(?P<href>/newsevents/speech/(?P<slug>[^"/]+)\.htm)"\s*>'
    r'\s*<em>(?P<title>.*?)</em>\s*</a>',
    re.IGNORECASE | re.DOTALL,
)


# Speaker line. The Fed page renders ``<p class="news__speaker">``
# followed by the role + full name (e.g. ``Vice Chair Philip N.
# Jefferson``). Future-event rows (Watch Live only, no transcript
# yet) omit this paragraph entirely.
_SPEAKER_RE = re.compile(
    r'<p class="news__speaker">\s*(?P<speaker>.*?)\s*</p>',
    re.IGNORECASE | re.DOTALL,
)


# Venue line — the first free-text ``<p>`` after the speaker line.
# Optional, but the page convention is to lead with ``"At …"``.
_VENUE_RE = re.compile(
    r'<p class="news__speaker">.*?</p>\s*'
    r'<p>(?P<venue>(?!<a\b).*?)</p>',
    re.IGNORECASE | re.DOTALL,
)


_TAG_RE = re.compile(r"<[^>]+>")


def _strip_tags(text: str) -> str:
    return _TAG_RE.sub("", text)


def _clean_text(text: str) -> str:
    return html_lib.unescape(_strip_tags(text)).replace("\xa0", " ").strip()


@dataclass(frozen=True)
class FedSpeech:
    """One parsed Fed speech entry."""

    delivery_date: date
    slug: str
    url: str
    title: str
    speaker: str | None      # ``None`` for future-event rows that lack the speaker line
    venue: str | None        # ``None`` when the page omits the venue paragraph


@dataclass(frozen=True)
class FedSpeechesRawRecord:
    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class FedSpeechesEventRecord:
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


def parse_speeches_archive(html: str | bytes) -> list[FedSpeech]:
    """Walk the Fed speeches archive page and yield one entry per row.

    Returns the speeches ordered by delivery date ascending. Raises
    :class:`FedSpeechesArchiveParseError` when zero rows match the
    row regex — the page either drifted or the year carried no
    speeches at all (extremely unlikely for any Fed year on file).
    """
    if isinstance(html, (bytes, bytearray)):
        html = html.decode("utf-8", errors="replace")

    rows: list[FedSpeech] = []
    for match in _ROW_RE.finditer(html):
        try:
            raw_date = match.group("date")
            month_str, day_str, year_str = raw_date.split("/")
            delivery_date = date(int(year_str), int(month_str), int(day_str))
        except ValueError:
            continue
        body = match.group("body")
        link_match = _LINK_RE.search(body)
        if link_match is None:
            continue
        slug = link_match.group("slug")
        href = link_match.group("href")
        title = _clean_text(link_match.group("title"))
        if not title:
            continue
        speaker_match = _SPEAKER_RE.search(body)
        speaker = (
            _clean_text(speaker_match.group("speaker"))
            if speaker_match is not None
            else None
        )
        venue_match = _VENUE_RE.search(body)
        venue = (
            _clean_text(venue_match.group("venue"))
            if venue_match is not None
            else None
        )
        rows.append(FedSpeech(
            delivery_date=delivery_date,
            slug=slug,
            url=FED_SPEECHES_BASE_URL + href,
            title=title,
            speaker=speaker or None,
            venue=venue or None,
        ))

    if not rows:
        raise FedSpeechesArchiveParseError(
            "Fed speeches archive parsed zero entries — DOM/API drift",
        )

    rows.sort(key=lambda s: (s.delivery_date, s.slug))
    return rows


_HASH_FIELDS: tuple[str, ...] = (
    "delivery_date", "slug", "title", "speaker", "venue",
)


def _content_hash(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    for field_name in _HASH_FIELDS:
        v = payload.get(field_name)
        parts.append("" if v is None else str(v))
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def speech_to_records(
    speech: FedSpeech,
    *,
    snapshot_epoch_ms: int,
    spec: FedSpeechesIndicatorSpec | None = None,
) -> tuple[FedSpeechesRawRecord, FedSpeechesEventRecord]:
    """Project a :class:`FedSpeech` to (raw, event) records."""
    resolved_spec = spec or INDICATOR_REGISTRY["FED_SPEECHES"]

    indicator_canonical = canonicalize_indicator(resolved_spec.indicator)
    # Anchor pairs the slug with the delivery date so the same
    # transcript URL re-delivered at multiple venues across different
    # days lands as distinct calendar events. Bowman 2025-02-05 →
    # 2025-02-07 → 2025-02-11 (Iowa / Wisconsin / Kansas, all
    # ``bowman20250205a``) is the worked example.
    anchor = f"{speech.delivery_date.isoformat()}:{speech.slug}"
    provider_event_id = synthesize_event_id(
        PROVIDER,
        resolved_spec.country_code,
        indicator_canonical,
        anchor,
    )

    # Anchor the event at midnight UTC on the delivery date. The Fed
    # archive lists the calendar day only — wall-clock delivery
    # times aren't published in the listing — so the precision flag
    # carries the truth and the timestamp is just a sortable anchor
    # consistent with other ``date``-precision rows in the schema.
    event_time_utc = datetime(
        speech.delivery_date.year,
        speech.delivery_date.month,
        speech.delivery_date.day,
        tzinfo=timezone.utc,
    ).isoformat()

    # Title pattern: ``"Fed Speech — Speaker: Title"`` so a list
    # display surface (calendar grid, daily digest) shows speaker +
    # subject without needing to join. Falls back to the bare title
    # when the page omits the speaker paragraph (future event with
    # only Watch Live posted).
    if speech.speaker:
        display_title = f"{resolved_spec.title} — {speech.speaker}: {speech.title}"
    else:
        display_title = f"{resolved_spec.title}: {speech.title}"

    reference_label = speech.delivery_date.strftime("%B %Y")
    payload: dict[str, Any] = {
        "kind":          "fed_speech",
        "delivery_date": speech.delivery_date.isoformat(),
        "slug":          speech.slug,
        "title":         speech.title,
        "speaker":       speech.speaker,
        "venue":         speech.venue,
        "source_url":    speech.url,
    }
    content_hash = _content_hash(payload)
    payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()

    raw_record = FedSpeechesRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = FedSpeechesEventRecord(
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
        currency="USD",
        unit=resolved_spec.unit,
        actual=None,
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="US Federal Reserve",
        source_url=speech.url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=snapshot_epoch_ms,
    )
    return raw_record, event_record


__all__ = [
    "PROVIDER",
    "FED_SPEECHES_BASE_URL",
    "FED_SPEECHES_URL_TEMPLATE",
    "FedSpeech",
    "FedSpeechesArchiveParseError",
    "FedSpeechesEventRecord",
    "FedSpeechesRawRecord",
    "parse_speeches_archive",
    "speech_to_records",
]

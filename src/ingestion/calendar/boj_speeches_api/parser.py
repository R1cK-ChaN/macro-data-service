"""Bank of Japan speeches archive HTML → calendar projection.

The BoJ exposes one speeches archive page per calendar year at
``boj.or.jp/en/about/press/koen_<YYYY>/index.htm``. Each speech row
is a ``<tr>`` carrying three ``<td>`` cells:

  1. ``<td>Mon. DD, YYYY</td>`` — delivery date as a US-format
     month abbreviation, day, comma-year (with ``&nbsp;`` between
     the month and the day, and again between the day and the year).
     ``Sept.`` is the four-letter abbreviation BoJ uses for
     September; the rest are three-letter (``Jan.`` … ``Dec.``).
  2. ``<td>FAMILYNAME Givenname, Role</td>`` — speaker line. Role
     values seen across recent years: ``Governor``, ``Deputy
     Governor``, ``Member of the Policy Board``, ``Executive
     Director``, ``Executive Officer``, ``Counsellor`` (advisor).
  3. ``<td><a href="<slug>.htm">"Title" (Event description)</a></td>``
     — speech link with the title in double quotes followed by a
     parenthesised event / venue note.

The parser keeps only rows whose role parses to a rate-setting
position (Governor + Deputy Governor + Member of the Policy Board)
to match the issue's "rate-setters only" scope. Executive
directors / counsellors are skipped — they don't vote on monetary
policy and would dilute the Policy-Board anchor signal.

``provider_event_id`` keys on ``synthesize_event_id`` with
``anchor = f"{date}:{slug}"``. The slug is unique per BoJ speech
URL and stable across re-scrapes.
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

from .indicators import INDICATOR_REGISTRY, BojSpeechesIndicatorSpec

PROVIDER = "boj-speeches"
BOJ_SPEECHES_BASE_URL = "https://www.boj.or.jp"
BOJ_SPEECHES_URL_TEMPLATE = (
    BOJ_SPEECHES_BASE_URL + "/en/about/press/koen_{year}/index.htm"
)


class BojSpeechesArchiveParseError(ValueError):
    """BoJ speeches archive page did not expose a parseable list."""


_MONTH_TOKENS: dict[str, int] = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4,
    "MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8,
    "SEP": 9, "SEPT": 9,                           # BoJ uses ``Sept.``
    "OCT": 10, "NOV": 11, "DEC": 12,
}


# Rate-setting roles (Policy Board members + Governor + Deputies).
# Role text is normalised to lowercase before comparison so spelling
# variants the page introduces (``Member of Policy Board`` vs
# ``Member of the Policy Board``) still match.
_RATE_SETTING_ROLES: frozenset[str] = frozenset({
    "governor",
    "deputy governor",
    "member of the policy board",
    "member of policy board",
})


# Whole-row matcher. The page wraps every speech in a single
# ``<tr>...</tr>`` block; anchoring on the three ``<td>`` cells is
# resilient against the surrounding TBODY / THEAD / WHOIS-of-page
# wrappers BoJ embeds.
_ROW_RE = re.compile(
    r'<tr>\s*'
    r'<td>(?P<date_cell>[^<]+)</td>\s*'
    r'<td>(?P<speaker_cell>[^<]+)</td>\s*'
    r'<td>(?P<link_cell>.+?)</td>\s*'
    r'</tr>',
    re.IGNORECASE | re.DOTALL,
)


_LINK_RE = re.compile(
    r'<a\s+href="(?P<href>/en/about/press/koen_(?P<year>\d{4})/'
    r'(?P<slug>[^"/]+)\.htm)"\s*>(?P<text>.+?)</a>',
    re.IGNORECASE | re.DOTALL,
)


_DATE_RE = re.compile(
    r"^(?P<month>[A-Za-z]+)\.\s*(?P<day>\d{1,2}),\s*(?P<year>\d{4})$",
)


@dataclass(frozen=True)
class BojSpeech:
    """One parsed BoJ speech entry."""

    delivery_date: date
    slug: str
    url: str
    title: str
    speaker: str            # ``"FAMILY Given"`` form (no role suffix)
    role: str               # ``"Governor"`` / ``"Deputy Governor"`` / ``"Member of the Policy Board"``


@dataclass(frozen=True)
class BojSpeechesRawRecord:
    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class BojSpeechesEventRecord:
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


def _normalise_cell(text: str) -> str:
    return html_lib.unescape(text).replace("\xa0", " ").strip()


def _split_speaker_role(cell: str) -> tuple[str, str] | None:
    """Return ``(speaker, role)`` from the speaker cell.

    BoJ writes ``"FAMILY Given, Role"`` with a single comma. The
    role is everything after the last comma; the speaker name is
    everything before. Returns ``None`` for cells that lack a comma
    (typically a header row that slipped through the row regex).
    """
    if "," not in cell:
        return None
    speaker, _, role = cell.rpartition(",")
    return speaker.strip(), role.strip()


def _parse_date_cell(cell: str) -> date | None:
    match = _DATE_RE.match(cell)
    if match is None:
        return None
    month_token = match.group("month").upper()
    month = _MONTH_TOKENS.get(month_token)
    if month is None:
        return None
    try:
        return date(
            int(match.group("year")), month, int(match.group("day")),
        )
    except ValueError:
        return None


_TITLE_QUOTE_RE = re.compile(
    r'^"(?P<title>.+?)"\s*\(?(?P<event>.*?)\)?\s*$',
    re.DOTALL,
)


def _split_title(text: str) -> tuple[str, str]:
    """Return ``(title, event)`` from the BoJ link's text.

    BoJ wraps the speech title in double quotes followed by an
    optional ``(Event description)`` parenthesised note. Falls back
    to ``(text, "")`` for atypical formatting (no quotes)."""
    cleaned = _normalise_cell(text)
    match = _TITLE_QUOTE_RE.match(cleaned)
    if match is None:
        return cleaned, ""
    return (match.group("title") or "").strip(), (match.group("event") or "").strip()


def parse_speeches_archive(html: str | bytes) -> list[BojSpeech]:
    """Walk the BoJ speeches archive page and yield rate-setter rows.

    Returns the speeches ordered by delivery date ascending. Raises
    :class:`BojSpeechesArchiveParseError` when zero rows match the
    row regex — the page either drifted or no speeches are listed
    yet for the requested year.
    """
    if isinstance(html, (bytes, bytearray)):
        html = html.decode("utf-8", errors="replace")

    matched_rows = list(_ROW_RE.finditer(html))
    if not matched_rows:
        raise BojSpeechesArchiveParseError(
            "BoJ speeches archive parsed zero rows — DOM/API drift",
        )

    rows: list[BojSpeech] = []
    for match in matched_rows:
        date_cell = _normalise_cell(match.group("date_cell"))
        speaker_cell = _normalise_cell(match.group("speaker_cell"))
        link_cell = match.group("link_cell")
        delivery_date = _parse_date_cell(date_cell)
        if delivery_date is None:
            continue
        split = _split_speaker_role(speaker_cell)
        if split is None:
            continue
        speaker, role = split
        if role.lower() not in _RATE_SETTING_ROLES:
            continue
        link_match = _LINK_RE.search(link_cell)
        if link_match is None:
            continue
        slug = link_match.group("slug")
        href = link_match.group("href")
        title, _event_note = _split_title(link_match.group("text"))
        if not title:
            continue
        rows.append(BojSpeech(
            delivery_date=delivery_date,
            slug=slug,
            url=BOJ_SPEECHES_BASE_URL + href,
            title=title,
            speaker=speaker,
            role=role,
        ))

    rows.sort(key=lambda s: (s.delivery_date, s.slug))
    return rows


_HASH_FIELDS: tuple[str, ...] = (
    "delivery_date", "slug", "title", "speaker", "role",
)


def _content_hash(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    for field_name in _HASH_FIELDS:
        v = payload.get(field_name)
        parts.append("" if v is None else str(v))
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def speech_to_records(
    speech: BojSpeech,
    *,
    snapshot_epoch_ms: int,
    spec: BojSpeechesIndicatorSpec | None = None,
) -> tuple[BojSpeechesRawRecord, BojSpeechesEventRecord]:
    """Project a :class:`BojSpeech` to (raw, event) records."""
    resolved_spec = spec or INDICATOR_REGISTRY["BOJ_SPEECHES"]

    indicator_canonical = canonicalize_indicator(resolved_spec.indicator)
    anchor = f"{speech.delivery_date.isoformat()}:{speech.slug}"
    provider_event_id = synthesize_event_id(
        PROVIDER,
        resolved_spec.country_code,
        indicator_canonical,
        anchor,
    )

    event_time_utc = datetime(
        speech.delivery_date.year,
        speech.delivery_date.month,
        speech.delivery_date.day,
        tzinfo=timezone.utc,
    ).isoformat()

    speaker_label = f"{speech.role} {speech.speaker}".strip()
    display_title = f"{resolved_spec.title} — {speaker_label}: {speech.title}"

    reference_label = speech.delivery_date.strftime("%B %Y")
    payload: dict[str, Any] = {
        "kind":          "boj_speech",
        "delivery_date": speech.delivery_date.isoformat(),
        "slug":          speech.slug,
        "title":         speech.title,
        "speaker":       speech.speaker,
        "role":          speech.role,
        "source_url":    speech.url,
    }
    content_hash = _content_hash(payload)
    payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()

    raw_record = BojSpeechesRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = BojSpeechesEventRecord(
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
        currency="JPY",
        unit=resolved_spec.unit,
        actual=None,
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="Bank of Japan",
        source_url=speech.url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=snapshot_epoch_ms,
    )
    return raw_record, event_record


__all__ = [
    "PROVIDER",
    "BOJ_SPEECHES_BASE_URL",
    "BOJ_SPEECHES_URL_TEMPLATE",
    "BojSpeech",
    "BojSpeechesArchiveParseError",
    "BojSpeechesEventRecord",
    "BojSpeechesRawRecord",
    "parse_speeches_archive",
    "speech_to_records",
]

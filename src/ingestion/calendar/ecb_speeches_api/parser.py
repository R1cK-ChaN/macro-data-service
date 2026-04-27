"""ECB speeches CSV → calendar projection.

The ECB exposes the official speeches dataset as a single
pipe-separated CSV at
``ecb.europa.eu/press/key/shared/data/all_ECB_speeches.csv``. Per the
download page documentation: UTF-8 (no BOM), CRLF newlines, columns
``date|speakers|title|subtitle|contents``. The dataset covers
Executive Board members only and refreshes monthly. Each row
represents one speech delivered on the date in column 1. A handful
of historical rows carry an empty ``speakers`` field (typically
press conference excerpts attributed to the Governing Council
collectively); the parser keeps these rows but flags them with
``speaker=None`` so the projector can fall back to the bare title.

Schedule-only slice — values stay ``actual=NULL``. Mirrors the
BOK / RBI deferral pattern: speeches serve as event anchors for
downstream research. The CSV's ``contents`` column carries the
full transcript text but is **not** persisted into the raw payload
— at ~57 MB across the historical dataset it would balloon
``cal_econ_raw`` for a feature explicitly out of scope in P1
(transcript NLP per the issue body). A future NLP slice can
re-fetch the CSV (single GET, monthly refresh) when needed.

``provider_event_id`` keys on
``synthesize_event_id(provider, country, canonical, anchor)`` with
``anchor = f"{date}:{title_slug}"``. The CSV does not carry a
stable upstream id; combining the delivery date with a hash of the
title is the most stable disambiguator (different speeches by the
same speaker on the same day have distinct titles).
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    synthesize_event_id,
)

from .indicators import INDICATOR_REGISTRY, EcbSpeechesIndicatorSpec

PROVIDER = "ecb-speeches"
ECB_SPEECHES_BASE_URL = "https://www.ecb.europa.eu"
ECB_SPEECHES_CSV_URL = (
    ECB_SPEECHES_BASE_URL + "/press/key/shared/data/all_ECB_speeches.csv"
)


class EcbSpeechesCsvParseError(ValueError):
    """ECB speeches CSV did not expose a parseable row layout."""


_EXPECTED_HEADER: tuple[str, ...] = (
    "date", "speakers", "title", "subtitle", "contents",
)


_TITLE_SLUG_NON_ALNUM = re.compile(r"[^a-z0-9]+")


def _title_slug(title: str) -> str:
    """Stable lowercase slug for use in the row anchor.

    The ECB publishes title revisions occasionally (typo fixes,
    capitalisation drift). To keep the synthesized event id stable
    across such drift the slug is computed from the lowercase
    alphanumeric form, then truncated to 16 chars and suffixed with
    an 8-char hash of the full title — enough entropy that titles
    differing only past character 16 still hash apart, while a one-
    character typo fix doesn't change the surface form."""
    normalized = _TITLE_SLUG_NON_ALNUM.sub("-", title.lower()).strip("-")
    digest = hashlib.sha1(title.encode("utf-8"), usedforsecurity=False).hexdigest()[:8]
    return f"{normalized[:16]}-{digest}" if normalized else digest


@dataclass(frozen=True)
class EcbSpeech:
    """One parsed speech entry from the official CSV."""

    delivery_date: date
    speaker: str | None      # None for empty-speaker rows
    title: str
    subtitle: str            # always present in the CSV but may be empty
    has_contents: bool       # transcript text present in the CSV row


@dataclass(frozen=True)
class EcbSpeechesRawRecord:
    provider: str
    provider_event_id: str
    snapshot_epoch_ms: int
    content_hash: str
    payload_json: str
    fetched_at: str


@dataclass(frozen=True)
class EcbSpeechesEventRecord:
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


def parse_speeches_csv(csv_text: str | bytes) -> list[EcbSpeech]:
    """Parse the pipe-separated CSV into a list of speeches.

    Returns the speeches ordered by delivery date ascending. Raises
    :class:`EcbSpeechesCsvParseError` when the header is missing /
    drifted, no data rows are present, or any row exposes an
    unparseable date — those signal an upstream format change the
    caller must surface, not swallow.
    """
    if isinstance(csv_text, (bytes, bytearray)):
        csv_text = csv_text.decode("utf-8-sig", errors="replace")

    reader = csv.reader(io.StringIO(csv_text), delimiter="|")
    try:
        header = next(reader)
    except StopIteration:
        raise EcbSpeechesCsvParseError("ECB speeches CSV is empty")
    normalized_header = tuple(col.strip().lower() for col in header)
    if normalized_header != _EXPECTED_HEADER:
        raise EcbSpeechesCsvParseError(
            f"unexpected CSV header: {normalized_header!r}",
        )

    rows: list[EcbSpeech] = []
    for line_no, row in enumerate(reader, start=2):
        if not row or all(not col.strip() for col in row):
            continue
        if len(row) < len(_EXPECTED_HEADER):
            # Pad missing trailing fields rather than skipping — the
            # ``contents`` column is sometimes empty without a final
            # ``|`` separator on the row.
            row = row + [""] * (len(_EXPECTED_HEADER) - len(row))
        date_str = row[0].strip()
        try:
            delivery_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError as exc:
            raise EcbSpeechesCsvParseError(
                f"unparseable date on row {line_no}: {date_str!r}",
            ) from exc
        speaker_raw = row[1].strip()
        title = row[2].strip()
        subtitle = row[3].strip()
        has_contents = bool(row[4].strip())
        if not title:
            # Title is the only structurally required column for an
            # event record. Empty title rows surface as a header-
            # level format issue.
            raise EcbSpeechesCsvParseError(
                f"empty title on row {line_no}",
            )
        rows.append(EcbSpeech(
            delivery_date=delivery_date,
            speaker=speaker_raw or None,
            title=title,
            subtitle=subtitle,
            has_contents=has_contents,
        ))

    if not rows:
        raise EcbSpeechesCsvParseError(
            "ECB speeches CSV header present but zero data rows",
        )

    rows.sort(key=lambda s: (s.delivery_date, s.title))
    return rows


_HASH_FIELDS: tuple[str, ...] = (
    "delivery_date", "speaker", "title", "subtitle",
)


def _content_hash(payload: dict[str, Any]) -> str:
    parts: list[str] = []
    for field_name in _HASH_FIELDS:
        v = payload.get(field_name)
        parts.append("" if v is None else str(v))
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def speech_to_records(
    speech: EcbSpeech,
    *,
    snapshot_epoch_ms: int,
    spec: EcbSpeechesIndicatorSpec | None = None,
) -> tuple[EcbSpeechesRawRecord, EcbSpeechesEventRecord]:
    """Project an :class:`EcbSpeech` to (raw, event) records."""
    resolved_spec = spec or INDICATOR_REGISTRY["ECB_SPEECHES"]

    indicator_canonical = canonicalize_indicator(resolved_spec.indicator)
    anchor = f"{speech.delivery_date.isoformat()}:{_title_slug(speech.title)}"
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

    if speech.speaker:
        display_title = f"{resolved_spec.title} — {speech.speaker}: {speech.title}"
    else:
        display_title = f"{resolved_spec.title}: {speech.title}"

    reference_label = speech.delivery_date.strftime("%B %Y")
    payload: dict[str, Any] = {
        "kind":          "ecb_speech",
        "delivery_date": speech.delivery_date.isoformat(),
        "speaker":       speech.speaker,
        "title":         speech.title,
        "subtitle":      speech.subtitle,
        "has_contents":  speech.has_contents,
        "source_url":    ECB_SPEECHES_CSV_URL,
    }
    content_hash = _content_hash(payload)
    payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()

    raw_record = EcbSpeechesRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = EcbSpeechesEventRecord(
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
        currency="EUR",
        unit=resolved_spec.unit,
        actual=None,
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="European Central Bank",
        source_url=ECB_SPEECHES_CSV_URL,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=snapshot_epoch_ms,
    )
    return raw_record, event_record


__all__ = [
    "PROVIDER",
    "ECB_SPEECHES_BASE_URL",
    "ECB_SPEECHES_CSV_URL",
    "EcbSpeech",
    "EcbSpeechesCsvParseError",
    "EcbSpeechesEventRecord",
    "EcbSpeechesRawRecord",
    "parse_speeches_csv",
    "speech_to_records",
]

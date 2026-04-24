"""Scrape the Consumer Confidence Survey landing page.

``esri.cao.go.jp/en/stat/shouhi/shouhi-e.html`` re-publishes every
month with the latest release in place. Two deterministic sentences
sit in the page body:

- ``The Survey of March 2026 was released on April 9th, 2026``
- ``The Consumer Confidence Index (seasonally adjusted series) in
  March 2026 was 33.3, down 6.4 points from the previous month.``

Nothing else on the page carries a monthly anchor, and there is no
per-release archive URL — CAO overwrites the landing page on each
publication. The value-side sweep therefore pulls ``shouhi-e.html``
once per pass, derives ``(reference_date, release_date, cci_sa)``
from the two sentences above, and projects a single ``(raw, event)``
tuple whose ``provider_event_id`` matches the schedule-side write.

``previous`` / ``revised`` / ``forecast`` are left ``None``. The
previous-month value is implicitly derivable from the second
sentence's delta but the explicit surfaces live in
``shouhi2.xlsx`` (the seasonally-adjusted time series); pulling
that in would widen the slice without adding trader-facing signal
beyond what TE already carries.

Fetch + parse + project are separable: tests feed fixture HTML to
:func:`parse_consumer_confidence_summary`; live callers drive
:func:`fetch_consumer_confidence_summary_html`.
:func:`consumer_confidence_to_records` emits the ``(raw, event)``
tuple for the matched release.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

import requests
from bs4 import BeautifulSoup

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    parse_scheduled_release_time,
    synthesize_event_id,
)

from .indicators import CaoIndicatorSpec, INDICATOR_REGISTRY
from .parser import (
    CAO_CONSUMER_CONFIDENCE_RELEASE_TIME_LOCAL,
    CAO_CONSUMER_CONFIDENCE_URL,
    CAO_RELEASE_TZ,
    PROVIDER,
    CaoCalendarEventRecord,
    CaoCalendarRawRecord,
)
from .scraper import CAO_BROWSER_HEADERS

logger = logging.getLogger(__name__)


class CaoConsumerConfidenceParseError(Exception):
    """Landing page didn't carry a parseable Consumer Confidence block."""


@dataclass(frozen=True)
class ConsumerConfidenceSummary:
    """Parsed Consumer Confidence landing-page outcome."""

    reference_date: date
    reference_label: str              # "March 2026"
    release_date: date
    cci_seasonally_adjusted: float    # headline index value


_MONTH_NAMES: dict[str, int] = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}

# "The Survey of March 2026 was released on April 9th, 2026"
_RELEASE_SENTENCE_RE = re.compile(
    r"The\s+Survey\s+of\s+"
    r"(?P<ref_month>[A-Za-z]+)\s+(?P<ref_year>\d{4})\s+"
    r"was\s+released\s+on\s+"
    r"(?P<rel_month>[A-Za-z]+)\s+(?P<rel_day>\d{1,2})"
    r"(?:st|nd|rd|th)?\s*,\s*(?P<rel_year>\d{4})",
    re.IGNORECASE,
)

# "The Consumer Confidence Index (seasonally adjusted series) in
#  March 2026 was 33.3, down 6.4 points from the previous month."
#
# Match the seasonally-adjusted series specifically — the page also
# carries the original (non-SA) series elsewhere and the headline
# that trades off is always the SA figure.
_VALUE_SENTENCE_RE = re.compile(
    r"Consumer\s+Confidence\s+Index\s+"
    r"\(\s*seasonally\s+adjusted\s+series\s*\)\s+"
    r"in\s+(?P<month>[A-Za-z]+)\s+(?P<year>\d{4})\s+"
    r"was\s+(?P<value>\d+(?:\.\d+)?)",
    re.IGNORECASE,
)


def _resolve_month_name(name: str) -> int:
    key = (name or "").strip().lower()
    month = _MONTH_NAMES.get(key)
    if month is None:
        raise CaoConsumerConfidenceParseError(
            f"unknown month name: {name!r}"
        )
    return month


def parse_consumer_confidence_summary(
    html: str,
) -> ConsumerConfidenceSummary:
    """Extract the headline Consumer Confidence release from the landing page.

    Raises :class:`CaoConsumerConfidenceParseError` when the landing
    page lacks either the release-announcement sentence or the
    SA-series headline sentence, or when the two disagree on the
    reference month (a stale / half-updated page state).
    """
    soup = BeautifulSoup(html, "html.parser")
    # BS4's ``get_text(" ")`` keeps inline whitespace but drops
    # ``<br>`` to space; that's enough for both sentence shapes.
    text = soup.get_text(" ", strip=True)

    release_match = _RELEASE_SENTENCE_RE.search(text)
    if release_match is None:
        raise CaoConsumerConfidenceParseError(
            "CAO Consumer Confidence landing page: "
            "release-announcement sentence not found"
        )

    value_match = _VALUE_SENTENCE_RE.search(text)
    if value_match is None:
        raise CaoConsumerConfidenceParseError(
            "CAO Consumer Confidence landing page: "
            "seasonally-adjusted headline value not found"
        )

    ref_month = _resolve_month_name(release_match.group("ref_month"))
    ref_year = int(release_match.group("ref_year"))
    reference_date = date(year=ref_year, month=ref_month, day=1)
    reference_label = reference_date.strftime("%B %Y")

    # Cross-check the two sentences — a stale cache or a mid-edit
    # page could disagree. Loud-fail so we don't stamp the wrong
    # reference on the current row.
    value_month = _resolve_month_name(value_match.group("month"))
    value_year = int(value_match.group("year"))
    if (value_year, value_month) != (ref_year, ref_month):
        raise CaoConsumerConfidenceParseError(
            f"CAO Consumer Confidence page: reference-month "
            f"mismatch between release sentence "
            f"({ref_year}-{ref_month:02d}) and value sentence "
            f"({value_year}-{value_month:02d})"
        )

    rel_month = _resolve_month_name(release_match.group("rel_month"))
    rel_day = int(release_match.group("rel_day"))
    rel_year = int(release_match.group("rel_year"))
    try:
        release_date = date(year=rel_year, month=rel_month, day=rel_day)
    except ValueError as exc:
        raise CaoConsumerConfidenceParseError(
            f"invalid release-date in announcement sentence"
        ) from exc

    try:
        cci = float(value_match.group("value"))
    except ValueError as exc:
        raise CaoConsumerConfidenceParseError(
            f"unparseable Consumer Confidence value: "
            f"{value_match.group('value')!r}"
        ) from exc

    return ConsumerConfidenceSummary(
        reference_date=reference_date,
        reference_label=reference_label,
        release_date=release_date,
        cci_seasonally_adjusted=cci,
    )


# ──────────────────────────────────────────────────────────────────────────
# HTTP fetch
# ──────────────────────────────────────────────────────────────────────────


def fetch_consumer_confidence_summary_html(
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """GET the Consumer Confidence landing page and return HTML text.

    Landing page advertises ``charset=UTF-8`` — we defer to
    :attr:`requests.Response.text` so decoding honours the response's
    own Content-Type header if it ever drifts.
    """
    owned_session = session is None
    s = session or requests.Session()
    try:
        response = s.get(
            CAO_CONSUMER_CONFIDENCE_URL,
            headers=CAO_BROWSER_HEADERS,
            timeout=timeout,
        )
        response.raise_for_status()
        return response.text
    finally:
        if owned_session:
            s.close()


# ──────────────────────────────────────────────────────────────────────────
# Value-side projection
# ──────────────────────────────────────────────────────────────────────────


_HASH_FIELDS: tuple[str, ...] = (
    "indicator", "cci_seasonally_adjusted", "event_time_utc",
)


def _content_hash(payload: dict[str, Any]) -> str:
    parts = []
    for field_name in _HASH_FIELDS:
        value = payload.get(field_name)
        parts.append("" if value is None else str(value))
    joined = "|".join(parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def consumer_confidence_to_records(
    summary: ConsumerConfidenceSummary,
    *,
    snapshot_epoch_ms: int,
    event_time_utc: str | None = None,
    observed_at_epoch_ms: int | None = None,
    spec: CaoIndicatorSpec | None = None,
) -> tuple[CaoCalendarRawRecord, CaoCalendarEventRecord]:
    """Project a :class:`ConsumerConfidenceSummary` into ``(raw, event)`` records.

    Event-time resolution mirrors the MoF / Tankan pattern:

    - ``event_time_utc`` (caller-supplied ISO string) — used verbatim.
      Value-side sweeps pass in the stored schedule-side timestamp so
      a later upsert doesn't shift an already-stamped row.
    - Otherwise, project ``summary.release_date`` through
      :func:`parse_scheduled_release_time` with 14:00 JST.
    """
    resolved_spec = spec or INDICATOR_REGISTRY["CONSUMER_CONFIDENCE"]

    if event_time_utc is None:
        scheduled = parse_scheduled_release_time(
            summary.release_date,
            CAO_CONSUMER_CONFIDENCE_RELEASE_TIME_LOCAL,
            default_tz=CAO_RELEASE_TZ,
        )
        event_time_utc = scheduled.utc.isoformat()

    indicator_canonical = canonicalize_indicator(resolved_spec.indicator)
    provider_event_id = synthesize_event_id(
        PROVIDER,
        resolved_spec.country_code,
        indicator_canonical,
        summary.reference_date.isoformat(),
    )

    # Render ``actual`` with the same decimal precision the landing
    # page uses (one decimal) so downstream display doesn't need to
    # normalise. ``str(float)`` loses trailing zeros (``33.0`` →
    # ``"33.0"``) which is fine; padding an extra zero isn't.
    actual_str = f"{summary.cci_seasonally_adjusted:g}"
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc,
    ).isoformat()
    observed = (
        observed_at_epoch_ms
        if observed_at_epoch_ms is not None
        else snapshot_epoch_ms
    )

    payload: dict[str, Any] = {
        "kind":             "cao_consumer_confidence_value",
        "indicator":        resolved_spec.indicator,
        "reference_date":   summary.reference_date.isoformat(),
        "reference_label":  summary.reference_label,
        "release_date":     summary.release_date.isoformat(),
        "cci_seasonally_adjusted": summary.cci_seasonally_adjusted,
        "event_time_utc":   event_time_utc,
        "source_url":       CAO_CONSUMER_CONFIDENCE_URL,
    }
    content_hash = _content_hash(payload)
    payload_json = json.dumps(payload, sort_keys=True, ensure_ascii=False)

    raw_record = CaoCalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = CaoCalendarEventRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        event_time_utc=event_time_utc,
        event_time_precision="datetime",
        reference_date=summary.reference_date.isoformat(),
        reference_label=summary.reference_label,
        country_code=resolved_spec.country_code,
        indicator_id=None,
        category=resolved_spec.category,
        title=resolved_spec.title,
        importance=resolved_spec.importance,
        currency="",
        unit=resolved_spec.unit,
        actual=actual_str,
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="Cabinet Office Japan (ESRI)",
        source_url=CAO_CONSUMER_CONFIDENCE_URL,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=observed,
    )
    return raw_record, event_record

"""HCOB / S&P Global Germany PMI release-date parser."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    parse_scheduled_release_time,
    synthesize_event_id,
)

from .indicators import (
    HCOB_PRESS_RELEASES_URL,
    HCOB_RELEASE_DATES_URL,
    HCOBIndicatorSpec,
    specs_for_calendar_title,
)
from .parser import (
    PROVIDER,
    HCOBCalendarEventRecord,
    HCOBCalendarRawRecord,
)

HCOB_RELEASE_TZ = "UTC"


class HCOBScheduleParseError(ValueError):
    """Raised when the S&P Global PMI release-dates page cannot be projected."""


@dataclass(frozen=True)
class HCOBScheduleEntry:
    """One matched HCOB PMI schedule row, pre-projection."""

    series_id: str
    reference_date: str
    reference_label: str
    release_title: str
    release_date: date
    event_time_utc: str
    event_time_precision: str
    source_url: str
    raw: dict[str, Any]


_MONTHS: dict[str, int] = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}
_HEADING_RE = re.compile(
    r"(January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+(\d{1,2})",
    re.I,
)
_TIME_RE = re.compile(r"(\d{1,2}:\d{2})\s*UTC", re.I)


def _normalise_title(text: str) -> str:
    """Lowercase + collapse whitespace; preserve ``&`` so ``S&P`` matches."""
    return re.sub(r"\s+", " ", text).strip().lower()


def _resolve_year(release_month: int, release_day: int, today: date) -> int:
    """Map a month/day with no year to the next forward-looking year."""
    candidate = date(today.year, release_month, release_day)
    if candidate >= today:
        return today.year
    return today.year + 1


def _reference_date(release_date: date, release_kind: str) -> date:
    """Reference month for each PMI release kind.

    Flash variants are always for the current month (publishes on ~23rd).
    Final Manufacturing / Services publish on the 1st–3rd business day of
    month N reporting on month N-1.
    """
    if release_kind.startswith("pmi_flash"):
        return date(release_date.year, release_date.month, 1)
    # pmi_final_* — reference is the prior month.
    year = release_date.year
    month = release_date.month - 1
    if month == 0:
        year -= 1
        month = 12
    return date(year, month, 1)


def _reference_label_en(reference: date) -> str:
    return reference.strftime("%B %Y")


def _event_time(release_date: date, release_time: str) -> str:
    """Combine ``release_date`` + ``HH:MM UTC`` into an ISO UTC string."""
    scheduled = parse_scheduled_release_time(
        release_date,
        release_time,
        default_tz=HCOB_RELEASE_TZ,
    )
    return scheduled.utc.isoformat()


def _page_items(html: str | bytes) -> list[tuple[str, str]]:
    """Flat ``(heading_text, item_text)`` list; parses the calendar DOM.

    The S&P Global PMI calendar groups release rows under date headings.
    We walk the DOM in order, carrying the most recent heading forward.
    """
    text = html.decode("utf-8", errors="replace") if isinstance(html, bytes) else html
    soup = BeautifulSoup(text, "html.parser")
    pairs: list[tuple[str, str]] = []
    current_heading = ""
    for div in soup.find_all("div"):
        classes = div.get("class") or []
        if "listSubHeading" in classes:
            span = div.find("span")
            if span:
                current_heading = span.get_text(" ", strip=True)
        elif "listItem" in classes:
            if not current_heading:
                continue
            pairs.append((current_heading, div.get_text(" ", strip=True)))
    return pairs


def parse_release_dates_html(
    html: str | bytes,
    *,
    series_ids: set[str] | None = None,
    source_url: str | None = None,
    today: date | None = None,
    row_issues: list[str] | None = None,
) -> list[HCOBScheduleEntry]:
    """Extract HCOB Germany PMI schedule rows from the public PMI calendar."""
    anchor_today = today or datetime.now(timezone.utc).date()
    entries: list[HCOBScheduleEntry] = []

    for heading, item_text in _page_items(html):
        match = _HEADING_RE.search(heading)
        if not match:
            continue
        month_raw, day_raw = match.groups()
        try:
            month = _MONTHS[month_raw.lower()]
            day = int(day_raw)
            year = _resolve_year(month, day, anchor_today)
            release_date = date(year, month, day)
        except (KeyError, ValueError) as exc:
            if row_issues is not None:
                row_issues.append(
                    f"heading {heading!r}: {type(exc).__name__}: {exc}"
                )
                continue
            raise

        time_match = _TIME_RE.search(item_text)
        release_time = time_match.group(1) if time_match else "00:00"
        # Strip the leading "HH:MM UTC" prefix so the title matcher doesn't
        # see the time inside the release title.
        remainder = item_text
        if time_match:
            remainder = item_text[time_match.end():]
        title_text = _normalise_title(remainder)

        matched = specs_for_calendar_title(title_text)
        if not matched:
            continue
        for spec in matched:
            if series_ids is not None and spec.series_id not in series_ids:
                continue
            reference = _reference_date(release_date, spec.release_kind)
            entries.append(
                HCOBScheduleEntry(
                    series_id=spec.series_id,
                    reference_date=reference.isoformat(),
                    reference_label=_reference_label_en(reference),
                    release_title=spec.title,
                    release_date=release_date,
                    event_time_utc=_event_time(release_date, release_time),
                    event_time_precision="datetime",
                    source_url=source_url or HCOB_RELEASE_DATES_URL,
                    raw={
                        "upstream_title": item_text.strip(),
                        "release_date": release_date.isoformat(),
                        "release_time_utc": release_time,
                        "source_url": source_url or HCOB_RELEASE_DATES_URL,
                    },
                )
            )

    if not entries:
        where = f" at {source_url}" if source_url else ""
        raise HCOBScheduleParseError(
            f"S&P Global PMI calendar contained no matching HCOB Germany rows{where}"
        )
    entries.sort(key=lambda entry: (entry.event_time_utc, entry.series_id))
    return entries


def schedule_entry_to_records(
    entry: HCOBScheduleEntry,
    *,
    snapshot_epoch_ms: int,
    spec: HCOBIndicatorSpec | None = None,
) -> tuple[HCOBCalendarRawRecord, HCOBCalendarEventRecord]:
    """Project one HCOB schedule row to raw + event records."""
    from .indicators import INDICATOR_REGISTRY

    resolved_spec = spec or INDICATOR_REGISTRY.get(entry.series_id)
    if resolved_spec is None:
        raise KeyError(f"series_id {entry.series_id!r} not in HCOB INDICATOR_REGISTRY")
    provider_event_id = synthesize_event_id(
        PROVIDER,
        resolved_spec.country_code,
        canonicalize_indicator(resolved_spec.indicator),
        entry.reference_date,
    )
    payload = {
        "series_id": entry.series_id,
        "reference_date": entry.reference_date,
        "reference_label": entry.reference_label,
        "release_title": entry.release_title,
        "release_date": entry.release_date.isoformat(),
        "event_time_utc": entry.event_time_utc,
        "event_time_precision": entry.event_time_precision,
        "source_url": entry.source_url,
        "raw": entry.raw,
    }
    payload_json = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    content_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
    fetched_at = datetime.fromtimestamp(
        snapshot_epoch_ms / 1000, tz=timezone.utc
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
        event_time_utc=entry.event_time_utc,
        event_time_precision=entry.event_time_precision,
        reference_date=entry.reference_date,
        reference_label=entry.reference_label,
        country_code=resolved_spec.country_code,
        indicator_id=None,
        category=resolved_spec.category,
        title=resolved_spec.title,
        importance=resolved_spec.importance,
        currency="",
        unit=resolved_spec.unit,
        actual=None,
        previous=None,
        revised=None,
        forecast=None,
        consensus_forecast=None,
        ticker="",
        source="HCOB / S&P Global",
        source_url=entry.source_url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=snapshot_epoch_ms,
    )
    return raw_record, event_record


_HCOB_BROWSER_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,de;q=0.8",
}


def fetch_release_dates_html(
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """GET the S&P Global PMI release-dates page."""
    http = session or requests.Session()
    response = http.get(
        HCOB_RELEASE_DATES_URL,
        headers=_HCOB_BROWSER_HEADERS,
        timeout=timeout,
    )
    response.raise_for_status()
    return response.text


def default_schedule_window(today: date | None = None) -> tuple[date, date]:
    """Default schedule window for the PMI calendar's rolling release list."""
    base = today or datetime.now(ZoneInfo("Europe/Berlin")).date()
    return base - timedelta(days=14), date(base.year + 1, 12, 31)


# ── press-release listing → PDF URL resolution ──────────────────────


@dataclass(frozen=True)
class HCOBResolvedPressRelease:
    """One press-release row resolved from the public listing page."""

    title: str
    release_date: str
    source_url: str
    raw: dict[str, Any]


_LISTING_MONTHS: dict[str, int] = _MONTHS  # alias for clarity at use sites


def _normalise_listing_title(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip().lower()


def _parse_listing_date(text: str) -> date | None:
    """Parse ``"April 23 2026 07:30 UTC"`` into a release ``date``."""
    cleaned = text.replace("\xa0", " ")
    match = re.search(
        r"(January|February|March|April|May|June|July|August|September|October|November|December)"
        r"\s+(\d{1,2})\s+(20\d{2})",
        cleaned,
        re.I,
    )
    if not match:
        return None
    month_raw, day_raw, year_raw = match.groups()
    try:
        return date(
            int(year_raw),
            _LISTING_MONTHS[month_raw.lower()],
            int(day_raw),
        )
    except (KeyError, ValueError):
        return None


def resolve_press_release_link(
    html: str | bytes,
    *,
    release_date: date,
    expected_listing_match: str,
) -> HCOBResolvedPressRelease:
    """Locate one press-release URL on the public listing page.

    The listing groups every release into rows with a date span, a
    title span, and a "View More" anchor that points at a GUID-keyed
    PDF. Each release also has a ``(Deutsch)`` German-language sibling
    we deliberately skip so the value parser receives English text.
    """
    text = html.decode("utf-8", errors="replace") if isinstance(html, bytes) else html
    soup = BeautifulSoup(text, "html.parser")
    expected = _normalise_listing_title(expected_listing_match)

    for item in soup.find_all("div", class_="listItem"):
        date_span = item.find("span", class_="releaseDate")
        title_span = item.find("span", class_="releaseTitle")
        anchor = item.find("a", href=True)
        if not (date_span and title_span and anchor):
            continue

        title_text = title_span.get_text(" ", strip=True)
        normalized = _normalise_listing_title(title_text)
        if normalized.endswith("(deutsch)"):
            continue
        if normalized != expected:
            continue

        listing_date = _parse_listing_date(date_span.get_text(" ", strip=True))
        if listing_date != release_date:
            continue

        href = str(anchor.get("href") or "")
        if not href:
            continue
        return HCOBResolvedPressRelease(
            title=title_text,
            release_date=release_date.isoformat(),
            source_url=href,
            raw={
                "listing_date": date_span.get_text(" ", strip=True),
                "listing_title": title_text,
            },
        )

    raise HCOBScheduleParseError(
        f"HCOB / S&P Global press release not found on listing for "
        f"{release_date.isoformat()} / {expected_listing_match!r}"
    )


def fetch_press_releases_listing_html(
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """GET the S&P Global PMI press-release listing page."""
    http = session or requests.Session()
    response = http.get(
        HCOB_PRESS_RELEASES_URL,
        headers=_HCOB_BROWSER_HEADERS,
        timeout=timeout,
    )
    response.raise_for_status()
    return response.text


def fetch_press_release_pdf_text(
    url: str,
    *,
    session: requests.Session | None = None,
    timeout: float = 60.0,
) -> str:
    """Download a press-release PDF and extract its text via ``pypdf``.

    The PMI press-release endpoint serves ``application/pdf`` directly
    (no intermediate HTML), so the response body is the PDF bytes.
    """
    from io import BytesIO

    from pypdf import PdfReader

    http = session or requests.Session()
    response = http.get(url, headers=_HCOB_BROWSER_HEADERS, timeout=timeout)
    response.raise_for_status()
    reader = PdfReader(BytesIO(response.content))
    parts = []
    for page in reader.pages:
        try:
            parts.append(page.extract_text() or "")
        except Exception:  # pypdf raises various decoding errors per page
            continue
    return "\n".join(parts)

"""France INSEE publication-calendar parser."""

from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import re
from typing import Any
from zoneinfo import ZoneInfo

import requests

from ingestion.calendar._official_shared import (
    canonicalize_indicator,
    synthesize_event_id,
)

from .indicators import (
    INSEE_PUBLICATION_CALENDAR_URL,
    INSEEIndicatorSpec,
    INDICATOR_REGISTRY,
)
from .parser import (
    PROVIDER,
    INSEECalendarEventRecord,
    INSEECalendarRawRecord,
    _normalise,
)

INSEE_AGENDA_URL = "https://www.insee.fr/en/agenda-diffusion"
INSEE_RELEASE_TZ = "Europe/Paris"


class INSEEScheduleParseError(ValueError):
    """Raised when INSEE's publication-calendar shape drifts."""


@dataclass(frozen=True)
class INSEEScheduleEntry:
    """One matched INSEE calendar row, pre-projection."""

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
    "janvier": 1,
    "fevrier": 2,
    "mars": 3,
    "avril": 4,
    "mai": 5,
    "juin": 6,
    "juillet": 7,
    "aout": 8,
    "septembre": 9,
    "octobre": 10,
    "novembre": 11,
    "decembre": 12,
}
_QUARTERS: dict[str, int] = {
    "first": 1,
    "1st": 1,
    "premier": 1,
    "second": 2,
    "2nd": 2,
    "deuxieme": 2,
    "third": 3,
    "3rd": 3,
    "troisieme": 3,
    "fourth": 4,
    "4th": 4,
    "quatrieme": 4,
}
_MONTH_RE = re.compile(r"^([a-z]+)\s+(20\d{2})$", re.I)
_QUARTER_RE = re.compile(
    r"^([a-z0-9]+)\s+(?:quarter|trimestre)\s+(20\d{2})$",
    re.I,
)


def _agenda_payload(
    *,
    start: int = 0,
    rows: int = 200,
) -> dict[str, Any]:
    return {
        "q": "*:*",
        "defType": None,
        "start": start,
        "sortFields": [{"field": "dateEmbargo_dt", "order": "asc"}],
        "filters": [],
        "rows": rows,
        "facetsQuery": [],
    }


def _coerce_payload(payload: str | bytes | dict[str, Any]) -> dict[str, Any]:
    if isinstance(payload, dict):
        return payload
    text = payload.decode("utf-8", errors="replace") if isinstance(payload, bytes) else payload
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        raise INSEEScheduleParseError("INSEE agenda response is not JSON") from exc
    if not isinstance(parsed, dict):
        raise INSEEScheduleParseError("INSEE agenda response root is not an object")
    return parsed


def _family_id(doc: dict[str, Any]) -> str:
    family = doc.get("famille") if isinstance(doc.get("famille"), dict) else {}
    return str(family.get("id") or "")


def _release_title(doc: dict[str, Any]) -> str:
    family = doc.get("famille") if isinstance(doc.get("famille"), dict) else {}
    conjoncture = (
        family.get("facetteConjoncture")
        if isinstance(family.get("facetteConjoncture"), dict)
        else {}
    )
    title = conjoncture.get("libelleEn") or conjoncture.get("libelleFr")
    return str(title or "")


def _month_number(raw: str) -> int:
    key = _normalise(raw)
    month = _MONTHS.get(key)
    if month is None:
        raise INSEEScheduleParseError(f"unknown INSEE month name: {raw!r}")
    return month


def _quarter_end(year: int, quarter: int) -> date:
    month = quarter * 3
    return date(year, month, calendar.monthrange(year, month)[1])


def _parse_reference(label: str, spec: INSEEIndicatorSpec) -> date:
    norm = _normalise(label)
    if spec.reference_cadence == "monthly":
        match = _MONTH_RE.match(norm)
        if not match:
            raise INSEEScheduleParseError(f"unparseable INSEE month: {label!r}")
        month_raw, year_raw = match.groups()
        return date(int(year_raw), _month_number(month_raw), 1)
    if spec.reference_cadence == "quarterly":
        match = _QUARTER_RE.match(norm)
        if not match:
            raise INSEEScheduleParseError(f"unparseable INSEE quarter: {label!r}")
        quarter_raw, year_raw = match.groups()
        quarter = _QUARTERS.get(_normalise(quarter_raw))
        if quarter is None:
            raise INSEEScheduleParseError(f"unknown INSEE quarter: {label!r}")
        return _quarter_end(int(year_raw), quarter)
    raise KeyError(f"unknown INSEE cadence: {spec.reference_cadence!r}")


def _parse_event_time(raw: str) -> datetime:
    try:
        value = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise INSEEScheduleParseError(f"unparseable INSEE embargo time: {raw!r}") from exc
    if value.tzinfo is None:
        value = value.replace(tzinfo=ZoneInfo(INSEE_RELEASE_TZ))
    return value.astimezone(timezone.utc)


def parse_agenda_json(
    payload: str | bytes | dict[str, Any],
    *,
    series_ids: set[str] | None = None,
    row_issues: list[str] | None = None,
) -> list[INSEEScheduleEntry]:
    """Extract CPI/GDP rows from INSEE's publication-calendar JSON."""
    data = _coerce_payload(payload)
    docs = data.get("documents")
    if not isinstance(docs, list):
        raise INSEEScheduleParseError("INSEE agenda documents not found")
    by_family = {
        spec.family_id: spec
        for spec in INDICATOR_REGISTRY.values()
        if series_ids is None or spec.series_id in series_ids
    }
    entries: list[INSEEScheduleEntry] = []
    for doc in docs:
        if not isinstance(doc, dict):
            continue
        spec = by_family.get(_family_id(doc))
        if spec is None:
            continue
        try:
            event_time = _parse_event_time(str(doc.get("embargo") or ""))
            reference_label = str(
                doc.get("periodeReferenceEn")
                or doc.get("periodeReference")
                or ""
            )
            reference = _parse_reference(reference_label, spec)
            release_title = _release_title(doc) or spec.schedule_title_en
        except Exception as exc:
            if row_issues is not None:
                row_issues.append(f"{spec.series_id}: {type(exc).__name__}: {exc}")
                continue
            raise
        entries.append(
            INSEEScheduleEntry(
                series_id=spec.series_id,
                reference_date=reference.isoformat(),
                reference_label=reference_label,
                release_title=release_title,
                release_date=event_time.date(),
                event_time_utc=event_time.isoformat(),
                event_time_precision="datetime",
                source_url=INSEE_PUBLICATION_CALENDAR_URL,
                raw={
                    "calendar_id": doc.get("id"),
                    "family_id": spec.family_id,
                    "release_title": release_title,
                    "periodeReference": doc.get("periodeReference"),
                    "periodeReferenceEn": doc.get("periodeReferenceEn"),
                    "embargo": doc.get("embargo"),
                },
            )
        )
    entries.sort(key=lambda entry: (entry.event_time_utc, entry.series_id))
    return entries


def schedule_entry_to_records(
    entry: INSEEScheduleEntry,
    *,
    snapshot_epoch_ms: int,
    spec: INSEEIndicatorSpec | None = None,
) -> tuple[INSEECalendarRawRecord, INSEECalendarEventRecord]:
    """Project one INSEE calendar row to raw + event records."""
    resolved_spec = spec or INDICATOR_REGISTRY.get(entry.series_id)
    if resolved_spec is None:
        raise KeyError(f"series_id {entry.series_id!r} not in INSEE INDICATOR_REGISTRY")
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

    raw_record = INSEECalendarRawRecord(
        provider=PROVIDER,
        provider_event_id=provider_event_id,
        snapshot_epoch_ms=snapshot_epoch_ms,
        content_hash=content_hash,
        payload_json=payload_json,
        fetched_at=fetched_at,
    )
    event_record = INSEECalendarEventRecord(
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
        source="INSEE",
        source_url=entry.source_url,
        content_hash=content_hash,
        last_update_epoch_ms=None,
        observed_at_epoch_ms=snapshot_epoch_ms,
    )
    return raw_record, event_record


_INSEE_BROWSER_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122 Safari/537.36"
    ),
    "Accept": "application/json,text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,fr;q=0.8",
}


def fetch_agenda_json(
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
    rows: int = 200,
) -> dict[str, Any]:
    """POST INSEE's publication-calendar JSON endpoint."""
    http = session or requests.Session()
    start = 0
    documents: list[Any] = []
    num_founds: int | None = None
    base: dict[str, Any] | None = None
    while True:
        body = _agenda_payload(start=start, rows=rows)
        response = http.post(
            INSEE_AGENDA_URL,
            params={"q": "*:*"},
            json=body,
            headers=_INSEE_BROWSER_HEADERS,
            timeout=timeout,
        )
        response.raise_for_status()
        payload = response.json()
        if base is None:
            base = dict(payload)
        page_docs = payload.get("documents") or []
        if not isinstance(page_docs, list):
            raise INSEEScheduleParseError("INSEE agenda documents not found")
        documents.extend(page_docs)
        num_founds = int(payload.get("numFounds") or len(documents))
        start += len(page_docs)
        if len(page_docs) == 0 or start >= num_founds:
            break
    out = base or {}
    out["documents"] = documents
    out["numFounds"] = num_founds if num_founds is not None else len(documents)
    return out


def default_schedule_window(today: date | None = None) -> tuple[date, date]:
    """Default schedule window for INSEE's current-year calendar."""
    base = today or datetime.now(ZoneInfo(INSEE_RELEASE_TZ)).date()
    return base - timedelta(days=14), date(base.year, 12, 31)

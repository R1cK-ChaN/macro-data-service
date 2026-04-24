"""Drive France INSEE schedule and value ingestion."""

from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
import json
from typing import Any, Callable, Iterable

import requests

from .indicators import (
    INSEEIndicatorSpec,
    INDICATOR_REGISTRY,
    reference_label_en,
    press_release_url,
)
from .parser import (
    PROVIDER,
    INSEECalendarEventRecord,
    INSEECalendarRawRecord,
    parse_observation,
    parse_press_release_value,
)
from .projector import project_events, project_schedule_events, store_raw
from .schedule import (
    _INSEE_BROWSER_HEADERS,
    default_schedule_window,
    fetch_agenda_json,
    parse_agenda_json,
    schedule_entry_to_records,
)

logger = logging.getLogger(__name__)

INSEE_SOLR_URL = "https://www.insee.fr/en/solr/consultation"


@dataclass
class FetchRunSummary:
    """Outcome of one ``fetch_insee_calendar`` invocation."""

    series_planned: list[str] = field(default_factory=list)
    series_unknown: list[str] = field(default_factory=list)
    series_ok: list[str] = field(default_factory=list)
    series_empty: list[str] = field(default_factory=list)
    series_failed: list[tuple[str, str]] = field(default_factory=list)
    dry_run: bool = True
    observations_seen: int = 0
    pending_releases: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    wall_seconds: float = 0.0


@dataclass
class ScheduleRunSummary:
    """Outcome of one ``schedule_insee_calendar`` invocation."""

    series_planned: list[str] = field(default_factory=list)
    series_unknown: list[str] = field(default_factory=list)
    series_ok: list[str] = field(default_factory=list)
    series_empty: list[str] = field(default_factory=list)
    start_date: str = ""
    end_date: str = ""
    dry_run: bool = True
    entries_parsed: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    row_issues: list[str] = field(default_factory=list)
    fetch_error: str | None = None
    wall_seconds: float = 0.0


@dataclass(frozen=True)
class INSEEResolvedRelease:
    """One release page resolved from INSEE search."""

    document_id: str
    title: str
    subtitle: str
    source_url: str
    raw: dict[str, Any]


class INSEEReleaseResolutionError(ValueError):
    """Raised when INSEE search cannot resolve a due release page."""


def _resolve_series(
    series_ids: Iterable[str] | None,
) -> tuple[list[str], list[str]]:
    """Split caller-supplied ids into known + unknown registry ids."""
    if series_ids is None:
        return list(INDICATOR_REGISTRY.keys()), []
    known: list[str] = []
    unknown: list[str] = []
    for sid in series_ids:
        if sid in INDICATOR_REGISTRY:
            known.append(sid)
        else:
            unknown.append(sid)
    return known, unknown


def _coerce_date(raw: str | date | None) -> date | None:
    if raw is None or raw == "":
        return None
    if isinstance(raw, date):
        return raw
    return date.fromisoformat(str(raw)[:10])


def schedule_insee_calendar(
    connection: sqlite3.Connection,
    *,
    start_date: str | date | None = None,
    end_date: str | date | None = None,
    series_ids: Iterable[str] | None = None,
    dry_run: bool = True,
    session: requests.Session | None = None,
    snapshot_epoch_ms: int | None = None,
    agenda_fetcher: Callable[[], dict[str, Any] | str | bytes] | None = None,
) -> ScheduleRunSummary:
    """Fetch INSEE publication-calendar rows for whitelisted indicators."""
    started = time.monotonic()
    default_start, default_end = default_schedule_window()
    resolved_start = _coerce_date(start_date) or default_start
    resolved_end = _coerce_date(end_date) or default_end
    if resolved_end < resolved_start:
        resolved_start, resolved_end = resolved_end, resolved_start

    known, unknown = _resolve_series(series_ids)
    summary = ScheduleRunSummary(
        series_planned=list(known),
        series_unknown=list(unknown),
        start_date=resolved_start.isoformat(),
        end_date=resolved_end.isoformat(),
        dry_run=dry_run,
    )
    if unknown:
        logger.warning("INSEE schedule fetch: unknown series skipped: %s", unknown)
    if dry_run or not known:
        summary.wall_seconds = time.monotonic() - started
        return summary

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )
    try:
        payload = (
            agenda_fetcher()
            if agenda_fetcher is not None
            else fetch_agenda_json(session=session)
        )
        entries = parse_agenda_json(
            payload,
            series_ids=set(known),
            row_issues=summary.row_issues,
        )
    except Exception as exc:
        logger.warning("INSEE calendar fetch failed: %s", exc)
        summary.fetch_error = str(exc)
        summary.wall_seconds = time.monotonic() - started
        return summary

    entries = [
        entry for entry in entries
        if resolved_start <= entry.release_date <= resolved_end
    ]
    summary.entries_parsed = len(entries)
    hits: dict[str, int] = {sid: 0 for sid in known}
    raw_records: list[INSEECalendarRawRecord] = []
    event_records: list[INSEECalendarEventRecord] = []
    for entry in entries:
        spec = INDICATOR_REGISTRY[entry.series_id]
        raw_rec, event_rec = schedule_entry_to_records(
            entry,
            snapshot_epoch_ms=snapshot,
            spec=spec,
        )
        raw_records.append(raw_rec)
        event_records.append(event_rec)
        hits[entry.series_id] += 1

    for sid in known:
        if hits.get(sid, 0) > 0:
            summary.series_ok.append(sid)
        else:
            summary.series_empty.append(sid)
    summary.rows_raw_inserted = store_raw(connection, raw_records)
    summary.events_upserted = project_schedule_events(connection, event_records)
    summary.wall_seconds = time.monotonic() - started
    return summary


def _pending_rows(
    connection: sqlite3.Connection,
    series_ids: set[str],
    *,
    now_utc: datetime,
) -> list[sqlite3.Row]:
    titles = [INDICATOR_REGISTRY[sid].title for sid in series_ids]
    if not titles:
        return []
    placeholders = ",".join("?" for _ in titles)
    return connection.execute(
        f"""
        SELECT provider_event_id, reference_date, reference_label,
               event_time_utc, event_time_precision, source_url, title
        FROM cal_econ_event
        WHERE provider = ?
          AND actual IS NULL
          AND reference_date IS NOT NULL
          AND event_time_utc <= ?
          AND title IN ({placeholders})
        ORDER BY event_time_utc ASC
        """,
        (PROVIDER, now_utc.isoformat(), *titles),
    ).fetchall()


def _series_id_for_title(title: str) -> str | None:
    for sid, spec in INDICATOR_REGISTRY.items():
        if spec.title == title:
            return sid
    return None


def _coerce_json(payload: dict[str, Any] | str | bytes) -> dict[str, Any]:
    if isinstance(payload, dict):
        return payload
    text = payload.decode("utf-8", errors="replace") if isinstance(payload, bytes) else payload
    parsed = json.loads(text)
    if not isinstance(parsed, dict):
        raise INSEEReleaseResolutionError("INSEE search response root is not an object")
    return parsed


def _search_payload(
    spec: INSEEIndicatorSpec,
    reference_label: str,
) -> dict[str, Any]:
    return {
        "q": reference_label,
        "defType": None,
        "start": 0,
        "sortFields": [{"field": "dateDiffusion", "order": "desc"}],
        "filters": [{"field": "familleId", "values": [spec.family_id]}],
        "rows": 5,
        "facetsQuery": [],
    }


def search_release_documents(
    spec: INSEEIndicatorSpec,
    reference_label: str,
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> dict[str, Any]:
    """Resolve candidate INSEE release pages via the official search JSON."""
    http = session or requests.Session()
    body = _search_payload(spec, reference_label)
    response = http.post(
        INSEE_SOLR_URL,
        params={"q": reference_label},
        json=body,
        headers=_INSEE_BROWSER_HEADERS,
        timeout=timeout,
    )
    response.raise_for_status()
    return response.json()


def resolve_release_document(
    payload: dict[str, Any] | str | bytes,
    *,
    spec: INSEEIndicatorSpec,
    reference_label: str,
) -> INSEEResolvedRelease:
    """Select the matching INSEE release page from a search response."""
    data = _coerce_json(payload)
    docs = data.get("documents")
    if not isinstance(docs, list):
        raise INSEEReleaseResolutionError("INSEE search documents not found")
    ref_norm = _normalised_reference(reference_label)
    for doc in docs:
        if not isinstance(doc, dict):
            continue
        family = doc.get("famille") if isinstance(doc.get("famille"), dict) else {}
        if str(family.get("id") or "") != spec.family_id:
            continue
        subtitle = str(doc.get("sousTitre") or "")
        title = str(doc.get("titre") or "")
        haystack = _normalised_reference(f"{subtitle} {title}")
        if ref_norm and ref_norm not in haystack:
            continue
        document_id = str(doc.get("id") or "")
        if not document_id:
            continue
        return INSEEResolvedRelease(
            document_id=document_id,
            title=title,
            subtitle=subtitle,
            source_url=press_release_url(document_id),
            raw={
                "id": document_id,
                "titre": title,
                "sousTitre": subtitle,
                "dateDiffusion": doc.get("dateDiffusion"),
                "embargo": doc.get("embargo"),
            },
        )
    raise INSEEReleaseResolutionError(
        f"INSEE release page not found for {spec.series_id} {reference_label!r}"
    )


def _normalised_reference(label: str) -> str:
    from .parser import _normalise

    return _normalise(label)


def fetch_press_release_html(
    url: str,
    *,
    session: requests.Session | None = None,
    timeout: float = 30.0,
) -> str:
    """GET one INSEE release page."""
    http = session or requests.Session()
    response = http.get(url, headers=_INSEE_BROWSER_HEADERS, timeout=timeout)
    response.raise_for_status()
    return response.text


def fetch_insee_calendar(
    connection: sqlite3.Connection,
    *,
    series_ids: Iterable[str] | None = None,
    dry_run: bool = True,
    session: requests.Session | None = None,
    snapshot_epoch_ms: int | None = None,
    search_fetcher: (
        Callable[[INSEEIndicatorSpec, str], dict[str, Any] | str | bytes] | None
    ) = None,
    html_fetcher: Callable[[str], str] | None = None,
    now_utc: datetime | None = None,
) -> FetchRunSummary:
    """Fetch due INSEE release pages and project value rows."""
    started = time.monotonic()
    known, unknown = _resolve_series(series_ids)
    summary = FetchRunSummary(
        series_planned=list(known),
        series_unknown=list(unknown),
        dry_run=dry_run,
    )
    if unknown:
        logger.warning("INSEE value fetch: unknown series skipped: %s", unknown)
    if dry_run or not known:
        summary.wall_seconds = time.monotonic() - started
        return summary

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )
    now = now_utc or datetime.now(timezone.utc)
    pending = _pending_rows(connection, set(known), now_utc=now)
    summary.pending_releases = len(pending)
    hits: dict[str, int] = {sid: 0 for sid in known}
    raw_records: list[INSEECalendarRawRecord] = []
    event_records: list[INSEECalendarEventRecord] = []

    for row in pending:
        sid = _series_id_for_title(row["title"])
        if sid is None or sid not in known:
            continue
        spec = INDICATOR_REGISTRY[sid]
        reference_date = str(row["reference_date"])
        reference = date.fromisoformat(reference_date)
        reference_label = str(
            row["reference_label"] or reference_label_en(spec, reference)
        )
        try:
            search_payload = (
                search_fetcher(spec, reference_label)
                if search_fetcher is not None
                else search_release_documents(
                    spec,
                    reference_label,
                    session=session,
                )
            )
            resolved = resolve_release_document(
                search_payload,
                spec=spec,
                reference_label=reference_label,
            )
            payload = (
                html_fetcher(resolved.source_url)
                if html_fetcher is not None
                else fetch_press_release_html(resolved.source_url, session=session)
            )
            obs = parse_press_release_value(
                payload,
                spec=spec,
                reference_date=reference_date,
                reference_label=reference_label,
                event_time_utc=str(row["event_time_utc"]),
                event_time_precision=str(row["event_time_precision"] or "datetime"),
                source_url=resolved.source_url,
            )
            raw_rec, event_rec = parse_observation(
                obs,
                snapshot_epoch_ms=snapshot,
                spec=spec,
            )
        except Exception as exc:
            logger.warning("INSEE value fetch failed for %s: %s", sid, exc)
            summary.series_failed.append((sid, str(exc)))
            continue
        raw_records.append(raw_rec)
        event_records.append(event_rec)
        hits[sid] += 1

    for sid in known:
        if hits.get(sid, 0) > 0:
            summary.series_ok.append(sid)
        elif all(failed_sid != sid for failed_sid, _ in summary.series_failed):
            summary.series_empty.append(sid)

    summary.observations_seen = len(event_records)
    summary.rows_raw_inserted = store_raw(connection, raw_records)
    summary.events_upserted = project_events(connection, event_records)
    summary.wall_seconds = time.monotonic() - started
    return summary

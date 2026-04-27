"""Drive INEGI calendar ingestion through the calendar projection.

Per pass: one POST per **distinct** ``idPrograma`` referenced by the
known indicator set, against
``inegi.org.mx/app/api/saladeprensa/api/saladeprensa/ObtenerFechasTabla/v3``.
The response is the full release list for that programme inside the
requested date window — typically 1-2 boletines per month per
indicator. Indicators that share a programme id (CPI / INPC_15 both
key on idPrograma 2353) are post-filtered against the same response by
:func:`announcement_matches_spec`.

Window: ``today − 90 days`` to ``today + 365 days`` by default. The
backward leg keeps the connector idempotent on recently-shipped rows
(content_hash drift detection); the forward leg covers INEGI's
published lookahead — INEGI typically publishes the following year's
calendar from October onward.

The projector's ``(provider, provider_event_id)`` upsert collapses
repeated sweeps to no-ops on rows already at the latest content_hash.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Callable, Iterable

import requests

from .indicators import INDICATOR_REGISTRY, INEGIIndicatorSpec
from .parser import (
    INEGI_CALENDAR_API_URL,
    INEGICalendarEventRecord,
    INEGICalendarParseError,
    INEGICalendarRawRecord,
    announcement_matches_spec,
    announcement_to_records,
    parse_release_calendar,
)
from .projector import project_events, store_raw

logger = logging.getLogger(__name__)


_INEGI_HEADERS: dict[str, str] = {
    # INEGI's public API rejects the default Python-requests UA on
    # some request paths. A browser-shaped UA matches the workaround
    # used by the IBGE / RBI / KOSTAT / TÜİK connectors.
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64; rv:120.0) "
        "Gecko/20100101 Firefox/120.0 (macro-data-service/0.1 calendar.inegi_api)"
    ),
    "Accept": "application/json,text/javascript;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-MX,es;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate",
    "Content-Type": "application/x-www-form-urlencoded",
    "X-Requested-With": "XMLHttpRequest",
}


# Rolling window: ~90 days behind to catch any post-publication
# correction in `subtitulo` / boletín URL, plus 365 days ahead to
# cover INEGI's lookahead. Past dates go through the same idempotent
# projection — INEGI doesn't backfill, so backward sweeps are no-ops
# after the first pass.
_DEFAULT_LOOKBACK_DAYS = 90
_DEFAULT_LOOKAHEAD_DAYS = 365


@dataclass
class FetchRunSummary:
    """Outcome of one ``fetch_inegi_calendar`` invocation."""

    indicators_planned: list[str] = field(default_factory=list)
    indicators_unknown: list[str] = field(default_factory=list)
    pids_planned: list[str] = field(default_factory=list)
    fecha_desde: str = ""
    fecha_hasta: str = ""
    dry_run: bool = True
    indicators_ok: list[str] = field(default_factory=list)
    indicators_empty: list[str] = field(default_factory=list)
    pids_fetched: int = 0
    announcements_seen: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
    fetch_error: str | None = None
    wall_seconds: float = 0.0


def _resolve_indicators(
    indicators: Iterable[str] | None,
) -> tuple[list[str], list[str]]:
    if indicators is None:
        return list(INDICATOR_REGISTRY.keys()), []
    known: list[str] = []
    unknown: list[str] = []
    for ind in indicators:
        if ind in INDICATOR_REGISTRY:
            known.append(ind)
        else:
            unknown.append(ind)
    return known, unknown


def _default_window(today: date | None = None) -> tuple[date, date]:
    base = today or datetime.now(timezone.utc).date()
    fecha_desde = base - timedelta(days=_DEFAULT_LOOKBACK_DAYS)
    fecha_hasta = base + timedelta(days=_DEFAULT_LOOKAHEAD_DAYS)
    return fecha_desde, fecha_hasta


def _live_fetcher(pid: str, fecha_desde: date, fecha_hasta: date) -> list[dict]:
    body = {
        "fechaDesde":  fecha_desde.isoformat(),
        "fechaHasta":  fecha_hasta.isoformat(),
        "titulo":      "",
        "idPrograma":  pid,
        "ordenarPor":  "fecha",
        "ordenarAsc":  "1",
        "desde":       "0",
        "tomar":       "400",
        "ingles":      "0",
        "ambito":      "-1",
        "tipoNoticia": "1,2,3,4,5,6,7,8",
    }
    response = requests.post(
        INEGI_CALENDAR_API_URL,
        headers=_INEGI_HEADERS,
        data=body,
        timeout=30.0,
    )
    response.raise_for_status()
    return response.json()


def fetch_inegi_calendar(
    connection: sqlite3.Connection,
    *,
    indicators: Iterable[str] | None = None,
    fecha_desde: date | None = None,
    fecha_hasta: date | None = None,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
    json_fetcher: Callable[[str, date, date], list[dict]] | None = None,
) -> FetchRunSummary:
    """Sweep INEGI's calendar JSON and project matching releases.

    Parameters
    ----------
    connection:
        Open SQLite connection. Caller manages commit / rollback.
    indicators:
        Optional subset of registry keys; defaults to every entry.
    fecha_desde, fecha_hasta:
        Optional date window override. Default is today − 90 days to
        today + 365 days.
    dry_run:
        When ``True`` no HTTP and no row writes — returns the plan only.
    snapshot_epoch_ms:
        Fetch-time anchor for raw rows. Defaults to "now UTC".
    json_fetcher:
        Test seam — when supplied, replaces the per-pid POST. Receives
        ``(pid, fecha_desde, fecha_hasta)``; returns the parsed JSON
        body (a list of dicts).
    """
    started = time.monotonic()
    known, unknown = _resolve_indicators(indicators)
    fd, fh = _default_window()
    if fecha_desde is not None:
        fd = fecha_desde
    if fecha_hasta is not None:
        fh = fecha_hasta

    known_specs: list[tuple[str, INEGIIndicatorSpec]] = [
        (ind, INDICATOR_REGISTRY[ind]) for ind in known
    ]
    pids_planned: list[str] = []
    seen_pids: set[str] = set()
    pid_to_specs: dict[str, list[tuple[str, INEGIIndicatorSpec]]] = {}
    for ind, spec in known_specs:
        for pid in spec.tematica_ids:
            pid_to_specs.setdefault(pid, []).append((ind, spec))
            if pid not in seen_pids:
                seen_pids.add(pid)
                pids_planned.append(pid)

    summary = FetchRunSummary(
        indicators_planned=list(known),
        indicators_unknown=list(unknown),
        pids_planned=list(pids_planned),
        fecha_desde=fd.isoformat(),
        fecha_hasta=fh.isoformat(),
        dry_run=dry_run,
    )
    if dry_run or not known or not pids_planned:
        summary.wall_seconds = time.monotonic() - started
        return summary

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )
    fetcher = json_fetcher or _live_fetcher

    indicators_ok: set[str] = set()
    indicators_empty: set[str] = set(known)
    raw_records: list[INEGICalendarRawRecord] = []
    event_records: list[INEGICalendarEventRecord] = []

    for pid in pids_planned:
        try:
            payload = fetcher(pid, fd, fh)
        except Exception as exc:  # pragma: no cover — exception passthrough
            logger.warning(
                "INEGI calendar fetch failed for idPrograma=%s: %s", pid, exc,
            )
            summary.fetch_error = str(exc)
            continue
        try:
            announcements = parse_release_calendar(
                payload,
                fetched_pid=pid,
                schedule_year=fd.year,
            )
        except INEGICalendarParseError as exc:
            logger.warning(
                "INEGI calendar parse failed for idPrograma=%s: %s", pid, exc,
            )
            summary.fetch_error = str(exc)
            continue
        summary.pids_fetched += 1

        specs_for_pid = pid_to_specs.get(pid, [])
        for announcement in announcements:
            for ind, spec in specs_for_pid:
                if not announcement_matches_spec(announcement, spec):
                    continue
                try:
                    raw_rec, event_rec = announcement_to_records(
                        announcement, spec=spec, snapshot_epoch_ms=snapshot,
                    )
                except (INEGICalendarParseError, ValueError, KeyError) as exc:
                    logger.warning(
                        "INEGI projection failed for %s on pid=%s "
                        "fecha=%s: %s",
                        ind, pid, announcement.fecha, exc,
                    )
                    continue
                raw_records.append(raw_rec)
                event_records.append(event_rec)
                indicators_ok.add(ind)
                indicators_empty.discard(ind)
                # First matched indicator wins for a given row — INPC
                # vs INPC_15 are mutually exclusive by the cadence
                # filter, Trade Balance variants by ``programa``
                # substring, so the inner loop is safe to short-circuit.
                break

    summary.indicators_ok = sorted(indicators_ok)
    summary.indicators_empty = sorted(indicators_empty)
    summary.announcements_seen = len(event_records)
    summary.rows_raw_inserted = store_raw(connection, raw_records)
    summary.events_upserted = project_events(connection, event_records)
    summary.wall_seconds = time.monotonic() - started
    return summary


__all__ = ["FetchRunSummary", "fetch_inegi_calendar"]

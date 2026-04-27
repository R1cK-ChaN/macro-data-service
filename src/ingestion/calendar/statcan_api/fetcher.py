"""Drive StatCan WDS ingestion through the calendar projection.

For each indicator in :data:`INDICATOR_REGISTRY`, hit the WDS
``getDataFromVectorsAndLatestNPeriods`` endpoint, parse the
latest observation, and project both the schedule timestamp and
the headline value into ``cal_econ_event``. Schedule and value
land together — StatCan does not expose a separate forward
calendar API; the ``releaseTime`` on each observation is the
publication anchor.

One POST per fetch — the WDS endpoint accepts up to 300
``(vectorId, latestN)`` tuples per call, so the entire P1
indicator set lands in a single round-trip.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Iterable

import requests

from .indicators import INDICATOR_REGISTRY, StatCanIndicatorSpec
from .parser import (
    STATCAN_WDS_BASE_URL,
    StatCanCalendarEventRecord,
    StatCanCalendarRawRecord,
    StatCanWDSParseError,
    parse_vector_response,
    value_observation_to_records,
)
from .projector import project_events, store_raw

logger = logging.getLogger(__name__)


_STATCAN_HEADERS: dict[str, str] = {
    "User-Agent": "macro-data-service/0.1 (calendar.statcan_api)",
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Accept-Encoding": "gzip, deflate",
}

_WDS_ENDPOINT = (
    f"{STATCAN_WDS_BASE_URL}/getDataFromVectorsAndLatestNPeriods"
)
# Latest-N window. ``latestN=4`` covers the four most recent
# observations for monthly series — enough headroom for the
# parser's "skip suppressed rows" pass without ballooning payload
# size on every fetch.
_LATEST_N = 4


@dataclass
class FetchRunSummary:
    """Outcome of one ``fetch_statcan_calendar`` invocation."""

    indicators_planned: list[str] = field(default_factory=list)
    indicators_unknown: list[str] = field(default_factory=list)
    dry_run: bool = True
    indicators_ok: list[str] = field(default_factory=list)
    indicators_empty: list[str] = field(default_factory=list)
    series_failed: list[tuple[str, str]] = field(default_factory=list)
    observations_seen: int = 0
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


def _live_fetcher(specs: list[StatCanIndicatorSpec]) -> str:
    """Default network fetcher — one POST batches every requested vector."""
    body = [
        {"vectorId": spec.vector_id, "latestN": _LATEST_N}
        for spec in specs
    ]
    response = requests.post(
        _WDS_ENDPOINT,
        headers=_STATCAN_HEADERS,
        data=json.dumps(body),
        timeout=30.0,
    )
    response.raise_for_status()
    return response.text


def fetch_statcan_calendar(
    connection: sqlite3.Connection,
    *,
    indicators: Iterable[str] | None = None,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
    json_fetcher: Callable[[list[StatCanIndicatorSpec]], str] | None = None,
) -> FetchRunSummary:
    """Sweep StatCan headline indicators into ``cal_econ_event``.

    Parameters
    ----------
    connection:
        Open SQLite connection. Caller manages commit / rollback.
    indicators:
        Optional subset of registry keys; defaults to every entry.
    dry_run:
        When ``True`` no HTTP and no row writes — returns the plan only.
    snapshot_epoch_ms:
        Fetch-time anchor for raw rows. Defaults to "now UTC".
    json_fetcher:
        Test seam — when supplied, replaces the HTTP POST. Receives
        the list of specs; returns the JSON body text.
    """
    started = time.monotonic()
    known, unknown = _resolve_indicators(indicators)
    summary = FetchRunSummary(
        indicators_planned=list(known),
        indicators_unknown=list(unknown),
        dry_run=dry_run,
    )
    if dry_run or not known:
        summary.wall_seconds = time.monotonic() - started
        return summary

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )
    fetcher = json_fetcher or _live_fetcher
    specs = [INDICATOR_REGISTRY[indicator] for indicator in known]

    try:
        text = fetcher(specs)
    except requests.exceptions.RequestException as exc:
        logger.warning("StatCan WDS request failed: %s", exc)
        summary.fetch_error = str(exc)
        for indicator in known:
            summary.series_failed.append((indicator, str(exc)))
            summary.indicators_empty.append(indicator)
        summary.wall_seconds = time.monotonic() - started
        return summary
    except Exception as exc:
        logger.warning("StatCan WDS request errored: %s", exc)
        summary.fetch_error = str(exc)
        for indicator in known:
            summary.series_failed.append((indicator, str(exc)))
            summary.indicators_empty.append(indicator)
        summary.wall_seconds = time.monotonic() - started
        return summary

    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        logger.warning("StatCan WDS returned invalid JSON: %s", exc)
        summary.fetch_error = str(exc)
        for indicator in known:
            summary.series_failed.append((indicator, str(exc)))
            summary.indicators_empty.append(indicator)
        summary.wall_seconds = time.monotonic() - started
        return summary

    raw_records: list[StatCanCalendarRawRecord] = []
    event_records: list[StatCanCalendarEventRecord] = []
    for indicator in known:
        spec = INDICATOR_REGISTRY[indicator]
        try:
            obs = parse_vector_response(payload, spec=spec)
            raw_rec, event_rec = value_observation_to_records(
                obs, snapshot_epoch_ms=snapshot, spec=spec,
            )
        except (StatCanWDSParseError, ValueError, KeyError) as exc:
            logger.warning(
                "StatCan parse failed for %s: %s", indicator, exc,
            )
            summary.series_failed.append((indicator, str(exc)))
            continue
        raw_records.append(raw_rec)
        event_records.append(event_rec)
        summary.indicators_ok.append(indicator)

    for indicator in known:
        if indicator not in summary.indicators_ok:
            summary.indicators_empty.append(indicator)

    summary.observations_seen = len(event_records)
    summary.rows_raw_inserted = store_raw(connection, raw_records)
    summary.events_upserted = project_events(connection, event_records)
    summary.wall_seconds = time.monotonic() - started
    return summary


__all__ = ["FetchRunSummary", "fetch_statcan_calendar"]

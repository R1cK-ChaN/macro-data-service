"""Drive ONS timeseries ingestion through the calendar projection.

For each indicator in :data:`INDICATOR_REGISTRY`, fetch the public
JSON timeseries, parse the latest observation, and project both
the schedule timestamp and the headline value into
``cal_econ_event``. Schedule and value land together — ONS does
not expose a separate forward calendar API; the
``updateDate`` on each observation is the publication anchor.
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

from .indicators import INDICATOR_REGISTRY, ONSIndicatorSpec
from .parser import (
    ONS_BASE_URL,
    ONSCalendarEventRecord,
    ONSCalendarRawRecord,
    ONSTimeseriesParseError,
    parse_timeseries_json,
    value_observation_to_records,
)
from .projector import project_events, store_raw

logger = logging.getLogger(__name__)


# ONS doesn't gate the public timeseries endpoint behind a UA
# challenge, but a polite identifier helps when the data team
# audits server logs. Same shape as the FRED / Eurostat default
# headers used elsewhere in this repo.
_ONS_HEADERS: dict[str, str] = {
    "User-Agent": "macro-data-service/0.1 (calendar.ons_api)",
    "Accept": "application/json",
    "Accept-Encoding": "gzip, deflate",
}


@dataclass
class FetchRunSummary:
    """Outcome of one ``fetch_ons_calendar`` invocation."""

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


def _build_url(spec: ONSIndicatorSpec) -> str:
    return (
        f"{ONS_BASE_URL}/{spec.path}/timeseries/{spec.ts_id}/"
        f"{spec.dataset_id}/data"
    )


def _live_fetcher(spec: ONSIndicatorSpec) -> str:
    """Default network fetcher — one GET per indicator."""
    response = requests.get(_build_url(spec), headers=_ONS_HEADERS, timeout=30.0)
    response.raise_for_status()
    return response.text


def fetch_ons_calendar(
    connection: sqlite3.Connection,
    *,
    indicators: Iterable[str] | None = None,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
    json_fetcher: Callable[[ONSIndicatorSpec], str] | None = None,
) -> FetchRunSummary:
    """Sweep ONS headline indicators into ``cal_econ_event``.

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
        Test seam — when supplied, replaces the HTTP GET. Receives
        the spec; returns the JSON body text.
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

    raw_records: list[ONSCalendarRawRecord] = []
    event_records: list[ONSCalendarEventRecord] = []
    for indicator in known:
        spec = INDICATOR_REGISTRY[indicator]
        try:
            text = fetcher(spec)
        except requests.exceptions.RequestException as exc:
            logger.warning("ONS fetch failed for %s: %s", indicator, exc)
            summary.series_failed.append((indicator, str(exc)))
            continue
        except Exception as exc:
            logger.warning("ONS fetch errored for %s: %s", indicator, exc)
            summary.series_failed.append((indicator, str(exc)))
            continue
        try:
            obs = parse_timeseries_json(text, spec=spec)
            raw_rec, event_rec = value_observation_to_records(
                obs, snapshot_epoch_ms=snapshot, spec=spec,
            )
        except (ONSTimeseriesParseError, ValueError, KeyError, json.JSONDecodeError) as exc:
            logger.warning("ONS parse failed for %s: %s", indicator, exc)
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


__all__ = ["FetchRunSummary", "fetch_ons_calendar"]

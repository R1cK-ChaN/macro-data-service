"""Drive EIA v2 weekly-stocks ingestion through the calendar projection.

EIA publishes weekly energy stocks via the open ``api.eia.gov/v2``
JSON API; ``EIA_API_KEY`` is the only auth surface (already used by
the timeseries scrapers). This connector pulls the latest N weeks
of observations for each whitelisted indicator and writes both the
schedule row (event_time_utc derived from the period) and the
``actual`` value in one pass — schedule and value are not separate
sides for EIA because the JSON response carries both.

Nothing auto-runs: callers construct an ``EIAClient``, pick their
date window (default: last 60 days, comfortably covering the
schedule-aware burst plus the daily catch-up sweep), and invoke
``fetch_eia_calendar``. A dry-run path returns the indicator plan
without issuing any HTTP request.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable

from ingestion.timeseries.scrapers.eia import EIAClient, EIAObservation as EIATimeseriesObservation

from .indicators import INDICATOR_REGISTRY, EIAIndicatorSpec
from .parser import (
    EIACalendarEventRecord,
    EIACalendarRawRecord,
    EIAObservation,
    observation_to_records,
)
from .projector import project_events, store_raw

logger = logging.getLogger(__name__)


@dataclass
class FetchRunSummary:
    """Outcome of one ``fetch_eia_calendar`` invocation."""

    indicators_planned: list[str] = field(default_factory=list)
    indicators_unknown: list[str] = field(default_factory=list)
    indicators_ok: list[str] = field(default_factory=list)
    indicators_empty: list[str] = field(default_factory=list)
    series_failed: list[tuple[str, str]] = field(default_factory=list)
    dry_run: bool = True
    start: str = ""
    end: str = ""
    observations_seen: int = 0
    rows_raw_inserted: int = 0
    events_upserted: int = 0
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


def fetch_eia_calendar(
    connection: sqlite3.Connection,
    client: EIAClient,
    *,
    start: str | None = None,
    end: str | None = None,
    indicators: Iterable[str] | None = None,
    dry_run: bool = True,
    snapshot_epoch_ms: int | None = None,
    base_url: str | None = None,
) -> FetchRunSummary:
    """Pull EIA weekly observations and project them onto the calendar.

    Parameters
    ----------
    connection:
        Open SQLite connection. Caller commits / rolls back.
    client:
        Authenticated :class:`EIAClient`. Tests inject a fake.
    start:
        ISO date — earliest period to fetch. Defaults to ``today − 60``
        days, comfortably covering the burst window plus the daily
        catch-up. EIA's API doesn't publish a forward calendar, so
        the period column is the only anchor.
    end:
        ISO date — latest period. Defaults to today.
    indicators:
        Optional subset. Unknown ids land on
        ``summary.indicators_unknown`` and are skipped.
    dry_run:
        When ``True`` (default) no HTTP, no DB writes.
    """
    started = time.monotonic()
    known, unknown = _resolve_indicators(indicators)
    today = date.today()
    resolved_start = start or (today - timedelta(days=60)).isoformat()
    resolved_end = end or today.isoformat()
    summary = FetchRunSummary(
        indicators_planned=list(known),
        indicators_unknown=list(unknown),
        dry_run=dry_run,
        start=resolved_start,
        end=resolved_end,
    )
    if unknown:
        logger.warning("EIA fetch: unknown indicators skipped: %s", unknown)
    if dry_run or not known:
        summary.wall_seconds = time.monotonic() - started
        return summary

    snapshot = snapshot_epoch_ms or int(
        datetime.now(timezone.utc).timestamp() * 1000
    )
    raw_records: list[EIACalendarRawRecord] = []
    event_records: list[EIACalendarEventRecord] = []
    base = base_url or "https://api.eia.gov/v2"

    for indicator in known:
        spec = INDICATOR_REGISTRY[indicator]
        try:
            observations = _fetch_observations(
                client, spec,
                start=resolved_start, end=resolved_end,
            )
        except Exception as exc:
            logger.warning("EIA fetch failed for %s: %s", indicator, exc)
            summary.series_failed.append((indicator, str(exc)))
            continue
        if not observations:
            summary.indicators_empty.append(indicator)
            continue
        summary.indicators_ok.append(indicator)
        for obs in observations:
            try:
                raw_rec, event_rec = observation_to_records(
                    obs,
                    snapshot_epoch_ms=snapshot,
                    spec=spec,
                    base_url=base,
                )
            except (KeyError, ValueError) as exc:
                summary.series_failed.append(
                    (indicator, f"projection: {exc}"),
                )
                continue
            raw_records.append(raw_rec)
            event_records.append(event_rec)

    summary.observations_seen = len(event_records)
    summary.rows_raw_inserted = store_raw(connection, raw_records)
    summary.events_upserted = project_events(connection, event_records)
    summary.wall_seconds = time.monotonic() - started
    return summary


def _fetch_observations(
    client: EIAClient,
    spec: EIAIndicatorSpec,
    *,
    start: str,
    end: str,
) -> list[EIAObservation]:
    """Hit one EIA v2 endpoint and turn the response into observations.

    Reuses :class:`EIAClient.get_series` for the HTTP + retry / rate-
    limit handling. The route path mirrors the EIA documentation
    (``petroleum/sum/sndw/data/`` etc.); facets pin to one series so
    the response carries one row per period.
    """
    params: dict[str, Any] = {
        "frequency": "weekly",
        "data[]": "value",
        "end": end,
    }
    for facet, value in spec.facets:
        params[f"facets[{facet}][]"] = value

    raw = client.get_series(
        spec.route.rstrip("/"),
        params=params,
        series_id=spec.series_id,
        start=start,
        limit=120,
    )
    out: list[EIAObservation] = []
    for ts_obs in raw:
        if not ts_obs.date:
            continue
        out.append(
            EIAObservation(
                indicator=spec.indicator,
                period=ts_obs.date,
                value=_format_value(ts_obs.value),
                unit=ts_obs.unit or "",
                raw={"date": ts_obs.date, "value": ts_obs.value, "unit": ts_obs.unit},
            )
        )
    return out


def _format_value(value: Any) -> str:
    """Render a numeric value as the canonical ``cal_econ_event.actual`` string.

    Integers stay integer (``"464717"``); floats keep up to four
    fractional digits. Mirrors the rendering BLS / BEA already use
    so downstream comparators see consistent shapes.
    """
    if isinstance(value, int):
        return str(value)
    f = float(value)
    if float(int(f)) == f:
        return str(int(f))
    return f"{f:.4f}".rstrip("0").rstrip(".")


__all__ = ["FetchRunSummary", "fetch_eia_calendar"]

"""Recurring schedule refresh across every official-source connector.

The target end-state for issue #9 (item #1) — "every US / EU / China
headline release we care about lands in ``cal_econ_event`` from its
official source within minutes of publication" — needs the per-
connector ops to run on a recurring cron, not by hand.

This module is the **schedule-side half** of that story (P-sched-1):
a driver that invokes every connector's forward-looking schedule
scrape (BLS / BEA / ECB / Fed FOMC / Fed releasedates / NBS) in
sequence, with per-connector error isolation so one upstream outage
doesn't block the rest. The value-side sweep that fills ``actual``
once each release crosses its scheduled time is a follow-up slice.

Each connector gets its own connection lifecycle so a failure inside
one connector rolls back only that connector's partial writes — the
remaining connectors still commit. This is the correct semantics for
independent per-source caches: an NBS HTTP 403 shouldn't undo the
BLS schedule refresh that just succeeded.

Nothing runs automatically from this module — a caller (the
cron-scheduled service op :func:`.service._op_calendar_econ_refresh_schedules`
or a future operator-driven trigger) invokes :func:`refresh_all_schedules`.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable

from .bea_api import schedule_bea_calendar
from .bls_api import schedule_bls_calendar
from .ecb_api import schedule_ecb_calendar
from .fed_api import fetch_fed_calendar, fetch_fed_releasedates
from .nbs_api import fetch_nbs_calendar

logger = logging.getLogger(__name__)


# Connector identifier — also the key used in summary dicts and the
# ``connectors=[…]`` subset argument on the service op.
ConnectorName = str

_ConnectorFn = Callable[[sqlite3.Connection, bool], Any]


def _bls(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return schedule_bls_calendar(conn, dry_run=dry_run)


def _bea(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return schedule_bea_calendar(conn, dry_run=dry_run)


def _ecb(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return schedule_ecb_calendar(conn, dry_run=dry_run)


def _fed_fomc(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return fetch_fed_calendar(conn, dry_run=dry_run)


def _fed_releases(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return fetch_fed_releasedates(conn, dry_run=dry_run)


def _nbs(conn: sqlite3.Connection, dry_run: bool) -> Any:
    # ``calendar_url=None`` triggers the auto-discovery path added in
    # P5a: the fetcher resolves the current year's article from the
    # NBS release-calendar index before scraping it.
    return fetch_nbs_calendar(conn, calendar_url=None, dry_run=dry_run)


# Sequencing matters for operator inspection — BLS first (highest
# trader impact, cheapest surface), Fed / ECB in the middle, NBS last
# (most upstream-fragile so a failure there is easiest to triage at
# the end of the log).
_DEFAULT_CONNECTORS: tuple[tuple[ConnectorName, _ConnectorFn], ...] = (
    ("bls", _bls),
    ("bea", _bea),
    ("ecb", _ecb),
    ("fed-fomc", _fed_fomc),
    ("fed-releases", _fed_releases),
    ("nbs", _nbs),
)

ALL_CONNECTORS: tuple[ConnectorName, ...] = tuple(
    name for name, _ in _DEFAULT_CONNECTORS
)


@dataclass
class ConnectorResult:
    """Per-connector outcome of a single scheduler pass."""

    connector: ConnectorName
    ok: bool
    error: str | None = None
    summary: dict[str, Any] = field(default_factory=dict)
    wall_seconds: float = 0.0


@dataclass
class RefreshRunSummary:
    """Aggregate outcome of a :func:`refresh_all_schedules` pass."""

    connectors_planned: list[ConnectorName] = field(default_factory=list)
    dry_run: bool = True
    results: list[ConnectorResult] = field(default_factory=list)
    unknown_connectors: list[ConnectorName] = field(default_factory=list)
    wall_seconds: float = 0.0

    @property
    def ok_count(self) -> int:
        return sum(1 for r in self.results if r.ok)

    @property
    def failed_count(self) -> int:
        # Unknown connector names count as failures so a cron/operator
        # typo surfaces in the top-level envelope rather than silently
        # skipping the intended source.
        return sum(1 for r in self.results if not r.ok) + len(
            self.unknown_connectors
        )


def _summary_to_dict(summary: Any) -> dict[str, Any]:
    """Normalize a connector's RunSummary into a JSON-serializable dict.

    Connectors return dataclass instances with heterogeneous fields;
    this flattens the public surface into a dict of primitives so the
    service op can forward the result without importing every per-
    connector dataclass type.
    """
    if summary is None:
        return {}
    out: dict[str, Any] = {}
    for field_name, value in vars(summary).items():
        if field_name.startswith("_"):
            continue
        out[field_name] = value
    return out


def _summary_failure_reason(summary: Any) -> str | None:
    """Detect connector-level fetch failures stashed inside the summary.

    Connectors catch their HTTP exception and record it on the
    summary rather than propagating, so the driver would otherwise
    report a clean run when a source contributed zero fresh rows:

    - BEA / Fed-releasedates set ``fetch_error`` on a 502.
    - ECB collects per-page failures in ``fetch_errors``.
    - BLS collects per-series failures in ``series_failed`` (tuples
      of ``(series_id, reason)``) — a bls.gov 403 / layout drift lands
      every series there.

    Any non-empty marker flags ``ok=False`` so the top-level envelope
    surfaces the outage. Partial failures (one series out of nine)
    also register as non-ok so the operator notices rather than
    finding the loss weeks later; per-series detail stays visible in
    the summary dict.
    """
    if summary is None:
        return None
    fetch_error = getattr(summary, "fetch_error", None)
    if fetch_error:
        return str(fetch_error)
    fetch_errors = getattr(summary, "fetch_errors", None)
    if fetch_errors:
        return str(fetch_errors)
    series_failed = getattr(summary, "series_failed", None)
    if series_failed:
        count = len(series_failed)
        first = series_failed[0]
        return f"{count} series failed (e.g. {first})"
    return None


def refresh_all_schedules(
    connection_factory: Callable[[], sqlite3.Connection],
    *,
    dry_run: bool = True,
    connectors: Iterable[ConnectorName] | None = None,
    _connector_overrides: (
        dict[ConnectorName, _ConnectorFn] | None
    ) = None,
) -> RefreshRunSummary:
    """Refresh every connector's forward-looking schedule rows.

    Parameters
    ----------
    connection_factory:
        Callable returning a fresh :class:`sqlite3.Connection`. Each
        connector gets its own connection; the driver commits on
        success and rolls back on exception so partial success across
        connectors is the default.
    dry_run:
        When ``True`` (default) no HTTP call is made and no row is
        written; each per-connector dry-run returns its indicator /
        series plan so the caller can inspect the scope.
    connectors:
        Optional subset of connector names to run. Omit (or pass the
        full tuple) to run all six. Order follows
        :data:`_DEFAULT_CONNECTORS`.
    _connector_overrides:
        Test seam — swaps in fake per-connector functions so unit
        tests can exercise the isolation + aggregation logic without
        hitting real upstreams. Production callers omit this.
    """
    started = time.monotonic()
    requested = (
        tuple(connectors) if connectors is not None else ALL_CONNECTORS
    )
    valid_names = set(ALL_CONNECTORS)
    # A caller typo (``"fed-fomcc"``) would otherwise silently drop
    # below — the plan builds from ``_DEFAULT_CONNECTORS`` membership.
    # Keep the unknown names so the top-level summary surfaces them
    # and ``failed_count`` reflects the skipped source.
    unknown_connectors = [n for n in requested if n not in valid_names]
    selected_set = {n for n in requested if n in valid_names}
    overrides = _connector_overrides or {}

    plan: list[tuple[ConnectorName, _ConnectorFn]] = []
    for name, fn in _DEFAULT_CONNECTORS:
        if name not in selected_set:
            continue
        plan.append((name, overrides.get(name, fn)))

    run_summary = RefreshRunSummary(
        connectors_planned=[name for name, _ in plan],
        dry_run=dry_run,
        unknown_connectors=unknown_connectors,
    )

    for name, fn in plan:
        connector_started = time.monotonic()
        connection = connection_factory()
        try:
            summary = fn(connection, dry_run)
            if not dry_run:
                connection.commit()
        except Exception as exc:
            try:
                connection.rollback()
            except Exception:
                pass
            logger.warning(
                "calendar schedule refresh failed for %s: %s", name, exc,
            )
            run_summary.results.append(
                ConnectorResult(
                    connector=name,
                    ok=False,
                    error=str(exc),
                    wall_seconds=round(time.monotonic() - connector_started, 3),
                )
            )
            continue
        finally:
            try:
                connection.close()
            except Exception:
                pass

        failure_reason = _summary_failure_reason(summary)
        run_summary.results.append(
            ConnectorResult(
                connector=name,
                ok=failure_reason is None,
                error=failure_reason,
                summary=_summary_to_dict(summary),
                wall_seconds=round(time.monotonic() - connector_started, 3),
            )
        )

    run_summary.wall_seconds = time.monotonic() - started
    return run_summary

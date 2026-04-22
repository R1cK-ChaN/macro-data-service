"""Recurring schedule + value-side refresh across official-source connectors.

The target end-state for issue #9 (item #1) — "every US / EU / China
headline release we care about lands in ``cal_econ_event`` from its
official source within minutes of publication" — needs the per-
connector ops to run on a recurring cron, not by hand.

Two driver entry points, one shared per-connector loop:

- :func:`refresh_all_schedules` (P-sched-1) — schedule-side: invokes
  every connector's forward-looking schedule scrape (BLS / BEA / ECB
  / Fed FOMC / Fed releasedates / NBS). Daily-cron candidate.
- :func:`sweep_value_side` (P-sched-2) — value-side: invokes every
  connector's value-bearing scrape (BLS / BEA / ECB / Fed-values).
  NBS has no value-side op yet and is not in the plan. Frequent-cron
  candidate — repeatedly runs to pick up new values once the release
  crosses its scheduled time, so the calendar's ``actual`` fills
  within minutes of publication.

Each connector gets its own connection lifecycle so a failure inside
one connector rolls back only that connector's partial writes — the
remaining connectors still commit. This is the correct semantics for
independent per-source caches: an NBS HTTP 403 shouldn't undo the
BLS schedule refresh that just succeeded.

Nothing runs automatically from this module — a caller (the
cron-scheduled service op
:func:`.service._op_calendar_econ_refresh_schedules` /
:func:`.service._op_calendar_econ_sweep_values` or a future
operator-driven trigger) invokes the entry points.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable

# Default ECB window: ~180 days back in ``"YYYY-MM"`` SDMX form.
# The ECB FM dataflow only publishes observations on Governing
# Council meeting days (~8 per year), so six months reliably covers
# the last three-to-four policy decisions. Without a bound, each
# frequent-cron sweep would issue an unbounded SDMX request and
# re-project the full ECB history — wasteful on bandwidth and on
# the projector's merge CASE.
_ECB_CRON_WINDOW_DAYS = 180

from .bea_api import fetch_bea_calendar, schedule_bea_calendar
from .bls_api import fetch_bls_calendar, schedule_bls_calendar
from .ecb_api import fetch_ecb_calendar, schedule_ecb_calendar
from .fed_api import (
    fetch_fed_calendar,
    fetch_fed_releasedates,
    fetch_fed_statement_values,
)
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


# Value-side connector identifiers. NBS is absent — it has no
# value-side op (the yearly calendar is schedule-only; per-release
# value scraping for CPI / PPI / etc. is a future slice). Fed's FOMC
# calendar doesn't belong here either — the value-bearing Fed op is
# ``fetch_fed_statement_values`` exposed as ``fed-values`` below.
ALL_VALUE_SIDE_CONNECTORS: tuple[ConnectorName, ...] = (
    "bls", "bea", "ecb", "fed-values",
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
    - Fed-values collects per-URL failures in ``fetch_failures`` /
      ``parse_failures`` (tuples of ``(closing_iso, reason)``). A 404
      on a single statement page or an upstream layout drift lands
      there without raising.

    Any non-empty marker flags ``ok=False`` so the top-level envelope
    surfaces the outage. Partial failures (one page out of eight)
    also register as non-ok so the operator notices rather than
    finding the loss weeks later; per-item detail stays visible in
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
    fetch_failures = getattr(summary, "fetch_failures", None)
    if fetch_failures:
        count = len(fetch_failures)
        first = fetch_failures[0]
        return f"{count} fetch failures (e.g. {first})"
    parse_failures = getattr(summary, "parse_failures", None)
    if parse_failures:
        count = len(parse_failures)
        first = parse_failures[0]
        return f"{count} parse failures (e.g. {first})"
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


# ──────────────────────────────────────────────────────────────────────────
# Value-side sweep (P-sched-2)
# ──────────────────────────────────────────────────────────────────────────


def sweep_value_side(
    connection_factory: Callable[[], sqlite3.Connection],
    *,
    dry_run: bool = True,
    start_year: int | None = None,
    end_year: int | None = None,
    start_period: str | None = None,
    end_period: str | None = None,
    connectors: Iterable[ConnectorName] | None = None,
    _connector_overrides: (
        dict[ConnectorName, _ConnectorFn] | None
    ) = None,
) -> RefreshRunSummary:
    """Invoke every value-side connector to fill ``actual`` on recent rows.

    Frequent-cron candidate — repeatedly runs to pick up new values
    once a release crosses its scheduled time, so a BLS CPI 08:30 ET
    release lands on the calendar within minutes of publication.

    Parameters
    ----------
    connection_factory:
        Callable returning a fresh :class:`sqlite3.Connection`. Each
        connector gets its own connection; commit on success, rollback
        on exception, so partial success across connectors is allowed.
    dry_run:
        When ``True`` (default) no HTTP and no DB writes. Connector
        dry-runs return their plan.
    start_year / end_year:
        Year window passed through to BLS / BEA value-side ops (which
        require it). Default: ``(current_year − 1, current_year)`` — a
        two-year window large enough that the freshness guard still
        skips recomputing settled rows while any just-published
        observation lands.
    start_period / end_period:
        Optional SDMX-period strings forwarded to ECB. ``None`` means
        the ECB op picks its own window.
    connectors:
        Optional subset of :data:`ALL_VALUE_SIDE_CONNECTORS`. Omit to
        run all four.
    _connector_overrides:
        Test seam — swap in fake functions to exercise isolation +
        aggregation without hitting real upstreams.

    Operator note: BLS and BEA need API-key env vars
    (``BLS_API_KEY`` / ``BEA_API_KEY``). A missing key raises from the
    per-connector shim and lands as ``ok=False`` on the result rather
    than aborting the sweep — the other connectors still run.
    """
    started = time.monotonic()
    now_utc = datetime.now(timezone.utc)
    resolved_start_year = start_year if start_year is not None else now_utc.year - 1
    resolved_end_year = end_year if end_year is not None else now_utc.year
    resolved_start_period = start_period if start_period is not None else (
        (now_utc - timedelta(days=_ECB_CRON_WINDOW_DAYS)).strftime("%Y-%m")
    )
    resolved_end_period = end_period  # ``None`` → ECB client defaults to "latest"

    def _bls_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        # Dry-run doesn't issue HTTP, so we defer the client construction
        # + API-key check to execute mode. Calling the library function
        # with ``dry_run=True`` returns the plan regardless of auth.
        from ingestion.timeseries.scrapers.bls import BLSClient
        client = BLSClient()
        if not dry_run and not client.api_key:
            raise RuntimeError("BLS_API_KEY not set")
        return fetch_bls_calendar(
            conn, client,
            start_year=resolved_start_year,
            end_year=resolved_end_year,
            dry_run=dry_run,
        )

    def _bea_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        from ingestion.timeseries.scrapers.bea import BEAClient
        client = BEAClient()
        if not dry_run and not client.api_key:
            raise RuntimeError("BEA_API_KEY not set")
        return fetch_bea_calendar(
            conn, client,
            start_year=resolved_start_year,
            end_year=resolved_end_year,
            dry_run=dry_run,
        )

    def _ecb_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        # ECB SDMX has no auth — plain HTTP against data-api.ecb.europa.eu.
        from ingestion.timeseries.sdmx.providers.ecb import ECBClient
        client = ECBClient()
        return fetch_ecb_calendar(
            conn, client,
            start_period=resolved_start_period,
            end_period=resolved_end_period,
            dry_run=dry_run,
        )

    def _fed_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        # fetch_fed_statement_values auto-discovers past FOMC rows
        # with ``actual IS NULL`` — no year window needed.
        return fetch_fed_statement_values(conn, dry_run=dry_run)

    value_side_map: dict[ConnectorName, _ConnectorFn] = {
        "bls":        _bls_values,
        "bea":        _bea_values,
        "ecb":        _ecb_values,
        "fed-values": _fed_values,
    }

    requested = (
        tuple(connectors) if connectors is not None else ALL_VALUE_SIDE_CONNECTORS
    )
    valid_names = set(ALL_VALUE_SIDE_CONNECTORS)
    unknown_connectors = [n for n in requested if n not in valid_names]
    selected_set = {n for n in requested if n in valid_names}
    overrides = _connector_overrides or {}

    plan: list[tuple[ConnectorName, _ConnectorFn]] = []
    for name in ALL_VALUE_SIDE_CONNECTORS:
        if name not in selected_set:
            continue
        plan.append((name, overrides.get(name, value_side_map[name])))

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
                "calendar value-side sweep failed for %s: %s", name, exc,
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

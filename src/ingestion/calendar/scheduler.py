"""Recurring schedule + value-side refresh across official-source connectors.

The target end-state for issue #9 (item #1) — "every US / EU / China
headline release we care about lands in ``cal_econ_event`` from its
official source within minutes of publication" — needs the per-
connector ops to run on a recurring cron, not by hand.

Two driver entry points, one shared per-connector loop:

- :func:`refresh_all_schedules` (P-sched-1) — schedule-side: invokes
  every connector's forward-looking schedule scrape (BLS / BEA / Census
  / ISM / U Michigan / Conference Board / NAR / ECB / Eurostat / Destatis / INE / Fed FOMC /
  Fed releasedates / NBS / Statistics Bureau JP / BoJ / BoJ Tankan / MoF JP / CAO /
  CAO GDP / METI).
  Daily-cron candidate.
- :func:`sweep_value_side` (P-sched-2) — value-side: invokes every
  connector's value-bearing scrape (BLS / BEA / Census / ISM /
  U Michigan / Conference Board / NAR / ECB / Eurostat / Destatis / INE / Fed-values / BoJ-values /
  Statistics Bureau JP-values / BoJ Tankan-values / MoF JP-values /
  CAO-values / CAO GDP-values / METI-values).
  Frequent-cron candidate — repeatedly runs to pick up new values once
  the release crosses its scheduled time, so the calendar's ``actual``
  fills within minutes of publication.

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
from .boj_api import fetch_boj_calendar, fetch_boj_statement_values
from .boj_tankan_api import (
    fetch_boj_tankan_calendar,
    fetch_boj_tankan_outlines,
)
from .cao_api import (
    fetch_cao_calendar,
    fetch_cao_consumer_confidence_values,
)
from .cao_gdp_api import fetch_cao_gdp_calendar, fetch_cao_gdp_values
from .mof_api import fetch_mof_calendar, fetch_mof_trade_values
from .meti_api import fetch_meti_calendar, fetch_meti_values
from .stat_bureau_api import fetch_stat_bureau_calendar, fetch_stat_bureau_values
from .census_api import fetch_census_calendar, schedule_census_calendar
from .conference_board_api import (
    fetch_conference_board_calendar,
    schedule_conference_board_calendar,
)
from .ecb_api import fetch_ecb_calendar, schedule_ecb_calendar
from .eurostat_api import fetch_eurostat_calendar, schedule_eurostat_calendar
from .destatis_api import fetch_destatis_calendar, schedule_destatis_calendar
from .ine_api import fetch_ine_calendar, schedule_ine_calendar
from .fed_api import (
    fetch_fed_calendar,
    fetch_fed_releasedates,
    fetch_fed_statement_values,
)
from .ism_api import fetch_ism_calendar, schedule_ism_calendar
from .nar_api import fetch_nar_calendar, schedule_nar_calendar
from .nbs_api import fetch_nbs_calendar
from .umich_api import fetch_umich_calendar, schedule_umich_calendar
from .scheduler_state import (
    DAILY_BUDGET_CAPS,
    get_connector_state,
    is_budget_exhausted,
    is_cooling,
    mark_connector_failure,
    mark_connector_success,
    record_connector_requests,
    today_utc_iso,
)

logger = logging.getLogger(__name__)


# Connector identifier — also the key used in summary dicts and the
# ``connectors=[…]`` subset argument on the service op.
ConnectorName = str

_ConnectorFn = Callable[[sqlite3.Connection, bool], Any]


def _bls(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return schedule_bls_calendar(conn, dry_run=dry_run)


def _bea(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return schedule_bea_calendar(conn, dry_run=dry_run)


def _census(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return schedule_census_calendar(conn, dry_run=dry_run)


def _ism(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return schedule_ism_calendar(conn, dry_run=dry_run)


def _umich(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return schedule_umich_calendar(conn, dry_run=dry_run)


def _conference_board(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return schedule_conference_board_calendar(conn, dry_run=dry_run)


def _nar(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return schedule_nar_calendar(conn, dry_run=dry_run)


def _ecb(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return schedule_ecb_calendar(conn, dry_run=dry_run)


def _eurostat(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return schedule_eurostat_calendar(conn, dry_run=dry_run)


def _destatis(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return schedule_destatis_calendar(conn, dry_run=dry_run)


def _ine(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return schedule_ine_calendar(conn, dry_run=dry_run)


def _fed_fomc(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return fetch_fed_calendar(conn, dry_run=dry_run)


def _fed_releases(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return fetch_fed_releasedates(conn, dry_run=dry_run)


def _nbs(conn: sqlite3.Connection, dry_run: bool) -> Any:
    # ``calendar_url=None`` triggers the auto-discovery path added in
    # P5a: the fetcher resolves the current year's article from the
    # NBS release-calendar index before scraping it.
    return fetch_nbs_calendar(conn, calendar_url=None, dry_run=dry_run)


def _boj(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return fetch_boj_calendar(conn, dry_run=dry_run)


def _boj_tankan(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return fetch_boj_tankan_calendar(conn, dry_run=dry_run)


def _mof(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return fetch_mof_calendar(conn, dry_run=dry_run)


def _cao(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return fetch_cao_calendar(conn, dry_run=dry_run)


def _cao_gdp(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return fetch_cao_gdp_calendar(conn, dry_run=dry_run)


def _meti(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return fetch_meti_calendar(conn, dry_run=dry_run)


def _stat_bureau(conn: sqlite3.Connection, dry_run: bool) -> Any:
    return fetch_stat_bureau_calendar(conn, dry_run=dry_run)


# Sequencing matters for operator inspection — BLS first (highest
# trader impact, cheapest surface), Fed / ECB in the middle, NBS last
# (most upstream-fragile so a failure there is easiest to triage at
# the end of the log).
_DEFAULT_CONNECTORS: tuple[tuple[ConnectorName, _ConnectorFn], ...] = (
    ("bls", _bls),
    ("bea", _bea),
    ("census", _census),
    ("ism", _ism),
    ("umich", _umich),
    ("conference-board", _conference_board),
    ("nar", _nar),
    ("ecb", _ecb),
    ("eurostat", _eurostat),
    ("destatis", _destatis),
    ("ine", _ine),
    ("fed-fomc", _fed_fomc),
    ("fed-releases", _fed_releases),
    ("nbs", _nbs),
    ("stat-bureau-jp", _stat_bureau),
    ("boj", _boj),
    ("boj-tankan", _boj_tankan),
    ("mof-jp", _mof),
    ("cao", _cao),
    ("cao-gdp", _cao_gdp),
    ("meti", _meti),
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
    "bls",
    "bea",
    "census",
    "ism",
    "umich",
    "conference-board",
    "nar",
    "ecb",
    "eurostat",
    "destatis",
    "ine",
    "fed-values",
    "stat-bureau-jp-values",
    "boj-values",
    "boj-tankan-values",
    "mof-jp-values",
    "cao-values",
    "cao-gdp-values",
    "meti-values",
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


def _summary_is_total_outage(summary: Any) -> bool:
    """True when the summary looks like a connector-wide failure.

    Partial failures (one BLS series out of nine, one FOMC statement
    URL out of eight, one ECB page of two) produce a non-None
    ``_summary_failure_reason`` so the operator sees ``ok=False`` on
    the card, but they should **not** trip the circuit breaker — the
    other items were written and the next sweep should still try the
    working subset.

    Full-outage signals (treat as breaker-relevant):

    - ``fetch_error`` singular string (BEA / Fed-releasedates) —
      set only when the connector's single HTTP surface is down.
    - ``fetch_errors`` dict (ECB press calendar + bulletin) **with
      zero ``entries_parsed`` / ``events_upserted``** — some pages
      failed and none delivered rows. If any page parsed entries,
      partial.
    - BLS ``series_failed`` covering every planned series (no
      ``series_ok``).
    - Auto-discovery value sweeps with ``pending_releases`` where
      every due release failed and zero observations landed.
    - Fed-values ``meetings_fetched == 0`` when the plan had
      meetings — the op auto-discovers past FOMC rows; a planned
      run that failed on every URL is a statement-page outage.

    Returns False when no markers are present (clean run) or when
    the markers look partial.
    """
    if summary is None:
        return False
    if getattr(summary, "fetch_error", None):
        return True
    fetch_errors = getattr(summary, "fetch_errors", None)
    if fetch_errors:
        entries_parsed = getattr(summary, "entries_parsed", None)
        events_upserted = getattr(summary, "events_upserted", None)
        parsed_something = (
            (entries_parsed is not None and entries_parsed > 0)
            or (events_upserted is not None and events_upserted > 0)
        )
        if not parsed_something:
            return True
    series_planned = getattr(summary, "series_planned", None)
    series_failed = getattr(summary, "series_failed", None)
    series_ok = getattr(summary, "series_ok", None)
    pending_releases = getattr(summary, "pending_releases", None)
    observations_seen = getattr(summary, "observations_seen", None)
    if (
        pending_releases is not None
        and pending_releases > 0
        and observations_seen == 0
        and series_failed
        and len(series_failed) >= pending_releases
    ):
        return True
    if series_failed and series_planned is not None:
        if not series_ok and len(series_failed) >= len(series_planned):
            return True
    meetings_planned = getattr(summary, "meetings_planned", None)
    meetings_fetched = getattr(summary, "meetings_fetched", None)
    if (
        meetings_planned is not None
        and meetings_fetched == 0
        and meetings_planned > 0
    ):
        return True
    # BoJ Tankan value-side mirrors the same shape but names the
    # counters ``releases_*`` (the Tankan surface is "releases", not
    # "meetings"). Without this branch, a run where every outline URL
    # 404s would set ``ok=False`` via ``fetch_failures`` without
    # tripping the breaker, and the frequent cron would keep polling
    # the broken surface.
    releases_planned = getattr(summary, "releases_planned", None)
    releases_fetched = getattr(summary, "releases_fetched", None)
    if (
        releases_planned is not None
        and releases_fetched == 0
        and releases_planned > 0
    ):
        return True
    return False


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


def _run_connector_with_breaker(
    name: ConnectorName,
    fn: _ConnectorFn,
    *,
    connection_factory: Callable[[], sqlite3.Connection],
    state_conn: sqlite3.Connection,
    dry_run: bool,
    log_prefix: str,
    budget_cap: int | None = None,
) -> ConnectorResult:
    """Run one connector with the P-sched-3 circuit breaker applied.

    Lifecycle per connector:

    1. Read persisted state. If ``cooling_until_ms`` is still in the
       future — or the connector's daily request budget is exhausted
       at ``budget_cap`` — skip and return ``ok=False`` carrying the
       skip reason. No data connection is opened for either skip path.
    2. Otherwise open a data connection, invoke ``fn``, commit on
       success / rollback on exception (mirroring the pre-breaker
       semantics).
    3. In execute mode, persist the updated state (reset on success,
       increment on failure) and accumulate any ``requests_made``
       surfaced by the connector summary (or attached to an exception)
       into today's budget. Dry-run checks state but never mutates.

    ``budget_cap`` is None by default because only the value-side
    sweep driver hits the API surface that carries a cap (BLS
    ``api.bls.gov``, 500 queries / key / UTC day). Schedule-side
    refresh scrapes HTML (``bls.gov/schedule/…``) which has no
    documented cap, so a same-connector exhausted-cap state must not
    freeze the forward schedule for the rest of the day. The
    success/failure/request-recording path still runs on both sides
    so the breaker state and the budget counter stay coherent
    regardless of which driver triggered the run.

    The helper uses a caller-supplied ``state_conn`` — separate from
    the per-connector data connection — so a data-side rollback can't
    unwind the circuit-breaker counter.
    """
    connector_started = time.monotonic()
    # ``start_ms`` answers "is the breaker cooling right now?" at the
    # point the driver decided whether to skip. ``failure_ms`` is
    # resolved fresh at mark time (below) because a slow connector
    # can consume minutes between start and the failure signal, and
    # using the stale start timestamp for ``cooling_until_ms``
    # computation would shorten the effective cool-down window.
    start_ms = int(time.time() * 1000)

    state = get_connector_state(state_conn, name)
    if is_cooling(state, start_ms):
        cooling_until_iso = datetime.fromtimestamp(
            (state.cooling_until_ms or 0) / 1000, tz=timezone.utc,
        ).isoformat()
        reason = (
            f"circuit breaker cooling until {cooling_until_iso} "
            f"({state.consecutive_failures} consecutive failures; "
            f"last: {state.last_error or '-'})"
        )
        return ConnectorResult(
            connector=name,
            ok=False,
            error=reason,
            wall_seconds=round(time.monotonic() - connector_started, 3),
        )

    # Budget-aware skip. Schedule-side refresh passes
    # ``budget_cap=None`` because its BLS path scrapes HTML (uncapped);
    # value-side sweep passes ``DAILY_BUDGET_CAPS.get(name)`` so only
    # capped connectors (BLS today) can short-circuit on exhaustion.
    skip_check_today_iso = today_utc_iso()
    if is_budget_exhausted(state, budget_cap, today_iso=skip_check_today_iso):
        reason = (
            f"daily request budget exhausted "
            f"({state.requests_today}/{budget_cap} on {skip_check_today_iso})"
        )
        return ConnectorResult(
            connector=name,
            ok=False,
            error=reason,
            wall_seconds=round(time.monotonic() - connector_started, 3),
        )

    data_conn = connection_factory()
    try:
        summary = fn(data_conn, dry_run)
        if not dry_run:
            data_conn.commit()
    except Exception as exc:
        try:
            data_conn.rollback()
        except Exception:
            pass
        logger.warning(
            "calendar %s failed for %s: %s", log_prefix, name, exc,
        )
        if not dry_run:
            # Fresh timestamp so ``cooling_until_ms`` is anchored on
            # the actual failure moment, not the start of a slow run.
            mark_connector_failure(
                state_conn, name, error=str(exc),
                now_ms=int(time.time() * 1000),
            )
            # Record requests consumed before the exception so a
            # partial-then-fail BLS run (chunk 1 succeeded, chunk 2
            # 500'd) still ticks the persisted counter. The
            # connector shim attaches ``exc.requests_made`` before
            # re-raising; absent attribute means the connector
            # consumed zero capped requests (or doesn't track).
            exc_requests_made = getattr(exc, "requests_made", None)
            if isinstance(exc_requests_made, int) and exc_requests_made > 0:
                record_connector_requests(
                    state_conn, name, exc_requests_made,
                    today_iso=today_utc_iso(),
                )
            state_conn.commit()
        return ConnectorResult(
            connector=name,
            ok=False,
            error=str(exc),
            wall_seconds=round(time.monotonic() - connector_started, 3),
        )
    finally:
        try:
            data_conn.close()
        except Exception:
            pass

    failure_reason = _summary_failure_reason(summary)
    if not dry_run:
        # Partial failures (one BLS series, one FOMC URL) land as
        # ``ok=False`` on the card but must not trip the breaker —
        # only a connector-wide outage does. ``_summary_is_total_outage``
        # distinguishes the two.
        if failure_reason is not None and _summary_is_total_outage(summary):
            mark_connector_failure(
                state_conn, name, error=failure_reason,
                now_ms=int(time.time() * 1000),
            )
        else:
            # Reset on clean run OR on partial failure — the breaker
            # is about "is this source reachable at all?", not "did
            # every item land?". A partial run proves reachability
            # and resets the consecutive counter.
            mark_connector_success(state_conn, name)

        # Accumulate any requests the connector surfaced into today's
        # budget. Summaries without ``requests_made`` (BEA / ECB / Fed
        # / NBS today) leave the counter untouched. ``today_iso`` is
        # resolved at record time — a value-side sweep that crossed
        # UTC midnight attributes consumption to the current day. The
        # client counter resets internally at midnight, so the
        # observable delta already reflects post-midnight requests
        # only; attributing them to the new day keeps the persisted
        # counter aligned with the client's reset semantics.
        requests_made = getattr(summary, "requests_made", None)
        if isinstance(requests_made, int) and requests_made > 0:
            record_connector_requests(
                state_conn, name, requests_made,
                today_iso=today_utc_iso(),
            )
        state_conn.commit()

    return ConnectorResult(
        connector=name,
        ok=failure_reason is None,
        error=failure_reason,
        summary=_summary_to_dict(summary),
        wall_seconds=round(time.monotonic() - connector_started, 3),
    )


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
        full tuple) to run the full roster. Order follows
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

    state_conn = connection_factory()
    try:
        for name, fn in plan:
            run_summary.results.append(_run_connector_with_breaker(
                name, fn,
                connection_factory=connection_factory,
                state_conn=state_conn,
                dry_run=dry_run,
                log_prefix="schedule refresh",
                budget_cap=None,
            ))
    finally:
        try:
            state_conn.close()
        except Exception:
            pass

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
        Year window passed through to BLS / BEA / Census value-side ops.
        Default: ``(current_year − 1, current_year)`` — a
        two-year window large enough that the freshness guard still
        skips recomputing settled rows while any just-published
        observation lands.
    start_period / end_period:
        Optional SDMX-period strings forwarded to ECB and Eurostat.
        Destatis uses the resolved year bounds.
        ``None`` means the driver picks its recent window.
    connectors:
        Optional subset of :data:`ALL_VALUE_SIDE_CONNECTORS`. Omit to
        run the full value-side plan.
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
        # Snapshot the client's daily counter so the driver can still
        # attribute consumed requests to the budget when ``get_series``
        # raises mid-call (chunk N succeeded, chunk N+1 500'd). Without
        # this the persisted counter would undershoot actual BLS API
        # usage and the breaker's budget skip could be defeated.
        requests_before = client.daily_query_count
        try:
            return fetch_bls_calendar(
                conn, client,
                start_year=resolved_start_year,
                end_year=resolved_end_year,
                dry_run=dry_run,
            )
        except Exception as exc:
            consumed = max(0, client.daily_query_count - requests_before)
            if consumed > 0:
                exc.requests_made = consumed  # read by scheduler.py
            raise

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

    def _census_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        from ingestion.calendar.census_api import CensusEITSClient
        client = CensusEITSClient()
        return fetch_census_calendar(
            conn, client,
            start_year=resolved_start_year,
            end_year=resolved_end_year,
            dry_run=dry_run,
        )

    def _ism_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        return fetch_ism_calendar(conn, dry_run=dry_run)

    def _umich_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        return fetch_umich_calendar(conn, dry_run=dry_run)

    def _conference_board_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        return fetch_conference_board_calendar(conn, dry_run=dry_run)

    def _nar_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        return fetch_nar_calendar(conn, dry_run=dry_run)

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

    def _eurostat_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        # Eurostat JSON-stat has no auth.
        from ingestion.timeseries.sdmx.providers.eurostat import EurostatClient
        client = EurostatClient()
        return fetch_eurostat_calendar(
            conn,
            client,
            start_period=resolved_start_period,
            end_period=resolved_end_period,
            dry_run=dry_run,
        )

    def _destatis_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        from ingestion.calendar.destatis_api import DestatisGenesisClient
        client = DestatisGenesisClient.from_env()
        return fetch_destatis_calendar(
            conn,
            client,
            start_year=resolved_start_year,
            end_year=resolved_end_year,
            dry_run=dry_run,
        )

    def _ine_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        return fetch_ine_calendar(conn, dry_run=dry_run)

    def _fed_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        # fetch_fed_statement_values auto-discovers past FOMC rows
        # with ``actual IS NULL`` — no year window needed.
        return fetch_fed_statement_values(conn, dry_run=dry_run)

    def _stat_bureau_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        return fetch_stat_bureau_values(conn, dry_run=dry_run)

    def _boj_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        # Auto-discovers past BoJ MPM rows with ``actual IS NULL`` —
        # mirrors the Fed-values shape, no year window needed.
        return fetch_boj_statement_values(conn, dry_run=dry_run)

    def _boj_tankan_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        # Tankan's schedule source (``yoshi/index.htm``) is past-only
        # — a newly-published release appears there only after
        # 08:50 JST on release day. The daily schedule pass may
        # therefore lag the release by several hours, which would
        # leave the value-side sweep with no ``actual IS NULL`` row
        # to discover. Seed the schedule side first so the same
        # sweep that observes the release also fills its value.
        fetch_boj_tankan_calendar(conn, dry_run=dry_run)
        return fetch_boj_tankan_outlines(conn, dry_run=dry_run)

    def _mof_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        # Auto-discovers past Balance-of-Trade rows with
        # ``actual IS NULL`` and fetches each release's XML feed.
        # Unlike Tankan, MoF's schedule page is forward-looking —
        # calendar rows exist well before the release — so no
        # in-sweep seed is needed.
        return fetch_mof_trade_values(conn, dry_run=dry_run)

    def _cao_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        # The ``shouhi-e.html`` landing page carries at most one
        # release at a time — CAO overwrites it on each new
        # publication. The value scraper does a single GET per
        # sweep, parses the reference month + CCI value from the
        # two deterministic sentences, and upserts onto the
        # matching pending row. project_events' full upsert handles
        # both the insert-new and update-existing paths, so an
        # in-sweep schedule seed isn't required.
        return fetch_cao_consumer_confidence_values(conn, dry_run=dry_run)

    def _cao_gdp_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        # The SNA archive is past-only, so seed the staged GDP rows
        # from the archive before discovering pending CSV values.
        fetch_cao_gdp_calendar(conn, dry_run=dry_run)
        return fetch_cao_gdp_values(conn, dry_run=dry_run)

    def _meti_values(conn: sqlite3.Connection, dry_run: bool) -> Any:
        return fetch_meti_values(conn, dry_run=dry_run)

    value_side_map: dict[ConnectorName, _ConnectorFn] = {
        "bls":        _bls_values,
        "bea":        _bea_values,
        "census":     _census_values,
        "ism":        _ism_values,
        "umich":      _umich_values,
        "conference-board": _conference_board_values,
        "nar":        _nar_values,
        "ecb":        _ecb_values,
        "eurostat":   _eurostat_values,
        "destatis":   _destatis_values,
        "ine":        _ine_values,
        "fed-values": _fed_values,
        "stat-bureau-jp-values": _stat_bureau_values,
        "boj-values": _boj_values,
        "boj-tankan-values": _boj_tankan_values,
        "mof-jp-values": _mof_values,
        "cao-values": _cao_values,
        "cao-gdp-values": _cao_gdp_values,
        "meti-values": _meti_values,
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

    state_conn = connection_factory()
    try:
        for name, fn in plan:
            run_summary.results.append(_run_connector_with_breaker(
                name, fn,
                connection_factory=connection_factory,
                state_conn=state_conn,
                dry_run=dry_run,
                log_prefix="value-side sweep",
                budget_cap=DAILY_BUDGET_CAPS.get(name),
            ))
    finally:
        try:
            state_conn.close()
        except Exception:
            pass

    run_summary.wall_seconds = time.monotonic() - started
    return run_summary

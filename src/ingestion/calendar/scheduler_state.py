"""Persistent circuit-breaker state for the calendar scheduler (P-sched-3).

The P-sched-1 / P-sched-2 drivers isolate per-connector exceptions
and in-summary failure markers so one bad upstream doesn't block
the rest. But a connector that's been broken for hours still gets
re-invoked on every sweep — bandwidth spent on a known-bad surface,
log noise, and wall-clock slowed by failing HTTP / parse round-trips.

This module persists a small circuit-breaker state per scheduler
connector (``"bls"`` / ``"bea"`` / ``"census"`` / ``"ecb"`` /
``"fed-fomc"`` / ``"fed-releases"`` / ``"fed-values"`` / ``"nbs"``). After
:data:`FAILURE_THRESHOLD` consecutive failures, the connector enters
a cool-down of :data:`COOLDOWN_SECONDS`. Subsequent sweeps skip the
connector until ``cooling_until_ms`` passes. A successful run
(anywhere in the cool-down window or afterwards) resets the counter
and clears the cool-down.

State rows live in the ``calendar_connector_state`` table, keyed by
connector name. The scheduler uses a separate connection for state
reads/writes so a connector's data-side rollback doesn't unwind the
failure counter.

**Budget tracking (P-sched-3-budget).** BLS caps its API at
500 queries/day; a frequent value-side cron that keeps calling after
exhaustion burns log noise and triggers 429s on the next day's first
attempt. ``requests_today`` / ``requests_day_utc`` persist a UTC-day
counter scoped to the same ``calendar_connector_state`` row; the
counter rolls over when ``requests_day_utc`` differs from today.
:data:`DAILY_BUDGET_CAPS` holds per-connector caps — connectors
missing from the dict are treated as uncapped. Orthogonal to the
consecutive-failure breaker: the budget stays exhausted even after a
``mark_connector_success`` because exhaustion is a same-day
volume-based skip, not a flakiness signal.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone

# After this many consecutive failures, the connector enters a
# cool-down. Three is enough signal to distinguish a transient 502
# from a persistent outage without opening the breaker on a single
# flaky request.
FAILURE_THRESHOLD = 3

# Fifteen minutes. Long enough that an outage has a chance to clear,
# short enough that an operator investigating the health dashboard
# doesn't wait hours for the next attempt once they restore upstream
# access.
COOLDOWN_SECONDS = 900

# Per-connector daily request caps. Only connectors whose upstream
# publishes a hard daily limit live here; omitted connectors are
# treated as uncapped. BLS's API ceiling is 500/day per registered
# key — we match the in-memory soft-cap in ``BLSClient`` (490) so the
# scheduler stops before the upstream starts 429ing.
DAILY_BUDGET_CAPS: dict[str, int] = {
    "bls": 490,
}


@dataclass(frozen=True)
class ConnectorState:
    """Persistent circuit-breaker state for one connector."""

    connector: str
    consecutive_failures: int = 0
    last_error: str | None = None
    last_failure_at_ms: int | None = None
    cooling_until_ms: int | None = None
    requests_today: int = 0
    requests_day_utc: str | None = None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def today_utc_iso() -> str:
    """UTC calendar day as ``"YYYY-MM-DD"`` — the budget rollover key."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _row_to_state(connector: str, row: tuple | None) -> ConnectorState:
    if row is None:
        return ConnectorState(connector=connector)
    (
        failures, last_error, last_failure, cooling_until,
        requests_today, requests_day_utc,
    ) = row
    return ConnectorState(
        connector=connector,
        consecutive_failures=int(failures or 0),
        last_error=last_error,
        last_failure_at_ms=(
            int(last_failure) if last_failure is not None else None
        ),
        cooling_until_ms=(
            int(cooling_until) if cooling_until is not None else None
        ),
        requests_today=int(requests_today or 0),
        requests_day_utc=requests_day_utc,
    )


def get_connector_state(
    connection: sqlite3.Connection, connector: str,
) -> ConnectorState:
    """Read state for ``connector``; return a fresh default when absent."""
    row = connection.execute(
        """
        SELECT consecutive_failures, last_error,
               last_failure_at_ms, cooling_until_ms,
               requests_today, requests_day_utc
        FROM calendar_connector_state
        WHERE connector = ?
        """,
        (connector,),
    ).fetchone()
    return _row_to_state(connector, row)


def list_connector_states(
    connection: sqlite3.Connection,
) -> list[ConnectorState]:
    """Return every persisted connector state row.

    Used by the health dashboard (P-sched-4) to surface cooling /
    budget-exhausted connectors in ``/health``. Connectors that have
    never run (no row) are not returned — callers that want a full
    roster pair this with :data:`ingestion.calendar.scheduler.ALL_CONNECTORS`
    and synthesize defaults for absent names.
    """
    rows = connection.execute(
        """
        SELECT connector, consecutive_failures, last_error,
               last_failure_at_ms, cooling_until_ms,
               requests_today, requests_day_utc
        FROM calendar_connector_state
        ORDER BY connector
        """,
    ).fetchall()
    return [_row_to_state(row[0], row[1:]) for row in rows]


def is_cooling(state: ConnectorState, now_ms: int) -> bool:
    """True when the connector is in its cool-down window at ``now_ms``."""
    if state.cooling_until_ms is None:
        return False
    return now_ms < state.cooling_until_ms


def is_budget_exhausted(
    state: ConnectorState,
    cap: int | None,
    *,
    today_iso: str,
) -> bool:
    """True when the connector's same-day request count has hit ``cap``.

    Returns False when ``cap`` is ``None`` (uncapped connector) or when
    ``state.requests_day_utc`` is a different UTC day — the stored
    count belongs to yesterday and will be rolled over on the next
    :func:`record_connector_requests` call.
    """
    if cap is None:
        return False
    if state.requests_day_utc != today_iso:
        return False
    return state.requests_today >= cap


def mark_connector_success(
    connection: sqlite3.Connection, connector: str,
) -> None:
    """Clear the failure counter and cool-down window.

    Idempotent — running twice has the same effect. ``INSERT OR REPLACE``
    on the PK keeps the state row fresh whether or not a row already
    exists. Budget columns (``requests_today`` / ``requests_day_utc``)
    are left untouched — they roll over on UTC day change, not on
    success/failure. A successful run that doesn't consume requests
    shouldn't zero out the running count for today.
    """
    connection.execute(
        """
        INSERT INTO calendar_connector_state (
            connector, consecutive_failures,
            last_error, last_failure_at_ms, cooling_until_ms,
            requests_today, requests_day_utc,
            updated_at
        ) VALUES (?, 0, NULL, NULL, NULL, 0, NULL, ?)
        ON CONFLICT(connector) DO UPDATE SET
            consecutive_failures = 0,
            last_error           = NULL,
            last_failure_at_ms   = NULL,
            cooling_until_ms     = NULL,
            updated_at           = excluded.updated_at
        """,
        (connector, _now_iso()),
    )


def mark_connector_failure(
    connection: sqlite3.Connection,
    connector: str,
    *,
    error: str,
    now_ms: int,
    threshold: int = FAILURE_THRESHOLD,
    cooldown_seconds: int = COOLDOWN_SECONDS,
) -> ConnectorState:
    """Increment the failure counter; trip the breaker when ``threshold`` hit.

    Returns the new :class:`ConnectorState` so callers can log the
    tripped / still-closed decision without a second read. When
    ``consecutive_failures`` reaches ``threshold``, ``cooling_until_ms``
    is set to ``now_ms + cooldown_seconds * 1000`` — the next sweep
    will see :func:`is_cooling` return ``True`` and skip the connector.
    Budget columns (``requests_today`` / ``requests_day_utc``) are left
    untouched for the same reason as in :func:`mark_connector_success`.
    """
    prior = get_connector_state(connection, connector)
    new_failures = prior.consecutive_failures + 1
    trip = new_failures >= threshold
    cooling_until = now_ms + cooldown_seconds * 1000 if trip else None
    connection.execute(
        """
        INSERT INTO calendar_connector_state (
            connector, consecutive_failures,
            last_error, last_failure_at_ms, cooling_until_ms,
            requests_today, requests_day_utc,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, 0, NULL, ?)
        ON CONFLICT(connector) DO UPDATE SET
            consecutive_failures = excluded.consecutive_failures,
            last_error           = excluded.last_error,
            last_failure_at_ms   = excluded.last_failure_at_ms,
            cooling_until_ms     = excluded.cooling_until_ms,
            updated_at           = excluded.updated_at
        """,
        (
            connector, new_failures, error, now_ms, cooling_until,
            _now_iso(),
        ),
    )
    return ConnectorState(
        connector=connector,
        consecutive_failures=new_failures,
        last_error=error,
        last_failure_at_ms=now_ms,
        cooling_until_ms=cooling_until,
        requests_today=prior.requests_today,
        requests_day_utc=prior.requests_day_utc,
    )


def record_connector_requests(
    connection: sqlite3.Connection,
    connector: str,
    requests_made: int,
    *,
    today_iso: str,
) -> ConnectorState:
    """Accumulate ``requests_made`` into today's counter; roll over on new day.

    The counter rolls over when the stored ``requests_day_utc``
    differs from ``today_iso`` — the new day starts at exactly
    ``requests_made``, not ``prior + requests_made``. This mirrors how
    the in-memory ``BLSClient._check_daily_budget`` resets at UTC day
    change so the two agree on the same calendar day.

    A zero ``requests_made`` is still worth recording once per day
    because it writes ``requests_day_utc`` — next call on the same day
    can then increment from zero without creating a brand-new row, and
    a bare call on a new UTC day resets the prior-day counter.
    Callers that know the connector consumed no upstream requests
    (dry-run, all-cached) can skip the record call entirely.
    """
    prior = get_connector_state(connection, connector)
    if prior.requests_day_utc == today_iso:
        new_total = prior.requests_today + max(0, requests_made)
    else:
        new_total = max(0, requests_made)
    connection.execute(
        """
        INSERT INTO calendar_connector_state (
            connector, consecutive_failures,
            last_error, last_failure_at_ms, cooling_until_ms,
            requests_today, requests_day_utc,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(connector) DO UPDATE SET
            requests_today   = excluded.requests_today,
            requests_day_utc = excluded.requests_day_utc,
            updated_at       = excluded.updated_at
        """,
        (
            connector,
            prior.consecutive_failures,
            prior.last_error,
            prior.last_failure_at_ms,
            prior.cooling_until_ms,
            new_total,
            today_iso,
            _now_iso(),
        ),
    )
    return ConnectorState(
        connector=connector,
        consecutive_failures=prior.consecutive_failures,
        last_error=prior.last_error,
        last_failure_at_ms=prior.last_failure_at_ms,
        cooling_until_ms=prior.cooling_until_ms,
        requests_today=new_total,
        requests_day_utc=today_iso,
    )

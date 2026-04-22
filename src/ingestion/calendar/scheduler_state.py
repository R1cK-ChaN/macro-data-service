"""Persistent circuit-breaker state for the calendar scheduler (P-sched-3).

The P-sched-1 / P-sched-2 drivers isolate per-connector exceptions
and in-summary failure markers so one bad upstream doesn't block
the rest. But a connector that's been broken for hours still gets
re-invoked on every sweep — bandwidth spent on a known-bad surface,
log noise, and wall-clock slowed by failing HTTP / parse round-trips.

This module persists a small circuit-breaker state per scheduler
connector (``"bls"`` / ``"bea"`` / ``"ecb"`` / ``"fed-fomc"`` /
``"fed-releases"`` / ``"fed-values"`` / ``"nbs"``). After
:data:`FAILURE_THRESHOLD` consecutive failures, the connector enters
a cool-down of :data:`COOLDOWN_SECONDS`. Subsequent sweeps skip the
connector until ``cooling_until_ms`` passes. A successful run
(anywhere in the cool-down window or afterwards) resets the counter
and clears the cool-down.

State rows live in the ``calendar_connector_state`` table, keyed by
connector name. The scheduler uses a separate connection for state
reads/writes so a connector's data-side rollback doesn't unwind the
failure counter.
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


@dataclass(frozen=True)
class ConnectorState:
    """Persistent circuit-breaker state for one connector."""

    connector: str
    consecutive_failures: int = 0
    last_error: str | None = None
    last_failure_at_ms: int | None = None
    cooling_until_ms: int | None = None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_connector_state(
    connection: sqlite3.Connection, connector: str,
) -> ConnectorState:
    """Read state for ``connector``; return a fresh default when absent."""
    row = connection.execute(
        """
        SELECT consecutive_failures, last_error,
               last_failure_at_ms, cooling_until_ms
        FROM calendar_connector_state
        WHERE connector = ?
        """,
        (connector,),
    ).fetchone()
    if row is None:
        return ConnectorState(connector=connector)
    failures, last_error, last_failure, cooling_until = row
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
    )


def is_cooling(state: ConnectorState, now_ms: int) -> bool:
    """True when the connector is in its cool-down window at ``now_ms``."""
    if state.cooling_until_ms is None:
        return False
    return now_ms < state.cooling_until_ms


def mark_connector_success(
    connection: sqlite3.Connection, connector: str,
) -> None:
    """Clear the failure counter and cool-down window.

    Idempotent — running twice has the same effect. ``INSERT OR REPLACE``
    on the PK keeps the state row fresh whether or not a row already
    exists.
    """
    connection.execute(
        """
        INSERT INTO calendar_connector_state (
            connector, consecutive_failures,
            last_error, last_failure_at_ms, cooling_until_ms,
            updated_at
        ) VALUES (?, 0, NULL, NULL, NULL, ?)
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
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?)
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
    )

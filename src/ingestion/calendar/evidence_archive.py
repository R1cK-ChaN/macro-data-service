"""Wayback evidence-archive submitter for ``calendar_event_vintages`` rows.

Issue #36 — every vintage row is a PIT observation that needs a
third-party-verifiable citation anchor. This module submits each
row's ``source_url`` to the Internet Archive's Save Page Now (SPN)
endpoint and persists the returned snapshot URL onto the row.

Design — sweep-tail-only submission:

* Vintage writers never call this module synchronously; they leave
  ``evidence_archive_url`` NULL on insert.
* :func:`archive_pending` runs at the tail of each hourly value-side
  sweep, scans rows with NULL ``evidence_archive_url`` and non-empty
  ``source_url``, calls SPN per row, then writes results back in a
  single batch. Failures stamp ``evidence_last_attempt_at`` so the
  same N unarchivable URLs at the head of the queue rotate to the
  back instead of blocking newer pending rows on every cycle.
* Each row submits independently — two vintages with identical
  ``source_url`` produce two SPN calls so each PIT observation gets
  its own ``web.archive.org/web/<ts>/`` timestamp.

Network calls happen *before* any ``UPDATE`` is issued so SQLite's
writer lock is not held across seconds-long Wayback HTTP. Overlapping
schedule/value ingests would otherwise hit ``database is locked``.

Anonymous SPN quota (~15 saves/min) is well above the expected
~hundreds-per-day vintage write rate. Optional S3-style auth via
``WAYBACK_S3_ACCESSKEY`` / ``WAYBACK_S3_SECRET`` env vars unlocks
higher quota when the deployment needs it.

Set ``MACRO_DATA_WAYBACK_DISABLED=1`` to make :func:`archive_pending`
a no-op (default for tests; production leaves it unset). Only the
exact string ``"1"`` disables — ``"0"`` and other values do not.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Callable

logger = logging.getLogger(__name__)

WAYBACK_SAVE_ENDPOINT = "https://web.archive.org/save/"
# Anonymous SPN quota is ~15 saves/min; a hourly sweep with limit=12
# stays comfortably under that even when bursting. Deployments with
# ``WAYBACK_S3_*`` env vars can pass a higher ``limit`` explicitly.
DEFAULT_RETRY_LIMIT = 12
DEFAULT_TIMEOUT_SECONDS = 15.0
DISABLED_ENV_VAR = "MACRO_DATA_WAYBACK_DISABLED"

Submitter = Callable[[str], str | None]


def _is_disabled() -> bool:
    """Explicit truthy parse — only the literal ``"1"`` disables.

    ``os.environ.get(...)`` returning ``"0"`` is non-empty (truthy in
    Python's bool coercion); a naive check would silently suppress
    archival when ops set ``MACRO_DATA_WAYBACK_DISABLED=0`` to mean
    "enabled". Match what the docstring promises.
    """
    return os.environ.get(DISABLED_ENV_VAR, "").strip() == "1"


def submit_save_request(
    source_url: str,
    *,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> str | None:
    """Submit ``source_url`` to Wayback's Save Page Now endpoint.

    Returns the resolved snapshot URL on success, ``None`` on any
    failure (HTTP error, timeout, malformed response). Network errors
    are swallowed by design — the caller decides retry policy.
    """
    if not source_url:
        return None
    target = WAYBACK_SAVE_ENDPOINT + source_url
    headers = {"User-Agent": "macro-data-service/evidence-archive (+issue-36)"}
    s3_key = os.environ.get("WAYBACK_S3_ACCESSKEY", "").strip()
    s3_secret = os.environ.get("WAYBACK_S3_SECRET", "").strip()
    if s3_key and s3_secret:
        headers["Authorization"] = f"LOW {s3_key}:{s3_secret}"
    try:
        req = urllib.request.Request(target, headers=headers, method="GET")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            content_loc = resp.headers.get("Content-Location") or ""
            if content_loc.startswith("/web/"):
                return "https://web.archive.org" + content_loc
            final_url = resp.geturl()
            if "/web/" in final_url:
                return final_url
    except (urllib.error.URLError, OSError, ValueError) as exc:
        logger.debug("wayback save failed for %s: %r", source_url, exc)
    return None


def archive_pending(
    connection: sqlite3.Connection,
    *,
    limit: int = DEFAULT_RETRY_LIMIT,
    submitter: Submitter | None = None,
) -> dict[str, int]:
    """Submit pending vintage rows to Wayback and persist snapshot URLs.

    Scan order — ``COALESCE(evidence_last_attempt_at, '') ASC, observed_at
    ASC`` — surfaces never-tried rows first, then oldest-tried rows. A
    block of unarchivable URLs that fail repeatedly gets stamped with
    ``evidence_last_attempt_at`` and rotates to the back of the queue,
    so newer pending rows still get cycle time.

    All network submissions complete *before* any ``UPDATE`` runs so
    SQLite's writer lock is not held across HTTP. Overlapping ingests
    would otherwise see ``database is locked`` while this loop waits
    on Wayback.

    ``submitter`` lets tests inject a stub. Defaults to
    :func:`submit_save_request`.

    Returns a dict with keys ``scanned`` (rows considered),
    ``archived`` (rows successfully updated) and ``failed``
    (submission attempts that returned ``None``).
    """
    counters = {"scanned": 0, "archived": 0, "failed": 0}
    if _is_disabled():
        return counters

    submit = submitter or submit_save_request

    rows = connection.execute(
        "SELECT id, source_url FROM calendar_event_vintages "
        "WHERE evidence_archive_url IS NULL AND source_url != '' "
        "ORDER BY COALESCE(evidence_last_attempt_at, '') ASC, "
        "julianday(observed_at) ASC, id ASC LIMIT ?",
        (limit,),
    ).fetchall()
    counters["scanned"] = len(rows)
    if not rows:
        return counters

    # Phase 1 — network only, no DB writes. Collect (id, snapshot|None).
    submissions: list[tuple[int, str | None]] = []
    for row in rows:
        if isinstance(row, sqlite3.Row):
            vintage_id = row["id"]
            url = row["source_url"]
        else:
            vintage_id, url = row[0], row[1]
        submissions.append((vintage_id, submit(url)))

    # Phase 2 — short DB-only write phase. Writer lock is held only
    # while the loop runs, not during the preceding HTTP calls.
    now_iso = datetime.now(timezone.utc).isoformat()
    for vintage_id, snapshot in submissions:
        if snapshot:
            connection.execute(
                "UPDATE calendar_event_vintages "
                "SET evidence_archive_url = ?, evidence_last_attempt_at = ? "
                "WHERE id = ?",
                (snapshot, now_iso, vintage_id),
            )
            counters["archived"] += 1
        else:
            connection.execute(
                "UPDATE calendar_event_vintages "
                "SET evidence_last_attempt_at = ? WHERE id = ?",
                (now_iso, vintage_id),
            )
            counters["failed"] += 1

    return counters

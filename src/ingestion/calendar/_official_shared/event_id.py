"""Synthesize a stable ``provider_event_id`` for connectors without one.

Upstream sources in the issue-9 scope do not all expose a persistent
event identifier. BEA's release calendar is an HTML table with no ids;
BLS's release-schedule pages don't carry a numeric id; the Fed's
release-dates page is likewise unstructured. We need a deterministic
id the projector can use as the ``(provider, provider_event_id)``
uniqueness key so revisions upsert rather than duplicate.

The synthesised id is:

    sha256(provider | country | indicator_canonical | anchor)

``anchor`` is the most stable upstream-identity string each connector
has for "this release" — typically the reference period (``"2024-04"``,
``"Q2 2024"``, meeting date, …). Crucially this is *not*
``event_time_utc``: for BLS / BEA the scheduled release datetime
arrives later than the value itself (via the schedule scraper), so a
datetime-anchored id would change when the schedule is upgraded and
produce duplicate calendar rows. Anchoring on the reference period
makes the id stable across the value → value+schedule lifecycle.

Each component is lowercased (for provider) or uppercased (for
country) first, then joined with ``"|"``.

Why SHA-256 and not a prefixed concatenation: the `(provider,
provider_event_id)` PK is a TEXT column, and a 64-char hex string
keeps the key-space uniform. Human-readability of the id isn't
load-bearing — debuggers can rehydrate via the raw JSON payload.
"""

from __future__ import annotations

import hashlib


def synthesize_event_id(
    provider: str,
    country: str,
    indicator_canonical: str,
    anchor: str,
) -> str:
    """Return the deterministic id.

    Parameters
    ----------
    provider:
        The ``cal_provider.provider_id`` value (e.g. ``"bls"``). Case-
        normalized to lowercase inside the hash.
    country:
        ISO-3166-1 alpha-2. Case-normalized to uppercase.
    indicator_canonical:
        Output of :func:`canonicalize_indicator`. Used as-is — callers
        must canonicalize before hashing so two spellings of the same
        indicator collapse to the same id.
    anchor:
        The most stable upstream-identity string for "this release".
        For BLS / BEA: the reference-period date (``"2024-04-01"`` for
        April CPI). For Fed FOMC: the meeting date. The anchor **must
        not** be a value that changes between snapshots (e.g. a
        placeholder ``event_time_utc`` that a later scraper will
        overwrite with the true scheduled time) or the id cycles and
        the projector generates duplicate rows.

    Returns
    -------
    str
        64-character hex SHA-256 digest.
    """
    parts = "|".join(
        [
            (provider or "").lower(),
            (country or "").upper(),
            indicator_canonical or "",
            anchor or "",
        ]
    )
    return hashlib.sha256(parts.encode("utf-8")).hexdigest()

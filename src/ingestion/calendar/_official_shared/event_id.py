"""Synthesize a stable ``provider_event_id`` for connectors without one.

Upstream sources in the issue-9 scope do not all expose a persistent
event identifier. BEA's release calendar is an HTML table with no ids;
BLS's release-schedule pages don't carry a numeric id; the Fed's
release-dates page is likewise unstructured. We need a deterministic
id the projector can use as the ``(provider, provider_event_id)``
uniqueness key so revisions upsert rather than duplicate.

The synthesised id is:

    sha256(provider | country | indicator_canonical | event_time_utc)

Each component is lowercased (for provider) or uppercased (for
country) first, then joined with ``"|"``. The same scheduled release
produces the same id across snapshots — which is the property the
projector's upsert relies on.

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
    event_time_utc: str,
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
    event_time_utc:
        ISO-8601 datetime string, including the UTC offset. Used as-is
        so the id commits to a specific scheduled time: two releases
        of the same indicator with different scheduled times are
        distinct events and get distinct ids.

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
            event_time_utc or "",
        ]
    )
    return hashlib.sha256(parts.encode("utf-8")).hexdigest()

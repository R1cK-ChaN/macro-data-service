from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any

from ._types import CheckResult, ValidationLayer, ValidationSeverity


def _hash_observations(observations: list[dict[str, Any]]) -> str:
    """Produce a deterministic hash of observation data for comparison."""
    normalized = []
    for obs in observations:
        d = obs.get("date", "")
        v = obs.get("value")
        if d and v is not None:
            normalized.append({"date": str(d), "value": float(v)})
    normalized.sort(key=lambda x: x["date"])
    return hashlib.sha256(
        json.dumps(normalized, sort_keys=True).encode()
    ).hexdigest()


def check_data_diff(
    source: str,
    series_id: str,
    api_observations: list[dict[str, Any]],
    db_observations: list[dict[str, Any]],
) -> list[CheckResult]:
    """Compare API-fetched data against database-stored data.

    Detects:
    - Data revisions (value changed for an existing date)
    - Silent drops (date in DB but not in API)
    - New additions (date in API but not in DB)
    """
    results: list[CheckResult] = []
    now = datetime.now(UTC).isoformat()

    # Build date-indexed maps
    api_map: dict[str, float] = {}
    for obs in api_observations:
        d = obs.get("date", "")
        v = obs.get("value")
        if d and v is not None:
            api_map[str(d)] = float(v)

    db_map: dict[str, float] = {}
    for obs in db_observations:
        d = obs.get("date", "")
        v = obs.get("value")
        if d and v is not None:
            db_map[str(d)] = float(v)

    api_dates = set(api_map.keys())
    db_dates = set(db_map.keys())

    added = sorted(api_dates - db_dates)
    removed = sorted(db_dates - api_dates)
    common = api_dates & db_dates

    revised: list[tuple[str, float, float]] = []
    for d in sorted(common):
        api_v = api_map[d]
        db_v = db_map[d]
        if abs(api_v - db_v) > 1e-10:
            revised.append((d, db_v, api_v))

    # ── Hash comparison ──────────────────────────────────────────
    api_hash = _hash_observations(api_observations)
    db_hash = _hash_observations(db_observations)
    hash_match = api_hash == db_hash

    results.append(
        CheckResult(
            check_name="diff_hash_match",
            layer=ValidationLayer.DATA_DIFF,
            passed=hash_match,
            severity=ValidationSeverity.INFO if hash_match else ValidationSeverity.WARNING,
            message=(
                f"{series_id}: API/DB hashes match"
                if hash_match
                else f"{series_id}: API/DB hashes differ"
            ),
            source=source,
            series_id=series_id,
            timestamp=now,
            details={
                "api_hash": api_hash[:16],
                "db_hash": db_hash[:16],
                "api_count": len(api_map),
                "db_count": len(db_map),
            },
        )
    )

    # ── Added dates ──────────────────────────────────────────────
    if added:
        results.append(
            CheckResult(
                check_name="diff_new_dates",
                layer=ValidationLayer.DATA_DIFF,
                passed=True,
                severity=ValidationSeverity.INFO,
                message=f"{series_id}: {len(added)} new dates in API (latest: {added[-1]})",
                source=source,
                series_id=series_id,
                timestamp=now,
                details={"added_count": len(added), "added_dates": added[:10]},
            )
        )

    # ── Removed dates ────────────────────────────────────────────
    if removed:
        results.append(
            CheckResult(
                check_name="diff_removed_dates",
                layer=ValidationLayer.DATA_DIFF,
                passed=False,
                severity=ValidationSeverity.WARNING,
                message=f"{series_id}: {len(removed)} dates in DB but not in API",
                source=source,
                series_id=series_id,
                timestamp=now,
                details={"removed_count": len(removed), "removed_dates": removed[:10]},
            )
        )

    # ── Revised values ───────────────────────────────────────────
    if revised:
        results.append(
            CheckResult(
                check_name="diff_revised_values",
                layer=ValidationLayer.DATA_DIFF,
                passed=True,  # Revisions are expected for macro data
                severity=ValidationSeverity.INFO,
                message=f"{series_id}: {len(revised)} values revised",
                source=source,
                series_id=series_id,
                timestamp=now,
                details={
                    "revision_count": len(revised),
                    "revisions": [
                        {"date": d, "old": old, "new": new}
                        for d, old, new in revised[:10]
                    ],
                },
            )
        )

    return results

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any

from ._store import ValidationStore
from ._types import (
    CheckResult,
    SchemaFingerprint,
    ValidationLayer,
    ValidationSeverity,
)


def _extract_fingerprint(
    source: str,
    endpoint: str,
    items: list[dict[str, Any]],
) -> SchemaFingerprint:
    """Build a SchemaFingerprint from a sample of raw API response dicts."""
    all_fields: set[str] = set()
    type_counts: dict[str, dict[str, int]] = {}

    for item in items:
        if not isinstance(item, dict):
            continue
        for key, value in item.items():
            all_fields.add(key)
            type_name = type(value).__name__
            type_counts.setdefault(key, {})
            type_counts[key][type_name] = type_counts[key].get(type_name, 0) + 1

    field_names = tuple(sorted(all_fields))
    field_types: dict[str, str] = {}
    for fname in field_names:
        counts = type_counts.get(fname, {})
        if counts:
            field_types[fname] = max(counts, key=lambda t: counts[t])
        else:
            field_types[fname] = "unknown"

    sample_hash = hashlib.sha256(
        json.dumps(field_names, sort_keys=True).encode()
    ).hexdigest()[:16]

    return SchemaFingerprint(
        source=source,
        endpoint=endpoint,
        field_names=field_names,
        field_types=field_types,
        sample_hash=sample_hash,
        captured_at=datetime.now(UTC).isoformat(),
    )


def _fingerprint_to_dict(fp: SchemaFingerprint) -> dict[str, Any]:
    return {
        "source": fp.source,
        "endpoint": fp.endpoint,
        "field_names": list(fp.field_names),
        "field_types": fp.field_types,
        "sample_hash": fp.sample_hash,
        "captured_at": fp.captured_at,
    }


def _fingerprint_from_dict(d: dict[str, Any]) -> SchemaFingerprint:
    return SchemaFingerprint(
        source=d["source"],
        endpoint=d["endpoint"],
        field_names=tuple(d["field_names"]),
        field_types=d["field_types"],
        sample_hash=d["sample_hash"],
        captured_at=d["captured_at"],
    )


def check_schema(
    source: str,
    endpoint: str,
    sample_items: list[Any],
    validation_store: ValidationStore,
    *,
    max_sample: int = 10,
) -> list[CheckResult]:
    """Compare current API response structure against stored baseline.

    On first run, captures and stores the baseline, returning INFO results.
    On subsequent runs, detects field additions, removals, and type changes.
    """
    results: list[CheckResult] = []
    now = datetime.now(UTC).isoformat()

    dicts = [
        item for item in sample_items[:max_sample] if isinstance(item, dict)
    ]
    if not dicts:
        results.append(
            CheckResult(
                check_name="schema_sample_available",
                layer=ValidationLayer.SCHEMA,
                passed=False,
                severity=ValidationSeverity.WARNING,
                message=f"No dict-type items available for schema check on {source}/{endpoint}",
                source=source,
                timestamp=now,
            )
        )
        return results

    current = _extract_fingerprint(source, endpoint, dicts)
    stored = validation_store.get_baseline(source, endpoint, "schema_fingerprint")

    if stored is None:
        validation_store.save_baseline(
            source,
            endpoint,
            "schema_fingerprint",
            _fingerprint_to_dict(current),
            now,
        )
        results.append(
            CheckResult(
                check_name="schema_baseline_captured",
                layer=ValidationLayer.SCHEMA,
                passed=True,
                severity=ValidationSeverity.INFO,
                message=f"Schema baseline captured for {source}/{endpoint}: {len(current.field_names)} fields",
                source=source,
                timestamp=now,
                details={"field_count": len(current.field_names)},
            )
        )
        return results

    baseline = _fingerprint_from_dict(stored)

    baseline_fields = set(baseline.field_names)
    current_fields = set(current.field_names)
    added = current_fields - baseline_fields
    removed = baseline_fields - current_fields

    if removed:
        results.append(
            CheckResult(
                check_name="schema_fields_removed",
                layer=ValidationLayer.SCHEMA,
                passed=False,
                severity=ValidationSeverity.ERROR,
                message=f"{source}/{endpoint}: {len(removed)} fields removed: {sorted(removed)}",
                source=source,
                timestamp=now,
                details={"removed_fields": sorted(removed)},
            )
        )
    if added:
        results.append(
            CheckResult(
                check_name="schema_fields_added",
                layer=ValidationLayer.SCHEMA,
                passed=True,
                severity=ValidationSeverity.INFO,
                message=f"{source}/{endpoint}: {len(added)} new fields: {sorted(added)}",
                source=source,
                timestamp=now,
                details={"added_fields": sorted(added)},
            )
        )

    type_changes: dict[str, tuple[str, str]] = {}
    common = baseline_fields & current_fields
    for fname in common:
        old_type = baseline.field_types.get(fname, "unknown")
        new_type = current.field_types.get(fname, "unknown")
        if old_type != new_type:
            type_changes[fname] = (old_type, new_type)

    if type_changes:
        results.append(
            CheckResult(
                check_name="schema_type_changes",
                layer=ValidationLayer.SCHEMA,
                passed=False,
                severity=ValidationSeverity.ERROR,
                message=f"{source}/{endpoint}: {len(type_changes)} type changes detected",
                source=source,
                timestamp=now,
                details={
                    "type_changes": {
                        k: {"old": v[0], "new": v[1]}
                        for k, v in type_changes.items()
                    }
                },
            )
        )

    if not removed and not type_changes:
        results.append(
            CheckResult(
                check_name="schema_consistent",
                layer=ValidationLayer.SCHEMA,
                passed=True,
                severity=ValidationSeverity.INFO,
                message=f"{source}/{endpoint}: schema consistent with baseline",
                source=source,
                timestamp=now,
            )
        )
        # Update baseline to include any new fields
        if added:
            validation_store.save_baseline(
                source,
                endpoint,
                "schema_fingerprint",
                _fingerprint_to_dict(current),
                now,
            )

    return results

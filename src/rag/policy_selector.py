"""Policy selector — mode-based, simplified (no canary rollout)."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional

from .policy_loader import PolicyStore

log = logging.getLogger(__name__)


@dataclass
class SelectionResult:
    policy: Dict[str, Any]
    selection_reason: str


def _version_tuple(value: str) -> tuple:
    if not value:
        return (0, 0, 0)
    parts = value.split(".")
    try:
        return tuple(int(p) for p in parts)
    except Exception:
        return (0, 0, 0)


def _policy_sort_key(policy: Dict[str, Any]) -> str:
    return policy.get("version", "0.0.0")


def select_policy(
    store: PolicyStore,
    mode: str,
    policy_id: Optional[str] = None,
) -> SelectionResult:
    if policy_id:
        policy = store.by_id.get(policy_id)
        if not policy:
            raise ValueError("POLICY_NOT_FOUND")
        return SelectionResult(policy=policy, selection_reason="override")

    candidates = [p for p in store.policies if p.get("mode") == mode]
    if not candidates:
        raise ValueError(f"NO_POLICY_FOR_MODE:{mode}")

    # Prefer stable track, highest version
    stable = [p for p in candidates if p.get("track", "stable") == "stable"]
    pool = stable if stable else candidates
    pool.sort(key=_policy_sort_key, reverse=True)
    return SelectionResult(policy=pool[0], selection_reason="stable_default")

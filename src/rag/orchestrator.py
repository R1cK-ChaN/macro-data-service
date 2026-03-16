"""Orchestrator — fallback stage application.

Simplified from rag-service ``app/retrieval/orchestrator.py``.
Drops request-size limits and response-size capping.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List


def apply_fallback(policy: Dict[str, Any], stage: Dict[str, Any]) -> Dict[str, Any]:
    policy = deepcopy(policy)
    action = stage.get("action")
    params = stage.get("params") or {}

    if action == "relax_source_type":
        add = params.get("add", [])
        policy.setdefault("route", {}).setdefault("filters", {}).setdefault(
            "source_type", {}
        ).setdefault("include", [])
        policy["route"]["filters"]["source_type"]["include"] = list(
            set(policy["route"]["filters"]["source_type"]["include"]) | set(add)
        )
    elif action == "increase_budget":
        mult = params.get("multiply", 1.0)
        budget = policy.setdefault("route", {}).setdefault("budget", {})
        budget["candidate_budget"] = int(budget.get("candidate_budget", 40) * mult)
        budget["final_context_k"] = int(budget.get("final_context_k", 8) * mult)
    elif action == "disable_filters":
        policy.setdefault("route", {})["filters"] = {}

    return policy

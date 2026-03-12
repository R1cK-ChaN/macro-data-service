"""YAML policy loader — simplified from rag-service (no JSON schema validation)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, List

import yaml


@dataclass
class PolicyStore:
    policies: List[Dict[str, Any]]
    by_id: Dict[str, Dict[str, Any]]


def load_policies(policy_dir: str) -> PolicyStore:
    policies: List[Dict[str, Any]] = []
    by_id: Dict[str, Dict[str, Any]] = {}

    if not policy_dir or not os.path.isdir(policy_dir):
        return PolicyStore(policies=policies, by_id=by_id)

    for root, _, files in os.walk(policy_dir):
        for name in files:
            if not (name.endswith(".yaml") or name.endswith(".yml")):
                continue
            path = os.path.join(root, name)
            with open(path, "r", encoding="utf-8") as f:
                policy = yaml.safe_load(f)
            if not isinstance(policy, dict) or "id" not in policy:
                continue
            policy_id = policy["id"]
            policies.append(policy)
            by_id[policy_id] = policy

    return PolicyStore(policies=policies, by_id=by_id)

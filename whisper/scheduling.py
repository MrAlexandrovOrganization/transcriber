"""Operator-owned pool priorities; workers cannot choose their own priority."""

from __future__ import annotations

import json
import re


def parse_policies(value: str) -> dict[str, dict[str, int]]:
    """An empty setting retains the legacy preferred-worker policy."""
    if not value.strip():
        return {}
    try:
        policies = json.loads(value)
        if not isinstance(policies, dict) or not 1 <= len(policies) <= 128:
            raise ValueError()
        for worker_id, policy in policies.items():
            if (
                worker_id in {"ingress", "other"}
                or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", worker_id)
                or not isinstance(policy, dict)
                or set(policy) != {"priority", "max_concurrency"}
                or type(policy["priority"]) is not int
                or not 0 <= policy["priority"] <= 10000
                or type(policy["max_concurrency"]) is not int
                or not 0 <= policy["max_concurrency"] <= 32
            ):
                raise ValueError()
    except (ValueError, TypeError):
        # Configuration may contain accidental secrets; never echo it.
        raise ValueError("Invalid WORKER_POLICIES configuration") from None
    return policies


def worker_concurrency(value: str) -> int:
    try:
        concurrency = int(value)
        if not 1 <= concurrency <= 16:
            raise ValueError()
    except ValueError:
        raise ValueError("WORKER_CONCURRENCY must be within 1..16") from None
    return concurrency

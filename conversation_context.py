"""Build the only context shape that a conversation provider may receive."""

from __future__ import annotations

from typing import Any


def _dataset_summary(dataset: object) -> dict[str, Any] | None:
    if not isinstance(dataset, dict) or not isinstance(dataset.get("summary"), dict):
        return None
    summary = dataset["summary"]
    return {
        key: summary[key]
        for key in ("dataset_id", "revision", "status", "artifacts", "checks", "issues")
        if key in summary
    }


def _routing_summary(routing: object) -> dict[str, Any] | None:
    if not isinstance(routing, dict):
        return None
    value = {
        key: routing[key]
        for key in ("mode", "requested_mode", "effective_mode", "reason")
        if key in routing
    }
    return value or None


def _plan_summary(plan: object) -> dict[str, Any] | None:
    if not isinstance(plan, dict):
        return None
    options = plan.get("options") if isinstance(plan.get("options"), dict) else {}
    steps = [
        {
            key: step[key]
            for key in ("id", "label", "depends_on", "is_deliverable")
            if key in step
        }
        for step in plan.get("steps", [])
        if isinstance(step, dict)
    ]
    value = {
        key: plan[key]
        for key in ("title", "summary", "goals", "ready_to_execute")
        if key in plan
    }
    if "viral_type" in options:
        value["viral_type"] = options["viral_type"]
    routing = _routing_summary(plan.get("routing"))
    if routing:
        value["routing"] = routing
    if steps:
        value["steps"] = steps
    return value or None


def build_conversation_context(
    *,
    routing: object = None,
    dataset: object = None,
    plan: object = None,
) -> dict[str, Any]:
    """Return safe summaries without paths, fingerprints, execution specs, or runner data."""

    return {
        "routing": _routing_summary(routing),
        "dataset": _dataset_summary(dataset),
        "plan": _plan_summary(plan),
    }

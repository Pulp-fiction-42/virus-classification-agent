"""Strict evaluation split and metric protocol for the four provider tasks."""

from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1
SPLITS = ("development", "training", "validation", "final_test")
EXPECTED_METRICS = {
    "schema_pass_rate",
    "legal_action_or_boundary_pass_rate",
    "provider_error_rate",
    "total_latency_ms",
    "citation_semantic_applicability",
    "conflict_omission_and_hallucinated_fact_rate",
    "human_revision_cost",
}
MAX_PROTOCOL_BYTES = 512 * 1024


class EvaluationProtocolError(ValueError):
    pass


def _digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _mapping(value: Any, name: str, fields: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != fields:
        raise EvaluationProtocolError(f"{name} fields are invalid")
    return value


def _text(value: Any, name: str, maximum: int = 1000) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > maximum
        or any(character in value for character in ("\0", "\n", "\r"))
    ):
        raise EvaluationProtocolError(f"{name} is invalid")
    return value


def _ids(value: Any, name: str) -> list[str]:
    if not isinstance(value, list) or any(
        not isinstance(item, str) or not item for item in value
    ):
        raise EvaluationProtocolError(f"{name} must be a list of IDs")
    if value != sorted(value) or len(value) != len(set(value)):
        raise EvaluationProtocolError(f"{name} must be sorted and unique")
    return value


def load_protocol(
    path: Path, suite_path: Path, suite: dict[str, Any]
) -> dict[str, Any]:
    if not path.is_file() or path.is_symlink():
        raise EvaluationProtocolError("evaluation protocol must be a regular file")
    raw = path.read_bytes()
    if not raw or len(raw) > MAX_PROTOCOL_BYTES:
        raise EvaluationProtocolError("evaluation protocol size is invalid")
    try:
        value = _mapping(
            json.loads(raw),
            "evaluation protocol",
            {
                "schema_version",
                "protocol_id",
                "suite_id",
                "suite_sha256",
                "splits",
                "groups",
                "metrics",
                "release_gate",
            },
        )
    except (UnicodeError, json.JSONDecodeError) as error:
        raise EvaluationProtocolError("evaluation protocol JSON is invalid") from error
    if value["schema_version"] != SCHEMA_VERSION:
        raise EvaluationProtocolError("evaluation protocol schema is unsupported")
    protocol_id = _text(value["protocol_id"], "protocol_id", 128)
    suite_id = _text(value["suite_id"], "suite_id", 128)
    if suite_id != suite.get("suite_id"):
        raise EvaluationProtocolError("evaluation suite ID mismatch")
    suite_digest = value["suite_sha256"]
    if not isinstance(suite_digest, str) or suite_digest != _digest(
        suite_path.read_bytes()
    ):
        raise EvaluationProtocolError("evaluation suite SHA-256 mismatch")
    suite_cases = suite.get("cases", [])
    if not isinstance(suite_cases, list) or any(
        not isinstance(case, dict)
        or not isinstance(case.get("id"), str)
        or not case["id"]
        for case in suite_cases
    ):
        raise EvaluationProtocolError("evaluation suite case IDs are invalid")
    suite_ids = sorted(case["id"] for case in suite_cases)
    if len(suite_ids) != len(set(suite_ids)):
        raise EvaluationProtocolError("evaluation suite case IDs are invalid")

    splits = _mapping(value["splits"], "splits", set(SPLITS))
    normalized_splits = {}
    split_by_case = {}
    for split_name in SPLITS:
        split = _mapping(
            splits[split_name],
            f"splits.{split_name}",
            {"purpose", "allows_prompt_iteration", "case_ids"},
        )
        purpose = _text(split["purpose"], f"splits.{split_name}.purpose")
        if not isinstance(split["allows_prompt_iteration"], bool):
            raise EvaluationProtocolError(
                f"splits.{split_name}.allows_prompt_iteration must be boolean"
            )
        case_ids = _ids(split["case_ids"], f"splits.{split_name}.case_ids")
        for case_id in case_ids:
            if case_id in split_by_case:
                raise EvaluationProtocolError(
                    "evaluation case appears in multiple splits"
                )
            split_by_case[case_id] = split_name
        normalized_splits[split_name] = {
            "purpose": purpose,
            "allows_prompt_iteration": split["allows_prompt_iteration"],
            "case_ids": case_ids,
        }
    if sorted(split_by_case) != suite_ids:
        raise EvaluationProtocolError("evaluation splits do not partition the suite")
    if normalized_splits["development"]["allows_prompt_iteration"] is not True or any(
        normalized_splits[name]["allows_prompt_iteration"] for name in SPLITS[1:]
    ):
        raise EvaluationProtocolError(
            "evaluation split prompt-iteration policy is invalid"
        )

    groups = value["groups"]
    if not isinstance(groups, list) or not groups:
        raise EvaluationProtocolError("evaluation groups must be a non-empty list")
    normalized_groups = []
    grouped_cases = []
    group_ids = []
    for index, group in enumerate(groups):
        item = _mapping(group, f"groups[{index}]", {"group_id", "basis", "case_ids"})
        group_id = _text(item["group_id"], f"groups[{index}].group_id", 128)
        basis = _text(item["basis"], f"groups[{index}].basis")
        case_ids = _ids(item["case_ids"], f"groups[{index}].case_ids")
        split_names = {split_by_case.get(case_id) for case_id in case_ids}
        if None in split_names or len(split_names) != 1:
            raise EvaluationProtocolError("evaluation group crosses split boundaries")
        group_ids.append(group_id)
        grouped_cases.extend(case_ids)
        normalized_groups.append(
            {"group_id": group_id, "basis": basis, "case_ids": case_ids}
        )
    if len(group_ids) != len(set(group_ids)) or sorted(grouped_cases) != suite_ids:
        raise EvaluationProtocolError("evaluation groups must partition the suite")

    metrics = value["metrics"]
    if not isinstance(metrics, list):
        raise EvaluationProtocolError("evaluation metrics must be a list")
    normalized_metrics = []
    metric_ids = []
    for index, metric in enumerate(metrics):
        item = _mapping(
            metric,
            f"metrics[{index}]",
            {"metric_id", "category", "measurement", "automated", "direction"},
        )
        metric_id = _text(item["metric_id"], f"metrics[{index}].metric_id", 128)
        category = _text(item["category"], f"metrics[{index}].category", 128)
        measurement = _text(item["measurement"], f"metrics[{index}].measurement")
        if not isinstance(item["automated"], bool) or item["direction"] not in {
            "higher",
            "lower",
        }:
            raise EvaluationProtocolError("evaluation metric declaration is invalid")
        metric_ids.append(metric_id)
        normalized_metrics.append(
            {
                "metric_id": metric_id,
                "category": category,
                "measurement": measurement,
                "automated": item["automated"],
                "direction": item["direction"],
            }
        )
    if set(metric_ids) != EXPECTED_METRICS or len(metric_ids) != len(set(metric_ids)):
        raise EvaluationProtocolError(
            "evaluation metric set is incomplete or duplicated"
        )

    gate = _mapping(value["release_gate"], "release_gate", {"status", "blockers"})
    blockers = _ids(gate["blockers"], "release_gate.blockers")
    if gate["status"] != "blocked" or not blockers:
        raise EvaluationProtocolError(
            "unreviewed evaluation protocol must remain blocked"
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "protocol_id": protocol_id,
        "protocol_sha256": _digest(raw),
        "suite_id": suite_id,
        "suite_sha256": suite_digest,
        "splits": normalized_splits,
        "groups": normalized_groups,
        "metrics": normalized_metrics,
        "release_gate": {"status": gate["status"], "blockers": blockers},
    }


def _rate(numerator: int, denominator: int) -> dict[str, int | float | None]:
    return {
        "numerator": numerator,
        "denominator": denominator,
        "rate": numerator / denominator if denominator else None,
    }


def _percentile(values: list[int], percentile: float) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * percentile) - 1)]


def summarize_records(
    records: list[dict[str, Any]], protocol: dict[str, Any], *, model_evaluation: bool
) -> dict[str, Any]:
    case_split = {
        case_id: split_name
        for split_name, split in protocol["splits"].items()
        for case_id in split["case_ids"]
    }
    if sorted(case_split) != sorted(record["case"]["id"] for record in records):
        raise EvaluationProtocolError("evaluation records do not match protocol cases")
    automated = {
        "schema_pass_rate": _rate(
            sum(bool(record["schema_valid"]) for record in records), len(records)
        ),
        "legal_action_or_boundary_pass_rate": _rate(
            sum(bool(record["engineering_pass"]) for record in records), len(records)
        ),
    }
    if model_evaluation:
        automated["provider_error_rate"] = _rate(
            sum(record["result"]["error_code"] is not None for record in records),
            len(records),
        )
        latencies = [
            int(record["result"]["latency_ms"])
            for record in records
            if isinstance(record["result"].get("latency_ms"), (int, float))
            and not isinstance(record["result"].get("latency_ms"), bool)
            and record["result"]["latency_ms"] >= 0
        ]
        automated["total_latency_ms"] = {
            "status": "measured" if latencies else "unavailable",
            "count": len(latencies),
            "p50": _percentile(latencies, 0.5),
            "p95": _percentile(latencies, 0.95),
            "max": max(latencies) if latencies else None,
            "first_response_latency": "unavailable",
        }
    else:
        automated["provider_error_rate"] = {"status": "not_applicable"}
        automated["total_latency_ms"] = {"status": "not_applicable"}
    return {
        "protocol_id": protocol["protocol_id"],
        "protocol_sha256": protocol["protocol_sha256"],
        "split_counts": {
            split_name: sum(
                case_split[record["case"]["id"]] == split_name for record in records
            )
            for split_name in SPLITS
        },
        "automated": automated,
        "expert_pending": {
            "citation_semantic_applicability": "not_reviewed",
            "conflict_omission_and_hallucinated_fact_rate": "not_reviewed",
            "human_revision_cost": "not_measured",
        },
        "release_gate": protocol["release_gate"],
    }

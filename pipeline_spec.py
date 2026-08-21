"""Validation and deterministic compilation for analysis_pipeline specs."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from adaptive_routing import normalize_routing_request
from workflow_catalog import (
    ARTIFACTS,
    PAIRED_READ_TYPES,
    PRODUCER_BY_ARTIFACT,
    STAGE_BY_ID,
    STAGE_ORDER,
    stage_rules,
)


SAFE_ID_RE = re.compile(r"^[A-Za-z0-9_.-]+$")


class ExecutionSpecError(ValueError):
    """Raised when an execution spec cannot be validated or compiled."""


def _required_id(value: Any, field: str) -> str:
    if not isinstance(value, str) or not SAFE_ID_RE.fullmatch(value):
        raise ExecutionSpecError(f"{field} must contain only letters, digits, ._-")
    return value


def _absolute_path(value: Any, field: str, *, kind: str) -> str:
    if not isinstance(value, str) or not Path(value).is_absolute():
        raise ExecutionSpecError(f"{field} must be an absolute path")
    return value


def _normalize_input(raw: Any, index: int) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ExecutionSpecError(f"inputs[{index}] must be an object")
    input_id = _required_id(raw.get("id"), f"inputs[{index}].id")
    artifact_type = raw.get("type")
    artifact = ARTIFACTS.get(artifact_type)
    if artifact is None:
        raise ExecutionSpecError(f"inputs[{index}].type is unsupported")
    records = raw.get("records")
    if not isinstance(records, list) or not records:
        raise ExecutionSpecError(f"inputs[{index}].records must not be empty")
    if not artifact.collection and len(records) != 1:
        raise ExecutionSpecError(f"{artifact_type} requires exactly one record")

    normalized_records: list[dict[str, Any]] = []
    identities: set[tuple[str, ...]] = set()
    for record_index, record in enumerate(records):
        if not isinstance(record, dict):
            raise ExecutionSpecError(
                f"inputs[{index}].records[{record_index}] must be an object"
            )
        normalized: dict[str, Any] = {}
        for field in artifact.identity_fields:
            normalized[field] = _required_id(
                record.get(field),
                f"inputs[{index}].records[{record_index}].{field}",
            )
        identity = tuple(normalized[field] for field in artifact.identity_fields)
        if identity and identity in identities:
            raise ExecutionSpecError(f"{artifact_type} contains duplicate records")
        identities.add(identity)

        files = record.get("files")
        if not isinstance(files, dict):
            raise ExecutionSpecError(
                f"inputs[{index}].records[{record_index}].files must be an object"
            )
        normalized_files: dict[str, str] = {}
        for role in artifact.required_files:
            normalized_files[role] = _absolute_path(
                files.get(role),
                f"inputs[{index}].records[{record_index}].files.{role}",
                kind=artifact.kind(role),
            )
        normalized["files"] = normalized_files
        normalized_records.append(normalized)
    return {"id": input_id, "type": artifact_type, "records": normalized_records}


def _normalize_options(raw: Any) -> dict[str, Any]:
    value = raw if isinstance(raw, dict) else {}
    viral_type = value.get("viral_type", "RNA+DNA")
    if viral_type not in {"RNA", "DNA", "RNA+DNA"}:
        raise ExecutionSpecError("options.viral_type must be RNA, DNA, or RNA+DNA")
    return {"viral_type": viral_type}


def _choose_requirement(
    alternatives: tuple[str, ...],
    available: set[str],
    selected: set[str],
    bindings: dict[str, dict[str, str]],
    visiting: set[str],
) -> str:
    for artifact_type in alternatives:
        if artifact_type in available:
            return artifact_type
    missing = []
    failures = []
    for artifact_type in alternatives:
        producer = PRODUCER_BY_ARTIFACT.get(artifact_type)
        if producer is None:
            missing.append(artifact_type)
            continue
        available_before = set(available)
        selected_before = set(selected)
        bindings_before = dict(bindings)
        try:
            _select_stage(producer.id, available, selected, bindings, visiting)
            return artifact_type
        except ExecutionSpecError as exc:
            available.clear()
            available.update(available_before)
            selected.clear()
            selected.update(selected_before)
            bindings.clear()
            bindings.update(bindings_before)
            failures.append(str(exc))
            continue
    if failures and not missing:
        raise ExecutionSpecError(failures[-1])
    raise ExecutionSpecError(
        "missing required input artifact: " + " or ".join(missing or alternatives)
    )


def _select_stage(
    stage_id: str,
    available: set[str],
    selected: set[str],
    bindings: dict[str, dict[str, str]],
    visiting: set[str],
) -> None:
    if stage_id in selected:
        return
    if stage_id in visiting:
        raise ExecutionSpecError("analysis stage graph contains a cycle")
    stage = STAGE_BY_ID[stage_id]
    visiting.add(stage_id)
    try:
        stage_bindings: dict[str, str] = {}
        for index, alternatives in enumerate(stage.requirements):
            artifact_type = _choose_requirement(
                alternatives, available, selected, bindings, visiting
            )
            key = (
                "contigs" if stage.id == "virus_sequence_prep" else f"input_{index}"
            )
            stage_bindings[key] = artifact_type
    finally:
        visiting.remove(stage_id)
    bindings[stage_id] = stage_bindings
    selected.add(stage_id)
    available.add(stage.produces)


def _compile(
    inputs: list[dict[str, Any]], deliverables: list[str], options: dict[str, Any]
) -> tuple[list[dict[str, Any]], set[str]]:
    available = {item["type"] for item in inputs}
    selected: set[str] = set()
    bindings: dict[str, dict[str, str]] = {}
    for deliverable in deliverables:
        _select_stage(deliverable, available, selected, bindings, set())

    ordered = sorted(selected, key=STAGE_ORDER.__getitem__)
    produced_by = {
        STAGE_BY_ID[stage_id].produces: stage_id for stage_id in ordered
    }
    stages: list[dict[str, Any]] = []
    for stage_id in ordered:
        stage = STAGE_BY_ID[stage_id]
        dependencies = []
        for artifact_type in bindings[stage_id].values():
            producer_id = produced_by.get(artifact_type)
            if producer_id and producer_id not in dependencies:
                dependencies.append(producer_id)
        stages.append(
            {
                "id": stage.id,
                "label": stage.label,
                "workflow_id": stage.workflow_id,
                "rules": list(stage_rules(stage, options, bindings[stage_id])),
                "depends_on": dependencies,
                "produces": stage.produces,
                "is_deliverable": stage.id in deliverables,
                "bindings": bindings[stage_id],
            }
        )

    used_types = {
        artifact_type
        for stage in stages
        for artifact_type in stage["bindings"].values()
        if artifact_type not in produced_by
    }
    return stages, used_types


def _validate_sample_alignment(
    inputs: list[dict[str, Any]], used_types: set[str]
) -> None:
    sample_sets = {
        item["id"]: {record["sample_id"] for record in item["records"]}
        for item in inputs
        if item["type"] in used_types
        and "sample_id" in ARTIFACTS[item["type"]].identity_fields
    }
    if len(sample_sets) < 2:
        return
    expected_id, expected = next(iter(sample_sets.items()))
    for input_id, samples in sample_sets.items():
        if samples != expected:
            raise ExecutionSpecError(
                f"sample sets must match between {expected_id} and {input_id}"
            )


def normalize_execution_spec(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ExecutionSpecError("missing execution_spec")
    if raw.get("adapter") != "analysis_pipeline" or raw.get("version") != 2:
        raise ExecutionSpecError("execution_spec must use analysis_pipeline version 2")

    raw_inputs = raw.get("inputs")
    if not isinstance(raw_inputs, list) or not raw_inputs:
        raise ExecutionSpecError("analysis_pipeline requires at least one input")
    inputs = [_normalize_input(value, index) for index, value in enumerate(raw_inputs)]
    ids = [value["id"] for value in inputs]
    if len(ids) != len(set(ids)):
        raise ExecutionSpecError("execution input ids must be unique")
    types = [value["type"] for value in inputs]
    if len(types) != len(set(types)):
        raise ExecutionSpecError("only one input bundle of each artifact type is allowed")

    raw_deliverables = raw.get("deliverables")
    if not isinstance(raw_deliverables, list) or not raw_deliverables:
        raise ExecutionSpecError("deliverables must not be empty")
    deliverables: list[str] = []
    for value in raw_deliverables:
        if not isinstance(value, str) or value not in STAGE_BY_ID:
            raise ExecutionSpecError(f"unsupported deliverable: {value}")
        if value not in deliverables:
            deliverables.append(value)

    options = _normalize_options(raw.get("options"))
    stages, used_types = _compile(inputs, deliverables, options)
    unused = [item["id"] for item in inputs if item["type"] not in used_types]
    if unused:
        raise ExecutionSpecError("unused execution inputs: " + ", ".join(unused))
    _validate_sample_alignment(inputs, used_types)

    raw_policy = raw.get("policy")
    if isinstance(raw_policy, dict) and "requested_mode" in raw_policy:
        raw_policy = {
            "mode": raw_policy.get("requested_mode"),
            **(
                {"deadline_seconds": raw_policy["deadline_seconds"]}
                if "deadline_seconds" in raw_policy
                else {}
            ),
        }
    try:
        requested_policy = normalize_routing_request(raw_policy)
    except ValueError as exc:
        raise ExecutionSpecError(str(exc)) from exc
    paired_type = next((value for value in used_types if value in PAIRED_READ_TYPES), None)
    requested_mode = requested_policy["mode"]
    adaptive = requested_mode in {"auto", "emergency"} and paired_type is not None
    effective_mode = requested_mode if adaptive or requested_mode == "deep" else "standard"
    policy = {
        "requested_mode": requested_mode,
        "effective_mode": effective_mode,
        "adaptive": adaptive,
        "short_circuit_scope": "pipeline",
        "applicability_reason": (
            "paired_reads_available"
            if adaptive
            else (
                "paired_reads_unavailable"
                if requested_mode in {"auto", "emergency"}
                else "screening_not_requested"
            )
        ),
        **(
            {"adaptive_input_type": paired_type, "sample_fraction": 0.05}
            if adaptive
            else {}
        ),
        **(
            {"deadline_seconds": requested_policy["deadline_seconds"]}
            if "deadline_seconds" in requested_policy
            else {}
        ),
    }
    return {
        "version": 2,
        "adapter": "analysis_pipeline",
        "inputs": inputs,
        "deliverables": deliverables,
        "options": options,
        "policy": policy,
        "stages": stages,
    }


def normalize_plan_execution_spec(
    raw: Any, _steps: list[dict[str, Any]] | None = None
) -> dict[str, Any]:
    return normalize_execution_spec(raw)


def selected_rules(spec: dict[str, Any]) -> tuple[str, ...]:
    normalized = normalize_execution_spec(spec) if "stages" not in spec else spec
    adaptive = (
        ("adaptive_screen", "adaptive_route")
        if normalized["policy"].get("adaptive")
        else ()
    )
    return (
        *adaptive,
        *(rule for stage in normalized["stages"] for rule in stage["rules"]),
    )


def canonical_steps(spec: dict[str, Any]) -> list[dict[str, Any]]:
    normalized = normalize_execution_spec(spec) if "stages" not in spec else spec
    steps = []
    if normalized["policy"].get("adaptive"):
        steps.extend(
            [
                {
                    "id": "adaptive_screen",
                    "label": "5% 快速初筛",
                    "tool": "Kraken2",
                    "rules": ["adaptive_screen"],
                    "depends_on": [],
                    "is_deliverable": False,
                },
                {
                    "id": "adaptive_route",
                    "label": "自适应路由决策",
                    "rules": ["adaptive_route"],
                    "depends_on": ["adaptive_screen"],
                    "is_deliverable": False,
                },
            ]
        )
    for stage in normalized["stages"]:
        dependencies = list(stage["depends_on"])
        if normalized["policy"].get("adaptive") and not dependencies:
            dependencies = ["adaptive_route"]
        steps.append(
            {
                "id": stage["id"],
                "label": stage["label"],
                "rules": list(stage["rules"]),
                "depends_on": dependencies,
                "produces": stage["produces"],
                "is_deliverable": stage["is_deliverable"],
            }
        )
    return steps


def validate_plan_rules(
    spec: dict[str, Any], steps: list[dict[str, Any]]
) -> tuple[str, ...]:
    expected_steps = canonical_steps(spec)
    if [step.get("id") for step in steps] != [step["id"] for step in expected_steps]:
        raise ExecutionSpecError("plan steps do not match the compiled analysis pipeline")
    return selected_rules(spec)


def input_by_type(spec: dict[str, Any], *artifact_types: str) -> dict[str, Any] | None:
    return next(
        (item for item in spec.get("inputs", []) if item.get("type") in artifact_types),
        None,
    )

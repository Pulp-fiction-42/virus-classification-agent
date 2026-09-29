"""Repo-tracked workflow skill loading and immutable invocation compilation."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


class WorkflowSkillError(ValueError):
    def __init__(self, message: str, *, code: str) -> None:
        super().__init__(message)
        self.code = code


SKILL_ROOT = Path(__file__).resolve().parent / "skills" / "bioagent-w1-read-qc"
MANIFEST_PATH = SKILL_ROOT / "manifest.json"
INSTRUCTIONS_PATH = SKILL_ROOT / "SKILL.md"


def _canonical_digest(value: Any) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def load_workflow_skill() -> dict[str, Any]:
    try:
        manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
        instructions = INSTRUCTIONS_PATH.read_bytes()
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise WorkflowSkillError(
            "workflow skill release is unavailable", code="SKILL_NOT_FOUND"
        ) from exc
    required = {
        "schema_version",
        "id",
        "version",
        "capability_id",
        "stage_id",
        "tool",
        "preset_id",
        "defaults",
        "parameters",
        "input_artifacts",
        "output_artifacts",
        "compile_adapter",
    }
    if not isinstance(manifest, dict) or set(manifest) != required:
        raise WorkflowSkillError(
            "workflow skill manifest is invalid", code="SKILL_VERSION_UNAVAILABLE"
        )
    return {
        **manifest,
        "manifest_sha256": _canonical_digest(manifest),
        "instructions_sha256": hashlib.sha256(instructions).hexdigest(),
    }


def public_workflow_skill_catalog() -> list[dict[str, Any]]:
    skill = load_workflow_skill()
    return [
        {
            key: skill[key]
            for key in (
                "id",
                "version",
                "stage_id",
                "tool",
                "preset_id",
                "defaults",
                "parameters",
                "input_artifacts",
                "output_artifacts",
                "compile_adapter",
                "manifest_sha256",
                "instructions_sha256",
            )
        }
    ]


def _request(raw: Any) -> dict[str, Any] | None:
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise WorkflowSkillError(
            "workflow_skill must be an object", code="SKILL_VERSION_UNAVAILABLE"
        )
    allowed = {"skill_id", "skill_version", "preset_id"}
    unsupported = set(raw) - allowed
    if unsupported:
        raise WorkflowSkillError(
            "workflow skill request contains unsupported fields: "
            + ", ".join(sorted(unsupported)),
            code="PARAMETER_UNKNOWN",
        )
    return {
        "skill_id": raw.get("skill_id"),
        "skill_version": raw.get("skill_version"),
        "preset_id": raw.get("preset_id", "workflow_default"),
    }


def build_skill_invocation(
    *,
    selected_stage_ids: set[str],
    requested: Any,
    capability_parameters: list[dict[str, Any]],
) -> dict[str, Any] | None:
    parsed = _request(requested)
    if "read_qc" not in selected_stage_ids:
        if parsed is not None:
            raise WorkflowSkillError(
                "workflow skill is not applicable because read_qc is not selected",
                code="PARAMETER_NOT_APPLICABLE",
            )
        return None
    skill = load_workflow_skill()
    if parsed is None:
        parsed = {
            "skill_id": skill["id"],
            "skill_version": skill["version"],
            "preset_id": skill["preset_id"],
        }
    if parsed["skill_id"] != skill["id"]:
        raise WorkflowSkillError(
            f"workflow skill is not registered: {parsed['skill_id']}",
            code="SKILL_NOT_FOUND",
        )
    if parsed["skill_version"] != skill["version"]:
        raise WorkflowSkillError(
            f"workflow skill version is unavailable: {parsed['skill_id']}@{parsed['skill_version']}",
            code="SKILL_VERSION_UNAVAILABLE",
        )
    if parsed["preset_id"] != skill["preset_id"]:
        raise WorkflowSkillError(
            f"workflow skill preset is unavailable: {parsed['preset_id']}",
            code="PARAMETER_UNKNOWN",
        )
    selection = next(
        (
            item
            for item in capability_parameters
            if item.get("capability_id") == skill["capability_id"]
        ),
        None,
    )
    selected_values = dict((selection or {}).get("values") or {})
    allowed_parameters = set(skill["parameters"])
    if set(selected_values) - allowed_parameters:
        raise WorkflowSkillError(
            "workflow skill received an unpublished parameter", code="PARAMETER_UNKNOWN"
        )
    effective = {**skill["defaults"], **selected_values}
    overrides = {
        key: value
        for key, value in selected_values.items()
        if skill["defaults"].get(key) != value
    }
    frozen = {
        "schema_version": 1,
        "skill_id": skill["id"],
        "skill_version": skill["version"],
        "manifest_sha256": skill["manifest_sha256"],
        "instructions_sha256": skill["instructions_sha256"],
        "preset_id": skill["preset_id"],
        "tool": skill["tool"],
        "capability_id": skill["capability_id"],
        "compile_adapter": skill["compile_adapter"],
        "effective_parameters": {key: effective[key] for key in sorted(effective)},
        "overrides": {key: overrides[key] for key in sorted(overrides)},
    }
    return {**frozen, "compiled_sha256": _canonical_digest(frozen)}

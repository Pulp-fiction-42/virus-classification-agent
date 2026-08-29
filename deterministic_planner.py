"""Deterministic analysis planning and execution admission."""

from __future__ import annotations

import re
from typing import Any

from pipeline_spec import (
    ExecutionSpecError,
    canonical_steps,
    compile_execution_spec,
    normalize_execution_spec,
    validate_plan_rules,
)


PLANNER_VERSION = 1
GOAL_LABELS = {
    "contig_filter": "质控与组装",
    "virus_summary": "病毒识别",
    "host_summary": "宿主识别",
    "microbe_summary": "微生物谱",
    "virus_rpm": "病毒定量",
    "annotation_delivery": "病毒注释",
}
GOAL_ORDER = tuple(GOAL_LABELS)
VIRAL_TYPES = {"RNA", "DNA", "RNA+DNA"}
ROUTING_MODES = {"standard", "emergency"}
RUN_ID_RE = re.compile(r"^analysis-[A-Za-z0-9_.-]+$")


class PlannerError(ValueError):
    """Raised when structured planning input is invalid."""


def _dataset_inputs(dataset: dict[str, Any]) -> list[dict[str, Any]]:
    artifacts = dataset.get("artifacts")
    if not isinstance(artifacts, list) or not artifacts:
        raise PlannerError("analysis dataset has no assigned artifacts")

    inputs = []
    for artifact in artifacts:
        if not isinstance(artifact, dict):
            raise PlannerError("analysis dataset artifacts must be objects")
        records = []
        for record in artifact.get("records") or []:
            files = record.get("files") if isinstance(record, dict) else None
            if not isinstance(files, dict):
                raise PlannerError("analysis dataset record files are missing")
            records.append(
                {
                    key: value
                    for key, value in record.items()
                    if key not in {"record_id", "files"}
                }
                | {
                    "files": {
                        role: value.get("path")
                        for role, value in files.items()
                        if isinstance(value, dict)
                    }
                }
            )
        inputs.append(
            {
                "id": artifact.get("id"),
                "type": artifact.get("type"),
                "records": records,
            }
        )
    return inputs


def _normalize_goals(raw: Any) -> list[str]:
    if not isinstance(raw, list) or not raw:
        raise PlannerError("at least one analysis goal is required")
    unknown = {value for value in raw if value not in GOAL_LABELS}
    if unknown:
        raise PlannerError(
            "unsupported analysis goal: " + ", ".join(sorted(map(str, unknown)))
        )
    return [goal for goal in GOAL_ORDER if goal in raw]


def _normalize_options(raw: Any) -> dict[str, str]:
    value = raw if isinstance(raw, dict) else {}
    viral_type = value.get("viral_type", "RNA+DNA")
    if viral_type not in VIRAL_TYPES:
        raise PlannerError("viral_type must be RNA, DNA, or RNA+DNA")
    return {"viral_type": viral_type}


def _normalize_routing(raw: Any) -> dict[str, Any]:
    value = raw if isinstance(raw, dict) else {}
    mode = value.get("mode", "standard")
    if mode not in ROUTING_MODES:
        raise PlannerError("routing mode must be standard or emergency")
    deadline = value.get("deadline_seconds")
    if mode == "emergency" and deadline is not None:
        if (
            isinstance(deadline, bool)
            or not isinstance(deadline, (int, float))
            or deadline <= 0
        ):
            raise PlannerError("deadline_seconds must be positive")
        return {"mode": mode, "deadline_seconds": int(deadline)}
    return {"mode": mode}


def _dataset_label(dataset: dict[str, Any]) -> str:
    samples: list[str] = []
    for artifact in dataset.get("artifacts") or []:
        for record in artifact.get("records") or []:
            value = record.get("sample_id") or record.get("run_id")
            if isinstance(value, str) and value not in samples:
                samples.append(value)
    if samples:
        return "、".join(samples[:2]) + (
            f" 等 {len(samples)} 个样本" if len(samples) > 2 else ""
        )
    return str(dataset.get("id") or "数据集")


class DeterministicPlanner:
    def __init__(self, session_manager: Any):
        self.session_manager = session_manager

    def compile(
        self,
        *,
        dataset: dict[str, Any],
        goals: Any,
        options: Any,
        routing: Any,
    ) -> dict[str, Any]:
        if not isinstance(dataset, dict):
            raise PlannerError("analysis dataset is required")
        status = dataset.get("status")
        if status not in {"checking", "ready"}:
            raise PlannerError(
                "analysis dataset cannot be planned in its current state"
            )

        normalized_goals = _normalize_goals(goals)
        normalized_options = _normalize_options(options)
        normalized_routing = _normalize_routing(routing)
        try:
            execution_spec = compile_execution_spec(
                {
                    "version": 2,
                    "adapter": "analysis_pipeline",
                    "inputs": _dataset_inputs(dataset),
                    "deliverables": normalized_goals,
                    "options": normalized_options,
                    "policy": normalized_routing,
                }
            )
        except ExecutionSpecError as exc:
            raise PlannerError(str(exc)) from exc

        steps = canonical_steps(execution_spec)
        labels = [GOAL_LABELS[goal] for goal in normalized_goals]
        title = f"{_dataset_label(dataset)} · {'、'.join(labels)}"
        ready = status == "ready"
        plan = {
            "schema_version": 1,
            "planner_version": PLANNER_VERSION,
            "title": title,
            "summary": f"针对 {_dataset_label(dataset)} 执行{'、'.join(labels)}。",
            "dataset": dataset,
            "goals": normalized_goals,
            "options": normalized_options,
            "routing": {
                **execution_spec["policy"],
                "available_routes": (
                    ["A"]
                    if execution_spec["policy"].get("effective_mode") == "emergency"
                    else ["A", "B"]
                    if execution_spec["policy"].get("adaptive")
                    else []
                ),
            },
            "execution_spec": execution_spec,
            "steps": steps,
            "ready_to_execute": ready,
            **(
                {"blocking_reason": "数据集正在执行全量预检，完成前不能启动分析。"}
                if not ready
                else {}
            ),
            "decision_trace": [
                {
                    "kind": "dataset",
                    "status": status,
                    "revision": dataset.get("revision"),
                },
                {"kind": "goals", "values": normalized_goals},
                {"kind": "routing", "value": execution_spec["policy"]},
                {"kind": "compiler", "planner_version": PLANNER_VERSION},
            ],
        }
        try:
            plan["resource_estimate"] = (
                self.session_manager.resource_calibrator.estimate(execution_spec)
            )
        except Exception:
            pass
        return plan

    async def start(
        self,
        *,
        persisted_plan: dict[str, Any],
        dataset: dict[str, Any],
        chat_id: str,
        plan_id: str,
        plan_revision: int,
        run_id: str,
    ) -> dict[str, Any]:
        if not RUN_ID_RE.fullmatch(run_id):
            raise PlannerError("invalid analysis run ID")
        if dataset.get("status") != "ready":
            raise PlannerError("analysis dataset is not ready")
        compiled = self.compile(
            dataset=dataset,
            goals=persisted_plan.get("goals"),
            options=persisted_plan.get("options"),
            routing={
                "mode": (persisted_plan.get("routing") or {}).get("requested_mode"),
                **(
                    {"deadline_seconds": persisted_plan["routing"]["deadline_seconds"]}
                    if isinstance(persisted_plan.get("routing"), dict)
                    and "deadline_seconds" in persisted_plan["routing"]
                    else {}
                ),
            },
        )
        try:
            persisted_spec = normalize_execution_spec(
                persisted_plan.get("execution_spec")
            )
            validate_plan_rules(persisted_spec, persisted_plan.get("steps") or [])
        except ExecutionSpecError as exc:
            raise PlannerError(f"persisted analysis plan is invalid: {exc}") from exc
        if (
            persisted_spec != compiled["execution_spec"]
            or persisted_plan.get("steps") != compiled["steps"]
        ):
            raise PlannerError(
                "persisted analysis plan does not match the authoritative dataset"
            )

        run_dir = self.session_manager.analysis_run_root / run_id
        runtime_plan = {
            **persisted_plan,
            "dataset": dataset,
            "execution_spec": persisted_spec,
            "plan_ref": {"id": plan_id, "revision": plan_revision},
            "run": {
                "run_id": run_id,
                "run_dir": str(run_dir),
                "state_path": str(run_dir / ".bioagent" / "state.json"),
                "log_path": str(run_dir / ".bioagent" / "execution.log"),
            },
        }
        return await self.session_manager.launch_deterministic_plan(
            runtime_plan, chat_id
        )

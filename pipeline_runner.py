"""Execute a confirmed, compiled plan with explicit targets and rule admission."""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path
from typing import Any, Callable

from pipeline_spec import ExecutionSpecError, normalize_execution_spec, validate_plan_rules
from workflow_adapters import AdapterSettings, prepare_execution
from report_provenance import capture_runner_environment


def validate_execution_plan(plan: dict) -> dict:
    spec = normalize_execution_spec(plan.get("execution_spec"))
    validate_plan_rules(spec, plan.get("steps", []))
    expected = {stage["id"] for stage in spec["stages"]}
    planning = plan.get("planning", {})
    if plan.get("ready_to_execute") is not True or planning.get("ready_to_execute") is not True:
        raise ExecutionSpecError("当前方案未通过规划校验，不能执行。")
    if expected - set(planning.get("authorized_stages", [])):
        raise ExecutionSpecError("方案包含未经用户授权的前置步骤。")
    if planning.get("missing_dependencies") or planning.get("questions"):
        raise ExecutionSpecError("方案仍有未解决的输入或依赖问题。")
    if plan.get("plan_revision") != planning.get("plan_revision"):
        raise ExecutionSpecError("方案版本与确认版本不一致。")
    if plan.get("parameters", {}) != spec.get("parameters", {}) or spec.get("parameters", {}) != planning.get("parameters", {}):
        raise ExecutionSpecError("参数与已校验方案不一致。")
    # Rule lists as well as stage ids must match. Otherwise an altered card
    # could present one stage while requesting other Snakemake jobs.
    for step, stage in zip(plan["steps"], spec["stages"]):
        if step.get("rules") != stage["rules"]:
            raise ExecutionSpecError("步骤规则与编译结果不一致。")
    return spec


def _write(path: Path, state: dict) -> None:
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def run_pipeline(
    state_path: Path, *, settings: AdapterSettings | None = None,
    run_command: Callable[..., Any] = subprocess.run,
) -> dict:
    state_path = Path(state_path).resolve()
    state = json.loads(state_path.read_text(encoding="utf-8"))
    root = Path(state["run_dir"]).resolve()
    if state_path != root / ".bioagent/state.json":
        raise ExecutionSpecError("运行状态必须位于当前运行目录中。")
    if state.get("execution", {}).get("ok") is not None:
        # A follow-up report must never re-run a completed or failed analysis.
        return state
    log_path = root / ".bioagent/execution.log"
    try:
        spec = validate_execution_plan(state["confirmed_plan"])
        if state.get("execution_spec") != state["confirmed_plan"]["execution_spec"]:
            raise ExecutionSpecError("运行状态中的执行方案与已确认版本不一致。")
        for item in spec["inputs"]:
            from workflow_catalog import ARTIFACTS
            artifact = ARTIFACTS[item["type"]]
            for record in item["records"]:
                for role, value in record["files"].items():
                    path = Path(value)
                    valid = path.is_dir() if artifact.kind(role) == "directory" else path.is_file()
                    if not valid:
                        raise ExecutionSpecError(f"所需输入不存在或类型不符：{path}")
        prepared = prepare_execution(state, settings=settings)
        capture_runner_environment(state)
        for phase in prepared.phases:
            phase_steps = [step for step in state["steps"]
                           if any(stage["id"] == step["id"] and stage["workflow_id"] == phase.workflow_id
                                  for stage in spec["stages"])]
            dry_path = root / ".bioagent" / (phase.id + ".dryrun.log")
            with dry_path.open("w", encoding="utf-8") as dry_log:
                dry = run_command([*phase.command, "--dry-run"], stdout=dry_log,
                                  stderr=subprocess.STDOUT, text=True, check=False,
                                  env=phase.environment)
            dry_text = dry_path.read_text(encoding="utf-8", errors="replace")
            if dry.returncode:
                raise ExecutionSpecError(f"{phase.id} dry-run失败，未启动该阶段；请检查 {dry_path}")
            scheduled = set(re.findall(r"^\s*(?:local)?rule\s+([A-Za-z_][\w]*):", dry_text, re.M))
            unexpected = scheduled - set(phase.selected_rules)
            if unexpected or (not scheduled and "Nothing to be done" not in dry_text):
                raise ExecutionSpecError("dry-run包含未授权规则或无法确认调度范围：" + ", ".join(sorted(unexpected)))
            for step in phase_steps:
                step["status"] = "running"
            state["execution"]["current_step_id"] = phase_steps[0]["id"] if phase_steps else None
            state["execution"].setdefault("phases", []).append({
                "id": phase.id, "dryrun_log": str(dry_path), "scheduled_rules": sorted(scheduled),
                "targets": list(phase.targets),
            })
            _write(state_path, state)
            with log_path.open("a", encoding="utf-8") as log:
                log.write(f"\nStarting phase {phase.id}\n")
                log.flush()
                completed = run_command(phase.command, stdout=log, stderr=subprocess.STDOUT,
                                        text=True, check=False, env=phase.environment)
            if completed.returncode:
                raise ExecutionSpecError(f"{phase.id}执行失败，退出码{completed.returncode}。")
            missing = [path for path in phase.targets if not Path(path).is_file()]
            if missing:
                raise ExecutionSpecError("执行返回成功但缺少目标产物：" + ", ".join(missing))
            for step in phase_steps:
                step["status"] = "completed"
            _write(state_path, state)
        state["execution"].update(status="completed", ok=True, returncode=0, current_step_id=None)
    except Exception as exc:
        for step in state.get("steps", []):
            if step.get("status") == "running":
                step["status"] = "failed"
        state["execution"].update(status="failed", ok=False, returncode=1, current_step_id=None, error=str(exc))
        with log_path.open("a", encoding="utf-8") as log:
            log.write(f"\nExecution stopped: {exc}\n")
    _write(state_path, state)
    return state

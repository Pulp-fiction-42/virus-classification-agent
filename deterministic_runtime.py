"""Model-independent analysis execution runtime."""

from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import threading
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

import config
from pipeline_spec import (
    ExecutionSpecError,
    normalize_execution_spec,
    validate_plan_rules,
)
from remote_client import RemoteResourceCalibrator, RemoteWorkerClient
from resource_calibration import ResourceCalibrator
from runtime_errors import SessionStateError


class DeterministicRuntime:
    """Own resource estimation and runner admission without creating a model client."""

    def __init__(
        self,
        project_root: Path = config.PROJECT_ROOT,
        config_dir: Path = config.CLAUDE_STATE_DIR,
        *,
        analysis_run_root: Path = config.ANALYSIS_RUN_ROOT,
        analysis_data_root: Path = config.ANALYSIS_DATA_ROOT,
        resource_calibrator: ResourceCalibrator | None = None,
        runner_launcher: Callable[[Path, Path], int] | None = None,
    ) -> None:
        self.project_root = project_root.resolve()
        self.config_dir = config_dir.resolve()
        self.analysis_run_root = analysis_run_root.resolve()
        self.analysis_data_root = analysis_data_root.resolve()
        self.config_dir.mkdir(parents=True, exist_ok=True)
        self.resource_history_path = (
            self.config_dir / "bioagent" / "resource-history.sqlite3"
        )
        self.resource_history_path.parent.mkdir(parents=True, exist_ok=True)
        self.resource_calibrator = resource_calibrator or ResourceCalibrator(
            self.resource_history_path,
            self.analysis_run_root,
        )
        threading.Thread(
            target=self.resource_calibrator.backfill,
            name="bioagent-resource-backfill",
            daemon=True,
        ).start()
        self.runner_launcher = runner_launcher or self._launch_runner_process
        self._execution_lock = asyncio.Lock()

    async def launch_deterministic_plan(
        self, plan: dict[str, Any], chat_id: str
    ) -> dict[str, Any]:
        normalized_chat_id = self._validate_chat_id(chat_id)
        async with self._execution_lock:
            dataset = plan.get("dataset")
            self._validate_execution_dataset(plan, dataset)
            try:
                execution_spec = normalize_execution_spec(plan.get("execution_spec"))
                validate_plan_rules(execution_spec, plan.get("steps") or [])
            except ExecutionSpecError as exc:
                raise SessionStateError(f"分析方案执行区段无效：{exc}") from exc
            plan["execution_spec"] = execution_spec

            run = plan.get("run") if isinstance(plan.get("run"), dict) else {}
            state_path = Path(str(run.get("state_path", ""))).resolve()
            if state_path.exists():
                state = self._read_run_state(state_path)
                if (
                    state.get("run_id") != run.get("run_id")
                    or state.get("openwebui_chat_id") != normalized_chat_id
                    or state.get("plan_ref") != plan.get("plan_ref")
                ):
                    raise SessionStateError("现有分析状态与当前方案不匹配。")
                return self._analysis_run_item(
                    plan, str((state.get("execution") or {}).get("status") or "running")
                )

            state = self._write_initial_run_state(plan, normalized_chat_id)
            try:
                self.runner_launcher(
                    state_path, state_path.parent / "runner-process.log"
                )
            except Exception as exc:
                state["execution"].update(
                    {
                        "status": "failed",
                        "ok": False,
                        "returncode": -1,
                        "error": f"无法启动分析进程：{exc}",
                        "finished_at": datetime.now()
                        .astimezone()
                        .isoformat(timespec="seconds"),
                    }
                )
                state["technical_report"].update(
                    {"status": "unavailable", "available": False}
                )
                state["reports"]["final"].update(
                    {"status": "unavailable", "available": False}
                )
                self._write_json_atomic(state_path, state)
                raise SessionStateError(f"分析任务启动失败：{exc}") from exc
            return self._analysis_run_item(plan, "running")

    async def resume_deterministic_plan(
        self, plan: dict[str, Any], chat_id: str
    ) -> dict[str, Any]:
        raise SessionStateError("本机执行模式尚不支持中断恢复。")

    def _write_initial_run_state(
        self, plan: dict[str, Any], chat_id: str
    ) -> dict[str, Any]:
        run = plan.get("run") if isinstance(plan.get("run"), dict) else {}
        run_dir = Path(str(run.get("run_dir", ""))).resolve()
        if not self._is_under_analysis_root(run_dir):
            raise SessionStateError("分析运行目录不在 BIOAGENT_RUN_ROOT 内。")
        state_path = run_dir / ".bioagent" / "state.json"
        if Path(str(run.get("state_path", ""))).resolve() != state_path:
            raise SessionStateError("分析状态路径与运行目录不匹配。")
        if state_path.exists():
            raise SessionStateError("当前分析方案已经启动。")
        log_path = Path(
            str(run.get("log_path", run_dir / ".bioagent" / "execution.log"))
        ).resolve()
        if not self._is_under_analysis_root(log_path):
            raise SessionStateError("分析日志不在 BIOAGENT_RUN_ROOT 内。")

        execution_spec = normalize_execution_spec(plan.get("execution_spec"))
        validate_plan_rules(execution_spec, plan.get("steps") or [])
        state = {
            "version": 3,
            "run_id": run.get("run_id"),
            "run_dir": str(run_dir),
            "openwebui_chat_id": chat_id,
            "assistant_message_id": None,
            "plan_ref": plan.get("plan_ref"),
            "goals": plan.get("goals"),
            "options": plan.get("options"),
            "planner_version": plan.get("planner_version"),
            "decision_trace": [
                *(plan.get("decision_trace") or []),
                {"kind": "confirmation", "plan_ref": plan.get("plan_ref")},
            ],
            "title": plan.get("title"),
            "dataset": plan.get("dataset"),
            "plan": plan,
            "metadata_snapshot": plan.get("metadata_snapshot"),
            "execution_spec": execution_spec,
            "steps": [
                {
                    **{
                        key: step[key]
                        for key in (
                            "id",
                            "label",
                            "tool",
                            "rules",
                            "depends_on",
                            "produces",
                        )
                        if key in step
                    },
                    "is_deliverable": step.get("is_deliverable") is True,
                    "status": "pending",
                }
                for step in plan.get("steps", [])
                if isinstance(step, dict) and step.get("id") and step.get("label")
            ],
            "resource_estimate": plan.get("resource_estimate"),
            **(
                {"routing": plan["routing"]}
                if isinstance(plan.get("routing"), dict)
                else {}
            ),
            "resource_history_db": str(self.resource_history_path),
            "execution": {
                "started": True,
                "status": "running",
                "ok": None,
                "returncode": None,
                "current_step_id": None,
                "log": str(log_path),
            },
            "technical_report": {
                "status": "collecting",
                "available": False,
                "telemetry": str(run_dir / ".bioagent" / "telemetry.json"),
                "telemetry_version": 3,
                "agent_trace": str(run_dir / ".bioagent" / "agent-trace.jsonl"),
            },
            "reports": {
                "preliminary": {
                    "status": "pending",
                    "available": False,
                    "schema_version": 4,
                    "telemetry": str(
                        run_dir / ".bioagent" / "preliminary-telemetry.json"
                    ),
                },
                "final": {
                    "status": "collecting",
                    "available": False,
                    "schema_version": 4,
                    "telemetry": str(run_dir / ".bioagent" / "telemetry.json"),
                },
            },
            "standard_delivery": {
                "schema_version": 1,
                "status": "pending",
                "artifacts": {},
            },
        }
        try:
            bioagent_dir = run_dir / ".bioagent"
            bioagent_dir.mkdir(parents=True, exist_ok=True)
            log_path.touch(exist_ok=True)
            (bioagent_dir / "agent-trace.jsonl").touch(exist_ok=True)
            self._write_json_atomic(state_path, state)
        except OSError as exc:
            raise SessionStateError(
                "无法创建分析运行目录，请检查 BIOAGENT_RUN_ROOT 写权限。"
            ) from exc
        return state

    def _validate_execution_dataset(self, plan: dict[str, Any], current: Any) -> None:
        planned = plan.get("dataset")
        if not isinstance(planned, dict) or not isinstance(current, dict):
            raise SessionStateError("当前方案没有可验证的数据集。")
        if (planned.get("id"), planned.get("revision")) != (
            current.get("id"),
            current.get("revision"),
        ):
            raise SessionStateError("当前数据集 revision 与方案不一致。")
        if current.get("status") != "ready":
            raise SessionStateError("数据集尚未满足执行条件。")
        if planned.get("artifacts") != current.get("artifacts"):
            raise SessionStateError("数据集内容与方案快照不一致。")
        for artifact in current.get("artifacts", []):
            for record in artifact.get("records", []):
                for value in (record.get("files") or {}).values():
                    if not isinstance(value, dict) or not isinstance(
                        value.get("path"), str
                    ):
                        raise SessionStateError("数据集缺少有效文件指纹。")
                    path = Path(value["path"]).resolve()
                    if not self._is_under_data_root(path):
                        raise SessionStateError(
                            "数据集文件不在 BIOAGENT_DATA_ROOT 内。"
                        )
                    try:
                        stat = path.stat()
                    except OSError as exc:
                        raise SessionStateError("数据集文件已不可用。") from exc
                    if {
                        "device": stat.st_dev,
                        "inode": stat.st_ino,
                        "size": stat.st_size,
                        "mtime_ns": stat.st_mtime_ns,
                    } != value.get("fingerprint"):
                        raise SessionStateError("数据集文件在预检后发生变化。")

    def _launch_runner_process(self, state_path: Path, log_path: Path) -> int:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("ab") as runner_log:
            process = subprocess.Popen(
                [
                    sys.executable,
                    str(self.project_root / "analysis_runner.py"),
                    "--state",
                    str(state_path),
                ],
                cwd=self.project_root,
                stdin=subprocess.DEVNULL,
                stdout=runner_log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                close_fds=True,
            )
        threading.Thread(
            target=process.wait, name=f"bioagent-runner-{process.pid}", daemon=True
        ).start()
        return process.pid

    @staticmethod
    def _validate_chat_id(chat_id: str | None) -> str:
        if not isinstance(chat_id, str) or not chat_id or len(chat_id) > 200:
            raise SessionStateError("Open WebUI chat ID is missing or invalid")
        return chat_id

    def _is_under_analysis_root(self, path: Path) -> bool:
        try:
            path.resolve().relative_to(self.analysis_run_root)
            return True
        except ValueError:
            return False

    def _is_under_data_root(self, path: Path) -> bool:
        try:
            path.resolve().relative_to(self.analysis_data_root)
            return True
        except ValueError:
            return False

    def _read_run_state(self, state_path: Path) -> dict[str, Any]:
        if not self._is_under_analysis_root(state_path):
            raise SessionStateError("Analysis state is outside BIOAGENT_RUN_ROOT")
        value = json.loads(state_path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise SessionStateError("Analysis state must be a JSON object")
        return value

    @staticmethod
    def _analysis_run_item(plan: dict[str, Any], status: str) -> dict[str, Any]:
        run = plan.get("run") if isinstance(plan.get("run"), dict) else {}
        return {
            "type": "analysis_run",
            "id": f"analysis_run_{run.get('run_id')}",
            "status": status,
            "run": {
                key: run.get(key)
                for key in ("run_id", "run_dir", "state_path", "log_path")
            },
        }

    @staticmethod
    def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(f"{path.suffix}.tmp")
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        temporary.replace(path)


class RemoteDeterministicRuntime:
    """Deterministic runtime whose files and runner live on Cyclone."""

    def __init__(self, client: RemoteWorkerClient | None = None) -> None:
        self.remote = client or RemoteWorkerClient()
        self.analysis_run_root = config.ANALYSIS_RUN_ROOT
        self.analysis_data_root = config.ANALYSIS_DATA_ROOT
        self.resource_calibrator = RemoteResourceCalibrator(self.remote)

    async def launch_deterministic_plan(
        self, plan: dict[str, Any], chat_id: str
    ) -> dict[str, Any]:
        normalized_chat_id = DeterministicRuntime._validate_chat_id(chat_id)
        run = plan.get("run") if isinstance(plan.get("run"), dict) else {}
        return await self.remote.invoke(
            "launch",
            plan=plan,
            dataset=plan.get("dataset"),
            chat_id=normalized_chat_id,
            run_id=run.get("run_id"),
            queue_context=plan.get("queue_context"),
        )

    async def resume_deterministic_plan(
        self, plan: dict[str, Any], chat_id: str
    ) -> dict[str, Any]:
        normalized_chat_id = DeterministicRuntime._validate_chat_id(chat_id)
        run = plan.get("run") if isinstance(plan.get("run"), dict) else {}
        return await self.remote.invoke(
            "resume",
            plan=plan,
            dataset=plan.get("dataset"),
            chat_id=normalized_chat_id,
            run_id=run.get("run_id"),
        )

"""Standard-library RPC worker executed on Cyclone."""

from __future__ import annotations

import argparse
import base64
import fcntl
import gzip
import hashlib
import json
import os
import re
import shlex
import signal
import stat as stat_module
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, BinaryIO

RUN_ID_RE = re.compile(r"^analysis-[A-Za-z0-9_.-]+$")
TERMINAL_STATUSES = {"completed", "failed", "blocked", "interrupted"}
FASTQ_EXTENSION_RE = re.compile(r"(?i)(\.fastq|\.fq)(\.gz)?$")
QUICK_RECORD_LIMIT = 1_000
QUICK_BYTE_LIMIT = 8 * 1024 * 1024


class WorkerError(ValueError):
    pass


class _QuickLimitReached(Exception):
    pass


def _run_captured(
    command: list[str],
    *,
    timeout: int,
    environment: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run a tool in its own process group so a timeout cannot orphan children."""
    process = subprocess.Popen(
        command,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=environment,
        start_new_session=True,
    )
    try:
        stdout, stderr = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            stdout, stderr = process.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            try:
                stdout, stderr = process.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                if process.stdout is not None:
                    process.stdout.close()
                if process.stderr is not None:
                    process.stderr.close()
                stdout, stderr = "", ""
        raise subprocess.TimeoutExpired(
            command,
            timeout,
            output=stdout or exc.output,
            stderr=stderr or exc.stderr,
        ) from exc
    return subprocess.CompletedProcess(command, process.returncode, stdout, stderr)


def _now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _write_json_atomic(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    os.replace(temporary, path)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise WorkerError(f"无法读取远端状态：{path}") from exc
    if not isinstance(value, dict):
        raise WorkerError("远端状态格式无效")
    return value


def _lexical_under(path: Path, root: Path) -> bool:
    normalized = Path(os.path.normpath(str(path)))
    normalized_root = Path(os.path.normpath(str(root)))
    return normalized == normalized_root or normalized_root in normalized.parents


def _target_allowed(path: Path, roots: list[Path]) -> bool:
    return any(path == root or root in path.parents for root in roots)


def _stat(value: os.stat_result) -> dict[str, int]:
    return {
        "device": value.st_dev,
        "inode": value.st_ino,
        "size": value.st_size,
        "mtime_ns": value.st_mtime_ns,
    }


def _link_chain(path: Path, root: Path) -> list[dict[str, Any]]:
    chain = []
    relative = Path(os.path.normpath(str(path))).relative_to(root)
    current = root
    for part in relative.parts:
        current /= part
        try:
            link_stat = current.lstat()
        except OSError as exc:
            raise WorkerError(f"数据路径不可用：{path}") from exc
        if stat_module.S_ISLNK(link_stat.st_mode):
            try:
                target = current.resolve(strict=True)
                target_stat = target.stat()
            except (OSError, RuntimeError) as exc:
                raise WorkerError(f"数据链接损坏或形成循环：{current}") from exc
            chain.append(
                {
                    "path": str(current),
                    "lstat": _stat(link_stat),
                    "target": str(target),
                    "target_stat": _stat(target_stat),
                }
            )
    return chain


def resolve_source(
    raw_path: str,
    settings: dict[str, Any],
    *,
    expected_kind: str | None = None,
) -> dict[str, Any]:
    logical = Path(raw_path)
    data_root = Path(settings["data_root"])
    if not logical.is_absolute():
        raise WorkerError("Analysis dataset paths must be absolute")
    if not _lexical_under(logical, data_root):
        raise WorkerError("Analysis dataset path is outside the logical data root")
    try:
        data_root.resolve(strict=True)
        canonical = logical.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise WorkerError(f"Analysis dataset path is unavailable: {raw_path}") from exc
    allowed = [Path(value).resolve(strict=True) for value in settings["allowed_roots"]]
    if not _target_allowed(canonical, allowed):
        raise WorkerError("Analysis dataset symlink target is outside the allowlist")
    kind = (
        "directory"
        if canonical.is_dir()
        else "file"
        if canonical.is_file()
        else "other"
    )
    if kind == "other" or (expected_kind and kind != expected_kind):
        raise WorkerError(f"Analysis dataset path has the wrong kind: {raw_path}")
    if not os.access(canonical, os.R_OK):
        raise WorkerError(f"Analysis dataset path is not readable: {raw_path}")
    links = _link_chain(logical, data_root)
    target_stat = _stat(canonical.stat())
    return {
        "path": str(Path(os.path.normpath(str(logical)))),
        "canonical_path": str(canonical),
        "kind": kind,
        "is_symlink": bool(links),
        "fingerprint": {"target": target_stat, "links": links},
    }


def _same_fingerprint(source: dict[str, Any], settings: dict[str, Any]) -> bool:
    try:
        current = resolve_source(
            str(source["path"]),
            settings,
            expected_kind=str(source.get("kind") or "") or None,
        )
    except (KeyError, WorkerError):
        return False
    return current["fingerprint"] == source.get("fingerprint")


def list_files(request: dict[str, Any]) -> dict[str, Any]:
    settings = request["settings"]
    data_root = Path(settings["data_root"])
    raw = str(request.get("path") or "/")
    root_text = str(data_root)
    relative = (
        ""
        if raw in {"/", root_text}
        else raw[len(root_text) :].lstrip("/")
        if raw.startswith(f"{root_text}/")
        else raw.lstrip("/")
    )
    logical = data_root / relative
    source = resolve_source(str(logical), settings, expected_kind="directory")
    target = Path(source["canonical_path"])
    cursor = max(0, int(request.get("cursor") or 0))
    limit = min(500, max(1, int(request.get("limit") or 100)))
    try:
        children = sorted(
            target.iterdir(), key=lambda child: (not child.is_dir(), child.name.lower())
        )
    except OSError as exc:
        raise WorkerError(f"无法列出远端目录：{exc}") from exc
    entries = []
    for child in children[cursor : cursor + limit]:
        child_logical = logical / child.name
        try:
            item = resolve_source(str(child_logical), settings)
        except WorkerError:
            continue
        is_dir = item["kind"] == "directory"
        canonical = Path(item["canonical_path"])
        try:
            info = canonical.stat()
            has_children = is_dir and next(canonical.iterdir(), None) is not None
        except OSError:
            continue
        child_relative = "/".join(part for part in (relative, child.name) if part)
        entries.append(
            {
                "name": child.name,
                "type": item["kind"],
                "path": "/" if not child_relative else f"/{child_relative}",
                "abs_path": item["path"],
                "canonical_path": item["canonical_path"],
                "is_symlink": item["is_symlink"],
                "size": None if is_dir else info.st_size,
                "modified": int(info.st_mtime),
                "has_children": has_children,
            }
        )
    return {
        "root": str(data_root),
        "path": "/" if not relative else f"/{relative}",
        "abs_path": str(logical),
        "entries": entries,
        "next_cursor": cursor + limit if cursor + limit < len(children) else None,
    }


def _open_fastq(path: Path) -> BinaryIO:
    with path.open("rb") as raw:
        magic = raw.read(2)
    compressed = path.name.lower().endswith(".gz")
    if compressed != (magic == b"\x1f\x8b"):
        raise WorkerError("COMPRESSION_MISMATCH")
    return gzip.open(path, "rb") if compressed else path.open("rb")


def _read_record(
    handle: BinaryIO, byte_limit: int | None
) -> tuple[list[bytes] | None, int]:
    lines = []
    remaining = byte_limit
    for _ in range(4):
        if remaining is not None and remaining <= 0:
            raise _QuickLimitReached
        available = remaining
        line = handle.readline() if remaining is None else handle.readline(remaining)
        if remaining is not None:
            remaining -= len(line)
            if line and len(line) == available and not line.endswith((b"\n", b"\r")):
                raise _QuickLimitReached
        lines.append(line)
    count = sum(len(line) for line in lines)
    if not lines[0]:
        return None, count
    if any(not line for line in lines[1:]):
        raise WorkerError("FASTQ_TRUNCATED_RECORD")
    if not lines[0].startswith(b"@"):
        raise WorkerError("FASTQ_HEADER_INVALID")
    if not lines[2].startswith(b"+"):
        raise WorkerError("FASTQ_PLUS_INVALID")
    if len(lines[1].rstrip(b"\r\n")) != len(lines[3].rstrip(b"\r\n")):
        raise WorkerError("FASTQ_LENGTH_MISMATCH")
    return lines, count


def _read_id(header: bytes) -> bytes:
    return re.sub(rb"/[12]$", b"", header[1:].strip().split(None, 1)[0])


def check_pair(read1: Path, read2: Path, *, full: bool) -> dict[str, Any]:
    limit = None if full else QUICK_RECORD_LIMIT
    records = 0
    uncompressed = [0, 0]
    try:
        with _open_fastq(read1) as left, _open_fastq(read2) as right:
            while limit is None or records < limit:
                left_record, left_bytes = _read_record(
                    left, None if full else QUICK_BYTE_LIMIT - uncompressed[0]
                )
                right_record, right_bytes = _read_record(
                    right, None if full else QUICK_BYTE_LIMIT - uncompressed[1]
                )
                uncompressed[0] += left_bytes
                uncompressed[1] += right_bytes
                if left_record is None or right_record is None:
                    if left_record is not None or right_record is not None:
                        raise WorkerError("RECORD_COUNT_MISMATCH")
                    break
                if _read_id(left_record[0]) != _read_id(right_record[0]):
                    raise WorkerError("MATE_ID_MISMATCH")
                records += 1
                if not full and max(uncompressed) >= QUICK_BYTE_LIMIT:
                    break
    except _QuickLimitReached:
        pass
    except WorkerError:
        raise
    except (OSError, EOFError, gzip.BadGzipFile) as exc:
        raise WorkerError("COMPRESSION_MISMATCH") from exc
    return {
        "records": records,
        "compressed_bytes": [read1.stat().st_size, read2.stat().st_size],
        "uncompressed_bytes": uncompressed,
    }


def preflight(request: dict[str, Any]) -> dict[str, Any]:
    settings = request["settings"]
    full = request.get("full") is True
    results = []
    issues = []
    sources = request.get("sources") or []
    if not all(_same_fingerprint(source, settings) for source in sources):
        return {
            "status": "stale",
            "details": {},
            "issues": [{"code": "FINGERPRINT_CHANGED"}],
        }
    for artifact in request.get("artifacts") or []:
        if artifact.get("type") not in {
            "raw_paired_reads",
            "qc_paired_reads",
            "clean_run_paired_reads",
            "clean_sample_paired_reads",
        }:
            continue
        for record in artifact.get("records") or []:
            try:
                left = resolve_source(
                    record["files"]["read1"]["path"], settings, expected_kind="file"
                )
                right = resolve_source(
                    record["files"]["read2"]["path"], settings, expected_kind="file"
                )
                result = check_pair(
                    Path(left["canonical_path"]),
                    Path(right["canonical_path"]),
                    full=full,
                )
                results.append(
                    {
                        "artifact_id": artifact.get("id"),
                        "record_id": record.get("record_id"),
                        **result,
                    }
                )
                if result["records"] == 0:
                    issues.append(
                        {"code": "EMPTY_FASTQ", "artifact_id": artifact.get("id")}
                    )
            except (KeyError, WorkerError) as exc:
                issues.append(
                    {
                        "code": str(exc).strip("'") or "FASTQ_INVALID",
                        "artifact_id": artifact.get("id"),
                        "record_id": record.get("record_id"),
                    }
                )
    if not all(_same_fingerprint(source, settings) for source in sources):
        return {
            "status": "stale",
            "details": {},
            "issues": [{"code": "FINGERPRINT_CHANGED"}],
        }
    details = {
        "pairs": len(results),
        "record_count": sum(item["records"] for item in results),
        "compressed_bytes": sum(sum(item["compressed_bytes"]) for item in results),
        "uncompressed_bytes": sum(sum(item["uncompressed_bytes"]) for item in results),
        "records": results,
    }
    return {
        "status": "invalid" if issues else "ready",
        "details": details,
        "issues": issues,
    }


def _boot_id() -> str:
    try:
        return (
            Path("/proc/sys/kernel/random/boot_id").read_text(encoding="utf-8").strip()
        )
    except OSError:
        return "unknown"


def _pid_alive(pid: Any) -> bool:
    try:
        os.kill(int(pid), 0)
        return True
    except (OSError, TypeError, ValueError):
        return False


def _tmux_command(settings: dict[str, Any]) -> list[str]:
    return [str(settings["tmux"]), "-L", str(settings["tmux_socket"])]


def _session_exists(settings: dict[str, Any], run_id: str) -> bool:
    result = subprocess.run(
        [*_tmux_command(settings), "has-session", "-t", f"bioagent-{run_id}"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    return result.returncode == 0


def _state_path(settings: dict[str, Any], run_id: str) -> Path:
    if not RUN_ID_RE.fullmatch(run_id):
        raise WorkerError("invalid analysis run ID")
    return Path(settings["run_root"]) / run_id / ".bioagent" / "state.json"


def _start_session(settings: dict[str, Any], state_path: Path, version: str) -> None:
    run_id = state_path.parent.parent.name
    if _session_exists(settings, run_id):
        return
    state = _read_json(state_path)
    worker_command = "diagnose" if state.get("diagnostic") else "supervise"
    worker = Path(__file__).resolve()
    command = " ".join(
        shlex.quote(value)
        for value in (
            "env",
            *(
                f"{key}={value}"
                for key, value in sorted(
                    (settings.get("runner_environment") or {}).items()
                )
                if key.startswith("BIOAGENT_") and isinstance(value, str)
            ),
            str(settings["python"]),
            "-S",
            str(worker),
            worker_command,
            "--state",
            str(state_path),
            "--run-root",
            str(settings["run_root"]),
            "--version",
            version,
        )
    )
    result = subprocess.run(
        [
            *_tmux_command(settings),
            "new-session",
            "-d",
            "-s",
            f"bioagent-{run_id}",
            command,
        ],
        stdin=subprocess.DEVNULL,
        capture_output=True,
        check=False,
    )
    if result.returncode:
        raise WorkerError(
            result.stderr.decode("utf-8", errors="replace").strip()
            or "无法启动远端 tmux session"
        )


def _validate_dataset(dataset: dict[str, Any], settings: dict[str, Any]) -> None:
    if dataset.get("status") != "ready":
        raise WorkerError("analysis dataset is not ready")
    seen = 0
    for artifact in dataset.get("artifacts") or []:
        for record in artifact.get("records") or []:
            for source in (record.get("files") or {}).values():
                if not isinstance(source, dict) or not _same_fingerprint(
                    source, settings
                ):
                    raise WorkerError("数据集文件在预检后发生变化")
                seen += 1
    if not seen:
        raise WorkerError("analysis dataset has no executable files")


def launch(request: dict[str, Any]) -> dict[str, Any]:
    settings = request["settings"]
    run_id = str(request.get("run_id") or "")
    state_path = _state_path(settings, run_id)
    plan = request.get("plan")
    dataset = request.get("dataset")
    if not isinstance(plan, dict) or not isinstance(dataset, dict):
        raise WorkerError("analysis plan or dataset is missing")
    _validate_dataset(dataset, settings)
    if state_path.exists():
        state = _read_json(state_path)
        if state.get("openwebui_chat_id") != request.get("chat_id"):
            raise WorkerError("现有分析状态与当前方案不匹配")
        status = str((state.get("execution") or {}).get("status") or "queued")
        if status == "queued" and not _session_exists(settings, run_id):
            state["worker_version"] = request["worker_version"]
            _write_json_atomic(state_path, state)
            _start_session(settings, state_path, str(request["worker_version"]))
        return _run_item(run_id, settings, status)
    run_dir = state_path.parent.parent
    execution_spec = plan.get("execution_spec")
    steps = plan.get("steps") or []
    log_path = run_dir / ".bioagent" / "execution.log"
    state = {
        "version": 3,
        "run_id": run_id,
        "run_dir": str(run_dir),
        "openwebui_chat_id": request.get("chat_id"),
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
        "dataset": dataset,
        "plan": {**plan, "dataset": dataset},
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
            for step in steps
            if isinstance(step, dict) and step.get("id") and step.get("label")
        ],
        "resource_estimate": plan.get("resource_estimate"),
        **(
            {"routing": plan["routing"]}
            if isinstance(plan.get("routing"), dict)
            else {}
        ),
        "resource_history_db": str(
            Path(settings["run_root"]) / ".bioagent" / "resource-history.sqlite3"
        ),
        "worker_version": request["worker_version"],
        "execution": {
            "started": True,
            "status": "queued",
            "ok": None,
            "returncode": None,
            "current_step_id": None,
            "log": str(log_path),
            "queued_at": _now(),
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
                "telemetry": str(run_dir / ".bioagent" / "preliminary-telemetry.json"),
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
    state_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.touch(exist_ok=True)
    (state_path.parent / "agent-trace.jsonl").touch(exist_ok=True)
    _write_json_atomic(state_path, state)
    _start_session(settings, state_path, str(request["worker_version"]))
    return _run_item(run_id, settings, "queued")


def diagnostic_launch(request: dict[str, Any]) -> dict[str, Any]:
    """Launch a bounded no-work run used to verify queue and recovery semantics."""
    settings = request["settings"]
    run_id = str(request.get("run_id") or "")
    state_path = _state_path(settings, run_id)
    duration = max(1, min(120, int(request.get("duration_seconds") or 5)))
    if state_path.exists():
        state = _read_json(state_path)
        if not state.get("diagnostic"):
            raise WorkerError("existing run is not a diagnostic run")
        status = str((state.get("execution") or {}).get("status") or "queued")
        if status == "queued" and not _session_exists(settings, run_id):
            state["worker_version"] = request["worker_version"]
            _write_json_atomic(state_path, state)
            _start_session(settings, state_path, str(request["worker_version"]))
        return _run_item(run_id, settings, status)
    run_dir = state_path.parent.parent
    state = {
        "version": 3,
        "run_id": run_id,
        "run_dir": str(run_dir),
        "worker_version": request["worker_version"],
        "diagnostic": {"duration_seconds": duration},
        "execution": {
            "started": True,
            "status": "queued",
            "ok": None,
            "returncode": None,
            "queued_at": _now(),
            "log": str(run_dir / ".bioagent" / "execution.log"),
        },
        "technical_report": {"status": "collecting", "available": False},
        "reports": {
            "preliminary": {"status": "pending", "available": False},
            "final": {"status": "collecting", "available": False},
        },
    }
    state_path.parent.mkdir(parents=True, exist_ok=True)
    (state_path.parent / "execution.log").touch(exist_ok=True)
    _write_json_atomic(state_path, state)
    _start_session(settings, state_path, str(request["worker_version"]))
    return _run_item(run_id, settings, "queued")


def _run_item(run_id: str, settings: dict[str, Any], status: str) -> dict[str, Any]:
    run_dir = Path(settings["run_root"]) / run_id
    return {
        "type": "analysis_run",
        "id": f"analysis_run_{run_id}",
        "status": status,
        "run": {
            "run_id": run_id,
            "run_dir": str(run_dir),
            "state_path": str(run_dir / ".bioagent" / "state.json"),
            "log_path": str(run_dir / ".bioagent" / "execution.log"),
        },
    }


def _read_optional(path: Path, limit: int = 4 * 1024 * 1024) -> str | None:
    try:
        with path.open("rb") as stream:
            stream.seek(0, os.SEEK_END)
            size = stream.tell()
            stream.seek(max(0, size - limit))
            return stream.read().decode("utf-8", errors="replace")
    except OSError:
        return None


def snapshot(request: dict[str, Any]) -> dict[str, Any]:
    settings = request["settings"]
    run_id = request.get("run_id")
    run_dir = request.get("run_dir")
    if run_id:
        state_path = _state_path(settings, str(run_id))
    elif run_dir:
        candidate = Path(str(run_dir))
        root = Path(settings["run_root"])
        if not candidate.is_absolute() or not _lexical_under(candidate, root):
            raise WorkerError("Run directory is outside the analysis run root")
        state_path = candidate / ".bioagent" / "state.json"
    else:
        candidates = list(
            Path(settings["run_root"]).glob("analysis-*/.bioagent/state.json")
        )
        if not candidates:
            return {"found": False}
        state_path = max(candidates, key=lambda value: value.stat().st_mtime_ns)
    if not state_path.is_file():
        return {"found": False}
    state = _read_json(state_path)
    run_id = str(state.get("run_id") or state_path.parent.parent.name)
    supervisor_path = state_path.parent / "supervisor.json"
    supervisor = _read_json(supervisor_path) if supervisor_path.is_file() else {}
    status = str((state.get("execution") or {}).get("status") or "running")
    session_alive = _session_exists(settings, run_id)
    pid_alive = _pid_alive(
        supervisor.get("runner_pid") or supervisor.get("supervisor_pid")
    )
    same_boot = supervisor.get("boot_id") in {None, _boot_id()}
    if status == "queued" and not session_alive:
        state["worker_version"] = request["worker_version"]
        _write_json_atomic(state_path, state)
        _start_session(
            settings,
            state_path,
            str(request["worker_version"]),
        )
        session_alive = True
    elif status == "running" and (
        not same_boot or (not session_alive and not pid_alive)
    ):
        state["execution"].update(
            {
                "status": "interrupted",
                "ok": False,
                "error": "Cyclone 重启或远端执行进程已消失；产物已保留，任务未自动重跑。",
                "finished_at": _now(),
            }
        )
        state["technical_report"].update({"status": "ready", "available": True})
        _write_json_atomic(state_path, state)
        status = "interrupted"
    requested_report = request.get("report_kind")
    telemetry_path = state_path.parent / "telemetry.json"
    if requested_report in {"preliminary", "final"}:
        reports = state.get("reports") if isinstance(state.get("reports"), dict) else {}
        report = reports.get(requested_report)
        if isinstance(report, dict) and isinstance(report.get("telemetry"), str):
            candidate = Path(report["telemetry"])
            if candidate.is_absolute() and _lexical_under(candidate, state_path.parent.parent):
                telemetry_path = candidate
    telemetry_text = _read_optional(telemetry_path)
    try:
        telemetry = json.loads(telemetry_text) if telemetry_text else None
    except json.JSONDecodeError:
        telemetry = None
    trace_text = _read_optional(state_path.parent / "agent-trace.jsonl") or ""
    trace = []
    for line in trace_text.splitlines():
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(item, dict):
            trace.append(item)
    return {
        "found": True,
        "state": state,
        "supervisor": {
            **supervisor,
            "session_alive": session_alive,
            "pid_alive": pid_alive,
            "same_boot": same_boot,
        },
        "log": _read_optional(state_path.parent / "execution.log", 8 * 1024 * 1024)
        or "",
        "telemetry": telemetry,
        "agent_trace": trace,
        "updated_at": int(
            max(
                state_path.stat().st_mtime,
                supervisor_path.stat().st_mtime if supervisor_path.exists() else 0,
            )
        ),
    }


def resources(request: dict[str, Any]) -> dict[str, Any]:
    settings = request["settings"]
    cpu_total = os.cpu_count() or 0
    before = _cpu_times()
    time.sleep(0.1)
    after = _cpu_times()
    cpu_percent = 0.0
    if before and after and after[0] > before[0]:
        cpu_percent = (1 - (after[1] - before[1]) / (after[0] - before[0])) * 100
    memory_values: dict[str, int] = {}
    for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
        key, raw = line.split(":", 1)
        memory_values[key] = int(raw.strip().split()[0]) * 1024
    memory_total = memory_values.get("MemTotal", 0)
    memory_available = memory_values.get("MemAvailable", 0)
    storage = os.statvfs(settings["run_root"])
    storage_total = storage.f_frsize * storage.f_blocks
    storage_available = storage.f_frsize * storage.f_bavail
    statuses = []
    for state_path in Path(settings["run_root"]).glob(
        "analysis-*/.bioagent/state.json"
    ):
        try:
            statuses.append(
                str((_read_json(state_path).get("execution") or {}).get("status"))
            )
        except WorkerError:
            continue
    return {
        "cpu": {
            "used": cpu_total * cpu_percent / 100,
            "total": cpu_total,
            "percent": round(cpu_percent, 2),
        },
        "memory": {
            "used": memory_total - memory_available,
            "total": memory_total,
            "percent": round((memory_total - memory_available) / memory_total * 100, 2)
            if memory_total
            else 0,
        },
        "storage": {
            "used": storage_total - storage_available,
            "total": storage_total,
            "percent": round(
                (storage_total - storage_available) / storage_total * 100, 2
            )
            if storage_total
            else 0,
        },
        "source": "cyclone",
        "stale": False,
        "active_runs": {
            "running": statuses.count("running"),
            "queued": statuses.count("queued"),
        },
        "updated_at": int(time.time()),
    }


def _cpu_times() -> tuple[int, int] | None:
    try:
        values = [
            int(value)
            for value in Path("/proc/stat").read_text().splitlines()[0].split()[1:]
        ]
    except (OSError, ValueError, IndexError):
        return None
    return sum(values), sum(values[3:5])


def estimate(request: dict[str, Any]) -> dict[str, Any]:
    from resource_calibration import ResourceCalibrator

    settings = request["settings"]
    calibrator = ResourceCalibrator(
        Path(settings["run_root"]) / ".bioagent" / "resource-history.sqlite3",
        Path(settings["run_root"]),
    )
    return calibrator.estimate(request["execution_spec"])


def verify_evidence(request: dict[str, Any]) -> dict[str, Any]:
    from report_evidence import verify_report_evidence

    run_dir = Path(str(request.get("run_dir") or ""))
    root = Path(request["settings"]["run_root"])
    if not run_dir.is_absolute() or not _lexical_under(run_dir, root):
        raise WorkerError("Run directory is outside the analysis run root")
    telemetry = request.get("telemetry")
    if not isinstance(telemetry, dict):
        raise WorkerError("Report telemetry is missing")
    return verify_report_evidence(telemetry=telemetry, run_dir=run_dir)


def delivery_artifact(request: dict[str, Any]) -> dict[str, Any]:
    run_dir = Path(str(request.get("run_dir") or ""))
    root = Path(request["settings"]["run_root"])
    format_name = str(request.get("format") or "")
    if format_name not in {"html", "pdf"}:
        raise WorkerError("Unsupported delivery report format")
    if not run_dir.is_absolute() or not _lexical_under(run_dir, root):
        raise WorkerError("Run directory is outside the analysis run root")
    resolved_root = root.resolve()
    resolved_run = run_dir.resolve()
    if resolved_root not in resolved_run.parents:
        raise WorkerError("Run directory is outside the analysis run root")
    state_path = resolved_run / ".bioagent" / "state.json"
    state = _read_json(state_path)
    delivery = state.get("standard_delivery")
    artifacts = delivery.get("artifacts") if isinstance(delivery, dict) else None
    artifact = artifacts.get(format_name) if isinstance(artifacts, dict) else None
    if not (
        isinstance(delivery, dict)
        and delivery.get("schema_version") == 1
        and delivery.get("status") == "ready"
        and isinstance(artifact, dict)
    ):
        raise WorkerError("Delivery report is not ready")
    relative = artifact.get("path")
    if not isinstance(relative, str) or Path(relative).is_absolute():
        raise WorkerError("Delivery report artifact path is invalid")
    path = (resolved_run / relative).resolve()
    if resolved_run not in path.parents or not path.is_file():
        raise WorkerError("Delivery report artifact is unavailable")
    data = path.read_bytes()
    if len(data) > 32 * 1024 * 1024:
        raise WorkerError("Delivery report artifact exceeds the transfer limit")
    if len(data) != artifact.get("bytes") or hashlib.sha256(data).hexdigest() != artifact.get("sha256"):
        raise WorkerError("Delivery report artifact validation failed")
    return {
        "format": format_name,
        "media_type": artifact.get("media_type"),
        "sha256": artifact.get("sha256"),
        "content_base64": base64.b64encode(data).decode("ascii"),
    }


def _runner_environment(settings: dict[str, Any]) -> dict[str, str]:
    return {
        key: value
        for key, value in (settings.get("runner_environment") or {}).items()
        if key.startswith("BIOAGENT_") and isinstance(value, str)
    }


def toolchain(request: dict[str, Any]) -> dict[str, Any]:
    settings = request["settings"]
    runner_environment = _runner_environment(settings)
    workflow_paths = {
        key[len("BIOAGENT_") : -len("_SOURCE")].lower(): value
        for key, value in runner_environment.items()
        if key.startswith("BIOAGENT_WORKFLOW") and key.endswith("_SOURCE")
    }

    def version(command: list[str]) -> dict[str, Any]:
        try:
            completed = _run_captured(
                command,
                timeout=30,
            )
        except subprocess.TimeoutExpired:
            return {"path": command[0], "available": False, "error": "timeout"}
        except OSError:
            return {"path": command[0], "available": False, "error": "unavailable"}
        if completed.returncode:
            detail = (completed.stderr or completed.stdout).strip()
            return {
                "path": command[0],
                "available": False,
                "error": detail[-500:] or f"exit {completed.returncode}",
            }
        output = (completed.stdout or completed.stderr).strip().splitlines()
        return {
            "path": command[0],
            "available": True,
            "version": output[0] if output else "unknown",
        }

    python = version([str(settings["python"]), "--version"])
    tmux = version([str(settings["tmux"]), "-V"])
    tmux["socket"] = str(settings["tmux_socket"])
    snakemake = version([runner_environment["BIOAGENT_SNAKEMAKE"], "--version"])
    seqkit = version([runner_environment["BIOAGENT_SEQKIT"], "version"])
    report_python = version(
        [
            runner_environment.get("BIOAGENT_REPORT_PYTHON", ""),
            "-c",
            "import pandas, scipy, openpyxl; print('report-python ready')",
        ]
    )
    rscript = version(
        [runner_environment.get("BIOAGENT_RSCRIPT", ""), "--version"]
    )
    weasyprint = version(
        [runner_environment.get("BIOAGENT_WEASYPRINT", ""), "--version"]
    )
    workflows = {
        key: {
            "path": path,
            "available": (Path(path) / "Snakefile").is_file(),
        }
        for key, path in workflow_paths.items()
    }
    checks = [
        python,
        tmux,
        snakemake,
        seqkit,
        report_python,
        rscript,
        weasyprint,
        *workflows.values(),
    ]
    return {
        "ok": all(item["available"] for item in checks),
        "python": python,
        "tmux": tmux,
        "snakemake": snakemake,
        "seqkit": seqkit,
        "report_python": report_python,
        "rscript": rscript,
        "weasyprint": weasyprint,
        "workflows": workflows,
    }


def dry_run(request: dict[str, Any]) -> dict[str, Any]:
    from workflow_adapters import AdapterSettings, prepare_execution

    settings = request["settings"]
    run_id = str(request.get("run_id") or "")
    run_dir = _state_path(settings, run_id).parent.parent
    if run_dir.exists():
        raise WorkerError(f"远端 dry-run 目录已经存在：{run_dir}")
    execution_spec = request.get("execution_spec")
    if not isinstance(execution_spec, dict):
        raise WorkerError("dry-run execution_spec is missing")
    environment = os.environ.copy()
    environment.update(_runner_environment(settings))
    adapter_settings = AdapterSettings(
        workflow1_source=Path(environment["BIOAGENT_WORKFLOW1_SOURCE"]),
        workflow2_source=Path(environment["BIOAGENT_WORKFLOW2_SOURCE"]),
        workflow3_source=Path(environment["BIOAGENT_WORKFLOW3_SOURCE"]),
        workflow4_source=Path(environment["BIOAGENT_WORKFLOW4_SOURCE"]),
        workflow5_source=Path(environment["BIOAGENT_WORKFLOW5_SOURCE"]),
        workflow6_source=Path(environment["BIOAGENT_WORKFLOW6_SOURCE"]),
        snakemake=environment["BIOAGENT_SNAKEMAKE"],
        seqkit=environment["BIOAGENT_SEQKIT"],
        cores=int(environment.get("BIOAGENT_SNAKEMAKE_CORES", "8")),
    )
    prepared = prepare_execution(
        {"run_dir": str(run_dir)},
        adapter_settings,
        execution_spec=execution_spec,
    )
    phase_results = []
    for phase in prepared.phases:
        command = [*phase.command, "--dry-run"]
        try:
            completed = _run_captured(
                command,
                timeout=600,
                environment=environment,
            )
            output = (completed.stdout or "") + (completed.stderr or "")
            phase_results.append(
                {
                    "id": phase.id,
                    "workflow_id": phase.workflow_id,
                    "returncode": completed.returncode,
                    "ok": completed.returncode == 0,
                    "output_tail": output[-20_000:],
                }
            )
        except subprocess.TimeoutExpired as exc:
            stdout = (
                exc.stdout.decode("utf-8", errors="replace")
                if isinstance(exc.stdout, bytes)
                else exc.stdout or ""
            )
            stderr = (
                exc.stderr.decode("utf-8", errors="replace")
                if isinstance(exc.stderr, bytes)
                else exc.stderr or ""
            )
            output = (stdout + stderr)[-20_000:]
            phase_results.append(
                {
                    "id": phase.id,
                    "workflow_id": phase.workflow_id,
                    "returncode": 124,
                    "ok": False,
                    "output_tail": output,
                }
            )
    result = {
        "run_id": run_id,
        "run_dir": str(run_dir),
        "ok": all(item["ok"] for item in phase_results),
        "phases": phase_results,
    }
    _write_json_atomic(run_dir / ".bioagent" / "dry-run.json", result)
    return result


def handle(request: dict[str, Any]) -> dict[str, Any]:
    operation = request.get("operation")
    if operation == "health":
        settings = request["settings"]
        return {
            "worker_version": request["worker_version"],
            "python": settings["python"],
            "control_python": sys.executable,
            "tmux": settings["tmux"],
            "data_root": settings["data_root"],
            "run_root": settings["run_root"],
            "boot_id": _boot_id(),
            "runner_environment": _runner_environment(settings),
        }
    if operation == "toolchain":
        return toolchain(request)
    if operation == "list_files":
        return list_files(request)
    if operation == "resolve_sources":
        sources = [
            resolve_source(str(value), request["settings"])
            for value in request.get("paths") or []
        ]
        if len({source["path"] for source in sources}) != len(sources):
            raise WorkerError("The same logical path cannot be selected more than once")
        return {"sources": sources}
    if operation == "fingerprints_match":
        return {
            "matches": all(
                _same_fingerprint(source, request["settings"])
                for source in request.get("sources") or []
            )
        }
    if operation == "preflight":
        return preflight(request)
    if operation == "launch":
        return launch(request)
    if operation == "diagnostic_launch":
        return diagnostic_launch(request)
    if operation == "snapshot":
        return snapshot(request)
    if operation == "resources":
        return resources(request)
    if operation == "estimate":
        return estimate(request)
    if operation == "verify_evidence":
        return verify_evidence(request)
    if operation == "delivery_artifact":
        return delivery_artifact(request)
    if operation == "dry_run":
        return dry_run(request)
    raise WorkerError(f"unsupported worker operation: {operation}")


def supervise(args: argparse.Namespace) -> int:
    state_path = Path(args.state)
    run_root = Path(args.run_root)
    state = _read_json(state_path)
    run_id = str(state.get("run_id"))
    supervisor_path = state_path.parent / "supervisor.json"
    lock_path = run_root / ".bioagent" / "analysis.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+b") as lock:
        while True:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                _write_json_atomic(
                    supervisor_path,
                    {
                        "run_id": run_id,
                        "status": "queued",
                        "supervisor_pid": os.getpid(),
                        "boot_id": _boot_id(),
                        "worker_version": args.version,
                        "heartbeat_at": _now(),
                    },
                )
                time.sleep(2)
        state = _read_json(state_path)
        state["execution"].update({"status": "running", "started_at": _now()})
        _write_json_atomic(state_path, state)
        runner_log = state_path.parent / "runner-process.log"
        with runner_log.open("ab") as output:
            process = subprocess.Popen(
                [
                    sys.executable,
                    str(Path(__file__).with_name("analysis_runner.py")),
                    "--state",
                    str(state_path),
                ],
                cwd=Path(__file__).parent,
                stdin=subprocess.DEVNULL,
                stdout=output,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                close_fds=True,
            )
            stop = threading.Event()

            def heartbeat() -> None:
                while not stop.wait(2):
                    _write_json_atomic(
                        supervisor_path,
                        {
                            "run_id": run_id,
                            "status": "running",
                            "supervisor_pid": os.getpid(),
                            "runner_pid": process.pid,
                            "boot_id": _boot_id(),
                            "worker_version": args.version,
                            "heartbeat_at": _now(),
                        },
                    )

            thread = threading.Thread(target=heartbeat, daemon=True)
            thread.start()
            returncode = process.wait()
            stop.set()
            thread.join(timeout=3)
        final = _read_json(state_path)
        status = str((final.get("execution") or {}).get("status") or "")
        if status not in TERMINAL_STATUSES:
            final["execution"].update(
                {
                    "status": "completed" if returncode == 0 else "failed",
                    "ok": returncode == 0,
                    "returncode": returncode,
                    "finished_at": _now(),
                }
            )
            _write_json_atomic(state_path, final)
        _write_json_atomic(
            supervisor_path,
            {
                "run_id": run_id,
                "status": str((final.get("execution") or {}).get("status")),
                "supervisor_pid": os.getpid(),
                "runner_pid": process.pid,
                "boot_id": _boot_id(),
                "worker_version": args.version,
                "heartbeat_at": _now(),
                "finished_at": _now(),
                "returncode": returncode,
            },
        )
        return returncode


def diagnose(args: argparse.Namespace) -> int:
    state_path = Path(args.state)
    run_root = Path(args.run_root)
    state = _read_json(state_path)
    run_id = str(state.get("run_id"))
    duration = int((state.get("diagnostic") or {}).get("duration_seconds") or 5)
    supervisor_path = state_path.parent / "supervisor.json"
    lock_path = run_root / ".bioagent" / "analysis.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+b") as lock:
        while True:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                _write_json_atomic(
                    supervisor_path,
                    {
                        "run_id": run_id,
                        "status": "queued",
                        "supervisor_pid": os.getpid(),
                        "boot_id": _boot_id(),
                        "worker_version": args.version,
                        "heartbeat_at": _now(),
                    },
                )
                time.sleep(1)
        state = _read_json(state_path)
        state["execution"].update({"status": "running", "started_at": _now()})
        _write_json_atomic(state_path, state)
        deadline = time.monotonic() + duration
        while time.monotonic() < deadline:
            _write_json_atomic(
                supervisor_path,
                {
                    "run_id": run_id,
                    "status": "running",
                    "supervisor_pid": os.getpid(),
                    "boot_id": _boot_id(),
                    "worker_version": args.version,
                    "heartbeat_at": _now(),
                },
            )
            time.sleep(min(1, max(0, deadline - time.monotonic())))
        state = _read_json(state_path)
        state["execution"].update(
            {
                "status": "completed",
                "ok": True,
                "returncode": 0,
                "finished_at": _now(),
            }
        )
        state["technical_report"].update({"status": "ready", "available": True})
        _write_json_atomic(state_path, state)
        _write_json_atomic(
            supervisor_path,
            {
                "run_id": run_id,
                "status": "completed",
                "supervisor_pid": os.getpid(),
                "boot_id": _boot_id(),
                "worker_version": args.version,
                "heartbeat_at": _now(),
                "finished_at": _now(),
                "returncode": 0,
            },
        )
    return 0


def _rpc_response(request: Any) -> dict[str, Any]:
    try:
        if not isinstance(request, dict):
            raise WorkerError("RPC request must be an object")
        return {"ok": True, "result": handle(request)}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": str(exc) or exc.__class__.__name__}


def serve() -> int:
    sys.stdout.write('{"ready":true}\n')
    sys.stdout.flush()
    for raw in sys.stdin.buffer:
        try:
            request = json.loads(raw)
        except (UnicodeError, json.JSONDecodeError) as exc:
            response = {"ok": False, "error": f"invalid RPC JSON: {exc}"}
        else:
            response = _rpc_response(request)
        sys.stdout.write(json.dumps(response, ensure_ascii=False) + "\n")
        sys.stdout.flush()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("rpc")
    subparsers.add_parser("serve")
    supervisor = subparsers.add_parser("supervise")
    supervisor.add_argument("--state", required=True)
    supervisor.add_argument("--run-root", required=True)
    supervisor.add_argument("--version", required=True)
    diagnostic = subparsers.add_parser("diagnose")
    diagnostic.add_argument("--state", required=True)
    diagnostic.add_argument("--run-root", required=True)
    diagnostic.add_argument("--version", required=True)
    args = parser.parse_args()
    if args.command == "supervise":
        return supervise(args)
    if args.command == "diagnose":
        return diagnose(args)
    if args.command == "serve":
        return serve()
    try:
        request = json.load(sys.stdin)
    except (UnicodeError, json.JSONDecodeError) as exc:
        response = {"ok": False, "error": f"invalid RPC JSON: {exc}"}
    else:
        response = _rpc_response(request)
    sys.stdout.write(json.dumps(response, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

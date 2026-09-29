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
import shutil
import signal
import stat as stat_module
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, BinaryIO

from adaptive_routing import collect_resource_snapshot
from pipeline_spec import (
    ExecutionSpecError,
    normalize_execution_spec,
    validate_plan_rules,
)
from priority_reports import generate_priority_reports, report_artifact
from tool_library import ToolLibraryError, observe_tool_versions, validate_tool_contract
from workflow_catalog import CAPABILITY_MANIFESTS

RUN_ID_RE = re.compile(r"^analysis-[A-Za-z0-9_.-]+$")
TERMINAL_STATUSES = {"completed", "failed", "blocked", "interrupted"}
QUEUE_STATUSES = {"queued", "waiting_resources"}
QUEUE_POLICY_VERSION = 1
PROJECT_PRIORITY_ORDER = {"high": 0, "normal": 1, "low": 2}
SCREEN_THREADS = 4
SCREEN_MEMORY_MB = 4 * 1024
FULL_ANALYSIS_MINIMUM_THREADS = 8
FULL_ANALYSIS_MINIMUM_MEMORY_MB = 24 * 1024
RESOURCE_RETRY_SECONDS = 5
RECOVERY_CONTRACT_VERSION = 2
FASTQ_EXTENSION_RE = re.compile(r"(?i)(\.fastq|\.fq)(\.gz)?$")
QUICK_RECORD_LIMIT = 1_000
QUICK_BYTE_LIMIT = 8 * 1024 * 1024


class WorkerError(ValueError):
    pass


class RecoveryCheckUnavailable(WorkerError):
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


def _json_digest(value: Any) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _file_identity(path: Path) -> dict[str, Any]:
    try:
        resolved = path.resolve(strict=True)
    except (FileNotFoundError, NotADirectoryError) as exc:
        raise WorkerError(f"恢复依赖不可用：{path}") from exc
    except OSError as exc:
        raise RecoveryCheckUnavailable(f"恢复依赖暂时无法验证：{path}") from exc
    try:
        stat = resolved.stat()
        if not stat_module.S_ISREG(stat.st_mode):
            raise WorkerError(f"恢复依赖不可用：{path}")
        digest = hashlib.sha256()
        with resolved.open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                digest.update(chunk)
    except RecoveryCheckUnavailable:
        raise
    except OSError as exc:
        raise RecoveryCheckUnavailable(f"恢复依赖暂时无法验证：{path}") from exc
    return {
        "path": str(resolved),
        "size": stat.st_size,
        "sha256": digest.hexdigest(),
    }


def _tree_identity(path: Path) -> dict[str, Any]:
    try:
        root = path.resolve(strict=True)
    except (FileNotFoundError, NotADirectoryError) as exc:
        raise WorkerError(f"恢复依赖目录不可用：{path}") from exc
    except OSError as exc:
        raise RecoveryCheckUnavailable(f"恢复依赖暂时无法验证：{path}") from exc
    try:
        if not stat_module.S_ISDIR(root.stat().st_mode):
            raise WorkerError(f"恢复依赖目录不可用：{path}")
        digest = hashlib.sha256()
        count = 0
        total = 0
        ignored = {
            ".cache",
            ".git",
            ".snakemake",
            "__pycache__",
            "cache",
            "logs",
            "results",
        }
        for candidate in sorted(root.rglob("*")):
            relative = candidate.relative_to(root)
            if any(part in ignored for part in relative.parts):
                continue
            if candidate.is_symlink():
                digest.update(str(relative).encode())
                digest.update(b"\0link\0")
                digest.update(str(candidate.resolve()).encode())
                continue
            if not candidate.is_file():
                continue
            identity = _file_identity(candidate)
            digest.update(str(relative).encode())
            digest.update(b"\0")
            digest.update(identity["sha256"].encode())
            count += 1
            total += int(identity["size"])
    except RecoveryCheckUnavailable:
        raise
    except OSError as exc:
        raise RecoveryCheckUnavailable(f"恢复依赖暂时无法验证：{path}") from exc
    return {
        "path": str(root),
        "file_count": count,
        "bytes": total,
        "sha256": digest.hexdigest(),
    }


def _recovery_plan_payload(plan: dict[str, Any]) -> dict[str, Any]:
    return {
        key: plan.get(key)
        for key in (
            "plan_ref",
            "goals",
            "options",
            "routing",
            "execution_spec",
            "steps",
            "metadata_snapshot",
        )
    }


def _workflow1_config(workflow_root: Path) -> dict[str, dict[str, str]]:
    config_path = workflow_root / "config" / "config.yaml"
    try:
        lines = config_path.read_text(encoding="utf-8").splitlines()
    except (FileNotFoundError, NotADirectoryError) as exc:
        raise WorkerError("恢复依赖缺少 Workflow 1 配置") from exc
    except OSError as exc:
        raise RecoveryCheckUnavailable(
            "恢复依赖暂时无法验证：Workflow 1 配置"
        ) from exc
    values: dict[str, dict[str, str]] = {}
    section: str | None = None
    for line in lines:
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        heading = re.match(r"^([A-Za-z0-9_]+):\s*$", line)
        if heading:
            section = heading.group(1)
            values.setdefault(section, {})
            continue
        item = re.match(
            r"^\s{2}([A-Za-z0-9_]+):\s*[\"']?(.+?)[\"']?\s*$", line
        )
        if section and item:
            values[section][item.group(1)] = item.group(2)
    return values


def _required_config_path(
    config: dict[str, dict[str, str]], section: str, key: str
) -> Path:
    value = config.get(section, {}).get(key)
    if not value:
        raise WorkerError(f"恢复依赖缺少 Workflow 1 {section}.{key} 身份")
    return Path(value)


def _software_identity(
    path: Path,
    *,
    required_companions: tuple[str, ...] = (),
    optional_companions: tuple[str, ...] = (),
) -> dict[str, Any]:
    files = [{"role": "entrypoint", **_file_identity(path)}]
    for name in required_companions:
        files.append({"role": name, **_file_identity(path.with_name(name))})
    optional = []
    for name in optional_companions:
        candidate = path.with_name(name)
        if _dependency_file_exists(candidate):
            optional.append({"role": name, **_file_identity(candidate)})
    if optional_companions and not optional:
        raise WorkerError(f"恢复依赖缺少 {path.name} runtime 身份")
    files.extend(optional)
    return {"files": files, "sha256": _json_digest(files)}


def _dependency_file_exists(path: Path) -> bool:
    try:
        resolved = path.resolve(strict=True)
        return stat_module.S_ISREG(resolved.stat().st_mode)
    except (FileNotFoundError, NotADirectoryError):
        return False
    except OSError as exc:
        raise RecoveryCheckUnavailable(
            f"恢复依赖暂时无法验证：{path}"
        ) from exc


def _bowtie2_index_identity(prefix: Path) -> dict[str, Any]:
    layouts = (
        (".1.bt2", ".2.bt2", ".3.bt2", ".4.bt2", ".rev.1.bt2", ".rev.2.bt2"),
        (
            ".1.bt2l",
            ".2.bt2l",
            ".3.bt2l",
            ".4.bt2l",
            ".rev.1.bt2l",
            ".rev.2.bt2l",
        ),
    )
    selected = next(
        (
            suffixes
            for suffixes in layouts
            if all(
                _dependency_file_exists(Path(f"{prefix}{suffix}"))
                for suffix in suffixes
            )
        ),
        None,
    )
    if selected is None:
        raise WorkerError("恢复依赖缺少完整 Workflow 1 rRNA Bowtie2 索引")
    files = [
        {"suffix": suffix, **_file_identity(Path(f"{prefix}{suffix}"))}
        for suffix in selected
    ]
    return {
        "asset_id": "workflow1.rrna_bowtie2",
        "format": "bowtie2_large" if selected[0].endswith("bt2l") else "bowtie2",
        "files": files,
        "sha256": _json_digest(files),
    }


def _dependency_identity(
    state: dict[str, Any], settings: dict[str, Any]
) -> tuple[bool, dict[str, Any], str | None]:
    if state.get("diagnostic"):
        return True, {"kind": "diagnostic"}, None
    spec = state.get("execution_spec") or {}
    stages = spec.get("stages") if isinstance(spec, dict) else None
    if not isinstance(stages, list) or not stages:
        return False, {}, "RECOVERY_DEPENDENCY_IDENTITY_INCOMPLETE"
    stage_ids = {
        str(stage.get("id"))
        for stage in stages
        if isinstance(stage, dict) and stage.get("id")
    }
    workflow_ids = {
        str(stage.get("workflow_id"))
        for stage in stages
        if isinstance(stage, dict) and stage.get("workflow_id")
    }
    workflow1_stages = {
        "read_qc",
        "rrna_filter",
        "sample_merge",
        "assembly",
        "contig_filter",
    }
    if workflow_ids != {"workflow1"} or not stage_ids <= workflow1_stages:
        return False, {}, "RECOVERY_DEPENDENCY_IDENTITY_INCOMPLETE"
    environment = _runner_environment(settings)
    required = ("BIOAGENT_SNAKEMAKE", "BIOAGENT_WORKFLOW1_SOURCE")
    if stage_ids & {"sample_merge", "contig_filter"}:
        required = (*required, "BIOAGENT_SEQKIT")
    if any(not environment.get(key) for key in required):
        return False, {}, "RECOVERY_DEPENDENCY_IDENTITY_INCOMPLETE"
    try:
        workflow = Path(environment["BIOAGENT_WORKFLOW1_SOURCE"])
        config = _workflow1_config(workflow)
        software = {
            "snakemake": _software_identity(
                Path(environment["BIOAGENT_SNAKEMAKE"])
            )
        }
        if "read_qc" in stage_ids:
            software["fastp"] = _software_identity(
                _required_config_path(config, "software", "fastp")
            )
        if "rrna_filter" in stage_ids:
            software["bowtie2"] = _software_identity(
                _required_config_path(config, "software", "bowtie2"),
                required_companions=("bowtie2-align-s", "bowtie2-align-l"),
            )
            software["samtools"] = _software_identity(
                _required_config_path(config, "software", "samtools")
            )
        if stage_ids & {"sample_merge", "contig_filter"}:
            software["seqkit"] = _software_identity(
                Path(environment["BIOAGENT_SEQKIT"])
            )
        if "assembly" in stage_ids:
            software["megahit"] = _software_identity(
                _required_config_path(config, "software", "megahit"),
                optional_companions=(
                    "megahit_core",
                    "megahit_core_no_hw_accel",
                    "megahit_core_popcnt",
                ),
            )
        references = {}
        if "rrna_filter" in stage_ids:
            references["workflow1.rrna_bowtie2"] = _bowtie2_index_identity(
                _required_config_path(config, "database", "rrna_bowtie2")
            )
        manifest_ids = sorted(
            manifest.id
            for manifest in CAPABILITY_MANIFESTS.values()
            if manifest.stage_id in stage_ids
        )
        identity = {
            "kind": "workflow1",
            "stage_ids": sorted(stage_ids),
            "capability_ids": manifest_ids,
            "workflow": _tree_identity(workflow),
            "software": software,
            "references": references,
        }
    except RecoveryCheckUnavailable:
        return False, {}, "RECOVERY_CHECK_UNAVAILABLE"
    except (OSError, WorkerError):
        return False, {}, "RECOVERY_DEPENDENCY_IDENTITY_INCOMPLETE"
    return True, identity, None


def _attempt_dir(run_dir: Path, number: int) -> Path:
    return run_dir / ".bioagent" / "attempts" / f"{number:04d}"


def _append_attempt(
    state: dict[str, Any], *, trigger: str, status: str
) -> dict[str, Any]:
    attempts = state.setdefault("attempts", [])
    number = len(attempts) + 1
    run_dir = Path(str(state["run_dir"]))
    control_dir = _attempt_dir(run_dir, number)
    control_dir.mkdir(parents=True, exist_ok=False)
    attempt = {
        "number": number,
        "trigger": trigger,
        "status": status,
        "created_at": _now(),
        "control_dir": str(control_dir),
        "execution_log": str(control_dir / "execution.log"),
        "runner_log": str(control_dir / "runner-process.log"),
        "telemetry": str(control_dir / "telemetry.json"),
    }
    Path(attempt["execution_log"]).touch()
    attempts.append(attempt)
    state["current_attempt"] = number
    return attempt


def _current_attempt(state: dict[str, Any]) -> dict[str, Any] | None:
    attempts = state.get("attempts")
    return attempts[-1] if isinstance(attempts, list) and attempts else None


def _recovery_contract(
    state: dict[str, Any], settings: dict[str, Any], worker_version: str
) -> dict[str, Any]:
    complete, dependencies, blocker = _dependency_identity(state, settings)
    return {
        "schema_version": RECOVERY_CONTRACT_VERSION,
        "created_at": _now(),
        "plan_sha256": _json_digest(_recovery_plan_payload(state.get("plan") or {})),
        "dataset_sha256": _json_digest(state.get("dataset") or {}),
        "capability_sha256": _json_digest(
            (state.get("execution_spec") or {}).get("capability_contract") or {}
        ),
        "worker_version": worker_version,
        "dependencies_complete": complete,
        "dependencies": dependencies,
        "dependency_sha256": _json_digest(dependencies) if complete else None,
        "phase_checkpoints": {},
        "last_check": {
            "checked_at": _now(),
            "blocker_code": blocker,
        },
    }


def recovery_status(state: dict[str, Any]) -> dict[str, Any]:
    contract = state.get("recovery_contract")
    execution = state.get("execution") or {}
    attempts = state.get("attempts") if isinstance(state.get("attempts"), list) else []
    blocker = None
    checked_at = None
    if (
        not isinstance(contract, dict)
        or contract.get("schema_version") != RECOVERY_CONTRACT_VERSION
    ):
        blocker = "RECOVERY_LEGACY_STATE"
    else:
        last_check = contract.get("last_check") or {}
        blocker = last_check.get("blocker_code")
        checked_at = last_check.get("checked_at")
        if contract.get("dependencies_complete") is not True:
            blocker = blocker or "RECOVERY_DEPENDENCY_IDENTITY_INCOMPLETE"
    return {
        "available": bool(
            execution.get("status") == "interrupted"
            and isinstance(contract, dict)
            and contract.get("schema_version") == RECOVERY_CONTRACT_VERSION
            and contract.get("dependencies_complete") is True
            and not blocker
        ),
        "attempt_count": len(attempts),
        "current_attempt": state.get("current_attempt"),
        "blocker_code": blocker,
        "checked_at": checked_at,
    }


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


def _runner_alive(pid: Any, state_path: Path) -> bool:
    if not _pid_alive(pid):
        return False
    try:
        arguments = [
            value.decode("utf-8", errors="replace")
            for value in Path(f"/proc/{int(pid)}/cmdline").read_bytes().split(b"\0")
            if value
        ]
    except (OSError, TypeError, ValueError):
        return False
    try:
        state_index = arguments.index("--state")
    except ValueError:
        return False
    return state_index + 1 < len(arguments) and arguments[state_index + 1] == str(
        state_path
    )


def _tmux_command(settings: dict[str, Any]) -> list[str]:
    return [str(settings["tmux"]), "-L", str(settings["tmux_socket"])]


def _legacy_session_exists(settings: dict[str, Any], run_id: str) -> bool:
    result = subprocess.run(
        [*_tmux_command(settings), "has-session", "-t", f"bioagent-{run_id}"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    return result.returncode == 0


def _stop_legacy_session(settings: dict[str, Any], run_id: str) -> bool:
    subprocess.run(
        [
            *_tmux_command(settings),
            "kill-session",
            "-t",
            f"bioagent-{run_id}",
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    return not _legacy_session_exists(settings, run_id)


def _scheduler_session_name(settings: dict[str, Any]) -> str:
    digest = hashlib.sha256(str(Path(settings["run_root"])).encode()).hexdigest()[:12]
    return f"bioagent-scheduler-{digest}"


def _scheduler_session_exists(settings: dict[str, Any]) -> bool:
    result = subprocess.run(
        [
            *_tmux_command(settings),
            "has-session",
            "-t",
            _scheduler_session_name(settings),
        ],
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


def _ensure_scheduler(settings: dict[str, Any], version: str) -> None:
    if _scheduler_session_exists(settings):
        return
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
            "schedule",
            "--run-root",
            str(settings["run_root"]),
            "--version",
            version,
            "--aging-hours",
            str(settings.get("queue_aging_hours") or 24),
            "--tmux",
            str(settings["tmux"]),
            "--tmux-socket",
            str(settings["tmux_socket"]),
        )
    )
    result = subprocess.run(
        [
            *_tmux_command(settings),
            "new-session",
            "-d",
            "-s",
            _scheduler_session_name(settings),
            command,
        ],
        stdin=subprocess.DEVNULL,
        capture_output=True,
        check=False,
    )
    if result.returncode and not _scheduler_session_exists(settings):
        raise WorkerError(
            result.stderr.decode("utf-8", errors="replace").strip()
            or "无法启动远端队列调度器"
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


def _recovery_fingerprint_status(
    source: dict[str, Any], settings: dict[str, Any]
) -> bool | None:
    try:
        current = resolve_source(
            str(source["path"]),
            settings,
            expected_kind=str(source.get("kind") or "") or None,
        )
    except KeyError:
        return False
    except WorkerError as exc:
        message = str(exc).lower()
        if "unavailable" in message or "不可用" in message or "not readable" in message:
            return None
        return False
    return current["fingerprint"] == source.get("fingerprint")


def _recovery_dataset_status(
    dataset: dict[str, Any], settings: dict[str, Any]
) -> str:
    if dataset.get("status") != "ready":
        return "changed"
    seen = 0
    for artifact in dataset.get("artifacts") or []:
        for record in artifact.get("records") or []:
            for source in (record.get("files") or {}).values():
                if not isinstance(source, dict):
                    return "changed"
                status = _recovery_fingerprint_status(source, settings)
                if status is None:
                    return "unavailable"
                if status is False:
                    return "changed"
                seen += 1
    return "ready" if seen else "changed"


def _queue_context(request: dict[str, Any], plan: dict[str, Any]) -> dict[str, Any]:
    raw = request.get("queue_context")
    if not isinstance(raw, dict):
        raise WorkerError("analysis queue context is missing")
    project_id = raw.get("project_id")
    project_priority = raw.get("project_priority")
    if not isinstance(project_id, str) or not project_id:
        raise WorkerError("analysis queue project is invalid")
    if project_priority not in PROJECT_PRIORITY_ORDER:
        raise WorkerError("analysis queue priority is invalid")
    policy = (plan.get("execution_spec") or {}).get("policy") or {}
    return {
        "schema_version": 1,
        "policy_version": QUEUE_POLICY_VERSION,
        "project_id": project_id,
        "project_priority": project_priority,
        "routing_priority": (
            "emergency" if policy.get("requested_mode") == "emergency" else "normal"
        ),
        "enqueued_at": _now(),
        "waiting_stage": None,
        "waiting_reason_codes": [],
        "last_checked_at": None,
    }


def _legacy_queue(state: dict[str, Any]) -> dict[str, Any]:
    execution = state.get("execution") or {}
    return {
        "schema_version": 1,
        "policy_version": QUEUE_POLICY_VERSION,
        "project_id": "legacy",
        "project_priority": "normal",
        "routing_priority": (
            "emergency"
            if ((state.get("execution_spec") or {}).get("policy") or {}).get(
                "requested_mode"
            )
            == "emergency"
            else "normal"
        ),
        "enqueued_at": execution.get("queued_at") or _now(),
        "waiting_stage": None,
        "waiting_reason_codes": [],
        "last_checked_at": None,
    }


def _queue_record(state: dict[str, Any]) -> dict[str, Any]:
    queue = state.get("queue")
    if not isinstance(queue, dict) or queue.get("policy_version") != 1:
        queue = _legacy_queue(state)
        state["queue"] = queue
    return queue


def _timestamp(value: Any) -> float:
    if not isinstance(value, str):
        return time.time()
    try:
        return datetime.fromisoformat(value).timestamp()
    except ValueError:
        return time.time()


def _queue_sort_key(
    state: dict[str, Any], *, now: float, aging_seconds: float
) -> tuple[Any, ...]:
    queue = _queue_record(state)
    enqueued = _timestamp(queue.get("enqueued_at"))
    aged = now - enqueued >= aging_seconds
    if aged:
        return (0, enqueued, str(state.get("run_id") or ""))
    return (
        1,
        PROJECT_PRIORITY_ORDER.get(str(queue.get("project_priority")), 1),
        0 if queue.get("routing_priority") == "emergency" else 1,
        enqueued,
        str(state.get("run_id") or ""),
    )


def _queued_states(run_root: Path, aging_seconds: float) -> list[tuple[Path, dict[str, Any]]]:
    values: list[tuple[Path, dict[str, Any]]] = []
    for state_path in run_root.glob("analysis-*/.bioagent/state.json"):
        try:
            state = _read_json(state_path)
        except WorkerError:
            continue
        status = str((state.get("execution") or {}).get("status") or "")
        if status in QUEUE_STATUSES:
            values.append((state_path, state))
    now = time.time()
    return sorted(
        values,
        key=lambda item: _queue_sort_key(
            item[1], now=now, aging_seconds=aging_seconds
        ),
    )


def _resource_requirement(state: dict[str, Any]) -> tuple[str, int, float]:
    queue = _queue_record(state)
    if queue.get("waiting_stage") == "full_analysis":
        stage = "full_analysis"
    else:
        policy = ((state.get("execution_spec") or {}).get("policy") or {})
        stage = "screening" if policy.get("adaptive") else "full_analysis"
    if stage == "screening":
        return stage, SCREEN_THREADS, SCREEN_MEMORY_MB
    estimate = state.get("resource_estimate") or {}
    calibrated = estimate.get("status") == "calibrated"
    return (
        stage,
        int(estimate.get("cpu_cores") or FULL_ANALYSIS_MINIMUM_THREADS)
        if calibrated
        else FULL_ANALYSIS_MINIMUM_THREADS,
        float((estimate.get("peak_memory_mb") or {}).get("reserve"))
        if calibrated
        else FULL_ANALYSIS_MINIMUM_MEMORY_MB,
    )


def _admission_snapshot(state: dict[str, Any], run_root: Path) -> dict[str, Any]:
    diagnostic = state.get("diagnostic")
    if isinstance(diagnostic, dict):
        remaining = max(0, int(diagnostic.get("admission_blocks_remaining") or 0))
        if remaining:
            diagnostic["admission_blocks_remaining"] = remaining - 1
            return {
                "admission_ok": False,
                "reason_codes": ["DIAGNOSTIC_RESOURCE_WAIT"],
                "required_threads": 0,
                "required_memory_mb": 0,
                "stage": "full_analysis",
            }
        return {
            "admission_ok": True,
            "reason_codes": [],
            "required_threads": 0,
            "required_memory_mb": 0,
            "stage": "full_analysis",
        }
    stage, threads, memory = _resource_requirement(state)
    snapshot = collect_resource_snapshot(
        run_root,
        run_id=str(state.get("run_id") or ""),
        required_threads=threads,
        required_memory_mb=memory,
    )
    snapshot["stage"] = stage
    return snapshot


def _queue_status(
    state: dict[str, Any], settings: dict[str, Any]
) -> dict[str, Any]:
    queue = dict(_queue_record(state))
    status = str((state.get("execution") or {}).get("status") or "")
    position = None
    if status in QUEUE_STATUSES:
        aging_seconds = float(settings.get("queue_aging_hours") or 24) * 3600
        candidates = _queued_states(Path(settings["run_root"]), aging_seconds)
        position = next(
            (
                index
                for index, (_path, candidate) in enumerate(candidates, start=1)
                if candidate.get("run_id") == state.get("run_id")
            ),
            None,
        )
    queue["position"] = position
    return queue


def launch(request: dict[str, Any]) -> dict[str, Any]:
    settings = request["settings"]
    run_id = str(request.get("run_id") or "")
    state_path = _state_path(settings, run_id)
    plan = request.get("plan")
    dataset = request.get("dataset")
    if not isinstance(plan, dict) or not isinstance(dataset, dict):
        raise WorkerError("analysis plan or dataset is missing")
    raw_execution_spec = plan.get("execution_spec")
    if not isinstance(raw_execution_spec, dict) or not isinstance(
        raw_execution_spec.get("capability_contract"), dict
    ):
        raise WorkerError(
            "analysis execution spec is invalid: capability contract is missing"
        )
    raw_stage_ids = {
        stage.get("id")
        for stage in raw_execution_spec.get("stages") or []
        if isinstance(stage, dict)
    }
    if not isinstance(raw_execution_spec.get("tool_contract"), dict) or (
        "read_qc" in raw_stage_ids
        and not isinstance(raw_execution_spec.get("workflow_skill"), dict)
    ):
        raise WorkerError(
            "analysis execution spec is invalid: tool or workflow skill contract is missing"
        )
    try:
        execution_spec = normalize_execution_spec(raw_execution_spec)
        validate_plan_rules(execution_spec, plan.get("steps") or [])
    except ExecutionSpecError as exc:
        raise WorkerError(f"analysis execution spec is invalid: {exc}") from exc
    try:
        validate_tool_contract(
            execution_spec.get("tool_contract"),
            include_fastp=any(
                stage.get("id") == "read_qc"
                for stage in execution_spec.get("stages") or []
            ),
        )
        tool_observations = observe_tool_versions(execution_spec["tool_contract"])
    except ToolLibraryError as exc:
        raise WorkerError(f"{exc.code}: {exc}") from exc
    plan = {**plan, "execution_spec": execution_spec}
    _validate_dataset(dataset, settings)
    if state_path.exists():
        state = _read_json(state_path)
        if state.get("openwebui_chat_id") != request.get("chat_id"):
            raise WorkerError("现有分析状态与当前方案不匹配")
        status = str((state.get("execution") or {}).get("status") or "queued")
        if status in QUEUE_STATUSES:
            _ensure_scheduler(settings, str(request["worker_version"]))
        return _run_item(run_id, settings, status)
    run_dir = state_path.parent.parent
    execution_spec = plan["execution_spec"]
    steps = plan.get("steps") or []
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
        "tool_observations": tool_observations,
        "tool_results": [],
        "artifact_reports": {},
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
        "queue": _queue_context(request, plan),
        "execution": {
            "started": True,
            "status": "queued",
            "ok": None,
            "returncode": None,
            "current_step_id": None,
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
    recovery_contract = _recovery_contract(
        state, settings, str(request["worker_version"])
    )
    state_path.parent.mkdir(parents=True, exist_ok=True)
    attempt = _append_attempt(state, trigger="initial", status="queued")
    state["execution"]["log"] = attempt["execution_log"]
    state["technical_report"]["telemetry"] = attempt["telemetry"]
    state["reports"]["final"]["telemetry"] = attempt["telemetry"]
    state["recovery_contract"] = recovery_contract
    (state_path.parent / "agent-trace.jsonl").touch(exist_ok=True)
    _write_json_atomic(state_path, state)
    _ensure_scheduler(settings, str(request["worker_version"]))
    return _run_item(run_id, settings, "queued")


def diagnostic_launch(request: dict[str, Any]) -> dict[str, Any]:
    """Launch a bounded no-work run used to verify queue and recovery semantics."""
    settings = request["settings"]
    run_id = str(request.get("run_id") or "")
    state_path = _state_path(settings, run_id)
    duration = max(1, min(120, int(request.get("duration_seconds") or 5)))
    admission_blocks = max(0, min(10, int(request.get("admission_blocks") or 0)))
    if state_path.exists():
        state = _read_json(state_path)
        if not state.get("diagnostic"):
            raise WorkerError("existing run is not a diagnostic run")
        status = str((state.get("execution") or {}).get("status") or "queued")
        if status in QUEUE_STATUSES:
            _ensure_scheduler(settings, str(request["worker_version"]))
        return _run_item(run_id, settings, status)
    run_dir = state_path.parent.parent
    state = {
        "version": 3,
        "run_id": run_id,
        "run_dir": str(run_dir),
        "worker_version": request["worker_version"],
        "diagnostic": {
            "duration_seconds": duration,
            "admission_blocks": admission_blocks,
            "admission_blocks_remaining": admission_blocks,
        },
        "queue": _queue_context(
            request,
            {
                "execution_spec": {
                    "policy": {
                        "requested_mode": request.get("routing_mode") or "standard"
                    }
                }
            },
        ),
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
    attempt = _append_attempt(state, trigger="initial", status="queued")
    state["execution"]["log"] = attempt["execution_log"]
    state["recovery_contract"] = _recovery_contract(
        state, settings, str(request["worker_version"])
    )
    _write_json_atomic(state_path, state)
    _ensure_scheduler(settings, str(request["worker_version"]))
    return _run_item(run_id, settings, "queued")


def _set_recovery_blocker(
    state_path: Path, state: dict[str, Any], code: str | None
) -> None:
    contract = state.get("recovery_contract")
    if isinstance(contract, dict):
        contract["last_check"] = {
            "checked_at": _now(),
            "blocker_code": code,
        }
        _write_json_atomic(state_path, state)


def _recovery_reject(
    state_path: Path, state: dict[str, Any], code: str, message: str
) -> None:
    _set_recovery_blocker(state_path, state, code)
    raise WorkerError(f"{code}: {message}")


def _validate_phase_checkpoints(
    state_path: Path, state: dict[str, Any], contract: dict[str, Any]
) -> list[str]:
    run_dir = state_path.parent.parent.resolve()
    checkpoints = contract.get("phase_checkpoints")
    if not isinstance(checkpoints, dict):
        _recovery_reject(
            state_path,
            state,
            "RECOVERY_ARTIFACT_CHANGED",
            "恢复 checkpoint 格式无效",
        )
    completed: list[str] = []
    for phase_id, expected in checkpoints.items():
        if not isinstance(phase_id, str) or not isinstance(expected, dict):
            _recovery_reject(
                state_path,
                state,
                "RECOVERY_ARTIFACT_CHANGED",
                "恢复 checkpoint 格式无效",
            )
        results_dir = (run_dir / "results" / phase_id).resolve()
        if run_dir not in results_dir.parents:
            _recovery_reject(
                state_path,
                state,
                "RECOVERY_ARTIFACT_CHANGED",
                "恢复产物路径越界",
            )
        try:
            actual = _tree_identity(results_dir)
        except RecoveryCheckUnavailable as exc:
            raise WorkerError(
                "RECOVERY_CHECK_UNAVAILABLE: 已完成阶段产物暂时无法验证"
            ) from exc
        except WorkerError:
            _recovery_reject(
                state_path,
                state,
                "RECOVERY_ARTIFACT_CHANGED",
                f"已完成阶段 {phase_id} 的产物不可用",
            )
        if {
            key: actual.get(key) for key in ("file_count", "bytes", "sha256")
        } != {key: expected.get(key) for key in ("file_count", "bytes", "sha256")}:
            _recovery_reject(
                state_path,
                state,
                "RECOVERY_ARTIFACT_CHANGED",
                f"已完成阶段 {phase_id} 的产物发生变化",
            )
        completed.append(phase_id)
    return completed


def resume(request: dict[str, Any]) -> dict[str, Any]:
    settings = request["settings"]
    run_id = str(request.get("run_id") or "")
    state_path = _state_path(settings, run_id)
    if not state_path.is_file():
        raise WorkerError("RECOVERY_RUN_NOT_FOUND: 分析任务不存在")
    lock_path = state_path.parent / "recovery.lock"
    lock_path.touch(exist_ok=True)
    with lock_path.open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        state = _read_json(state_path)
        execution = state.get("execution") or {}
        status = str(execution.get("status") or "")
        latest = _current_attempt(state)
        if (
            status in QUEUE_STATUSES | {"running"}
            and isinstance(latest, dict)
            and latest.get("trigger") == "manual_resume"
        ):
            _ensure_scheduler(settings, str(request["worker_version"]))
            return _run_item(run_id, settings, status)
        if status != "interrupted":
            raise WorkerError(
                "RECOVERY_RUN_NOT_INTERRUPTED: 只有已中断任务可以恢复"
            )
        contract = state.get("recovery_contract")
        if (
            not isinstance(contract, dict)
            or contract.get("schema_version") != RECOVERY_CONTRACT_VERSION
        ):
            raise WorkerError(
                "RECOVERY_LEGACY_STATE: 旧任务缺少恢复契约，请建立新方案与运行"
            )
        if contract.get("dependencies_complete") is not True:
            _recovery_reject(
                state_path,
                state,
                "RECOVERY_DEPENDENCY_IDENTITY_INCOMPLETE",
                "当前能力缺少完整工具或参考资产身份，请建立新运行",
            )
        if state.get("openwebui_chat_id") != request.get("chat_id"):
            _recovery_reject(
                state_path,
                state,
                "RECOVERY_CONTEXT_CHANGED",
                "任务与当前会话不匹配",
            )
        plan = request.get("plan")
        dataset = request.get("dataset")
        if not isinstance(plan, dict) or not isinstance(dataset, dict):
            if not state.get("diagnostic"):
                raise WorkerError(
                    "RECOVERY_CONTEXT_CHANGED: 恢复请求缺少权威方案或数据集"
                )
            plan = state.get("plan") or {}
            dataset = state.get("dataset") or {}
        if state.get("plan_ref") != plan.get("plan_ref"):
            _recovery_reject(
                state_path,
                state,
                "RECOVERY_CONTEXT_CHANGED",
                "方案 revision 已改变",
            )
        if contract.get("plan_sha256") != _json_digest(
            _recovery_plan_payload(plan)
        ) or contract.get("dataset_sha256") != _json_digest(dataset):
            _recovery_reject(
                state_path,
                state,
                "RECOVERY_CONTEXT_CHANGED",
                "冻结方案或数据集已改变",
            )
        if contract.get("worker_version") != str(request.get("worker_version")):
            _recovery_reject(
                state_path,
                state,
                "RECOVERY_WORKER_CHANGED",
                "worker 版本已改变",
            )
        if dataset:
            dataset_status = _recovery_dataset_status(dataset, settings)
            if dataset_status == "unavailable":
                raise WorkerError(
                    "RECOVERY_CHECK_UNAVAILABLE: 输入指纹暂时无法验证"
                )
            if dataset_status != "ready":
                _recovery_reject(
                    state_path,
                    state,
                    "RECOVERY_INPUT_CHANGED",
                    "输入文件或指纹已改变",
                )
        complete, dependencies, blocker = _dependency_identity(state, settings)
        if blocker == "RECOVERY_CHECK_UNAVAILABLE":
            raise WorkerError("RECOVERY_CHECK_UNAVAILABLE: 恢复依赖暂时无法验证")
        if (
            not complete
            or blocker
            or contract.get("dependency_sha256") != _json_digest(dependencies)
        ):
            _recovery_reject(
                state_path,
                state,
                blocker or "RECOVERY_DEPENDENCY_CHANGED",
                "工具、workflow 或参考资产身份已改变",
            )
        supervisor_path = state_path.parent / "supervisor.json"
        supervisor = _read_json(supervisor_path) if supervisor_path.is_file() else {}
        if _runner_alive(supervisor.get("runner_pid"), state_path):
            _recovery_reject(
                state_path,
                state,
                "RECOVERY_RUNNER_ACTIVE",
                "远端 runner 仍在运行",
            )
        completed_phases = _validate_phase_checkpoints(state_path, state, contract)
        preliminary = (state.get("reports") or {}).get("preliminary") or {}
        if preliminary.get("available") is not True:
            # Screening outputs are reusable only after the immutable preliminary
            # report has been published. A crash before publication must not make
            # the next attempt trust partial screening files.
            screening_root = state_path.parent.parent / "results" / "screening"
            if screening_root.exists():
                shutil.rmtree(screening_root)
            (state_path.parent / "preliminary-telemetry.json").unlink(missing_ok=True)
        workflow_root = state_path.parent.parent / "workflows"
        if workflow_root.exists():
            shutil.rmtree(workflow_root)
        attempt = _append_attempt(state, trigger="manual_resume", status="queued")
        now = _now()
        state["resume_context"] = {
            "attempt": attempt["number"],
            "completed_phase_ids": completed_phases,
            "requested_at": now,
        }
        for phase in state.get("phases") or []:
            if isinstance(phase, dict) and phase.get("id") not in completed_phases:
                phase["status"] = "pending"
        for step in state.get("steps") or []:
            if isinstance(step, dict) and step.get("status") != "completed":
                step["status"] = "pending"
        execution.update(
            {
                "status": "queued",
                "ok": None,
                "returncode": None,
                "current_step_id": None,
                "queued_at": now,
                "log": attempt["execution_log"],
            }
        )
        for key in ("error", "finished_at", "pid", "cancelled"):
            execution.pop(key, None)
        queue = _queue_record(state)
        queue.update(
            {
                "enqueued_at": now,
                "waiting_stage": (
                    "full_analysis"
                    if isinstance((state.get("reports") or {}).get("preliminary"), dict)
                    and state["reports"]["preliminary"].get("available") is True
                    else None
                ),
                "waiting_reason_codes": [],
                "last_checked_at": None,
            }
        )
        state.setdefault("technical_report", {}).update(
            {
                "status": "collecting",
                "available": False,
                "telemetry": attempt["telemetry"],
            }
        )
        state.setdefault("reports", {}).setdefault("final", {}).update(
            {
                "status": "collecting",
                "available": False,
                "telemetry": attempt["telemetry"],
            }
        )
        state["standard_delivery"] = {
            "schema_version": 1,
            "status": "pending",
            "artifacts": {},
        }
        contract["last_check"] = {"checked_at": now, "blocker_code": None}
        _write_json_atomic(state_path, state)
    _ensure_scheduler(settings, str(request["worker_version"]))
    return _run_item(run_id, settings, "queued")


def _run_item(run_id: str, settings: dict[str, Any], status: str) -> dict[str, Any]:
    run_dir = Path(settings["run_root"]) / run_id
    state_path = run_dir / ".bioagent" / "state.json"
    log_path = run_dir / ".bioagent" / "execution.log"
    if state_path.is_file():
        try:
            state = _read_json(state_path)
            value = (state.get("execution") or {}).get("log")
            if isinstance(value, str):
                log_path = Path(value)
        except WorkerError:
            pass
    return {
        "type": "analysis_run",
        "id": f"analysis_run_{run_id}",
        "status": status,
        "run": {
            "run_id": run_id,
            "run_dir": str(run_dir),
            "state_path": str(state_path),
            "log_path": str(log_path),
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
    session_alive = _scheduler_session_exists(settings)
    pid_alive = _runner_alive(supervisor.get("runner_pid"), state_path)
    same_boot = supervisor.get("boot_id") in {None, _boot_id()}
    if status in QUEUE_STATUSES or (status == "running" and not session_alive):
        _ensure_scheduler(settings, str(request["worker_version"]))
        session_alive = True
    requested_report = request.get("report_kind")
    telemetry_path = state_path.parent / "telemetry.json"
    if requested_report in {"preliminary", "final"}:
        reports = state.get("reports") if isinstance(state.get("reports"), dict) else {}
        report = reports.get(requested_report)
        if isinstance(report, dict) and isinstance(report.get("telemetry"), str):
            candidate = Path(report["telemetry"])
            if candidate.is_absolute() and _lexical_under(
                candidate, state_path.parent.parent
            ):
                telemetry_path = candidate
    elif isinstance((state.get("technical_report") or {}).get("telemetry"), str):
        candidate = Path(state["technical_report"]["telemetry"])
        if candidate.is_absolute() and _lexical_under(
            candidate, state_path.parent.parent
        ):
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
        "queue": _queue_status(state, settings),
        "recovery": recovery_status(state),
        "log": _read_optional(
            Path(str((state.get("execution") or {}).get("log") or state_path.parent / "execution.log")),
            8 * 1024 * 1024,
        )
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
            "waiting_resources": statuses.count("waiting_resources"),
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
    if len(data) != artifact.get("bytes") or hashlib.sha256(
        data
    ).hexdigest() != artifact.get("sha256"):
        raise WorkerError("Delivery report artifact validation failed")
    return {
        "format": format_name,
        "media_type": artifact.get("media_type"),
        "sha256": artifact.get("sha256"),
        "content_base64": base64.b64encode(data).decode("ascii"),
    }


def generate_artifact_reports(request: dict[str, Any]) -> dict[str, Any]:
    run_dir = Path(str(request.get("run_dir") or ""))
    root = Path(request["settings"]["run_root"])
    if not run_dir.is_absolute() or not _lexical_under(run_dir, root):
        raise WorkerError("Run directory is outside the analysis run root")
    resolved_root = root.resolve()
    resolved_run = run_dir.resolve()
    if resolved_root not in resolved_run.parents:
        raise WorkerError("Run directory is outside the analysis run root")
    baseline_state_path = None
    baseline_run_dir = request.get("baseline_run_dir")
    if baseline_run_dir:
        baseline = Path(str(baseline_run_dir))
        if not baseline.is_absolute() or not _lexical_under(baseline, root):
            raise WorkerError(
                "Baseline run directory is outside the analysis run root"
            )
        resolved_baseline = baseline.resolve()
        if resolved_root not in resolved_baseline.parents:
            raise WorkerError(
                "Baseline run directory is outside the analysis run root"
            )
        baseline_state_path = resolved_baseline / ".bioagent" / "state.json"
        if not baseline_state_path.is_file():
            raise WorkerError("Baseline run state is unavailable")
    try:
        reports = generate_priority_reports(
            resolved_run / ".bioagent" / "state.json",
            reasoning_plan=request.get("reasoning_plan"),
            baseline_state_path=baseline_state_path,
            weasyprint=(request["settings"].get("runner_environment") or {}).get(
                "BIOAGENT_WEASYPRINT",
                "/home/liufangzhou/.pixi/bin/bioagent-weasyprint",
            ),
        )
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as exc:
        raise WorkerError(str(exc)) from exc
    return {"artifact_reports": reports}


def artifact_report(request: dict[str, Any]) -> dict[str, Any]:
    run_dir = Path(str(request.get("run_dir") or ""))
    root = Path(request["settings"]["run_root"])
    if not run_dir.is_absolute() or not _lexical_under(run_dir, root):
        raise WorkerError("Run directory is outside the analysis run root")
    resolved_root = root.resolve()
    resolved_run = run_dir.resolve()
    if resolved_root not in resolved_run.parents:
        raise WorkerError("Run directory is outside the analysis run root")
    state = _read_json(resolved_run / ".bioagent" / "state.json")
    try:
        metadata, data = report_artifact(
            state,
            resolved_run,
            kind=str(request.get("kind") or ""),
            format_name=str(request.get("format") or ""),
        )
    except ValueError as exc:
        raise WorkerError(str(exc)) from exc
    if len(data) > 32 * 1024 * 1024:
        raise WorkerError("Priority report artifact exceeds the transfer limit")
    return {
        "media_type": metadata.get("media_type"),
        "sha256": metadata.get("sha256"),
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
    rscript = version([runner_environment.get("BIOAGENT_RSCRIPT", ""), "--version"])
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
    dependencies_complete, dependencies, dependency_blocker = _dependency_identity(
        {"execution_spec": execution_spec}, settings
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
        "dependency_identity": {
            "schema_version": RECOVERY_CONTRACT_VERSION,
            "complete": dependencies_complete,
            "blocker_code": dependency_blocker,
            "sha256": (
                _json_digest(dependencies) if dependencies_complete else None
            ),
            "dependencies": dependencies,
        },
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
    if operation == "resume":
        return resume(request)
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
    if operation == "generate_artifact_reports":
        return generate_artifact_reports(request)
    if operation == "artifact_report":
        return artifact_report(request)
    if operation == "dry_run":
        return dry_run(request)
    raise WorkerError(f"unsupported worker operation: {operation}")


def run_diagnostic(args: argparse.Namespace) -> int:
    state_path = Path(args.state)
    state = _read_json(state_path)
    diagnostic = state.setdefault("diagnostic", {})
    diagnostic["starts"] = int(diagnostic.get("starts") or 0) + 1
    duration = int(diagnostic.get("duration_seconds") or 5)
    _write_json_atomic(state_path, state)
    time.sleep(duration)
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
    attempt = _current_attempt(state)
    if isinstance(attempt, dict):
        attempt.update({"status": "completed", "finished_at": _now()})
    _write_json_atomic(state_path, state)
    return 0


def _interrupt_missing_runner(state_path: Path, state: dict[str, Any]) -> None:
    state["execution"].update(
        {
            "status": "interrupted",
            "ok": False,
            "error": "Cyclone 重启或远端执行进程已消失；产物已保留，任务未自动重跑。",
            "finished_at": _now(),
        }
    )
    state.setdefault("technical_report", {}).update(
        {"status": "ready", "available": True}
    )
    attempt = _current_attempt(state)
    if isinstance(attempt, dict):
        attempt.update({"status": "interrupted", "finished_at": _now()})
    _write_json_atomic(state_path, state)


def _running_process(run_root: Path) -> tuple[Path, dict[str, Any], dict[str, Any]] | None:
    for state_path in run_root.glob("analysis-*/.bioagent/state.json"):
        try:
            state = _read_json(state_path)
        except WorkerError:
            continue
        if (state.get("execution") or {}).get("status") != "running":
            continue
        supervisor_path = state_path.parent / "supervisor.json"
        supervisor = _read_json(supervisor_path) if supervisor_path.is_file() else {}
        same_boot = supervisor.get("boot_id") in {None, _boot_id()}
        if same_boot and _runner_alive(supervisor.get("runner_pid"), state_path):
            return state_path, state, supervisor
        _interrupt_missing_runner(state_path, state)
        _write_json_atomic(
            supervisor_path,
            {
                **supervisor,
                "status": "interrupted",
                "scheduler_pid": os.getpid(),
                "boot_id": _boot_id(),
                "heartbeat_at": _now(),
                "finished_at": _now(),
            },
        )
    return None


def _runner_command(state_path: Path, state: dict[str, Any]) -> list[str]:
    worker = Path(__file__).resolve()
    if state.get("diagnostic"):
        return [sys.executable, "-S", str(worker), "run-diagnostic", "--state", str(state_path)]
    return [
        sys.executable,
        str(worker.with_name("analysis_runner.py")),
        "--state",
        str(state_path),
    ]


def _run_candidate(
    state_path: Path,
    state: dict[str, Any],
    *,
    version: str,
) -> int:
    run_id = str(state.get("run_id") or state_path.parent.parent.name)
    supervisor_path = state_path.parent / "supervisor.json"
    queue = _queue_record(state)
    waiting_stage = queue.get("waiting_stage")
    queue.update(
        {
            "waiting_stage": (
                "full_analysis" if waiting_stage == "full_analysis" else None
            ),
            "waiting_reason_codes": [],
            "last_checked_at": _now(),
        }
    )
    state["execution"].update(
        {"status": "running", "started_at": _now(), "ok": None}
    )
    attempt = _current_attempt(state)
    if isinstance(attempt, dict):
        attempt.update({"status": "running", "started_at": _now()})
    _write_json_atomic(state_path, state)
    runner_log = (
        Path(str(attempt["runner_log"]))
        if isinstance(attempt, dict) and isinstance(attempt.get("runner_log"), str)
        else state_path.parent / "runner-process.log"
    )
    runner_log.parent.mkdir(parents=True, exist_ok=True)
    with runner_log.open("ab") as output:
        process = subprocess.Popen(
            _runner_command(state_path, state),
            cwd=Path(__file__).resolve().parent,
            stdin=subprocess.DEVNULL,
            stdout=output,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            close_fds=True,
        )
        while process.poll() is None:
            _write_json_atomic(
                supervisor_path,
                {
                    "run_id": run_id,
                    "status": "running",
                    "scheduler_pid": os.getpid(),
                    "runner_pid": process.pid,
                    "boot_id": _boot_id(),
                    "worker_version": version,
                    "heartbeat_at": _now(),
                },
            )
            time.sleep(1)
        returncode = int(process.returncode or 0)
    final = _read_json(state_path)
    status = str((final.get("execution") or {}).get("status") or "")
    if status not in TERMINAL_STATUSES | QUEUE_STATUSES:
        final["execution"].update(
            {
                "status": "completed" if returncode == 0 else "failed",
                "ok": returncode == 0,
                "returncode": returncode,
                "finished_at": _now(),
            }
        )
        _write_json_atomic(state_path, final)
        status = str(final["execution"]["status"])
    final_attempt = _current_attempt(final)
    if isinstance(final_attempt, dict) and status in TERMINAL_STATUSES:
        final_attempt.update({"status": status, "finished_at": _now()})
        _write_json_atomic(state_path, final)
    _write_json_atomic(
        supervisor_path,
        {
            "run_id": run_id,
            "status": status,
            "scheduler_pid": os.getpid(),
            "runner_pid": process.pid,
            "boot_id": _boot_id(),
            "worker_version": version,
            "heartbeat_at": _now(),
            "finished_at": _now(),
            "returncode": returncode,
        },
    )
    return returncode


def schedule(args: argparse.Namespace) -> int:
    run_root = Path(args.run_root)
    aging_seconds = float(args.aging_hours) * 3600
    if aging_seconds <= 0:
        raise WorkerError("queue aging hours must be positive")
    lock_path = run_root / ".bioagent" / "analysis.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    while True:
        running = _running_process(run_root)
        if running:
            time.sleep(1)
            continue
        candidates = _queued_states(run_root, aging_seconds)
        if not candidates:
            return 0
        ran = False
        waiting = False
        for state_path, state in candidates:
            run_id = str(state.get("run_id") or state_path.parent.parent.name)
            legacy_settings = {
                "tmux": args.tmux,
                "tmux_socket": args.tmux_socket,
            }
            if _legacy_session_exists(legacy_settings, run_id):
                if not _stop_legacy_session(legacy_settings, run_id):
                    waiting = True
                    continue
                state = _read_json(state_path)
                if (state.get("execution") or {}).get("status") not in QUEUE_STATUSES:
                    waiting = True
                    break
            admission = _admission_snapshot(state, run_root)
            queue = _queue_record(state)
            queue.update(
                {
                    "last_checked_at": _now(),
                    "waiting_stage": admission.get("stage") or queue.get("waiting_stage"),
                    "waiting_reason_codes": list(admission.get("reason_codes") or []),
                }
            )
            if not admission.get("admission_ok"):
                state["execution"].update({"status": "waiting_resources", "ok": None})
                _write_json_atomic(state_path, state)
                waiting = True
                continue
            with lock_path.open("a+b") as lock:
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    waiting = True
                    break
                _run_candidate(state_path, state, version=args.version)
                ran = True
            break
        if not ran:
            time.sleep(RESOURCE_RETRY_SECONDS if waiting else 1)


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
    scheduler = subparsers.add_parser("schedule")
    scheduler.add_argument("--run-root", required=True)
    scheduler.add_argument("--version", required=True)
    scheduler.add_argument("--aging-hours", required=True, type=float)
    scheduler.add_argument("--tmux", required=True)
    scheduler.add_argument("--tmux-socket", required=True)
    diagnostic = subparsers.add_parser("run-diagnostic")
    diagnostic.add_argument("--state", required=True)
    args = parser.parse_args()
    if args.command == "schedule":
        return schedule(args)
    if args.command == "run-diagnostic":
        return run_diagnostic(args)
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

"""Deterministic Snakemake execution and telemetry collection for BioAgent."""

from __future__ import annotations

import argparse
import json
import re
import shlex
import signal
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from workflow_adapters import (
    AdapterSettings,
    PreparedExecution,
    prepare_execution,
)


RULE_START_RE = re.compile(r"^(?:local)?rule\s+([A-Za-z0-9_]+):$")
FINISHED_RE = re.compile(r"Finished jobid:\s*(\d+)\s+\(Rule:\s*([A-Za-z0-9_]+)\)")
TIMESTAMP_RE = re.compile(r"^\[([A-Z][a-z]{2} .+ \d{4})\]$")
SECRET_RE = re.compile(
    r"(?i)(authorization|api[_-]?key|auth[_-]?token|secret|password)"
    r"(\s*[:=]\s*|\s+)([^\s\"']+)"
)
ENV_SECRET_RE = re.compile(
    r"(?i)\b([A-Z0-9_]*(?:TOKEN|SECRET|PASSWORD|API_KEY)[A-Z0-9_]*)=([^\s\"']+)"
)
UPSTREAM_WILDCARDS = {
    "fastp": "run_index",
    "bowtie2": "run_index",
    "merge": "sample_index",
    "megahit": "sample_index",
    "rename_filter": "sample_index",
}
SEQKIT_COLUMNS = {
    "file": "file",
    "format": "format",
    "type": "type",
    "num_seqs": "num_seqs",
    "sum_len": "sum_len",
    "min_len": "min_len",
    "avg_len": "avg_len",
    "max_len": "max_len",
    "Q1": "q1",
    "Q2": "median",
    "Q3": "q3",
    "N50": "n50",
    "Q20(%)": "q20_percent",
    "Q30(%)": "q30_percent",
    "AvgQual": "avg_quality",
    "GC(%)": "gc_percent",
}


class RunnerCancelled(RuntimeError):
    pass


def _now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def redact_text(value: str) -> str:
    value = ENV_SECRET_RE.sub(lambda match: f"{match.group(1)}=[REDACTED]", value)
    value = re.sub(
        r"(?i)\b(Bearer)\s+[A-Za-z0-9._~+/=-]+",
        r"\1 [REDACTED]",
        value,
    )
    value = SECRET_RE.sub(
        lambda match: f"{match.group(1)}{match.group(2)}[REDACTED]", value
    )
    return value


def redact_value(value: Any, key: str = "") -> Any:
    if re.search(r"(?i)(token|secret|password|api[_-]?key|authorization)", key):
        return "[REDACTED]"
    if isinstance(value, dict):
        return {
            item_key: redact_value(item_value, str(item_key))
            for item_key, item_value in value.items()
        }
    if isinstance(value, list):
        return [redact_value(item) for item in value]
    return redact_text(value) if isinstance(value, str) else value


def _read_state(path: Path) -> dict[str, Any]:
    state = json.loads(path.read_text(encoding="utf-8"))
    if state.get("version") != 2:
        raise ValueError("analysis runner requires state version 2")
    return state


def _instrument_workflow1(workflow_dir: Path, benchmark_dir: Path) -> None:
    rules_path = workflow_dir / "rules" / "upstream.smk"
    text = rules_path.read_text(encoding="utf-8")
    text = text.replace("{params.sk} stat -j", "{params.sk} stats -a -T -j")
    text = text.replace(
        "{params.sk} stats -j 1 -a",
        "{params.sk} stats -a -T -j 1",
    )

    lines = text.splitlines()
    output: list[str] = []
    current_rule: str | None = None
    current_has_benchmark = False
    for line in lines:
        if match := re.match(r"^rule\s+([A-Za-z0-9_]+):", line):
            current_rule = match.group(1)
            current_has_benchmark = False
        elif current_rule and line.startswith("    benchmark:"):
            current_has_benchmark = True

        if (
            current_rule in UPSTREAM_WILDCARDS
            and line.startswith("    shell:")
            and not current_has_benchmark
        ):
            wildcard = UPSTREAM_WILDCARDS[current_rule]
            benchmark_path = benchmark_dir / f"{current_rule}.{{{wildcard}}}.jsonl"
            output.extend(
                [
                    "    benchmark:",
                    f'        "{benchmark_path}"',
                ]
            )
            current_has_benchmark = True
        output.append(line)

    rules_path.write_text("\n".join(output) + "\n", encoding="utf-8")


def _sample_paths(workflow_dir: Path) -> list[Path]:
    samples_path = workflow_dir / "data" / "samples.txt"
    if not samples_path.is_file():
        return []
    paths: list[Path] = []
    for raw_line in samples_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        fields = line.split()
        for value in fields[-2:]:
            path = Path(value)
            if path.is_absolute() and path.is_file() and path not in paths:
                paths.append(path)
    return paths


def _collect_input_stats(
    seqkit: str | None,
    workflow_dir: Path,
    destination: Path,
) -> dict[str, Any]:
    paths = _sample_paths(workflow_dir)
    if not paths:
        return {"paths": [], "status": "empty"}
    if not seqkit:
        return {
            "paths": [_file_record(path, "input") for path in paths],
            "status": "missing_seqkit",
        }

    started_at = _now()
    result = subprocess.run(
        [seqkit, "stats", "-Ta", *map(str, paths)],
        check=False,
        capture_output=True,
        text=True,
    )
    destination.write_text(result.stdout, encoding="utf-8")
    return {
        "paths": [_file_record(path, "input") for path in paths],
        "status": "completed" if result.returncode == 0 else "failed",
        "returncode": result.returncode,
        "started_at": started_at,
        "finished_at": _now(),
        **(
            {"error": redact_text(result.stderr.strip())}
            if result.stderr.strip()
            else {}
        ),
    }


def _parse_number(value: str) -> int | float | str | None:
    normalized = value.replace(",", "").strip()
    if not normalized or normalized in {"-", "NA"}:
        return None
    try:
        number = float(normalized)
    except ValueError:
        return value
    return int(number) if number.is_integer() else number


def _parse_seqkit_table(path: Path, source: str) -> list[dict[str, Any]]:
    try:
        lines = [
            line
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
    except (OSError, UnicodeError):
        return []
    if len(lines) < 2:
        return []

    tabular = "\t" in lines[0]
    header = lines[0].split("\t") if tabular else re.split(r"\s+", lines[0].strip())
    records = []
    for line in lines[1:]:
        values = line.split("\t") if tabular else re.split(r"\s+", line.strip())
        if len(values) != len(header):
            continue
        raw = dict(zip(header, values, strict=True))
        record: dict[str, Any] = {"source": source}
        for column, target in SEQKIT_COLUMNS.items():
            if column in raw:
                record[target] = (
                    raw[column]
                    if column in {"file", "format", "type"}
                    else _parse_number(raw[column])
                )
        records.append(record)
    return records


def _parse_benchmarks(benchmark_dir: Path) -> list[dict[str, Any]]:
    jobs = []
    for path in sorted(benchmark_dir.glob("*.jsonl")):
        try:
            for line in path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                record = redact_value(json.loads(line))
                if not isinstance(record, dict):
                    continue
                record["benchmark_file"] = str(path)
                jobs.append(record)
        except (OSError, UnicodeError, json.JSONDecodeError):
            continue
    return jobs


def _parse_log(log_path: Path) -> dict[str, Any]:
    try:
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return {
            "jobs": [],
            "commands": [],
            "software": [],
            "rrna_alignment_percents": [],
        }

    jobs: list[dict[str, Any]] = []
    current_timestamp: str | None = None
    pending_rule: str | None = None
    pending_job: dict[str, Any] | None = None
    jobs_by_id: dict[str, dict[str, Any]] = {}
    commands: list[str] = []
    software: list[str] = []
    rrna_alignment_percents: list[float] = []

    for raw_line in lines:
        line = re.sub(r"\x1b\[[0-9;]*m", "", raw_line).strip()
        if timestamp_match := TIMESTAMP_RE.match(line):
            current_timestamp = timestamp_match.group(1)
            continue
        if match := RULE_START_RE.match(line):
            pending_rule = match.group(1)
            pending_job = {
                "rule": pending_rule,
                "started_at": current_timestamp,
                "status": "running",
            }
            jobs.append(pending_job)
            continue
        if pending_job and (match := re.match(r"jobid:\s*(\d+)", line)):
            pending_job["job_id"] = match.group(1)
            jobs_by_id[match.group(1)] = pending_job
            continue
        if match := FINISHED_RE.search(line):
            job = jobs_by_id.get(match.group(1))
            if job:
                job["finished_at"] = current_timestamp
                job["status"] = "completed"
            continue
        if match := re.search(r"([0-9.]+)% overall alignment rate", line):
            rrna_alignment_percents.append(float(match.group(1)))

        lower = line.lower()
        if any(
            marker in lower
            for marker in (
                "snakemake version:",
                "fastp v",
                "megahit v",
                "bowtie2 version",
                "seqkit v",
            )
        ):
            software.append(line)
        if (
            line
            and not line.startswith(("[", "input:", "output:", "reason:", "resources:"))
            and any(
                marker in lower
                for marker in (
                    "fastp",
                    "bowtie2",
                    "samtools",
                    "seqkit",
                    "megahit",
                    "gunzip",
                    "sed ",
                )
            )
            and ("--" in line or "|" in line or line.startswith(("/", "cat ", "sed ")))
        ):
            command = redact_text(line)
            if command not in commands:
                commands.append(command)

    return {
        "jobs": jobs,
        "commands": commands,
        "software": list(dict.fromkeys(software)),
        "rrna_alignment_percents": rrna_alignment_percents,
    }


def _parse_fastp(run_dir: Path) -> list[dict[str, Any]]:
    reports = []
    for path in run_dir.glob("**/*fastp*.json"):
        if ".snakemake" in path.parts or ".bioagent" in path.parts:
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            continue
        summary = data.get("summary")
        if isinstance(summary, dict):
            reports.append(
                {
                    "path": str(path),
                    "summary": summary,
                    "filtering_result": data.get("filtering_result"),
                    "duplication": data.get("duplication"),
                }
            )
    return reports


def _software_versions(
    runner_command: list[str],
    commands: list[str],
    seqkit: str | None,
) -> list[str]:
    version_args = {
        "snakemake": ["--version"],
        "fastp": ["--version"],
        "bowtie2": ["--version"],
        "samtools": ["--version"],
        "seqkit": ["version"],
        "megahit": ["--version"],
    }
    executables: dict[str, str] = {}
    candidates = [runner_command[0] if runner_command else "", seqkit or ""]
    for command in commands:
        try:
            candidates.extend(shlex.split(command))
        except ValueError:
            continue
    for candidate in candidates:
        name = Path(candidate.rstrip("|;&")).name.lower()
        for software in version_args:
            if name == software or name.startswith(f"{software}-"):
                executables.setdefault(software, candidate.rstrip("|;&"))

    versions = []
    for software, executable in executables.items():
        try:
            result = subprocess.run(
                [executable, *version_args[software]],
                check=False,
                capture_output=True,
                text=True,
                timeout=10,
            )
        except (OSError, subprocess.TimeoutExpired):
            continue
        version = (result.stdout or result.stderr).strip().splitlines()
        if version:
            versions.append(f"{software}: {redact_text(version[0][:500])}")
    return versions


def _file_record(path: Path, kind: str) -> dict[str, Any]:
    stat = path.stat()
    return {
        "path": str(path.resolve()),
        "kind": kind,
        "size": stat.st_size,
        "modified": int(stat.st_mtime),
    }


def _output_manifest(run_dir: Path) -> dict[str, Any]:
    files = []
    omitted = 0
    for path in sorted(run_dir.rglob("*")):
        if (
            not path.is_file()
            or path.is_symlink()
            or ".snakemake" in path.parts
            or ".bioagent" in path.parts
            or path.relative_to(run_dir).parts[0] == "workflow"
        ):
            continue
        if len(files) >= 5000:
            omitted += 1
            continue
        files.append(_file_record(path, "output"))
    return {"files": files, "omitted": omitted}


def _sequence_metrics(run_dir: Path, input_stats_path: Path) -> list[dict[str, Any]]:
    records = _parse_seqkit_table(input_stats_path, "raw_input")
    final_stats_path = run_dir / ".bioagent" / "final-sequences.tsv"
    records.extend(_parse_seqkit_table(final_stats_path, "final_filter"))
    for path in run_dir.glob("**/*.stat.txt"):
        if ".snakemake" not in path.parts:
            records.extend(_parse_seqkit_table(path, str(path)))
    return records


def _aggregate_sequences(records: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not records:
        return None
    sequence_counts = [
        record.get("num_seqs")
        for record in records
        if isinstance(record.get("num_seqs"), (int, float))
    ]
    base_counts = [
        record.get("sum_len")
        for record in records
        if isinstance(record.get("sum_len"), (int, float))
    ]
    return {
        "sequences": sum(sequence_counts) if sequence_counts else None,
        "bases": sum(base_counts) if base_counts else None,
    }


def _retention(before: Any, after: Any) -> float | None:
    if not isinstance(before, (int, float)) or not before:
        return None
    return (
        float(after) / float(before) * 100 if isinstance(after, (int, float)) else None
    )


def _upstream_metrics(
    sequence_metrics: list[dict[str, Any]],
    rrna_alignment_percents: list[float],
) -> dict[str, Any]:
    fastp_records = [
        record
        for record in sequence_metrics
        if "01_fastp_" in str(record.get("source"))
    ]
    rrna_records = [
        record
        for record in sequence_metrics
        if "/filter/" in str(record.get("source"))
        and "cleanreads_" in str(record.get("source"))
    ]
    assembly_records = [
        record
        for record in sequence_metrics
        if str(record.get("source")).endswith(".contigs.stat.txt")
    ]
    final_records = [
        record for record in sequence_metrics if record.get("source") == "final_filter"
    ]
    before_rrna = _aggregate_sequences(fastp_records)
    after_rrna = _aggregate_sequences(rrna_records)
    assembly = _aggregate_sequences(assembly_records)
    final = _aggregate_sequences(final_records)
    return {
        "rrna": {
            "before": before_rrna,
            "after": after_rrna,
            "reads_retained_percent": _retention(
                (before_rrna or {}).get("sequences"),
                (after_rrna or {}).get("sequences"),
            ),
            "bases_retained_percent": _retention(
                (before_rrna or {}).get("bases"),
                (after_rrna or {}).get("bases"),
            ),
            "alignment_percents": rrna_alignment_percents,
        },
        "assembly": assembly,
        "final_filter": {
            "before": assembly,
            "after": final,
            "sequences_retained_percent": _retention(
                (assembly or {}).get("sequences"),
                (final or {}).get("sequences"),
            ),
            "bases_retained_percent": _retention(
                (assembly or {}).get("bases"),
                (final or {}).get("bases"),
            ),
        },
    }


def _collect_final_stats(
    seqkit: str | None,
    run_dir: Path,
    destination: Path,
) -> dict[str, Any]:
    paths = sorted(run_dir.glob("**/*.contigs.1kb.renamed.fa"))
    paths = [path for path in paths if ".snakemake" not in path.parts]
    if not paths:
        return {"paths": [], "status": "not_produced"}
    if not seqkit:
        return {"paths": [str(path) for path in paths], "status": "missing_seqkit"}

    result = subprocess.run(
        [seqkit, "stats", "-Ta", *map(str, paths)],
        check=False,
        capture_output=True,
        text=True,
    )
    destination.write_text(result.stdout, encoding="utf-8")
    return {
        "paths": [_file_record(path, "final_filter") for path in paths],
        "status": "completed" if result.returncode == 0 else "failed",
        "returncode": result.returncode,
        **(
            {"error": redact_text(result.stderr.strip())}
            if result.stderr.strip()
            else {}
        ),
    }


def _stage_records(
    state: dict[str, Any],
    jobs: list[dict[str, Any]],
    log_jobs: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    logged = {(job.get("job_id"), job.get("rule")): job for job in log_jobs}
    stages = []
    for step in state.get("steps", []):
        rules = set(step.get("rules") or [])
        benchmark_jobs = [job for job in jobs if job.get("rule_name") in rules]
        stage_log_jobs = [job for job in log_jobs if job.get("rule") in rules]
        cpu_time = sum(float(job.get("cpu_time") or 0) for job in benchmark_jobs)
        wall_seconds = sum(float(job.get("s") or 0) for job in benchmark_jobs)
        for job in benchmark_jobs:
            log_job = logged.get((str(job.get("jobid")), job.get("rule_name")))
            if log_job:
                job.update(
                    {
                        "started_at": log_job.get("started_at"),
                        "finished_at": log_job.get("finished_at"),
                        "status": log_job.get("status"),
                    }
                )
        benchmark_keys = {
            (str(job.get("jobid")), job.get("rule_name")) for job in benchmark_jobs
        }
        combined_jobs = [
            *benchmark_jobs,
            *[
                {
                    "jobid": job.get("job_id"),
                    "rule_name": job.get("rule"),
                    "started_at": job.get("started_at"),
                    "finished_at": job.get("finished_at"),
                    "status": job.get("status"),
                }
                for job in stage_log_jobs
                if (str(job.get("job_id")), job.get("rule")) not in benchmark_keys
            ],
        ]
        starts = [job["started_at"] for job in combined_jobs if job.get("started_at")]
        finishes = [
            job["finished_at"] for job in combined_jobs if job.get("finished_at")
        ]
        stages.append(
            {
                "id": step.get("id"),
                "label": step.get("label"),
                "rules": sorted(rules),
                "status": (
                    "completed"
                    if benchmark_jobs
                    and all(job.get("status") == "completed" for job in stage_log_jobs)
                    else step.get("status", "pending")
                ),
                "wall_seconds": wall_seconds,
                "cpu_time_seconds": cpu_time,
                "effective_cores": cpu_time / wall_seconds if wall_seconds else None,
                "max_job_rss_mb": max(
                    (float(job.get("max_rss") or 0) for job in benchmark_jobs),
                    default=None,
                ),
                "started_at": min(starts) if starts else None,
                "finished_at": max(finishes) if finishes else None,
                "jobs": combined_jobs,
            }
        )
    return stages


def run(args: argparse.Namespace) -> int:
    state_path = Path(args.state).resolve()
    state = _read_state(state_path)
    run_dir = Path(state["run_dir"]).resolve()
    if state_path != run_dir / ".bioagent" / "state.json":
        raise ValueError("state path does not match run directory")

    bioagent_dir = run_dir / ".bioagent"
    benchmark_dir = bioagent_dir / "benchmarks"
    telemetry_path = bioagent_dir / "telemetry.json"
    input_stats_path = bioagent_dir / "input-sequences.tsv"
    log_path = Path(state["execution"]["log"]).resolve()
    benchmark_dir.mkdir(parents=True, exist_ok=True)

    started_at = _now()
    state["execution"].update({"started_at": started_at, "status": "running"})
    _write_json_atomic(state_path, state)

    spec = state.get("execution_spec") or {}
    workflow_id = str(spec.get("adapter") or "unknown")
    telemetry: dict[str, Any] = {
        "version": 2,
        "run_id": state.get("run_id"),
        "chat_id": state.get("openwebui_chat_id"),
        "workflow_id": workflow_id,
        "started_at": started_at,
        "runner_command": [],
        "input_collection": {},
        "stages": [],
        "commands": [],
        "software": [],
        "sequence_metrics": [],
        "fastp": [],
        "manifest": {"files": [], "omitted": 0},
        "manifest_paths": {
            "input": str(bioagent_dir / "input-manifest.json"),
            "output": str(bioagent_dir / "output-manifest.json"),
        },
    }

    returncode = 1
    error: str | None = None
    cancelled_signal: int | None = None
    process: subprocess.Popen[str] | None = None
    previous_handlers: dict[int, Any] = {}
    prepared: PreparedExecution | None = None
    settings = AdapterSettings.from_env()

    def cancel(signum: int, _frame: Any) -> None:
        nonlocal cancelled_signal
        cancelled_signal = signum
        if process and process.poll() is None:
            process.terminate()
        raise RunnerCancelled(f"analysis runner cancelled by signal {signum}")

    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            previous_handlers[signum] = signal.signal(signum, cancel)
        except ValueError:
            previous_handlers = {}
            break
    try:
        prepared = prepare_execution(state, settings)
        workflow_id = prepared.workflow_id
        state["workflow_id"] = workflow_id
        state["workflow_dir"] = str(prepared.workflow_dir)
        state["execution_spec"] = spec
        _write_json_atomic(state_path, state)
        telemetry["workflow_id"] = workflow_id
        _instrument_workflow1(prepared.workflow_dir, benchmark_dir)
        telemetry["input_collection"] = _collect_input_stats(
            prepared.seqkit,
            prepared.workflow_dir,
            input_stats_path,
        )

        command = prepared.command
        telemetry["runner_command"] = [redact_text(value) for value in command]

        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("w", encoding="utf-8") as log_file:
            process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
            assert process.stdout is not None
            for line in process.stdout:
                log_file.write(line)
                log_file.flush()
                sys.stdout.write(line)
                sys.stdout.flush()
            process.stdout.close()
            returncode = process.wait()
    except Exception as exc:
        error = redact_text(str(exc))
        returncode = 128 + cancelled_signal if cancelled_signal else 1
        if process and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
    finally:
        log_data = _parse_log(log_path)
        benchmarks = _parse_benchmarks(benchmark_dir)
        telemetry["final_collection"] = _collect_final_stats(
            prepared.seqkit if prepared else settings.seqkit,
            run_dir,
            bioagent_dir / "final-sequences.tsv",
        )
        telemetry.update(
            {
                "finished_at": _now(),
                "returncode": returncode,
                "status": (
                    "completed"
                    if returncode == 0
                    else "cancelled"
                    if cancelled_signal
                    else "failed"
                ),
                "commands": log_data["commands"],
                "software": log_data["software"],
                "rrna_alignment_percents": log_data["rrna_alignment_percents"],
                "sequence_metrics": _sequence_metrics(run_dir, input_stats_path),
                "fastp": _parse_fastp(run_dir),
                "manifest": _output_manifest(run_dir),
                "stages": _stage_records(state, benchmarks, log_data["jobs"]),
                **({"error": error} if error else {}),
                **({"cancelled_signal": cancelled_signal} if cancelled_signal else {}),
            }
        )
        telemetry["software"] = list(
            dict.fromkeys(
                [
                    *telemetry["software"],
                    *_software_versions(
                        telemetry["runner_command"],
                        telemetry["commands"],
                        prepared.seqkit if prepared else settings.seqkit,
                    ),
                ]
            )
        )
        for stage in telemetry["stages"]:
            if returncode == 0:
                stage["status"] = "completed"
            elif any(job.get("status") == "running" for job in stage["jobs"]):
                stage["status"] = "failed"
                for job in stage["jobs"]:
                    if job.get("status") == "running":
                        job["status"] = "failed"
        if returncode != 0 and not any(
            stage.get("status") == "failed" for stage in telemetry["stages"]
        ):
            failed_stage = next(
                (
                    stage
                    for stage in telemetry["stages"]
                    if stage.get("status") != "completed"
                ),
                None,
            )
            if failed_stage:
                failed_stage["status"] = "failed"
        telemetry["upstream_metrics"] = _upstream_metrics(
            telemetry["sequence_metrics"],
            telemetry["rrna_alignment_percents"],
        )
        _write_json_atomic(
            bioagent_dir / "input-manifest.json",
            {
                "version": 1,
                "files": telemetry.get("input_collection", {}).get("paths", []),
            },
        )
        _write_json_atomic(
            bioagent_dir / "output-manifest.json",
            {"version": 1, **telemetry["manifest"]},
        )
        _write_json_atomic(telemetry_path, telemetry)

        completed_rules = {
            job.get("rule_name") for job in benchmarks if job.get("rule_name")
        }
        stages_by_id = {stage.get("id"): stage for stage in telemetry["stages"]}
        for step in state.get("steps", []):
            rules = set(step.get("rules") or [])
            stage = stages_by_id.get(step.get("id")) or {}
            if returncode == 0:
                step["status"] = "completed"
            elif rules and rules <= completed_rules:
                step["status"] = "completed"
            elif returncode != 0 and step.get("status") == "running":
                step["status"] = "failed"
            if stage.get("wall_seconds") is not None:
                step["duration_seconds"] = stage["wall_seconds"]
            stage_jobs = stage.get("jobs") or []
            starts = [job["started_at"] for job in stage_jobs if job.get("started_at")]
            finishes = [
                job["finished_at"] for job in stage_jobs if job.get("finished_at")
            ]
            if starts:
                step["started_at"] = min(starts)
            if finishes:
                step["finished_at"] = max(finishes)
        if returncode != 0:
            first_pending = next(
                (
                    step
                    for step in state.get("steps", [])
                    if step.get("status") == "pending"
                ),
                None,
            )
            if first_pending:
                first_pending["status"] = "failed"

        state["execution"].update(
            {
                "status": "completed" if returncode == 0 else "failed",
                "ok": returncode == 0,
                "returncode": returncode,
                "current_step_id": None,
                "finished_at": telemetry["finished_at"],
                **({"error": error} if error else {}),
                **({"cancelled": True} if cancelled_signal else {}),
            }
        )
        state["technical_report"] = {
            **(state.get("technical_report") or {}),
            "status": "ready",
            "available": True,
            "telemetry": str(telemetry_path),
            "telemetry_version": 2,
        }
        _write_json_atomic(state_path, state)
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)
    return returncode


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", required=True)
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(run(parse_args()))

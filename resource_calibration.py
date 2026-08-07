"""Deterministic resource estimates calibrated from completed analysis runs."""

from __future__ import annotations

import hashlib
import json
import math
import os
import sqlite3
import statistics
import subprocess
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from workflow_adapters import AdapterSettings, selected_rules


SCHEMA_VERSION = 1
LOW_MARGIN = 0.25
MEDIUM_MARGIN = 0.15
HIGH_MARGIN = 0.10
MAX_MATCHES = 20


def _stable_hash(value: Any) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _load_bucket(count: int | None) -> str:
    if count is None:
        return "unknown"
    if count <= 0:
        return "0"
    if count == 1:
        return "1"
    return "2+"


def count_active_runners(*, exclude_pid: int | None = None) -> int:
    count = 0
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit() or int(entry.name) == exclude_pid:
            continue
        try:
            arguments = [
                value.decode("utf-8", errors="replace")
                for value in (entry / "cmdline").read_bytes().split(b"\0")
                if value
            ]
        except OSError:
            continue
        if "--state" in arguments and any(
            Path(argument).name == "analysis_runner.py" for argument in arguments
        ):
            count += 1
    return count


def _host_memory_used_mb() -> float | None:
    values: dict[str, int] = {}
    try:
        for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
            key, raw = line.split(":", 1)
            values[key] = int(raw.strip().split()[0])
    except (OSError, ValueError, IndexError):
        return None
    total = values.get("MemTotal")
    available = values.get("MemAvailable")
    if total is None or available is None:
        return None
    return (total - available) / 1024


def process_tree_rss_mb(root_pid: int | None) -> float:
    if not root_pid:
        return 0.0
    processes: dict[int, tuple[int, int]] = {}
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            status = (entry / "status").read_text(encoding="utf-8")
        except OSError:
            continue
        parent = 0
        rss_kb = 0
        for line in status.splitlines():
            if line.startswith("PPid:"):
                parent = int(line.split()[1])
            elif line.startswith("VmRSS:"):
                rss_kb = int(line.split()[1])
        processes[int(entry.name)] = (parent, rss_kb)

    descendants = {root_pid}
    changed = True
    while changed:
        changed = False
        for pid, (parent, _rss) in processes.items():
            if parent in descendants and pid not in descendants:
                descendants.add(pid)
                changed = True
    return sum(processes.get(pid, (0, 0))[1] for pid in descendants) / 1024


class ProcessResourceMonitor:
    """Samples aggregate process-tree RSS and host contention without psutil."""

    def __init__(
        self,
        root_pid: Callable[[], int | None],
        *,
        interval_seconds: float = 1.0,
    ) -> None:
        self.root_pid = root_pid
        self.interval_seconds = interval_seconds
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._samples = 0
        self._other_runner_sum = 0
        self._max_other_runners = 0
        self._peak_tree_rss_mb = 0.0
        self._baseline_host_memory_mb: float | None = None
        self._peak_host_memory_mb: float | None = None

    def start(self) -> None:
        self._sample()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.wait(self.interval_seconds):
            self._sample()

    def _sample(self) -> None:
        tree_rss = process_tree_rss_mb(self.root_pid())
        host_memory = _host_memory_used_mb()
        other_runners = count_active_runners(exclude_pid=os.getpid())
        self._samples += 1
        self._other_runner_sum += other_runners
        self._max_other_runners = max(self._max_other_runners, other_runners)
        self._peak_tree_rss_mb = max(self._peak_tree_rss_mb, tree_rss)
        if host_memory is not None:
            if self._baseline_host_memory_mb is None:
                self._baseline_host_memory_mb = host_memory
            self._peak_host_memory_mb = max(
                self._peak_host_memory_mb or host_memory, host_memory
            )

    def stop(self) -> dict[str, Any]:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=max(2.0, self.interval_seconds * 2))
        self._sample()
        return {
            "sample_interval_seconds": self.interval_seconds,
            "samples": self._samples,
            "peak_process_tree_rss_mb": round(self._peak_tree_rss_mb, 3),
            "baseline_host_memory_used_mb": (
                round(self._baseline_host_memory_mb, 3)
                if self._baseline_host_memory_mb is not None
                else None
            ),
            "peak_host_memory_used_mb": (
                round(self._peak_host_memory_mb, 3)
                if self._peak_host_memory_mb is not None
                else None
            ),
            "mean_other_analysis_runs": (
                self._other_runner_sum / self._samples if self._samples else 0
            ),
            "max_other_analysis_runs": self._max_other_runners,
            "load_bucket": _load_bucket(self._max_other_runners),
        }


def _database_values(config_path: Path) -> dict[str, str]:
    try:
        lines = config_path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return {}
    values: dict[str, str] = {}
    in_database = False
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if not line.startswith((" ", "\t")):
            in_database = stripped == "database:"
            continue
        if not in_database:
            continue
        content = stripped.split("#", 1)[0].strip()
        if ":" not in content:
            continue
        key, value = content.split(":", 1)
        value = value.strip().strip("\"'")
        if key and value:
            values[key.strip()] = value
    return values


def _database_profile(config_paths: list[tuple[str, Path]]) -> tuple[str, list[str]]:
    values: dict[str, str] = {}
    for phase, path in config_paths:
        for key, value in _database_values(path).items():
            values[f"{phase}.{key}"] = value
    if not values:
        return "unknown", []
    versions = []
    for key, value in sorted(values.items()):
        parts = Path(value).parts
        label = "/".join(parts[-2:]) if len(parts) >= 2 else value
        versions.append(f"{key}={label}")
    return _stable_hash(values), versions


def _source_config_paths(
    spec: dict[str, Any], settings: AdapterSettings
) -> list[tuple[str, Path]]:
    paths: list[tuple[str, Path]] = []
    input_type = spec["inputs"][0]["type"]
    if spec["adapter"] == "workflow1" or input_type == "raw_paired_reads":
        paths.append(("upstream", settings.workflow1_source / "config" / "config.yaml"))
    if spec["adapter"] == "virus_classification":
        paths.append(
            ("classification", settings.workflow2_source / "config" / "config.yaml")
        )
    return paths


def _run_config_paths(state: dict[str, Any]) -> list[tuple[str, Path]]:
    run_dir = Path(str(state.get("run_dir", "")))
    spec = state.get("execution_spec") or {}
    if (run_dir / "workflows").is_dir():
        paths = []
        for phase in ("upstream", "classification"):
            path = run_dir / "workflows" / phase / "config" / "config.yaml"
            if path.is_file():
                paths.append((phase, path))
        return paths
    path = run_dir / "workflow" / "config" / "config.yaml"
    if path.is_file():
        phase = (
            "classification"
            if spec.get("adapter") == "virus_classification"
            else "upstream"
        )
        return [(phase, path)]
    return []


def _input_records(spec: dict[str, Any]) -> list[dict[str, Any]]:
    data = spec["inputs"][0]["data"]
    return data.get("runs") or data.get("samples") or []


def _input_paths(spec: dict[str, Any]) -> list[str]:
    paths: list[str] = []
    for record in _input_records(spec):
        for key in ("read1", "read2", "path"):
            if isinstance(record.get(key), str):
                paths.append(record[key])
    return paths


def _workflow_signature(spec: dict[str, Any]) -> tuple[str, list[str]]:
    rules = list(selected_rules(spec))
    return _stable_hash(rules), rules


def _duration_seconds(started_at: Any, finished_at: Any) -> float | None:
    if not isinstance(started_at, str) or not isinstance(finished_at, str):
        return None
    try:
        return (
            datetime.fromisoformat(finished_at) - datetime.fromisoformat(started_at)
        ).total_seconds()
    except ValueError:
        return None


def _telemetry_input_metrics(
    telemetry: dict[str, Any],
) -> tuple[int | None, int | None]:
    raw = [
        record
        for record in telemetry.get("sequence_metrics") or []
        if record.get("source") == "raw_input"
    ]
    sequences = [record.get("num_seqs") for record in raw]
    bases = [record.get("sum_len") for record in raw]
    sequence_total = sum(
        value for value in sequences if isinstance(value, (int, float))
    )
    base_total = sum(value for value in bases if isinstance(value, (int, float)))
    return int(sequence_total) if sequence_total else None, int(
        base_total
    ) if base_total else None


def _max_benchmark_rss(telemetry: dict[str, Any]) -> float | None:
    values = [
        job.get("max_rss")
        for stage in telemetry.get("stages") or []
        for job in stage.get("jobs") or []
        if isinstance(job.get("max_rss"), (int, float))
    ]
    return max(values) if values else None


def build_run_resource_profile(
    state: dict[str, Any],
    telemetry: dict[str, Any],
    settings: AdapterSettings,
    load_bucket: str,
) -> dict[str, Any]:
    spec = state.get("execution_spec") or {}
    workflow_signature, rules = _workflow_signature(spec)
    database_signature, database_versions = _database_profile(_run_config_paths(state))
    sequences, bases = _telemetry_input_metrics(telemetry)
    records = _input_records(spec)
    input_type = spec["inputs"][0]["type"]
    return {
        "version": 1,
        "workflow_signature": workflow_signature,
        "rules": rules,
        "input_type": input_type,
        "viral_type": spec.get("viral_type") or "",
        "database_signature": database_signature,
        "database_versions": database_versions,
        "sequences": sequences,
        "bases": bases,
        "sample_count": len(records),
        "cpu_cores": settings.cores,
        "load_bucket": load_bucket,
    }


class ResourceCalibrator:
    def __init__(
        self,
        database_path: Path,
        run_root: Path,
        settings: AdapterSettings | None = None,
    ) -> None:
        self.database_path = database_path.resolve()
        self.run_root = run_root.resolve()
        self.settings = settings or AdapterSettings.from_env()
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 30000")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS input_stats (
                    cache_key TEXT PRIMARY KEY,
                    input_kind TEXT NOT NULL,
                    sequences INTEGER NOT NULL,
                    bases INTEGER NOT NULL,
                    file_size INTEGER NOT NULL,
                    mtime_ns INTEGER NOT NULL,
                    updated_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS run_observations (
                    run_id TEXT PRIMARY KEY,
                    telemetry_mtime_ns INTEGER NOT NULL,
                    terminal_status TEXT NOT NULL,
                    telemetry_version INTEGER,
                    workflow_signature TEXT,
                    rules_json TEXT,
                    input_type TEXT,
                    viral_type TEXT,
                    database_signature TEXT,
                    database_versions_json TEXT,
                    sequences INTEGER,
                    bases INTEGER,
                    sample_count INTEGER,
                    input_fingerprint TEXT,
                    cpu_cores INTEGER,
                    load_bucket TEXT,
                    duration_seconds REAL,
                    peak_memory_mb REAL,
                    memory_source TEXT,
                    complete_resource_telemetry INTEGER NOT NULL DEFAULT 0,
                    started_at TEXT,
                    finished_at TEXT,
                    updated_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS run_observation_match_idx
                ON run_observations (
                    terminal_status,
                    workflow_signature,
                    input_type,
                    viral_type,
                    database_signature
                );
                """
            )
            connection.execute(
                "INSERT OR REPLACE INTO metadata(key, value) VALUES('schema_version', ?)",
                (str(SCHEMA_VERSION),),
            )

    def backfill(self) -> None:
        for telemetry_path in sorted(self.run_root.glob("*/.bioagent/telemetry.json")):
            state_path = telemetry_path.with_name("state.json")
            if not state_path.is_file():
                continue
            try:
                telemetry = json.loads(telemetry_path.read_text(encoding="utf-8"))
                state = json.loads(state_path.read_text(encoding="utf-8"))
            except (OSError, UnicodeError, json.JSONDecodeError):
                continue
            run_id = telemetry.get("run_id")
            mtime_ns = telemetry_path.stat().st_mtime_ns
            with self._connect() as connection:
                current = connection.execute(
                    "SELECT telemetry_mtime_ns FROM run_observations WHERE run_id = ?",
                    (run_id,),
                ).fetchone()
            if current and current["telemetry_mtime_ns"] == mtime_ns:
                continue
            self.record_run(state, telemetry, telemetry_mtime_ns=mtime_ns)

    def _cached_file_stats(self, path_value: str) -> tuple[str, int, int]:
        path = Path(path_value).resolve()
        stat = path.stat()
        cache_key = _stable_hash([str(path), stat.st_size, stat.st_mtime_ns])
        with self._connect() as connection:
            row = connection.execute(
                "SELECT input_kind, sequences, bases FROM input_stats WHERE cache_key = ?",
                (cache_key,),
            ).fetchone()
        if row:
            return str(row["input_kind"]), int(row["sequences"]), int(row["bases"])

        result = subprocess.run(
            [self.settings.seqkit, "stats", "-T", "-a", str(path)],
            check=True,
            capture_output=True,
            text=True,
        )
        lines = [line for line in result.stdout.splitlines() if line.strip()]
        if len(lines) != 2:
            raise ValueError(f"seqkit did not return one record for {path}")
        record = dict(zip(lines[0].split("\t"), lines[1].split("\t"), strict=True))
        kind = "contigs" if record.get("format") == "FASTA" else "reads"
        sequences = int(record["num_seqs"])
        bases = int(record["sum_len"])
        with self._connect() as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO input_stats(
                    cache_key, input_kind, sequences, bases, file_size, mtime_ns, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    cache_key,
                    kind,
                    sequences,
                    bases,
                    stat.st_size,
                    stat.st_mtime_ns,
                    time.time(),
                ),
            )
        return kind, sequences, bases

    def _planning_profile(self, spec: dict[str, Any]) -> dict[str, Any]:
        stats = [self._cached_file_stats(path) for path in _input_paths(spec)]
        kinds = {kind for kind, _sequences, _bases in stats}
        if len(kinds) != 1:
            raise ValueError("analysis inputs do not share one sequence format")
        sequences = sum(item[1] for item in stats)
        bases = sum(item[2] for item in stats)
        workflow_signature, rules = _workflow_signature(spec)
        database_signature, database_versions = _database_profile(
            _source_config_paths(spec, self.settings)
        )
        sample_count = len(_input_records(spec))
        input_type = spec["inputs"][0]["type"]
        return {
            "workflow_signature": workflow_signature,
            "rules": rules,
            "input_type": input_type,
            "viral_type": spec.get("viral_type") or "",
            "database_signature": database_signature,
            "database_versions": database_versions,
            "sequences": sequences,
            "bases": bases,
            "sample_count": sample_count,
            "input_fingerprint": _stable_hash(
                [input_type, sample_count, sequences, bases]
            ),
            "cpu_cores": self.settings.cores,
            "load_bucket": _load_bucket(count_active_runners()),
        }

    @staticmethod
    def _resource_floor_mb(rules: list[str]) -> int:
        if any(
            rule in rules
            for rule in ("rna_virusidentification", "dna_virusidentification")
        ):
            return 128 * 1024
        if "megahit" in rules:
            return 24 * 1024
        return 8 * 1024

    @staticmethod
    def _empty_estimate(
        rules: list[str], cpu_cores: int, reason: str
    ) -> dict[str, Any]:
        return {
            "version": 1,
            "status": "insufficient",
            "duration_seconds": {"p50": None, "p90": None, "budget": None},
            "peak_memory_mb": {
                "p50": None,
                "p90": None,
                "reserve": ResourceCalibrator._resource_floor_mb(rules),
            },
            "cpu_cores": cpu_cores,
            "confidence": "low",
            "matched_runs": 0,
            "distinct_inputs": 0,
            "basis": reason,
            "warning": "历史依据不足，暂不提供 P50/P90；仅显示保守硬件下限。",
        }

    def estimate(self, spec: dict[str, Any]) -> dict[str, Any]:
        workflow_signature, rules = _workflow_signature(spec)
        try:
            profile = self._planning_profile(spec)
        except (OSError, subprocess.SubprocessError, ValueError) as exc:
            return self._empty_estimate(
                rules,
                self.settings.cores,
                f"无法读取输入规模：{exc}",
            )

        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM run_observations
                WHERE terminal_status = 'completed'
                  AND workflow_signature = ?
                  AND input_type = ?
                  AND viral_type = ?
                  AND database_signature = ?
                  AND duration_seconds IS NOT NULL
                  AND peak_memory_mb IS NOT NULL
                """,
                (
                    workflow_signature,
                    profile["input_type"],
                    profile["viral_type"],
                    profile["database_signature"],
                ),
            ).fetchall()

        ranked: list[tuple[float, sqlite3.Row]] = []
        for row in rows:
            ratios = []
            for field in ("sequences", "bases"):
                historical = row[field]
                current = profile[field]
                if not historical or not current:
                    ratios = []
                    break
                ratio = current / historical
                if ratio < 0.5 or ratio > 2:
                    ratios = []
                    break
                ratios.append(abs(math.log2(ratio)))
            if ratios:
                ranked.append((max(ratios), row))
        ranked.sort(key=lambda item: (item[0], -float(item[1]["updated_at"])))

        exact_load = [
            item for item in ranked if item[1]["load_bucket"] == profile["load_bucket"]
        ]
        if len(exact_load) >= 5:
            matches = exact_load[:MAX_MATCHES]
        else:
            legacy = [item for item in ranked if item[1]["load_bucket"] == "unknown"]
            matches = sorted(
                [*exact_load, *legacy],
                key=lambda item: (item[0], -float(item[1]["updated_at"])),
            )[:MAX_MATCHES]

        if not matches:
            return self._empty_estimate(
                rules,
                profile["cpu_cores"],
                "没有匹配同流程、同数据库和相近输入规模的历史 run。",
            )

        observations = [item[1] for item in matches]
        durations = sorted(float(row["duration_seconds"]) for row in observations)
        memories = sorted(float(row["peak_memory_mb"]) for row in observations)
        p90_index = max(0, math.ceil(len(observations) * 0.9) - 1)
        distinct_inputs = len({row["input_fingerprint"] for row in observations})
        complete_fraction = sum(
            int(row["complete_resource_telemetry"]) for row in observations
        ) / len(observations)
        relaxed_load = any(row["load_bucket"] == "unknown" for row in observations)
        if (
            len(observations) >= 20
            and distinct_inputs >= 10
            and complete_fraction == 1
            and not relaxed_load
        ):
            confidence, margin = "high", HIGH_MARGIN
        elif (
            len(observations) >= 5
            and distinct_inputs >= 3
            and complete_fraction >= 0.8
            and not relaxed_load
        ):
            confidence, margin = "medium", MEDIUM_MARGIN
        else:
            confidence, margin = "low", LOW_MARGIN

        duration_p50 = statistics.median(durations)
        duration_p90 = durations[p90_index]
        memory_p50 = statistics.median(memories)
        memory_p90 = memories[p90_index]
        memory_reserve = (
            math.ceil(
                max(memory_p90 * (1 + margin), self._resource_floor_mb(rules)) / 1024
            )
            * 1024
        )
        basis = f"匹配 {len(observations)} 个同流程、同数据库及相近输入规模的历史 run"
        if relaxed_load:
            basis += "，其中含负载未知的旧 telemetry"
        warning = (
            "历史样本不足，P90 稳定性有限，已保留 25% 安全余量。"
            if confidence == "low"
            else ""
        )
        return {
            "version": 1,
            "status": "insufficient" if confidence == "low" else "calibrated",
            "duration_seconds": {
                "p50": round(duration_p50),
                "p90": round(duration_p90),
                "budget": math.ceil(duration_p90 * (1 + margin)),
            },
            "peak_memory_mb": {
                "p50": round(memory_p50),
                "p90": round(memory_p90),
                "reserve": memory_reserve,
            },
            "cpu_cores": profile["cpu_cores"],
            "confidence": confidence,
            "matched_runs": len(observations),
            "distinct_inputs": distinct_inputs,
            "basis": basis,
            "warning": warning,
        }

    def record_run(
        self,
        state: dict[str, Any],
        telemetry: dict[str, Any],
        *,
        telemetry_mtime_ns: int = 0,
    ) -> None:
        run_id = telemetry.get("run_id") or state.get("run_id")
        if not isinstance(run_id, str) or not run_id:
            return
        spec = state.get("execution_spec") or (state.get("plan") or {}).get(
            "execution_spec"
        )
        workflow_signature = None
        rules: list[str] = []
        input_type = None
        viral_type = ""
        database_signature = "unknown"
        database_versions: list[str] = []
        sample_count = None
        if isinstance(spec, dict):
            try:
                workflow_signature, rules = _workflow_signature(spec)
                input_type = spec["inputs"][0]["type"]
                viral_type = spec.get("viral_type") or ""
                sample_count = len(_input_records(spec))
                database_signature, database_versions = _database_profile(
                    _run_config_paths(state)
                )
            except (KeyError, TypeError, ValueError):
                workflow_signature = None

        sequences, bases = _telemetry_input_metrics(telemetry)
        input_fingerprint = (
            _stable_hash([input_type, sample_count, sequences, bases])
            if sequences and bases
            else None
        )
        resource_usage = telemetry.get("resource_usage") or {}
        sampled_peak = resource_usage.get("peak_process_tree_rss_mb")
        benchmark_peak = _max_benchmark_rss(telemetry)
        peaks = [
            value
            for value in (sampled_peak, benchmark_peak)
            if isinstance(value, (int, float)) and value > 0
        ]
        peak_memory = max(peaks) if peaks else None
        complete = bool(
            isinstance(sampled_peak, (int, float))
            and sampled_peak > 0
            and resource_usage.get("load_bucket") in {"0", "1", "2+"}
        )
        load_bucket = resource_usage.get("load_bucket") or "unknown"
        duration = _duration_seconds(
            telemetry.get("started_at"), telemetry.get("finished_at")
        )
        with self._connect() as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO run_observations(
                    run_id, telemetry_mtime_ns, terminal_status, telemetry_version,
                    workflow_signature, rules_json, input_type, viral_type,
                    database_signature, database_versions_json, sequences, bases,
                    sample_count, input_fingerprint, cpu_cores, load_bucket,
                    duration_seconds, peak_memory_mb, memory_source,
                    complete_resource_telemetry, started_at, finished_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    telemetry_mtime_ns,
                    str(telemetry.get("status") or "unknown"),
                    telemetry.get("version"),
                    workflow_signature,
                    json.dumps(rules, ensure_ascii=False),
                    input_type,
                    viral_type,
                    database_signature,
                    json.dumps(database_versions, ensure_ascii=False),
                    sequences,
                    bases,
                    sample_count,
                    input_fingerprint,
                    self.settings.cores,
                    load_bucket,
                    duration,
                    peak_memory,
                    "process_tree+benchmark" if complete else "benchmark_max_job",
                    int(complete),
                    telemetry.get("started_at"),
                    telemetry.get("finished_at"),
                    time.time(),
                ),
            )

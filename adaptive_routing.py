"""Deterministic screening metrics and route selection for adaptive analysis."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

ROUTING_MODES = {"auto", "emergency", "standard"}
MIN_KNOWN_VIRUS_READS = 1_000
MIN_SAMPLE_FRACTION = 0.01
MIN_VIRAL_DOMINANCE = 0.80
CONFIDENCE_THRESHOLDS = {
    "top_species_reads": MIN_KNOWN_VIRUS_READS,
    "top_species_sample_fraction": MIN_SAMPLE_FRACTION,
    "top_species_viral_fraction": MIN_VIRAL_DOMINANCE,
}
MAX_CPU_PERCENT = 85.0
SCREEN_THREADS = 4
SCREEN_MEMORY_MB = 4 * 1024
MINIMUM_STORAGE_MB = 1024


def normalize_routing_request(raw: Any) -> dict[str, Any]:
    value = raw if isinstance(raw, dict) else {}
    mode = value.get("mode", "auto")
    if mode not in ROUTING_MODES:
        raise ValueError("routing.mode must be auto, emergency, or standard")
    unsupported = set(value) - {"mode"}
    if unsupported:
        raise ValueError(
            "unsupported routing fields: " + ", ".join(sorted(unsupported))
        )
    return {"mode": mode}


def parse_kraken_report(path: Path) -> dict[str, Any]:
    """Parse one Kraken report and retain species below the Viruses clade."""
    total_reads = 0
    viral_reads = 0
    species: list[dict[str, Any]] = []
    viral_depth: int | None = None
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        columns = line.split("\t")
        if len(columns) < 6:
            continue
        try:
            clade_reads = int(columns[1].strip())
        except ValueError:
            continue
        rank = columns[3].strip()
        taxid = columns[4].strip()
        raw_name = columns[5].rstrip()
        name = raw_name.strip()
        depth = len(raw_name) - len(raw_name.lstrip())
        if rank == "R" and taxid == "1":
            total_reads = max(total_reads, clade_reads)
        if taxid == "10239" or name == "Viruses":
            viral_reads = max(viral_reads, clade_reads)
            viral_depth = depth
            continue
        if viral_depth is not None and depth <= viral_depth:
            viral_depth = None
        if viral_depth is not None and rank == "S":
            species.append({"taxid": taxid, "name": name, "reads": clade_reads})
    return {
        "total_reads": total_reads,
        "viral_reads": viral_reads,
        "species": species,
    }


def screening_metrics(
    report: dict[str, Any], *, sample_id: str, sampled_pairs: int
) -> dict[str, Any]:
    top = max(report.get("species") or [], key=lambda item: item["reads"], default=None)
    top_reads = int((top or {}).get("reads") or 0)
    viral_reads = int(report.get("viral_reads") or 0)
    sample_fraction = top_reads / sampled_pairs if sampled_pairs else 0.0
    viral_fraction = top_reads / viral_reads if viral_reads else 0.0
    checks = [
        {
            "metric": "top_species_reads",
            "actual": top_reads,
            "threshold": MIN_KNOWN_VIRUS_READS,
            "operator": ">=",
            "passed": top_reads >= MIN_KNOWN_VIRUS_READS,
            "failure_reason_code": "TOP_SPECIES_READS_BELOW_MINIMUM",
        },
        {
            "metric": "top_species_sample_fraction",
            "actual": sample_fraction,
            "threshold": MIN_SAMPLE_FRACTION,
            "operator": ">=",
            "passed": sample_fraction >= MIN_SAMPLE_FRACTION,
            "failure_reason_code": "TOP_SPECIES_SAMPLE_FRACTION_BELOW_MINIMUM",
        },
        {
            "metric": "top_species_viral_fraction",
            "actual": viral_fraction,
            "threshold": MIN_VIRAL_DOMINANCE,
            "operator": ">=",
            "passed": viral_fraction >= MIN_VIRAL_DOMINANCE,
            "failure_reason_code": "TOP_SPECIES_VIRAL_FRACTION_BELOW_MINIMUM",
        },
    ]
    return {
        "sample_id": sample_id,
        "sampled_read_pairs": sampled_pairs,
        "viral_reads": viral_reads,
        "top_species": top,
        "top_species_sample_fraction": sample_fraction,
        "top_species_viral_fraction": viral_fraction,
        "confidence_checks": checks,
        "known_virus_high_confidence": bool(top)
        and all(check["passed"] for check in checks),
    }


def aggregate_screening(samples: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "sample_fraction": 0.05,
        "confidence_thresholds": dict(CONFIDENCE_THRESHOLDS),
        "samples": samples,
        "known_virus_high_confidence": bool(samples)
        and all(item.get("known_virus_high_confidence") is True for item in samples),
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


def _cpu_percent(interval_seconds: float = 0.1) -> float | None:
    before = _cpu_times()
    if before is None:
        return None
    time.sleep(interval_seconds)
    after = _cpu_times()
    if after is None or after[0] <= before[0]:
        return None
    total = after[0] - before[0]
    idle = after[1] - before[1]
    return round((total - idle) / total * 100, 2)


def _memory_available_mb() -> float | None:
    try:
        for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) / 1024
    except (OSError, ValueError, IndexError):
        return None
    return None


def _storage_available_mb(run_root: Path) -> float | None:
    try:
        values = os.statvfs(run_root)
    except OSError:
        return None
    return values.f_frsize * values.f_bavail / (1024 * 1024)


def count_allocated_threads(
    run_root: Path, *, exclude_run_id: str | None = None
) -> int:
    allocated = 0
    for state_path in run_root.glob("*/.bioagent/state.json"):
        try:
            state = json.loads(state_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            continue
        if state.get("run_id") == exclude_run_id:
            continue
        execution = state.get("execution") or {}
        if execution.get("status") != "running":
            continue
        estimate = state.get("resource_estimate") or {}
        allocated += int(estimate.get("cpu_cores") or 0)
    return allocated


def collect_resource_snapshot(
    run_root: Path,
    *,
    run_id: str | None = None,
    required_threads: int,
    required_memory_mb: float,
    required_storage_mb: float = MINIMUM_STORAGE_MB,
) -> dict[str, Any]:
    total_cores = os.cpu_count() or 1
    allocated = count_allocated_threads(run_root, exclude_run_id=run_id)
    cpu_percent = _cpu_percent()
    available_memory = _memory_available_mb()
    available_storage = _storage_available_mb(run_root)
    available_threads = max(0, total_cores - allocated)
    reason_codes = []
    if cpu_percent is not None and cpu_percent >= MAX_CPU_PERCENT:
        reason_codes.append("CPU_HIGH")
    if available_threads < required_threads:
        reason_codes.append("THREADS_INSUFFICIENT")
    if available_memory is not None and available_memory < required_memory_mb:
        reason_codes.append("MEMORY_INSUFFICIENT")
    if available_storage is not None and available_storage < required_storage_mb:
        reason_codes.append("STORAGE_INSUFFICIENT")
    return {
        "cpu_total": total_cores,
        "cpu_percent": cpu_percent,
        "memory_available_mb": available_memory,
        "storage_available_mb": available_storage,
        "allocated_threads": allocated,
        "available_threads": available_threads,
        "required_threads": required_threads,
        "required_memory_mb": round(required_memory_mb, 2),
        "required_storage_mb": round(required_storage_mb, 2),
        "admission_ok": not reason_codes,
        "reason_codes": reason_codes,
    }


def decide_route(
    routing: dict[str, Any],
    screening: dict[str, Any],
    *,
    full_resources: dict[str, Any] | None = None,
    history: dict[str, Any] | None = None,
) -> dict[str, Any]:
    mode = routing["mode"]
    high_confidence = bool(screening.get("known_virus_high_confidence"))
    reasons: list[str] = []

    if mode == "standard":
        selected_route = "B"
        reasons.append("USER_FORCED_STANDARD")
    elif mode == "emergency":
        selected_route = "A"
        reasons.append(
            "EMERGENCY_HIGH_CONFIDENCE"
            if high_confidence
            else "EMERGENCY_LOW_CONFIDENCE_REPORT"
        )
    elif high_confidence:
        selected_route = "A"
        reasons.append("KNOWN_VIRUS_HIGH_CONFIDENCE")
    else:
        selected_route = "B"
        reasons.append("KNOWN_VIRUS_CONFIDENCE_INSUFFICIENT")
        reasons.extend(
            str(check["failure_reason_code"])
            for sample in screening.get("samples") or []
            for check in sample.get("confidence_checks") or []
            if check.get("passed") is False and check.get("failure_reason_code")
        )

    blocked = (
        mode == "auto"
        and full_resources is not None
        and not full_resources.get("admission_ok", True)
    )
    if blocked:
        reasons.extend(full_resources.get("reason_codes") or [])

    return {
        "phase": "blocked" if blocked else "selected",
        "selected_route": selected_route,
        "confidence": "high" if high_confidence else "low",
        "reason_codes": list(dict.fromkeys(reasons)),
        "report_kind": "preliminary_alert" if high_confidence else "preliminary_screen",
    }

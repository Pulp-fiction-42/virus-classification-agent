"""Deterministic Snakemake execution and telemetry collection for BioAgent."""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import shlex
import signal
import sqlite3
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from adaptive_routing import (
    SCREEN_MEMORY_MB,
    SCREEN_THREADS,
    collect_resource_snapshot,
    decide_route,
)
from adaptive_screening import run_rapid_screen
from resource_calibration import (
    ProcessResourceMonitor,
    ResourceCalibrator,
    build_run_resource_profile,
)
from workflow_adapters import (
    AdapterSettings,
    PreparedExecution,
    PreparedPhase,
    WORKFLOW1_RULES,
    full_analysis_spec,
    prepare_execution,
    selected_rules,
)


FULL_ANALYSIS_MINIMUM_MEMORY_MB = 24 * 1024
FULL_ANALYSIS_MINIMUM_THREADS = 8


RULE_START_RE = re.compile(r"^(?:local)?rule\s+([A-Za-z0-9_]+):$")
FINISHED_RE = re.compile(r"Finished jobid:\s*(\d+)\s+\(Rule:\s*([A-Za-z0-9_]+)\)")
TIMESTAMP_RE = re.compile(r"^\[([A-Z][a-z]{2} .+ \d{4})\]$")
LOG_TIMESTAMP_FORMAT = "%a %b %d %H:%M:%S %Y"
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


def _parse_timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        parsed = datetime.strptime(value, LOG_TIMESTAMP_FORMAT)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime.now().astimezone().tzinfo)
    return parsed


def _canonical_timestamp(value: str) -> str:
    return _parse_timestamp(value).isoformat(timespec="seconds")


def _time_bounds(values: list[str]) -> tuple[str | None, str | None]:
    if not values:
        return None, None
    parsed = [_parse_timestamp(value) for value in values]
    return (
        min(parsed).isoformat(timespec="seconds"),
        max(parsed).isoformat(timespec="seconds"),
    )


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
    if state.get("version") != 3:
        raise ValueError("analysis runner requires state version 3")
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
        if (
            current_rule == "bowtie2"
            and "{params.sw}" in line
            and "-x {params.dbs}" in line
            and "bowtie2-summary.txt" not in line
        ):
            line = line.replace(
                "|\\",
                "2> {params.wd}/bowtie2-summary.txt |\\",
            )
        output.append(line)

    rules_path.write_text("\n".join(output) + "\n", encoding="utf-8")


def _instrument_workflow2(workflow_dir: Path, benchmark_dir: Path) -> None:
    wildcard_rules = {"rename_contigs": "part", "get_orfs": "part"}
    for rules_path in sorted((workflow_dir / "rules").glob("*.smk")):
        lines = rules_path.read_text(encoding="utf-8").splitlines()
        output: list[str] = []
        current_rule: str | None = None
        current_has_benchmark = False
        for line in lines:
            if match := re.match(r"^rule\s+([A-Za-z0-9_]+):", line):
                current_rule = match.group(1)
                current_has_benchmark = False
            elif current_rule and line.lstrip().startswith("benchmark:"):
                current_has_benchmark = True
            if (
                current_rule
                and line.lstrip().startswith("shell:")
                and not current_has_benchmark
            ):
                wildcard = wildcard_rules.get(current_rule)
                suffix = f".{{{wildcard}}}" if wildcard else ""
                output.extend(
                    [
                        f"{line[: len(line) - len(line.lstrip())]}benchmark:",
                        (
                            f'{line[: len(line) - len(line.lstrip())]}    "'
                            f"{benchmark_dir / f'{current_rule}{suffix}.jsonl'}"
                            '"'
                        ),
                    ]
                )
                current_has_benchmark = True
            output.append(line)
        rules_path.write_text("\n".join(output) + "\n", encoding="utf-8")

    iterative_path = workflow_dir / "scripts" / "iterativehmmsearch.py"
    iterative_text = iterative_path.read_text(encoding="utf-8")
    helper_marker = "\ndef iterative_hmmsearch(query, wdir, ncpu, iteration_num, evalue, motifcov):\n"
    helper = """
def finish_empty_search(wdir, current_iteration, iteration_num):
    for iteration in range(current_iteration + 1, iteration_num + 1):
        iteration_dir = os.path.join(wdir, 'Iteration_%d' % iteration)
        os.makedirs(iteration_dir, exist_ok=True)
        open(os.path.join(iteration_dir, 'hmmsearch.domtblout'), 'a').close()


def iterative_hmmsearch(query, wdir, ncpu, iteration_num, evalue, motifcov):
"""
    if helper_marker not in iterative_text:
        raise ValueError("workflow2 iterative search could not be instrumented")
    iterative_text = iterative_text.replace(helper_marker, "\n" + helper, 1)

    hit_marker = (
        "        sys.stderr.write(curtime()+'[INFO] hmmsearch done, %s sequences "
        "hit (Time Elapsed: %.3fs)\\n' % (num_hitseq, _t3-_t2))\n"
    )
    hit_exit = (
        hit_marker
        + """
        if int(num_hitseq) == 0:
            finish_empty_search(wdir, i, iteration_num)
            sys.stderr.write(curtime()+'[INFO] No sequences hit; search finished with an empty result\\n')
            return
"""
    )
    if hit_marker not in iterative_text:
        raise ValueError("workflow2 empty HMM result could not be instrumented")
    iterative_text = iterative_text.replace(hit_marker, hit_exit, 1)

    filtered_marker = (
        '        sys.stderr.write(curtime()+"[INFO] %d sequences covered >%d%% '
        'RdRp motif region (Time Elapsed: %.3fs)\\n" % '
        "(num_filtered, int(100*motifcov), _t5-_t4))\n"
    )
    filtered_exit = (
        filtered_marker
        + """
        if num_filtered == 0:
            finish_empty_search(wdir, i, iteration_num)
            sys.stderr.write(curtime()+'[INFO] No sequences passed motif coverage; search finished with an empty result\\n')
            return
"""
    )
    if filtered_marker not in iterative_text:
        raise ValueError("workflow2 empty motif result could not be instrumented")
    iterative_text = iterative_text.replace(filtered_marker, filtered_exit, 1)
    iterative_path.write_text(iterative_text, encoding="utf-8")

    script_path = workflow_dir / "scripts" / "table_summarize.py"
    script_path.write_text(
        script_path.read_text(encoding="utf-8")
        + """

truepos_viruses.to_csv('summary/tables/virus_candidates.csv', index=False)
truepos_viruses.sort_values('qlen', ascending=False).drop_duplicates(
    'clusid', keep='first'
).to_csv('summary/tables/virus_representatives.csv', index=False)
truepos_vert_viruses.to_csv(
    'summary/tables/vertebrate_virus_candidates.csv', index=False
)
truepos_vert_viruses_rep.to_csv(
    'summary/tables/vertebrate_virus_representatives.csv', index=False
)
""",
        encoding="utf-8",
    )

    summarize_path = workflow_dir / "rules" / "summarize.smk"
    summarize_text = summarize_path.read_text(encoding="utf-8")
    marker = "\t\tvertvirus_seqid = os.path.join("
    addition = (
        "\t\tvirus_candidates_csv = os.path.join(config['wdir'], "
        "'summary/tables/virus_candidates.csv'),\n"
        "\t\tvirus_representatives_csv = os.path.join(config['wdir'], "
        "'summary/tables/virus_representatives.csv'),\n"
        "\t\tvertebrate_candidates_csv = os.path.join(config['wdir'], "
        "'summary/tables/vertebrate_virus_candidates.csv'),\n"
        "\t\tvertebrate_representatives_csv = os.path.join(config['wdir'], "
        "'summary/tables/vertebrate_virus_representatives.csv'),\n"
    )
    index = summarize_text.find(marker)
    if index < 0:
        raise ValueError("workflow2 summarize outputs could not be instrumented")
    summarize_text = summarize_text[:index] + addition + summarize_text[index:]
    summarize_path.write_text(summarize_text, encoding="utf-8")


def _sample_paths(workflow_dir: Path) -> list[Path]:
    paths: list[Path] = []
    for samples_path in (
        workflow_dir / "data" / "samples.txt",
        workflow_dir / "data" / "contigs_list.txt",
    ):
        if not samples_path.is_file():
            continue
        for raw_line in samples_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            fields = line.split()
            values = fields[-2:] if samples_path.name == "samples.txt" else fields[-1:]
            for value in values:
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


def _parse_benchmarks(
    benchmark_dir: Path, phase: str | None = None
) -> list[dict[str, Any]]:
    def normalize(value: Any) -> Any:
        if value == "NA":
            return None
        if isinstance(value, dict):
            return {key: normalize(item) for key, item in value.items()}
        if isinstance(value, list):
            return [normalize(item) for item in value]
        return value

    jobs = []
    for path in sorted(benchmark_dir.glob("*.jsonl")):
        try:
            for line in path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                record = redact_value(normalize(json.loads(line)))
                if not isinstance(record, dict):
                    continue
                record["benchmark_file"] = str(path)
                if phase:
                    record["phase"] = phase
                jobs.append(record)
        except (OSError, UnicodeError, json.JSONDecodeError):
            continue
    return jobs


def _parse_log(log_path: Path, phase: str | None = None) -> dict[str, Any]:
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
            current_timestamp = _canonical_timestamp(timestamp_match.group(1))
            continue
        if match := RULE_START_RE.match(line):
            pending_rule = match.group(1)
            pending_job = {
                "rule": pending_rule,
                "started_at": current_timestamp,
                "status": "running",
                **({"phase": phase} if phase else {}),
            }
            jobs.append(pending_job)
            continue
        if pending_job and (match := re.match(r"jobid:\s*(\d+)", line)):
            pending_job["job_id"] = match.group(1)
            jobs_by_id[match.group(1)] = pending_job
            continue
        if pending_job and (match := re.match(r"(input|output):\s*(.*)", line)):
            pending_job[f"{match.group(1)}s"] = [
                redact_text(value.strip())
                for value in match.group(2).split(",")
                if value.strip()
            ]
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
                    "diamond",
                    "mmseqs",
                    "blastn",
                    "blastx",
                    "checkv",
                    "hmmsearch",
                    "getorf",
                    "python",
                    "gunzip",
                    "sed ",
                )
            )
            and (
                "--" in line
                or "|" in line
                or line.startswith(("/", "cat ", "sed ", "cd ", "export "))
            )
        ):
            command = redact_text(line)
            if command not in commands:
                commands.append(command)
            if pending_job:
                pending_job.setdefault("commands", [])
                if command not in pending_job["commands"]:
                    pending_job["commands"].append(command)

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


def _combine_sequence_records(records: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not records:
        return None

    read_values = [
        float(record["num_seqs"])
        for record in records
        if isinstance(record.get("num_seqs"), (int, float))
    ]
    base_values = [
        float(record["sum_len"])
        for record in records
        if isinstance(record.get("sum_len"), (int, float))
    ]
    reads = sum(read_values)
    bases = sum(base_values)

    combined: dict[str, Any] = {
        "files": [record.get("file") for record in records if record.get("file")],
    }
    if read_values:
        combined["reads"] = int(reads) if reads.is_integer() else reads
    if base_values:
        combined["bases"] = int(bases) if bases.is_integer() else bases
    if read_values and base_values and reads:
        combined["mean_length"] = bases / reads

    for key in ("q20_percent", "q30_percent", "gc_percent", "avg_quality"):
        weighted = [
            (float(record[key]), float(record["sum_len"]))
            for record in records
            if isinstance(record.get(key), (int, float))
            and isinstance(record.get("sum_len"), (int, float))
        ]
        weight = sum(item[1] for item in weighted)
        if weight:
            combined[key] = (
                sum(value * item_weight for value, item_weight in weighted) / weight
            )

    for key, function in (("min_length", min), ("max_length", max)):
        source_key = "min_len" if key == "min_length" else "max_len"
        values = [
            float(record[source_key])
            for record in records
            if isinstance(record.get(source_key), (int, float))
        ]
        if values:
            combined[key] = function(values)
    return combined


def _combine_fastp_reports(reports: list[dict[str, Any]]) -> dict[str, Any] | None:
    summaries = [
        report.get("summary")
        for report in reports
        if isinstance(report.get("summary"), dict)
    ]
    if not summaries:
        return None

    def combine_side(side: str) -> dict[str, Any]:
        records = [
            summary.get(side)
            for summary in summaries
            if isinstance(summary.get(side), dict)
        ]
        read_values = [
            float(record["total_reads"])
            for record in records
            if isinstance(record.get("total_reads"), (int, float))
        ]
        base_values = [
            float(record["total_bases"])
            for record in records
            if isinstance(record.get("total_bases"), (int, float))
        ]
        reads = sum(read_values)
        bases = sum(base_values)
        combined: dict[str, Any] = {}
        if read_values:
            combined["reads"] = int(reads) if reads.is_integer() else reads
        if base_values:
            combined["bases"] = int(bases) if bases.is_integer() else bases
        if read_values and base_values and reads:
            combined["mean_length"] = bases / reads
        for rate_key, count_key in (
            ("q20_percent", "q20_bases"),
            ("q30_percent", "q30_bases"),
        ):
            has_count = any(
                isinstance(record.get(count_key), (int, float)) for record in records
            )
            count = sum(
                float(record[count_key])
                for record in records
                if isinstance(record.get(count_key), (int, float))
            )
            if bases and has_count:
                combined[rate_key] = count / bases * 100
            else:
                weighted = [
                    (
                        float(record[rate_key.removesuffix("_percent") + "_rate"]),
                        float(record["total_bases"]),
                    )
                    for record in records
                    if isinstance(
                        record.get(rate_key.removesuffix("_percent") + "_rate"),
                        (int, float),
                    )
                    and isinstance(record.get("total_bases"), (int, float))
                ]
                weight = sum(item[1] for item in weighted)
                if weight:
                    combined[rate_key] = (
                        sum(rate * item_weight for rate, item_weight in weighted)
                        / weight
                        * 100
                    )
        weighted_gc = [
            (float(record["gc_content"]), float(record["total_bases"]))
            for record in records
            if isinstance(record.get("gc_content"), (int, float))
            and isinstance(record.get("total_bases"), (int, float))
        ]
        gc_weight = sum(item[1] for item in weighted_gc)
        if gc_weight:
            combined["gc_percent"] = (
                sum(value * item_weight for value, item_weight in weighted_gc)
                / gc_weight
                * 100
            )
        return combined

    before = combine_side("before_filtering")
    after = combine_side("after_filtering")
    filtering: dict[str, int | float] = {}
    for report in reports:
        values = report.get("filtering_result")
        if not isinstance(values, dict):
            continue
        for key, value in values.items():
            if isinstance(value, (int, float)):
                filtering[key] = filtering.get(key, 0) + value

    duplication_values = []
    for report, summary in zip(reports, summaries, strict=False):
        duplication = report.get("duplication")
        before_filtering = summary.get("before_filtering")
        if (
            isinstance(duplication, dict)
            and isinstance(duplication.get("rate"), (int, float))
            and isinstance(before_filtering, dict)
            and isinstance(before_filtering.get("total_reads"), (int, float))
        ):
            duplication_values.append(
                (
                    float(duplication["rate"]),
                    float(before_filtering["total_reads"]),
                )
            )
    duplication_weight = sum(item[1] for item in duplication_values)
    result: dict[str, Any] = {
        "before": before,
        "after": after,
        "filtering": filtering,
        "reports": [report.get("path") for report in reports if report.get("path")],
    }
    if before.get("reads"):
        result["reads_retained_percent"] = (
            float(after.get("reads") or 0) / float(before["reads"]) * 100
        )
    if before.get("bases"):
        result["bases_retained_percent"] = (
            float(after.get("bases") or 0) / float(before["bases"]) * 100
        )
    if duplication_weight:
        result["duplication_percent"] = (
            sum(rate * weight for rate, weight in duplication_values)
            / duplication_weight
            * 100
        )
    return result


def _bowtie_alignment_by_run(run_dir: Path) -> dict[str, float]:
    values: dict[str, float] = {}
    for path in run_dir.glob("results/**/filter/*/bowtie2-summary.txt"):
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        matches = re.findall(r"([0-9.]+)% overall alignment rate", text)
        if matches:
            values[path.parent.name] = float(matches[-1])
    return values


def _workflow1_report_data(
    state: dict[str, Any],
    run_dir: Path,
    sequence_metrics: list[dict[str, Any]],
    fastp_reports: list[dict[str, Any]],
    rrna_alignment_percents: list[float] | None = None,
) -> dict[str, Any]:
    spec = state.get("execution_spec") or {}
    if (
        spec.get("adapter") == "virus_classification"
        and ((spec.get("inputs") or [{}])[0]).get("type") == "raw_paired_reads"
    ):
        spec = {
            "adapter": "workflow1",
            "entry_rule": "fastp",
            "exit_rule": "rename_filter",
            "inputs": spec.get("inputs"),
        }
    entry_rule = spec.get("entry_rule")
    exit_rule = spec.get("exit_rule")
    upstream_results = run_dir / "results" / "upstream"
    selected = list(WORKFLOW1_RULES)[
        WORKFLOW1_RULES.index(entry_rule) : WORKFLOW1_RULES.index(exit_rule) + 1
    ]
    data = (spec.get("inputs") or [{}])[0].get("data") or {}
    records = data.get("runs") or data.get("samples") or []
    records = [record for record in records if isinstance(record, dict)]
    step_status = {
        rule: step.get("status", "pending")
        for step in state.get("steps", [])
        if isinstance(step, dict)
        for rule in step.get("rules") or []
    }
    provided_rule = {
        "bowtie2": "fastp",
        "merge": "bowtie2",
        "megahit": "merge",
        "rename_filter": "megahit",
    }.get(entry_rule)
    stages = []
    for rule in WORKFLOW1_RULES:
        if rule in selected:
            status = step_status.get(rule, "pending")
        elif rule == provided_rule:
            status = "provided"
        else:
            status = "not_selected"
        stages.append({"rule": rule, "status": status})

    metrics_by_file = {
        str(Path(str(record.get("file"))).resolve()): record
        for record in sequence_metrics
        if record.get("file")
    }

    def metric(path: str | Path) -> dict[str, Any] | None:
        return metrics_by_file.get(str(Path(path).resolve()))

    fastp_by_run: dict[str, list[dict[str, Any]]] = {}
    for report in fastp_reports:
        path = report.get("path")
        if path:
            fastp_by_run.setdefault(Path(str(path)).parent.name, []).append(report)
    alignment_by_run = _bowtie_alignment_by_run(run_dir)
    all_run_ids = [
        str(record.get("run_id")) for record in records if record.get("run_id")
    ]
    if rrna_alignment_percents and len(rrna_alignment_percents) == len(all_run_ids):
        for run_id, value in zip(
            all_run_ids,
            rrna_alignment_percents,
            strict=True,
        ):
            alignment_by_run.setdefault(run_id, float(value))

    sample_records: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        sample_id = record.get("sample_id")
        if isinstance(sample_id, str):
            sample_records.setdefault(sample_id, []).append(record)

    samples = []
    for sample_id, inputs in sample_records.items():
        run_ids = [str(record.get("run_id") or sample_id) for record in inputs]
        input_metrics = [
            value
            for record in inputs
            for path_key in ("read1", "read2", "path")
            if record.get(path_key)
            if (value := metric(record[path_key]))
        ]
        sample: dict[str, Any] = {
            "sample_id": sample_id,
            "run_ids": run_ids,
            "input": {
                "type": ((spec.get("inputs") or [{}])[0]).get("type"),
                "stage": {
                    "fastp": "raw",
                    "bowtie2": "post_fastp",
                    "merge": "post_rrna",
                    "megahit": "merged_reads",
                    "rename_filter": "assembly",
                }.get(entry_rule),
                "metrics": _combine_sequence_records(input_metrics),
            },
        }

        if "fastp" in selected:
            sample["fastp"] = _combine_fastp_reports(
                [
                    report
                    for run_id in run_ids
                    for report in fastp_by_run.get(run_id, [])
                ]
            )

        if "bowtie2" in selected:
            before_by_run: dict[str, list[dict[str, Any]]] = {}
            for run_id in run_ids:
                run_records = [
                    value
                    for mate in (1, 2)
                    if (
                        value := metric(
                            upstream_results
                            / "filter"
                            / run_id
                            / f"01_fastp_r{mate}.fq.gz"
                        )
                    )
                ]
                if entry_rule == "bowtie2" and not run_records:
                    run_records = [
                        value
                        for record in inputs
                        if str(record.get("run_id") or sample_id) == run_id
                        for path_key in ("read1", "read2", "path")
                        if record.get(path_key)
                        if (value := metric(record[path_key]))
                    ]
                before_by_run[run_id] = run_records
            before_records = [
                value
                for records_for_run in before_by_run.values()
                for value in records_for_run
            ]
            after_records = [
                value
                for run_id in run_ids
                for mate in (1, 2)
                if (
                    value := metric(
                        upstream_results
                        / "filter"
                        / run_id
                        / f"cleanreads_r{mate}.fq.gz"
                    )
                )
            ]
            before = _combine_sequence_records(before_records)
            after = _combine_sequence_records(after_records)
            rrna: dict[str, Any] = {"before": before, "after": after}
            if before and before.get("reads"):
                rrna["reads_retained_percent"] = (
                    float((after or {}).get("reads") or 0)
                    / float(before["reads"])
                    * 100
                )
            if before and before.get("bases"):
                rrna["bases_retained_percent"] = (
                    float((after or {}).get("bases") or 0)
                    / float(before["bases"])
                    * 100
                )
            alignments = []
            for run_id in run_ids:
                if run_id not in alignment_by_run:
                    continue
                alignment: dict[str, Any] = {
                    "run_id": run_id,
                    "alignment_percent": alignment_by_run[run_id],
                }
                run_before = _combine_sequence_records(before_by_run[run_id])
                if run_before and isinstance(run_before.get("reads"), (int, float)):
                    alignment["input_reads"] = run_before["reads"]
                alignments.append(alignment)
            if alignments:
                rrna["alignments"] = alignments
                weighted = [
                    (item["alignment_percent"], item["input_reads"])
                    for item in alignments
                    if isinstance(item.get("input_reads"), (int, float))
                    and item["input_reads"] > 0
                ]
                if weighted:
                    rrna["alignment_percent"] = sum(
                        value * weight for value, weight in weighted
                    ) / sum(weight for _, weight in weighted)
                elif len(alignments) == 1:
                    rrna["alignment_percent"] = alignments[0]["alignment_percent"]
            sample["rrna"] = rrna

        if "merge" in selected:
            sample["merge"] = _combine_sequence_records(
                [
                    value
                    for mate in (1, 2)
                    if (
                        value := metric(
                            upstream_results
                            / "merge"
                            / sample_id
                            / f"cleanreads_r{mate}.fq"
                        )
                    )
                ]
            )

        if "megahit" in selected:
            sample["assembly"] = _combine_sequence_records(
                [
                    value
                    for path in (
                        upstream_results
                        / "assembly"
                        / sample_id
                        / f"{sample_id}.contigs.fa",
                    )
                    if (value := metric(path))
                ]
            )
            assembly_record = metric(
                upstream_results / "assembly" / sample_id / f"{sample_id}.contigs.fa"
            )
            if sample.get("assembly") and assembly_record:
                for key in ("n50", "q1", "median", "q3"):
                    if assembly_record.get(key) is not None:
                        sample["assembly"][key] = assembly_record[key]

        if "rename_filter" in selected:
            final_records = [
                record
                for record in sequence_metrics
                if Path(str(record.get("file", ""))).name
                == f"{sample_id}.contigs.1kb.renamed.fa"
            ]
            sample["final_filter"] = _combine_sequence_records(final_records)
            final_record = final_records[0] if final_records else None
            if sample.get("final_filter") and final_record:
                for key in ("n50", "q1", "median", "q3"):
                    if final_record.get(key) is not None:
                        sample["final_filter"][key] = final_record[key]

        samples.append(sample)

    return {
        "schema_version": 1,
        "workflow": "workflow1",
        "entry_rule": entry_rule,
        "exit_rule": exit_rule,
        "stages": stages,
        "samples": samples,
    }


CLASSIFICATION_FIELDS = (
    "qseqid",
    "qlen",
    "viral_type",
    "checkv_quality",
    "miuvig_quality",
    "completeness",
    "contamination",
    "clusid",
    "rdrpscan_svorder",
    "nr_svorder",
    "nr_svfamily",
    "nr_svgenus",
    "nr_sscinames",
    "nr_besthit",
    "nr_pident",
    "nr_vote_vfamily",
    "approval_rate",
    "vhdb_svorder",
    "vhdb_svfamily",
    "vhdb_svname",
)


def _csv_value(value: str) -> Any:
    normalized = value.strip()
    if not normalized or normalized.lower() in {"nan", "none"} or normalized == "-":
        return None
    if normalized.lower() in {"true", "false"}:
        return normalized.lower() == "true"
    try:
        number = float(normalized)
    except ValueError:
        return normalized
    return int(number) if number.is_integer() else number


def _read_csv_records(path: Path) -> list[dict[str, Any]]:
    try:
        with path.open(encoding="utf-8", newline="") as stream:
            return [
                {key: _csv_value(value) for key, value in row.items() if key}
                for row in csv.DictReader(stream)
            ]
    except (OSError, UnicodeError, csv.Error):
        return []


def _classification_report_data(
    state: dict[str, Any],
    run_dir: Path,
) -> dict[str, Any]:
    spec = state.get("execution_spec") or {}
    input_data = (spec.get("inputs") or [{}])[0]
    records = (
        (input_data.get("data") or {}).get("runs")
        or (input_data.get("data") or {}).get("samples")
        or []
    )
    sample_ids = list(
        dict.fromkeys(
            str(record["sample_id"])
            for record in records
            if isinstance(record, dict) and record.get("sample_id")
        )
    )
    sample_prefixes = sorted(sample_ids, key=len, reverse=True)
    tables_dir = run_dir / "results" / "classification" / "summary" / "tables"
    candidates = _read_csv_records(tables_dir / "virus_candidates.csv")
    representatives = _read_csv_records(tables_dir / "virus_representatives.csv")
    vertebrate_candidates = _read_csv_records(
        tables_dir / "vertebrate_virus_candidates.csv"
    )
    vertebrate_representatives = _read_csv_records(
        tables_dir / "vertebrate_virus_representatives.csv"
    )

    normalized_representatives = []
    for record in representatives:
        qseqid = str(record.get("qseqid") or "")
        sample_id = next(
            (
                sample
                for sample in sample_prefixes
                if qseqid == sample or qseqid.startswith(f"{sample}-")
            ),
            None,
        )
        normalized_representatives.append(
            {
                **({"sample_id": sample_id} if sample_id else {}),
                **{
                    field: record.get(field)
                    for field in CLASSIFICATION_FIELDS
                    if record.get(field) is not None
                },
            }
        )

    return {
        "schema_version": 1,
        "viral_type": spec.get("viral_type"),
        "input_type": input_data.get("type"),
        "sample_ids": sample_ids,
        "candidate_count": len(candidates),
        "representative_count": len(representatives),
        "vertebrate_candidate_count": len(vertebrate_candidates),
        "vertebrate_representative_count": len(vertebrate_representatives),
        "representatives": normalized_representatives,
        "tables": {
            "candidates": str(tables_dir / "virus_candidates.csv"),
            "representatives": str(tables_dir / "virus_representatives.csv"),
            "vertebrate_candidates": str(
                tables_dir / "vertebrate_virus_candidates.csv"
            ),
            "vertebrate_representatives": str(
                tables_dir / "vertebrate_virus_representatives.csv"
            ),
        },
        "artifacts": {
            "完整证据表（XLSX）": str(tables_dir / "fullinfo.xlsx"),
            "病毒候选表（XLSX）": str(tables_dir / "viruses.xlsx"),
            "脊椎动物病毒表（XLSX）": str(tables_dir / "vertebrate_viruses.xlsx"),
            "病毒候选序列（FASTA）": str(
                run_dir
                / "results"
                / "classification"
                / "downstream"
                / "sequences"
                / "putative_viralcontigs"
                / "putative_viralcontigs.fna"
            ),
            "脊椎动物病毒序列（FASTA）": str(
                run_dir
                / "results"
                / "classification"
                / "summary"
                / "sequences"
                / "vert_viruses.fna"
            ),
            "脊椎动物代表序列（FASTA）": str(
                run_dir
                / "results"
                / "classification"
                / "summary"
                / "sequences"
                / "rep_vert_viruses.fna"
            ),
        },
    }


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
            or path.relative_to(run_dir).parts[0] == "workflows"
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
    logged = {
        (job.get("phase"), job.get("job_id"), job.get("rule")): job for job in log_jobs
    }
    stages = []
    for step in state.get("steps", []):
        rules = set(step.get("rules") or [])
        benchmark_jobs = [job for job in jobs if job.get("rule_name") in rules]
        stage_log_jobs = [job for job in log_jobs if job.get("rule") in rules]
        cpu_time = sum(float(job.get("cpu_time") or 0) for job in benchmark_jobs)
        wall_seconds = sum(float(job.get("s") or 0) for job in benchmark_jobs)
        for job in benchmark_jobs:
            log_job = logged.get(
                (
                    job.get("phase"),
                    str(job.get("jobid")),
                    job.get("rule_name"),
                )
            )
            if log_job:
                job.update(
                    {
                        "started_at": log_job.get("started_at"),
                        "finished_at": log_job.get("finished_at"),
                        "status": log_job.get("status"),
                        "inputs": log_job.get("inputs", []),
                        "outputs": log_job.get("outputs", []),
                        "commands": log_job.get("commands", []),
                    }
                )
        benchmark_keys = {
            (job.get("phase"), str(job.get("jobid")), job.get("rule_name"))
            for job in benchmark_jobs
        }
        combined_jobs = [
            *benchmark_jobs,
            *[
                {
                    "jobid": job.get("job_id"),
                    "rule_name": job.get("rule"),
                    "phase": job.get("phase"),
                    "started_at": job.get("started_at"),
                    "finished_at": job.get("finished_at"),
                    "status": job.get("status"),
                    "inputs": job.get("inputs", []),
                    "outputs": job.get("outputs", []),
                    "commands": job.get("commands", []),
                }
                for job in stage_log_jobs
                if (
                    job.get("phase"),
                    str(job.get("job_id")),
                    job.get("rule"),
                )
                not in benchmark_keys
            ],
        ]
        starts = [job["started_at"] for job in combined_jobs if job.get("started_at")]
        finishes = [
            job["finished_at"] for job in combined_jobs if job.get("finished_at")
        ]
        started_at, _ = _time_bounds(starts)
        _, finished_at = _time_bounds(finishes)
        elapsed_seconds = (
            max(
                0.0,
                (
                    _parse_timestamp(finished_at) - _parse_timestamp(started_at)
                ).total_seconds(),
            )
            if started_at and finished_at
            else None
        )
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
                "wall_seconds": elapsed_seconds,
                "job_wall_seconds": wall_seconds,
                "cpu_time_seconds": cpu_time,
                "effective_cores": (
                    cpu_time / elapsed_seconds if elapsed_seconds else None
                ),
                "max_job_rss_mb": max(
                    (
                        float(job["max_rss"])
                        for job in benchmark_jobs
                        if isinstance(job.get("max_rss"), (int, float))
                    ),
                    default=None,
                ),
                "started_at": started_at,
                "finished_at": finished_at,
                "jobs": combined_jobs,
            }
        )
    return stages


def _set_step_status(state: dict[str, Any], rule: str, status: str) -> None:
    for step in state.get("steps", []):
        if rule in (step.get("rules") or []):
            step["status"] = status


def _finish_routing_record(
    routing: dict[str, Any], *, phase: str, success: bool
) -> None:
    finished_at = _now()
    try:
        duration = max(
            0.0,
            (
                datetime.fromisoformat(finished_at)
                - datetime.fromisoformat(str(routing["started_at"]))
            ).total_seconds(),
        )
    except (KeyError, TypeError, ValueError):
        duration = None
    deadline = (routing.get("request") or {}).get("deadline_seconds")
    routing.update(
        {
            "phase": phase,
            "finished_at": finished_at,
            "actual_duration_seconds": duration,
            "deadline_met": (
                duration <= deadline
                if isinstance(duration, (int, float))
                and isinstance(deadline, (int, float))
                else None
            ),
            "success": success,
        }
    )


def _terminal_adaptive_state(
    state_path: Path,
    state: dict[str, Any],
    telemetry: dict[str, Any],
    *,
    status: str,
    returncode: int,
    error: str | None = None,
    report_available: bool = False,
) -> int:
    finished_at = _now()
    state["execution"].update(
        {
            "status": status,
            "ok": returncode == 0,
            "returncode": returncode,
            "current_step_id": None,
            "finished_at": finished_at,
            **({"error": error} if error else {}),
        }
    )
    state["technical_report"] = {
        **(state.get("technical_report") or {}),
        "status": "ready",
        "available": True,
        "telemetry": str(state_path.parent / "telemetry.json"),
        "telemetry_version": 3,
    }
    state["report"] = {
        "status": "ready" if report_available else "unavailable",
        "available": report_available,
        "schema_version": 2,
        "telemetry": str(state_path.parent / "telemetry.json"),
    }
    telemetry.update(
        {
            "finished_at": finished_at,
            "status": status,
            "returncode": returncode,
            **({"error": error} if error else {}),
        }
    )
    _write_json_atomic(state_path.parent / "telemetry.json", telemetry)
    _write_json_atomic(state_path, state)
    return returncode


def _run_adaptive(args: argparse.Namespace, state: dict[str, Any]) -> int:
    state_path = Path(args.state).resolve()
    run_dir = Path(state["run_dir"]).resolve()
    bioagent_dir = run_dir / ".bioagent"
    routing_path = bioagent_dir / "routing.json"
    log_path = Path(state["execution"]["log"]).resolve()
    spec = state["execution_spec"]
    routing_request = spec["routing"]
    full_spec = full_analysis_spec(spec)
    calibrator = ResourceCalibrator(
        Path(
            state.get("resource_history_db")
            or bioagent_dir / "resource-history.sqlite3"
        ),
        run_dir.parent,
    )
    history = calibrator.history_summary(full_spec)
    started_at = _now()
    routing_record: dict[str, Any] = {
        "version": 2,
        "run_id": state.get("run_id"),
        "request": routing_request,
        "phase": "screening_admission",
        "started_at": started_at,
        "screening": None,
        "decision": None,
        "resources": {},
        "history": history,
        "accuracy": None,
        "expert_feedback": None,
    }
    telemetry: dict[str, Any] = {
        "version": 3,
        "run_id": state.get("run_id"),
        "chat_id": state.get("openwebui_chat_id"),
        "workflow_id": "adaptive_virus_analysis",
        "started_at": started_at,
        "routing": routing_record,
        "stages": [],
        "commands": [],
        "software": [],
        "sequence_metrics": [],
        "fastp": [],
        "manifest": {"files": [], "omitted": 0},
    }
    state["workflow_id"] = "adaptive_virus_analysis"
    state["routing"] = routing_record
    state["execution"].update({"status": "running", "started_at": started_at})
    _set_step_status(state, "adaptive_screen", "running")

    screen_resources = collect_resource_snapshot(
        run_dir.parent,
        run_id=state.get("run_id"),
        required_threads=SCREEN_THREADS,
        required_memory_mb=SCREEN_MEMORY_MB,
    )
    routing_record["resources"]["screening"] = screen_resources
    if not screen_resources["admission_ok"]:
        routing_record.update(
            {
                "phase": "blocked",
                "decision": {
                    "phase": "blocked",
                    "selected_route": None,
                    "reason_codes": screen_resources["reason_codes"],
                },
            }
        )
        _finish_routing_record(routing_record, phase="blocked", success=False)
        _set_step_status(state, "adaptive_screen", "failed")
        _write_json_atomic(routing_path, routing_record)
        return _terminal_adaptive_state(
            state_path,
            state,
            telemetry,
            status="blocked",
            returncode=2,
            error="快速初筛资源不足",
        )

    try:
        routing_record["phase"] = "screening"
        _write_json_atomic(routing_path, routing_record)
        _write_json_atomic(state_path, state)
        screening = run_rapid_screen(spec, run_dir, threads=SCREEN_THREADS)
    except Exception as exc:
        error = redact_text(str(exc))
        routing_record["error"] = error
        _finish_routing_record(routing_record, phase="failed", success=False)
        _set_step_status(state, "adaptive_screen", "failed")
        _write_json_atomic(routing_path, routing_record)
        return _terminal_adaptive_state(
            state_path,
            state,
            telemetry,
            status="failed",
            returncode=1,
            error=error,
        )

    routing_record["screening"] = screening
    _set_step_status(state, "adaptive_screen", "completed")
    _set_step_status(state, "adaptive_route", "running")
    decision = decide_route(
        routing_request,
        screening,
        history=history,
    )
    if decision["selected_route"] == "B":
        estimate = state.get("resource_estimate") or {}
        calibrated_requirement = estimate.get("status") == "calibrated"
        full_resources = collect_resource_snapshot(
            run_dir.parent,
            run_id=state.get("run_id"),
            required_threads=(
                int(estimate.get("cpu_cores") or FULL_ANALYSIS_MINIMUM_THREADS)
                if calibrated_requirement
                else FULL_ANALYSIS_MINIMUM_THREADS
            ),
            required_memory_mb=(
                float((estimate.get("peak_memory_mb") or {}).get("reserve"))
                if calibrated_requirement
                else FULL_ANALYSIS_MINIMUM_MEMORY_MB
            ),
        )
        full_resources["requirement_source"] = (
            "calibrated" if calibrated_requirement else "minimum"
        )
        routing_record["resources"]["full_analysis"] = full_resources
        decision = decide_route(
            routing_request,
            screening,
            full_resources=full_resources,
            history=history,
        )
    routing_record.update({"phase": decision["phase"], "decision": decision})
    state["routing"] = routing_record
    _set_step_status(state, "adaptive_route", "completed")
    _write_json_atomic(routing_path, routing_record)
    _write_json_atomic(state_path, state)

    if decision["phase"] == "blocked":
        for step in state.get("steps", []):
            if step.get("status") == "pending":
                step["status"] = "skipped"
        _finish_routing_record(routing_record, phase="blocked", success=False)
        state["routing"] = routing_record
        telemetry["routing"] = routing_record
        telemetry["delivery_report"] = {
            "schema_version": 2,
            "kind": "adaptive_alert",
            "report_kind": "preliminary_screen",
            "routing": routing_record,
            "screening": screening,
        }
        _write_json_atomic(routing_path, routing_record)
        return _terminal_adaptive_state(
            state_path,
            state,
            telemetry,
            status="blocked",
            returncode=2,
            error="完整分析资源不足",
            report_available=True,
        )

    if decision["selected_route"] == "A":
        for step in state.get("steps", []):
            if step.get("status") == "pending":
                step["status"] = "skipped"
        _finish_routing_record(routing_record, phase="completed", success=True)
        state["routing"] = routing_record
        telemetry["routing"] = routing_record
        telemetry["delivery_report"] = {
            "schema_version": 2,
            "kind": "adaptive_alert",
            "report_kind": decision["report_kind"],
            "routing": routing_record,
            "screening": screening,
        }
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text(
            f"Adaptive route A completed: {decision['report_kind']}\n",
            encoding="utf-8",
        )
        _write_json_atomic(routing_path, routing_record)
        return _terminal_adaptive_state(
            state_path,
            state,
            telemetry,
            status="completed",
            returncode=0,
            report_available=True,
        )

    state["adaptive_execution_spec"] = spec
    state["selected_execution_spec"] = full_spec
    state["execution_spec"] = full_spec
    full_rules = set(selected_rules(full_spec))
    state["steps"] = [
        step
        for step in state.get("steps", [])
        if full_rules.intersection(step.get("rules") or [])
    ]
    routing_record["phase"] = "full_analysis"
    state["routing"] = routing_record
    _write_json_atomic(routing_path, routing_record)
    _write_json_atomic(state_path, state)
    result = run(args)
    final_state = _read_state(state_path)
    telemetry_path = bioagent_dir / "telemetry.json"
    final_telemetry = json.loads(telemetry_path.read_text(encoding="utf-8"))
    _finish_routing_record(
        routing_record,
        phase="completed" if result == 0 else "failed",
        success=result == 0,
    )
    final_state["routing"] = routing_record
    final_telemetry["routing"] = routing_record
    if isinstance(final_telemetry.get("delivery_report"), dict):
        final_telemetry["delivery_report"]["routing"] = routing_record
    _write_json_atomic(routing_path, routing_record)
    _write_json_atomic(state_path, final_state)
    _write_json_atomic(telemetry_path, final_telemetry)
    return result


def run(args: argparse.Namespace) -> int:
    state_path = Path(args.state).resolve()
    state = _read_state(state_path)
    state.setdefault("execution", {})["pid"] = os.getpid()
    _write_json_atomic(state_path, state)
    if (state.get("execution_spec") or {}).get("adapter") == "adaptive_virus_analysis":
        return _run_adaptive(args, state)
    run_dir = Path(state["run_dir"]).resolve()
    if state_path != run_dir / ".bioagent" / "state.json":
        raise ValueError("state path does not match run directory")

    bioagent_dir = run_dir / ".bioagent"
    benchmark_root = bioagent_dir / "benchmarks"
    phase_log_root = bioagent_dir / "phase-logs"
    telemetry_path = bioagent_dir / "telemetry.json"
    input_stats_path = bioagent_dir / "input-sequences.tsv"
    log_path = Path(state["execution"]["log"]).resolve()
    benchmark_root.mkdir(parents=True, exist_ok=True)
    phase_log_root.mkdir(parents=True, exist_ok=True)

    started_at = _now()
    state["execution"].update({"started_at": started_at, "status": "running"})
    _write_json_atomic(state_path, state)

    spec = state.get("execution_spec") or {}
    workflow_id = str(spec.get("adapter") or "unknown")
    telemetry: dict[str, Any] = {
        "version": 3,
        "run_id": state.get("run_id"),
        "chat_id": state.get("openwebui_chat_id"),
        "workflow_id": workflow_id,
        "started_at": started_at,
        "runner_commands": [],
        "phases": [],
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
    active_phase: PreparedPhase | None = None
    settings = AdapterSettings.from_env()
    resource_monitor = ProcessResourceMonitor(
        lambda: process.pid if process and process.poll() is None else None
    )
    resource_monitor.start()

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
        state["phases"] = [
            {
                "id": phase.id,
                "workflow_id": phase.workflow_id,
                "workflow_dir": str(phase.workflow_dir),
                "results_dir": str(phase.results_dir),
                "status": "pending",
            }
            for phase in prepared.phases
        ]
        state["execution_spec"] = spec
        _write_json_atomic(state_path, state)
        telemetry["workflow_id"] = workflow_id
        for phase in prepared.phases:
            phase_benchmarks = benchmark_root / phase.id
            phase_benchmarks.mkdir(parents=True, exist_ok=True)
            if phase.workflow_id == "workflow1":
                _instrument_workflow1(phase.workflow_dir, phase_benchmarks)
            else:
                _instrument_workflow2(phase.workflow_dir, phase_benchmarks)
        telemetry["input_collection"] = _collect_input_stats(
            prepared.seqkit,
            prepared.phases[0].workflow_dir,
            input_stats_path,
        )
        telemetry["runner_commands"] = [
            [redact_text(value) for value in phase.command] for phase in prepared.phases
        ]
        phase_environment = os.environ.copy()
        tool_dir = str(Path(prepared.seqkit).resolve().parent)
        phase_environment["PATH"] = os.pathsep.join(
            [tool_dir, phase_environment.get("PATH", "")]
        )

        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("w", encoding="utf-8") as log_file:
            for phase_index, phase in enumerate(prepared.phases):
                active_phase = phase
                phase_started_at = _now()
                state["phases"][phase_index].update(
                    {"status": "running", "started_at": phase_started_at}
                )
                _write_json_atomic(state_path, state)
                phase_log_path = phase_log_root / f"{phase.id}.log"
                header = f"\nBioAgent phase: {phase.id}\n"
                log_file.write(header)
                log_file.flush()
                sys.stdout.write(header)
                sys.stdout.flush()
                with phase_log_path.open("w", encoding="utf-8") as phase_log:
                    process = subprocess.Popen(
                        phase.command,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                        text=True,
                        bufsize=1,
                        env=phase_environment,
                    )
                    assert process.stdout is not None
                    for line in process.stdout:
                        phase_log.write(line)
                        phase_log.flush()
                        log_file.write(line)
                        log_file.flush()
                        sys.stdout.write(line)
                        sys.stdout.flush()
                    process.stdout.close()
                    returncode = process.wait()
                phase_finished_at = _now()
                phase_status = "completed" if returncode == 0 else "failed"
                state["phases"][phase_index].update(
                    {
                        "status": phase_status,
                        "returncode": returncode,
                        "finished_at": phase_finished_at,
                    }
                )
                telemetry["phases"].append(
                    {
                        "id": phase.id,
                        "workflow_id": phase.workflow_id,
                        "workflow_dir": str(phase.workflow_dir),
                        "results_dir": str(phase.results_dir),
                        "command": [redact_text(value) for value in phase.command],
                        "status": phase_status,
                        "returncode": returncode,
                        "started_at": phase_started_at,
                        "finished_at": phase_finished_at,
                        "log": str(phase_log_path),
                    }
                )
                _write_json_atomic(state_path, state)
                if returncode != 0:
                    break
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
        if active_phase and not any(
            phase.get("id") == active_phase.id for phase in telemetry["phases"]
        ):
            phase_status = "cancelled" if cancelled_signal else "failed"
            phase_finished_at = _now()
            for phase in state.get("phases", []):
                if phase.get("id") == active_phase.id:
                    phase.update(
                        {
                            "status": phase_status,
                            "returncode": returncode,
                            "finished_at": phase_finished_at,
                            **({"error": error} if error else {}),
                        }
                    )
            telemetry["phases"].append(
                {
                    "id": active_phase.id,
                    "workflow_id": active_phase.workflow_id,
                    "workflow_dir": str(active_phase.workflow_dir),
                    "results_dir": str(active_phase.results_dir),
                    "command": [redact_text(value) for value in active_phase.command],
                    "status": phase_status,
                    "returncode": returncode,
                    "finished_at": phase_finished_at,
                    **({"error": error} if error else {}),
                }
            )
    finally:
        resource_usage = resource_monitor.stop()
        phase_log_data = []
        benchmarks: list[dict[str, Any]] = []
        if prepared:
            for phase in prepared.phases:
                phase_log_path = phase_log_root / f"{phase.id}.log"
                phase_log_data.append(_parse_log(phase_log_path, phase.id))
                benchmarks.extend(
                    _parse_benchmarks(benchmark_root / phase.id, phase.id)
                )
        log_jobs = [job for data in phase_log_data for job in data.get("jobs", [])]
        commands = list(
            dict.fromkeys(
                command
                for data in phase_log_data
                for command in data.get("commands", [])
            )
        )
        logged_software = list(
            dict.fromkeys(
                item for data in phase_log_data for item in data.get("software", [])
            )
        )
        rrna_alignment_percents = [
            value
            for data in phase_log_data
            for value in data.get("rrna_alignment_percents", [])
        ]
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
                "commands": commands,
                "software": logged_software,
                "rrna_alignment_percents": rrna_alignment_percents,
                "sequence_metrics": _sequence_metrics(run_dir, input_stats_path),
                "fastp": _parse_fastp(run_dir),
                "manifest": _output_manifest(run_dir),
                "stages": _stage_records(state, benchmarks, log_jobs),
                **({"error": error} if error else {}),
                **({"cancelled_signal": cancelled_signal} if cancelled_signal else {}),
            }
        )
        benchmark_peak = max(
            (
                float(job["max_rss"])
                for stage in telemetry["stages"]
                for job in stage.get("jobs") or []
                if isinstance(job.get("max_rss"), (int, float))
            ),
            default=0.0,
        )
        resource_usage["peak_memory_mb"] = max(
            float(resource_usage["peak_process_tree_rss_mb"]), benchmark_peak
        )
        resource_usage["memory_source"] = "process_tree+benchmark"
        telemetry["resource_usage"] = resource_usage
        telemetry["software"] = list(
            dict.fromkeys(
                [
                    *telemetry["software"],
                    *_software_versions(
                        (
                            telemetry["runner_commands"][0]
                            if telemetry["runner_commands"]
                            else []
                        ),
                        telemetry["commands"],
                        prepared.seqkit if prepared else settings.seqkit,
                    ),
                ]
            )
        )
        rrna_alignment_by_run = _bowtie_alignment_by_run(run_dir)
        if rrna_alignment_by_run:
            telemetry["rrna_alignments"] = [
                {
                    "run_id": run_id,
                    "alignment_percent": alignment_percent,
                }
                for run_id, alignment_percent in rrna_alignment_by_run.items()
            ]
            telemetry["rrna_alignment_percents"] = list(rrna_alignment_by_run.values())
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
            if stage.get("started_at"):
                step["started_at"] = stage["started_at"]
            if stage.get("finished_at"):
                step["finished_at"] = stage["finished_at"]
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
            "telemetry_version": 3,
        }
        state["report"] = {
            "status": "ready" if returncode == 0 else "unavailable",
            "available": returncode == 0,
            "schema_version": 2,
            "telemetry": str(telemetry_path),
        }
        if workflow_id == "workflow1":
            telemetry["delivery_report"] = {
                "schema_version": 2,
                "kind": "upstream",
                "stages": telemetry["stages"],
                "upstream": _workflow1_report_data(
                    state,
                    run_dir,
                    telemetry["sequence_metrics"],
                    telemetry["fastp"],
                    telemetry["rrna_alignment_percents"],
                ),
            }
        else:
            has_upstream = (
                ((spec.get("inputs") or [{}])[0]).get("type") == "raw_paired_reads"
            )
            telemetry["delivery_report"] = {
                "schema_version": 2,
                "kind": "virus_classification",
                "stages": telemetry["stages"],
                **(
                    {
                        "upstream": _workflow1_report_data(
                            state,
                            run_dir,
                            telemetry["sequence_metrics"],
                            telemetry["fastp"],
                            telemetry["rrna_alignment_percents"],
                        )
                    }
                    if has_upstream
                    else {}
                ),
                "classification": _classification_report_data(state, run_dir),
            }
        telemetry["resource_profile"] = build_run_resource_profile(
            state,
            telemetry,
            settings,
            str(resource_usage["load_bucket"]),
        )
        _write_json_atomic(telemetry_path, telemetry)
        _write_json_atomic(state_path, state)
        history_path = state.get("resource_history_db")
        if isinstance(history_path, str) and history_path:
            try:
                ResourceCalibrator(
                    Path(history_path), run_dir.parent, settings
                ).record_run(
                    state,
                    telemetry,
                    telemetry_mtime_ns=telemetry_path.stat().st_mtime_ns,
                )
            except (OSError, sqlite3.Error, ValueError) as exc:
                print(
                    f"BioAgent resource history update failed: {exc}", file=sys.stderr
                )
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)
    return returncode


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    state_path = Path(args.state).resolve()
    try:
        return run(args)
    except Exception as exc:
        try:
            state = _read_state(state_path)
            state.setdefault("execution", {}).update(
                {
                    "status": "failed",
                    "ok": False,
                    "returncode": -1,
                    "current_step_id": None,
                    "error": str(exc) or exc.__class__.__name__,
                    "finished_at": _now(),
                }
            )
            state.setdefault("technical_report", {}).update(
                {"status": "unavailable", "available": False}
            )
            state.setdefault("report", {}).update(
                {"status": "unavailable", "available": False}
            )
            _write_json_atomic(state_path, state)
        except Exception as state_error:
            print(
                f"BioAgent runner could not terminalize state: {state_error}",
                file=sys.stderr,
            )
        print(f"BioAgent runner failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

"""Validated workflow execution specifications and run-local preparation."""

from __future__ import annotations

import os
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any


WORKFLOW1_RULES = (
    "fastp",
    "bowtie2",
    "merge",
    "megahit",
    "rename_filter",
)
WORKFLOW1_INPUTS = {
    "fastp": ("raw_paired_reads", "runs", ("run_id", "sample_id", "read1", "read2")),
    "bowtie2": (
        "fastp_paired_reads",
        "runs",
        ("run_id", "sample_id", "read1", "read2"),
    ),
    "merge": (
        "clean_paired_reads",
        "runs",
        ("run_id", "sample_id", "read1", "read2"),
    ),
    "megahit": (
        "merged_paired_reads",
        "samples",
        ("sample_id", "read1", "read2"),
    ),
    "rename_filter": ("contigs_fasta", "samples", ("sample_id", "path")),
}
SAFE_ID_RE = re.compile(r"^[A-Za-z0-9_.-]+$")


class ExecutionSpecError(ValueError):
    """Raised when a workflow execution specification is invalid."""


@dataclass(frozen=True)
class AdapterSettings:
    workflow1_source: Path
    snakemake: str
    seqkit: str
    cores: int

    @classmethod
    def from_env(cls) -> AdapterSettings:
        return cls(
            workflow1_source=Path(
                os.getenv(
                    "BIOAGENT_WORKFLOW1_SOURCE",
                    "/data/user/guojindan/Projects/Workflows/workflow1_upstream",
                )
            ),
            snakemake=os.getenv(
                "BIOAGENT_SNAKEMAKE",
                "/data/user/liufangzhou/tools/pixi/bin/snakemake",
            ),
            seqkit=os.getenv(
                "BIOAGENT_SEQKIT",
                "/data/user/guojindan/softwares/xvirmin/bin/seqkit",
            ),
            cores=int(os.getenv("BIOAGENT_SNAKEMAKE_CORES", "8")),
        )


@dataclass(frozen=True)
class PreparedExecution:
    workflow_id: str
    workflow_dir: Path
    command: list[str]
    seqkit: str
    selected_rules: tuple[str, ...]


def _required_id(value: Any, field: str) -> str:
    if not isinstance(value, str) or not SAFE_ID_RE.fullmatch(value):
        raise ExecutionSpecError(f"{field} must contain only letters, digits, ._-")
    return value


def _absolute_path(value: Any, field: str) -> str:
    if not isinstance(value, str) or not Path(value).is_absolute():
        raise ExecutionSpecError(f"{field} must be an absolute path")
    return value


def normalize_execution_spec(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ExecutionSpecError("missing execution_spec")
    if raw.get("adapter") != "workflow1":
        raise ExecutionSpecError("unsupported workflow adapter")

    entry_rule = raw.get("entry_rule")
    exit_rule = raw.get("exit_rule")
    if entry_rule not in WORKFLOW1_RULES or exit_rule not in WORKFLOW1_RULES:
        raise ExecutionSpecError("unsupported workflow1 rule boundary")
    if WORKFLOW1_RULES.index(entry_rule) > WORKFLOW1_RULES.index(exit_rule):
        raise ExecutionSpecError("entry_rule must not follow exit_rule")

    inputs = raw.get("inputs")
    if not isinstance(inputs, list) or len(inputs) != 1:
        raise ExecutionSpecError("workflow1 requires exactly one typed input")
    raw_input = inputs[0]
    if not isinstance(raw_input, dict):
        raise ExecutionSpecError("execution input must be an object")

    input_id = _required_id(raw_input.get("id"), "input.id")
    expected_type, collection_name, fields = WORKFLOW1_INPUTS[entry_rule]
    if raw_input.get("type") != expected_type:
        raise ExecutionSpecError(f"{entry_rule} requires input type {expected_type}")
    data = raw_input.get("data")
    records = data.get(collection_name) if isinstance(data, dict) else None
    if not isinstance(records, list) or not records:
        raise ExecutionSpecError(f"input.data.{collection_name} must not be empty")

    normalized_records = []
    seen_ids: set[tuple[str, ...]] = set()
    for index, record in enumerate(records):
        if not isinstance(record, dict):
            raise ExecutionSpecError(f"input record {index} must be an object")
        normalized: dict[str, str] = {}
        for field in fields:
            value = record.get(field)
            normalized[field] = (
                _absolute_path(value, f"input record {index}.{field}")
                if field in {"read1", "read2", "path"}
                else _required_id(value, f"input record {index}.{field}")
            )
        identity = tuple(
            normalized[field] for field in fields if field in {"run_id", "sample_id"}
        )
        if identity in seen_ids:
            raise ExecutionSpecError("execution input contains duplicate records")
        seen_ids.add(identity)
        normalized_records.append(normalized)

    return {
        "adapter": "workflow1",
        "entry_rule": entry_rule,
        "exit_rule": exit_rule,
        "inputs": [
            {
                "id": input_id,
                "type": expected_type,
                "data": {collection_name: normalized_records},
            }
        ],
    }


def selected_rules(spec: dict[str, Any]) -> tuple[str, ...]:
    entry_index = WORKFLOW1_RULES.index(spec["entry_rule"])
    exit_index = WORKFLOW1_RULES.index(spec["exit_rule"])
    return WORKFLOW1_RULES[entry_index : exit_index + 1]


def validate_plan_rules(
    spec: dict[str, Any], steps: list[dict[str, Any]]
) -> tuple[str, ...]:
    planned_rules = tuple(
        rule
        for step in steps
        for rule in (step.get("rules") or [])
        if isinstance(rule, str)
    )
    expected = selected_rules(spec)
    if planned_rules != expected:
        raise ExecutionSpecError(
            "steps[].rules must exactly match the selected workflow segment"
        )
    return expected


def _copy_workflow(source: Path, destination: Path) -> None:
    if not source.is_dir():
        raise ExecutionSpecError(f"workflow source does not exist: {source}")
    if destination.exists():
        raise ExecutionSpecError(f"run-local workflow already exists: {destination}")
    shutil.copytree(
        source,
        destination,
        ignore=shutil.ignore_patterns(
            ".git",
            ".snakemake",
            "logs",
            "results",
            "example",
            "__pycache__",
        ),
    )


def _input_records(spec: dict[str, Any]) -> list[dict[str, str]]:
    data = spec["inputs"][0]["data"]
    collection_name = WORKFLOW1_INPUTS[spec["entry_rule"]][1]
    return data[collection_name]


def _source(path: str) -> Path:
    source = Path(path).resolve()
    if not source.is_file():
        raise ExecutionSpecError(f"workflow input does not exist: {source}")
    return source


def _link(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.symlink_to(source)


def _placeholder(results_dir: Path, destination: Path) -> None:
    source = (
        results_dir.parent
        / ".bioagent"
        / "staged-inputs"
        / destination.relative_to(results_dir)
    )
    source.parent.mkdir(parents=True, exist_ok=True)
    source.touch()
    _link(source, destination)


def _prepare_workflow1_inputs(
    spec: dict[str, Any], workflow_dir: Path, results_dir: Path
) -> tuple[list[str], list[str]]:
    entry_rule = spec["entry_rule"]
    records = _input_records(spec)
    sample_lines: list[str] = []
    run_ids: list[str] = []
    sample_ids: list[str] = []

    if entry_rule in {"fastp", "bowtie2", "merge"}:
        for record in records:
            read1 = _source(record["read1"])
            read2 = _source(record["read2"])
            run_id = record["run_id"]
            sample_id = record["sample_id"]
            run_ids.append(run_id)
            sample_ids.append(sample_id)
            sample_lines.append(f"{run_id} {sample_id} {read1} {read2}")
            if entry_rule == "bowtie2":
                root = results_dir / "filter" / run_id
                _link(
                    read1,
                    root / "01_fastp_r1.fq.gz",
                )
                _link(
                    read2,
                    root / "01_fastp_r2.fq.gz",
                )
                for name in (
                    "01_fastp_r1.fq.stat.txt",
                    "01_fastp_r2.fq.stat.txt",
                    "01_fastp.html",
                    "01_fastp.json",
                ):
                    _placeholder(results_dir, root / name)
            elif entry_rule == "merge":
                root = results_dir / "filter" / run_id
                _link(
                    read1,
                    root / "cleanreads_r1.fq.gz",
                )
                _link(
                    read2,
                    root / "cleanreads_r2.fq.gz",
                )
                _placeholder(results_dir, root / "cleanreads_r1.fq.stat.txt")
                _placeholder(results_dir, root / "cleanreads_r2.fq.stat.txt")
    elif entry_rule == "megahit":
        for record in records:
            read1 = _source(record["read1"])
            read2 = _source(record["read2"])
            sample_id = record["sample_id"]
            run_ids.append(sample_id)
            sample_ids.append(sample_id)
            sample_lines.append(f"{sample_id} {sample_id} {read1} {read2}")
            _link(
                read1,
                results_dir / "merge" / sample_id / "cleanreads_r1.fq",
            )
            _link(
                read2,
                results_dir / "merge" / sample_id / "cleanreads_r2.fq",
            )
            root = results_dir / "merge" / sample_id
            _placeholder(results_dir, root / "cleanreads_r1.fq.stat.txt")
            _placeholder(results_dir, root / "cleanreads_r2.fq.stat.txt")
    else:
        for record in records:
            contigs = _source(record["path"])
            sample_id = record["sample_id"]
            run_ids.append(sample_id)
            sample_ids.append(sample_id)
            sample_lines.append(f"{sample_id} {sample_id} {contigs} {contigs}")
            _link(
                contigs,
                results_dir / "assembly" / sample_id / f"{sample_id}.contigs.fa",
            )
            _placeholder(
                results_dir,
                results_dir / "assembly" / sample_id / f"{sample_id}.contigs.stat.txt",
            )

    samples_path = workflow_dir / "data" / "samples.txt"
    samples_path.parent.mkdir(parents=True, exist_ok=True)
    samples_path.write_text("\n".join(sample_lines) + "\n", encoding="utf-8")
    return list(dict.fromkeys(run_ids)), list(dict.fromkeys(sample_ids))


def _rewrite_workflow1_config(workflow_dir: Path, results_dir: Path) -> Path:
    config_path = workflow_dir / "config" / "config.yaml"
    lines = []
    for line in config_path.read_text(encoding="utf-8").splitlines():
        if line.startswith("sample_list:"):
            lines.append(f'sample_list: "{workflow_dir / "data" / "samples.txt"}"')
        elif line.startswith("output_dir:"):
            lines.append(f'output_dir: "{results_dir}"')
        else:
            lines.append(line)
    config_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return config_path


def _workflow1_targets(
    exit_rule: str,
    results_dir: Path,
    run_ids: list[str],
    sample_ids: list[str],
) -> list[str]:
    targets: list[Path] = []
    if exit_rule == "fastp":
        for run_id in run_ids:
            root = results_dir / "filter" / run_id
            targets.extend(
                [
                    root / "01_fastp_r1.fq.gz",
                    root / "01_fastp_r2.fq.gz",
                    root / "01_fastp_r1.fq.stat.txt",
                    root / "01_fastp_r2.fq.stat.txt",
                    root / "01_fastp.html",
                    root / "01_fastp.json",
                ]
            )
    elif exit_rule == "bowtie2":
        for run_id in run_ids:
            root = results_dir / "filter" / run_id
            targets.extend(
                [
                    root / "cleanreads_r1.fq.gz",
                    root / "cleanreads_r2.fq.gz",
                    root / "cleanreads_r1.fq.stat.txt",
                    root / "cleanreads_r2.fq.stat.txt",
                ]
            )
    elif exit_rule == "merge":
        for sample_id in sample_ids:
            root = results_dir / "merge" / sample_id
            targets.extend(
                [
                    root / "cleanreads_r1.fq",
                    root / "cleanreads_r2.fq",
                    root / "cleanreads_r1.fq.stat.txt",
                    root / "cleanreads_r2.fq.stat.txt",
                ]
            )
    elif exit_rule == "megahit":
        for sample_id in sample_ids:
            root = results_dir / "assembly" / sample_id
            targets.extend(
                [
                    root / f"{sample_id}.contigs.fa",
                    root / f"{sample_id}.contigs.stat.txt",
                ]
            )
    else:
        targets.extend(
            results_dir / "assembly" / sample_id / f"{sample_id}.contigs.1kb.renamed.fa"
            for sample_id in sample_ids
        )
    return [str(path) for path in targets]


def prepare_execution(
    state: dict[str, Any],
    settings: AdapterSettings | None = None,
) -> PreparedExecution:
    settings = settings or AdapterSettings.from_env()
    spec = normalize_execution_spec(state.get("execution_spec"))
    validate_plan_rules(spec, state.get("steps") or [])

    run_dir = Path(state["run_dir"]).resolve()
    workflow_dir = run_dir / "workflow"
    results_dir = run_dir / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "logs").mkdir(parents=True, exist_ok=True)
    _copy_workflow(settings.workflow1_source, workflow_dir)
    run_ids, sample_ids = _prepare_workflow1_inputs(
        spec,
        workflow_dir,
        results_dir,
    )
    config_path = _rewrite_workflow1_config(workflow_dir, results_dir)
    targets = _workflow1_targets(
        spec["exit_rule"],
        results_dir,
        run_ids,
        sample_ids,
    )
    command = [
        settings.snakemake,
        "--directory",
        str(workflow_dir),
        "--snakefile",
        str(workflow_dir / "Snakefile"),
        "--configfile",
        str(config_path),
        "--cores",
        str(settings.cores),
        "--runtime-source-cache-path",
        str(run_dir / ".bioagent" / "snakemake-source-cache"),
        "--printshellcmds",
        "--benchmark-extended",
        *targets,
    ]
    return PreparedExecution(
        workflow_id="workflow1",
        workflow_dir=workflow_dir,
        command=command,
        seqkit=settings.seqkit,
        selected_rules=selected_rules(spec),
    )

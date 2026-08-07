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
WORKFLOW2_PREPROCESS_RULES = (
    "rename_contigs",
    "get_orfs",
    "merge_orfs",
    "merge_contigs",
)
WORKFLOW2_CANDIDATE_RULES = {
    "RNA": ("rna_virusidentification",),
    "DNA": ("dna_virusidentification",),
    "RNA+DNA": ("rna_virusidentification", "dna_virusidentification"),
}
WORKFLOW2_FETCH_RULES = (
    "coverage_calc",
    "fetch_contigs",
    "fetch_proteins",
)
WORKFLOW2_EVIDENCE_RULES = (
    "mmseqs_nuclclust",
    "fetch_nuclrep_prot",
    "mmseqs_aaclust",
    "cluster_summarize",
    "checkv",
    "megablast_nt",
    "blastx_nr",
    "blastx_vhdb",
    "hmmsearch_rdrpscan",
    "parse_blastresult",
)
WORKFLOW2_SUMMARY_RULES = (
    "summarize_tables",
    "summarize_sequences",
)
SAFE_ID_RE = re.compile(r"^[A-Za-z0-9_.-]+$")


class ExecutionSpecError(ValueError):
    """Raised when a workflow execution specification is invalid."""


@dataclass(frozen=True)
class AdapterSettings:
    workflow1_source: Path
    workflow2_source: Path
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
            workflow2_source=Path(
                os.getenv(
                    "BIOAGENT_WORKFLOW2_SOURCE",
                    "/data/user/guojindan/Projects/Workflows/workflow2_virusid_2026db",
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
class PreparedPhase:
    id: str
    workflow_id: str
    workflow_dir: Path
    results_dir: Path
    command: list[str]
    selected_rules: tuple[str, ...]


@dataclass(frozen=True)
class PreparedExecution:
    workflow_id: str
    phases: tuple[PreparedPhase, ...]
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


def _normalize_records(
    raw_input: dict[str, Any],
    *,
    expected_type: str,
    collection_name: str,
    fields: tuple[str, ...],
) -> dict[str, Any]:
    input_id = _required_id(raw_input.get("id"), "input.id")
    if raw_input.get("type") != expected_type:
        raise ExecutionSpecError(f"execution input must have type {expected_type}")
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
        "id": input_id,
        "type": expected_type,
        "data": {collection_name: normalized_records},
    }


def _normalize_workflow1(raw: dict[str, Any]) -> dict[str, Any]:
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
    expected_type, collection_name, fields = WORKFLOW1_INPUTS[entry_rule]
    normalized_input = _normalize_records(
        raw_input,
        expected_type=expected_type,
        collection_name=collection_name,
        fields=fields,
    )
    return {
        "adapter": "workflow1",
        "entry_rule": entry_rule,
        "exit_rule": exit_rule,
        "inputs": [normalized_input],
    }


def _normalize_virus_classification(raw: dict[str, Any]) -> dict[str, Any]:
    viral_type = raw.get("viral_type", "RNA+DNA")
    if viral_type not in WORKFLOW2_CANDIDATE_RULES:
        raise ExecutionSpecError("viral_type must be RNA, DNA, or RNA+DNA")
    inputs = raw.get("inputs")
    if not isinstance(inputs, list) or len(inputs) != 1:
        raise ExecutionSpecError(
            "virus_classification requires exactly one typed input"
        )
    raw_input = inputs[0]
    if not isinstance(raw_input, dict):
        raise ExecutionSpecError("execution input must be an object")

    input_type = raw_input.get("type")
    if input_type == "raw_paired_reads":
        normalized_input = _normalize_records(
            raw_input,
            expected_type="raw_paired_reads",
            collection_name="runs",
            fields=("run_id", "sample_id", "read1", "read2"),
        )
    elif input_type == "contigs_fasta":
        normalized_input = _normalize_records(
            raw_input,
            expected_type="contigs_fasta",
            collection_name="samples",
            fields=("sample_id", "path"),
        )
    else:
        raise ExecutionSpecError(
            "virus_classification requires raw_paired_reads or contigs_fasta"
        )
    return {
        "adapter": "virus_classification",
        "viral_type": viral_type,
        "inputs": [normalized_input],
    }


def normalize_execution_spec(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ExecutionSpecError("missing execution_spec")
    adapter = raw.get("adapter")
    if adapter == "workflow1":
        return _normalize_workflow1(raw)
    if adapter == "virus_classification":
        return _normalize_virus_classification(raw)
    raise ExecutionSpecError("unsupported workflow adapter")


def normalize_plan_execution_spec(
    raw: Any, steps: list[dict[str, Any]]
) -> dict[str, Any]:
    """Normalize a plan spec, inferring only unambiguous workflow1 boundaries."""
    candidate = raw
    if (
        isinstance(raw, dict)
        and raw.get("adapter") == "workflow1"
        and (raw.get("entry_rule") is None or raw.get("exit_rule") is None)
    ):
        planned_rules = tuple(
            rule
            for step in steps
            for rule in (step.get("rules") or [])
            if isinstance(rule, str)
        )
        inferred: tuple[str, str] | None = None
        if planned_rules:
            for start in range(len(WORKFLOW1_RULES)):
                selected = WORKFLOW1_RULES[start : start + len(planned_rules)]
                if selected == planned_rules:
                    inferred = (selected[0], selected[-1])
                    break
        if inferred is None:
            raise ExecutionSpecError(
                "workflow1 steps must be one non-empty contiguous rule segment"
            )

        candidate = dict(raw)
        for field, value in zip(("entry_rule", "exit_rule"), inferred):
            explicit = candidate.get(field)
            if explicit is not None and explicit != value:
                raise ExecutionSpecError(f"{field} conflicts with steps[].rules")
            candidate[field] = value

    spec = normalize_execution_spec(candidate)
    validate_plan_rules(spec, steps)
    return spec


def _workflow2_rules(
    viral_type: str, *, rename_contigs: bool = True
) -> tuple[str, ...]:
    return (
        *(
            WORKFLOW2_PREPROCESS_RULES
            if rename_contigs
            else WORKFLOW2_PREPROCESS_RULES[1:]
        ),
        *WORKFLOW2_CANDIDATE_RULES[viral_type],
        *WORKFLOW2_FETCH_RULES,
        *WORKFLOW2_EVIDENCE_RULES,
        *WORKFLOW2_SUMMARY_RULES,
    )


def selected_rules(spec: dict[str, Any]) -> tuple[str, ...]:
    if spec["adapter"] == "workflow1":
        entry_index = WORKFLOW1_RULES.index(spec["entry_rule"])
        exit_index = WORKFLOW1_RULES.index(spec["exit_rule"])
        return WORKFLOW1_RULES[entry_index : exit_index + 1]
    upstream = (
        WORKFLOW1_RULES if spec["inputs"][0]["type"] == "raw_paired_reads" else ()
    )
    return (
        *upstream,
        *_workflow2_rules(
            spec["viral_type"],
            rename_contigs=spec["inputs"][0]["type"] == "contigs_fasta",
        ),
    )


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
            "steps[].rules must exactly match the selected execution flow"
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
            "test",
            "__pycache__",
        ),
    )


def _input_records(spec: dict[str, Any]) -> list[dict[str, str]]:
    data = spec["inputs"][0]["data"]
    if spec["adapter"] == "workflow1":
        collection_name = WORKFLOW1_INPUTS[spec["entry_rule"]][1]
    else:
        collection_name = (
            "runs" if spec["inputs"][0]["type"] == "raw_paired_reads" else "samples"
        )
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
        results_dir.parent.parent
        / ".bioagent"
        / "staged-inputs"
        / results_dir.name
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
                _link(read1, root / "01_fastp_r1.fq.gz")
                _link(read2, root / "01_fastp_r2.fq.gz")
                for name in (
                    "01_fastp_r1.fq.stat.txt",
                    "01_fastp_r2.fq.stat.txt",
                    "01_fastp.html",
                    "01_fastp.json",
                ):
                    _placeholder(results_dir, root / name)
            elif entry_rule == "merge":
                root = results_dir / "filter" / run_id
                _link(read1, root / "cleanreads_r1.fq.gz")
                _link(read2, root / "cleanreads_r2.fq.gz")
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
            root = results_dir / "merge" / sample_id
            _link(read1, root / "cleanreads_r1.fq")
            _link(read2, root / "cleanreads_r2.fq")
            _placeholder(results_dir, root / "cleanreads_r1.fq.stat.txt")
            _placeholder(results_dir, root / "cleanreads_r2.fq.stat.txt")
    else:
        for record in records:
            contigs = _source(record["path"])
            sample_id = record["sample_id"]
            run_ids.append(sample_id)
            sample_ids.append(sample_id)
            sample_lines.append(f"{sample_id} {sample_id} {contigs} {contigs}")
            root = results_dir / "assembly" / sample_id
            _link(contigs, root / f"{sample_id}.contigs.fa")
            _placeholder(
                results_dir,
                root / f"{sample_id}.contigs.stat.txt",
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


def _snakemake_command(
    settings: AdapterSettings,
    *,
    run_dir: Path,
    phase_id: str,
    workflow_dir: Path,
    config_path: Path,
    targets: list[str],
) -> list[str]:
    return [
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
        str(run_dir / ".bioagent" / "snakemake-source-cache" / phase_id),
        "--printshellcmds",
        "--benchmark-extended",
        *targets,
    ]


def _prepare_workflow1_phase(
    spec: dict[str, Any],
    settings: AdapterSettings,
    run_dir: Path,
    *,
    phase_id: str = "upstream",
) -> tuple[PreparedPhase, list[str]]:
    workflow_dir = run_dir / "workflows" / phase_id
    results_dir = run_dir / "results" / phase_id
    results_dir.mkdir(parents=True, exist_ok=True)
    _copy_workflow(settings.workflow1_source, workflow_dir)
    run_ids, sample_ids = _prepare_workflow1_inputs(spec, workflow_dir, results_dir)
    config_path = _rewrite_workflow1_config(workflow_dir, results_dir)
    targets = _workflow1_targets(
        spec["exit_rule"],
        results_dir,
        run_ids,
        sample_ids,
    )
    phase = PreparedPhase(
        id=phase_id,
        workflow_id="workflow1",
        workflow_dir=workflow_dir,
        results_dir=results_dir,
        command=_snakemake_command(
            settings,
            run_dir=run_dir,
            phase_id=phase_id,
            workflow_dir=workflow_dir,
            config_path=config_path,
            targets=targets,
        ),
        selected_rules=selected_rules(spec),
    )
    return phase, sample_ids


def _rewrite_workflow2_config(
    workflow_dir: Path,
    results_dir: Path,
    *,
    viral_type: str,
    rename_contigs: bool,
) -> Path:
    config_path = workflow_dir / "config" / "config.yaml"
    replacements = {
        "input_list:": f'input_list: "{workflow_dir / "data" / "contigs_list.txt"}"',
        "wdir:": f'wdir: "{results_dir}"',
        "SEARCH_VIRALTYPE:": f'SEARCH_VIRALTYPE: "{viral_type}"',
        "RNA_SEARCHMODE:": 'RNA_SEARCHMODE: "fast"',
        "ITERATION_TIME:": "ITERATION_TIME: 5",
        "RENAME_CONTIGS:": f"RENAME_CONTIGS: {str(rename_contigs).lower()}",
    }
    script_names = {
        "iterhmmsearch": "iterativehmmsearch.py",
        "coverage_summarize": "parse_iterhmmresult.py",
        "cluster_summarize": "cluster_summarize.py",
        "blast_summarize": "parse_blastresult.py",
        "table_summarize": "table_summarize.py",
    }
    lines = []
    for line in config_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        replacement = next(
            (value for key, value in replacements.items() if line.startswith(key)),
            None,
        )
        if replacement is not None:
            lines.append(replacement)
            continue
        key = stripped.split(":", 1)[0] if ":" in stripped else ""
        if key in script_names:
            indent = line[: len(line) - len(line.lstrip())]
            lines.append(
                f'{indent}{key}: "{workflow_dir / "scripts" / script_names[key]}"'
            )
        else:
            lines.append(line)
    config_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return config_path


def _prepare_workflow2_phase(
    spec: dict[str, Any],
    settings: AdapterSettings,
    run_dir: Path,
    *,
    upstream_results: Path | None = None,
    upstream_sample_ids: list[str] | None = None,
) -> PreparedPhase:
    phase_id = "classification"
    workflow_dir = run_dir / "workflows" / phase_id
    results_dir = run_dir / "results" / phase_id
    results_dir.mkdir(parents=True, exist_ok=True)
    _copy_workflow(settings.workflow2_source, workflow_dir)

    input_path = workflow_dir / "data" / "contigs_list.txt"
    input_path.parent.mkdir(parents=True, exist_ok=True)
    if upstream_results is not None:
        sample_ids = upstream_sample_ids or []
        lines = [
            str(
                upstream_results
                / "assembly"
                / sample_id
                / f"{sample_id}.contigs.1kb.renamed.fa"
            )
            for sample_id in sample_ids
        ]
        rename_contigs = False
    else:
        lines = [
            f"{record['sample_id']} {_source(record['path'])}"
            for record in _input_records(spec)
        ]
        rename_contigs = True
    input_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    config_path = _rewrite_workflow2_config(
        workflow_dir,
        results_dir,
        viral_type=spec["viral_type"],
        rename_contigs=rename_contigs,
    )
    targets = [
        str(results_dir / "summary" / "sequences" / "rep_vert_viruses.fna"),
        str(results_dir / "summary" / "sequences" / "vert_viruses.fna"),
    ]
    return PreparedPhase(
        id=phase_id,
        workflow_id="workflow2",
        workflow_dir=workflow_dir,
        results_dir=results_dir,
        command=_snakemake_command(
            settings,
            run_dir=run_dir,
            phase_id=phase_id,
            workflow_dir=workflow_dir,
            config_path=config_path,
            targets=targets,
        ),
        selected_rules=_workflow2_rules(
            spec["viral_type"],
            rename_contigs=rename_contigs,
        ),
    )


def prepare_execution(
    state: dict[str, Any],
    settings: AdapterSettings | None = None,
) -> PreparedExecution:
    settings = settings or AdapterSettings.from_env()
    spec = normalize_execution_spec(state.get("execution_spec"))
    expected_rules = validate_plan_rules(spec, state.get("steps") or [])
    run_dir = Path(state["run_dir"]).resolve()
    (run_dir / "logs").mkdir(parents=True, exist_ok=True)

    if spec["adapter"] == "workflow1":
        phase, _sample_ids = _prepare_workflow1_phase(spec, settings, run_dir)
        phases = (phase,)
        workflow_id = "workflow1"
    elif spec["inputs"][0]["type"] == "raw_paired_reads":
        upstream_spec = {
            "adapter": "workflow1",
            "entry_rule": "fastp",
            "exit_rule": "rename_filter",
            "inputs": spec["inputs"],
        }
        upstream, sample_ids = _prepare_workflow1_phase(
            upstream_spec,
            settings,
            run_dir,
        )
        classification = _prepare_workflow2_phase(
            spec,
            settings,
            run_dir,
            upstream_results=upstream.results_dir,
            upstream_sample_ids=sample_ids,
        )
        phases = (upstream, classification)
        workflow_id = "virus_classification"
    else:
        phases = (_prepare_workflow2_phase(spec, settings, run_dir),)
        workflow_id = "virus_classification"

    return PreparedExecution(
        workflow_id=workflow_id,
        phases=phases,
        seqkit=settings.seqkit,
        selected_rules=expected_rules,
    )

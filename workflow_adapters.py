"""Validated workflow execution specifications and run-local preparation."""

from __future__ import annotations

import os
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pipeline_spec import (
    ExecutionSpecError,
    normalize_execution_spec,
    selected_rules,
    validate_plan_rules,
)
from workflow_catalog import (
    FASTP_CAPABILITY_ID,
    capability_parameter_arguments,
    capability_parameter_values,
)

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


@dataclass(frozen=True)
class AdapterSettings:
    workflow1_source: Path
    workflow2_source: Path
    snakemake: str
    seqkit: str
    cores: int
    python: str = sys.executable
    workflow3_source: Path = Path(
        "/data/user/guojindan/Projects/Workflows/workflow3_hostid"
    )
    workflow4_source: Path = Path(
        "/data/user/guojindan/Projects/Workflows/workflow4_kraken_bracken"
    )
    workflow5_source: Path = Path(
        "/data/user/guojindan/Projects/Workflows/workflow5_virus_quantification"
    )
    workflow6_source: Path = Path(
        "/data/user/guojindan/Projects/Workflows/workflow6_virus_annotation"
    )

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
            workflow3_source=Path(
                os.getenv(
                    "BIOAGENT_WORKFLOW3_SOURCE",
                    "/data/user/guojindan/Projects/Workflows/workflow3_hostid",
                )
            ),
            workflow4_source=Path(
                os.getenv(
                    "BIOAGENT_WORKFLOW4_SOURCE",
                    "/data/user/guojindan/Projects/Workflows/workflow4_kraken_bracken",
                )
            ),
            workflow5_source=Path(
                os.getenv(
                    "BIOAGENT_WORKFLOW5_SOURCE",
                    "/data/user/guojindan/Projects/Workflows/workflow5_virus_quantification",
                )
            ),
            workflow6_source=Path(
                os.getenv(
                    "BIOAGENT_WORKFLOW6_SOURCE",
                    "/data/user/guojindan/Projects/Workflows/workflow6_virus_annotation",
                )
            ),
            snakemake=os.getenv(
                "BIOAGENT_SNAKEMAKE",
                "/home/liufangzhou/.pixi/bin/snakemake",
            ),
            seqkit=os.getenv(
                "BIOAGENT_SEQKIT",
                "/home/liufangzhou/.pixi/bin/seqkit",
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
            "test_collection",
            "__pycache__",
        ),
    )
    shared_test_collection = source / "test_collection"
    if shared_test_collection.is_dir():
        (destination / "test_collection").symlink_to(
            shared_test_collection, target_is_directory=True
        )


def _input_records(spec: dict[str, Any]) -> list[dict[str, str]]:
    return spec["records"]


def _source(path: str, *, must_exist: bool = True) -> Path:
    source = Path(path).resolve()
    if must_exist and not source.is_file():
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
            _link(read1, root / "cleanreads_r1.fq.gz")
            _link(read2, root / "cleanreads_r2.fq.gz")
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


def _write_fastp_wrapper(
    workflow_dir: Path,
    *,
    python: str,
    executable: str,
    arguments: tuple[str, ...],
    values: dict[str, int | bool],
) -> Path:
    wrapper = workflow_dir / ".bioagent" / "fastp-wrapper"
    wrapper.parent.mkdir(parents=True, exist_ok=True)
    flag_variants = {
        "length_required": (("--length_required", True), ("-l", True)),
        "qualified_quality_phred": (
            ("--qualified_quality_phred", True),
            ("-q", True),
        ),
        "unqualified_percent_limit": (
            ("--unqualified_percent_limit", True),
            ("-u", True),
        ),
        "n_base_limit": (("--n_base_limit", True), ("-n", True)),
        "average_qual": (("--average_qual", True), ("-e", True)),
        "detect_adapter_for_pe": (
            ("--detect_adapter_for_pe", False),
            ("-2", False),
        ),
    }
    managed = {
        flag: takes_value
        for parameter_id in values
        for flag, takes_value in flag_variants[parameter_id]
    }
    wrapper.write_text(
        "\n".join(
            [
                f"#!{python}",
                "import os",
                "import sys",
                f"EXECUTABLE = {executable!r}",
                f"MANAGED = {dict(sorted(managed.items()))!r}",
                f"EXTRA = {list(arguments)!r}",
                "args = sys.argv[1:]",
                "kept = []",
                "index = 0",
                "while index < len(args):",
                "    argument = args[index]",
                "    key = argument.split('=', 1)[0]",
                "    takes_value = MANAGED.get(key)",
                "    if key in MANAGED:",
                "        index += 2 if takes_value and '=' not in argument else 1",
                "        continue",
                "    kept.append(argument)",
                "    index += 1",
                "os.execv(EXECUTABLE, [EXECUTABLE, *kept, *EXTRA])",
                "",
            ]
        ),
        encoding="utf-8",
    )
    wrapper.chmod(0o700)
    return wrapper


def _yaml_scalar(line: str) -> str:
    value = line.split(":", 1)[1].strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        value = value[1:-1]
    return value


def _rewrite_workflow1_config(
    workflow_dir: Path,
    results_dir: Path,
    *,
    seqkit: str,
    python: str = sys.executable,
    fastp_arguments: tuple[str, ...] = (),
    fastp_values: dict[str, int | bool] | None = None,
) -> Path:
    config_path = workflow_dir / "config" / "config.yaml"
    source_lines = config_path.read_text(encoding="utf-8").splitlines()
    software_lines: set[int] = set()
    in_software = False
    for index, line in enumerate(source_lines):
        if line and not line[0].isspace():
            in_software = line.strip() == "software:"
        elif in_software and line.strip():
            software_lines.add(index)
    fastp_executable = next(
        (
            _yaml_scalar(source_lines[index])
            for index in software_lines
            if source_lines[index].strip().startswith("fastp:")
        ),
        None,
    )
    if fastp_values and not fastp_executable:
        raise ExecutionSpecError("workflow1 fastp executable is not configured")
    fastp_wrapper = (
        _write_fastp_wrapper(
            workflow_dir,
            python=python,
            executable=str(fastp_executable),
            arguments=fastp_arguments,
            values=fastp_values,
        )
        if fastp_values
        else None
    )
    lines = []
    for index, line in enumerate(source_lines):
        if line.startswith("sample_list:"):
            lines.append(f'sample_list: "{workflow_dir / "data" / "samples.txt"}"')
        elif line.startswith("output_dir:"):
            lines.append(f'output_dir: "{results_dir}"')
        elif index in software_lines and line.strip().startswith("seqkit:"):
            indent = line[: len(line) - len(line.lstrip())]
            lines.append(f'{indent}seqkit: "{seqkit}"')
        elif (
            fastp_wrapper
            and index in software_lines
            and line.strip().startswith("fastp:")
        ):
            indent = line[: len(line) - len(line.lstrip())]
            lines.append(f'{indent}fastp: "{fastp_wrapper}"')
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
                    root / "cleanreads_r1.fq.gz",
                    root / "cleanreads_r2.fq.gz",
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
    phase_rules: tuple[str, ...] = (),
    fastp_arguments: tuple[str, ...] = (),
    fastp_values: dict[str, int | bool] | None = None,
) -> tuple[PreparedPhase, list[str]]:
    workflow_dir = run_dir / "workflows" / phase_id
    results_dir = run_dir / "results" / phase_id
    results_dir.mkdir(parents=True, exist_ok=True)
    _copy_workflow(settings.workflow1_source, workflow_dir)
    run_ids, sample_ids = _prepare_workflow1_inputs(spec, workflow_dir, results_dir)
    config_path = _rewrite_workflow1_config(
        workflow_dir,
        results_dir,
        seqkit=settings.seqkit,
        python=settings.python,
        fastp_arguments=fastp_arguments,
        fastp_values=fastp_values,
    )
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
        selected_rules=phase_rules,
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


def _workflow_stages(spec: dict[str, Any], workflow_id: str) -> list[dict[str, Any]]:
    return [stage for stage in spec["stages"] if stage["workflow_id"] == workflow_id]


def _phase_rules(stages: list[dict[str, Any]]) -> tuple[str, ...]:
    return tuple(rule for stage in stages for rule in stage["rules"])


def _context_item(
    context: dict[str, dict[str, Any]], artifact_type: str
) -> dict[str, Any]:
    item = context.get(artifact_type)
    if item is None:
        raise ExecutionSpecError(f"compiled artifact is unavailable: {artifact_type}")
    return item


def _rewrite_top_level(config_path: Path, replacements: dict[str, str]) -> Path:
    lines = []
    for line in config_path.read_text(encoding="utf-8").splitlines():
        key = line.split(":", 1)[0] if line and not line[0].isspace() else None
        lines.append(replacements.get(key, line))
    config_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return config_path


def _materialize_workspace(source: str, destination: Path) -> None:
    root = Path(source).resolve()
    if not root.is_dir():
        raise ExecutionSpecError(f"workflow input directory does not exist: {root}")
    for path in root.rglob("*"):
        relative = path.relative_to(root)
        target = destination / relative
        if path.is_dir():
            target.mkdir(parents=True, exist_ok=True)
        elif path.is_file() and not target.exists():
            _link(path, target)


def _copy_sample_list(source: str, workflow_dir: Path) -> Path:
    source_path = _source(source)
    destination = workflow_dir / "data/samples.txt"
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source_path, destination)
    return destination


def _sample_ids_from_list(path: Path) -> set[str]:
    return {
        line.split()[0]
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.split()
    }


def _new_phase(
    settings: AdapterSettings,
    run_dir: Path,
    *,
    phase_id: str,
    workflow_id: str,
    source: Path,
    targets: list[Path],
    rules: tuple[str, ...],
    configure: Any,
) -> PreparedPhase:
    workflow_dir = run_dir / "workflows" / phase_id
    results_dir = run_dir / "results" / phase_id
    results_dir.mkdir(parents=True, exist_ok=True)
    _copy_workflow(source, workflow_dir)
    config_path = configure(workflow_dir, results_dir)
    return PreparedPhase(
        id=phase_id,
        workflow_id=workflow_id,
        workflow_dir=workflow_dir,
        results_dir=results_dir,
        command=_snakemake_command(
            settings,
            run_dir=run_dir,
            phase_id=phase_id,
            workflow_dir=workflow_dir,
            config_path=config_path,
            targets=[str(path) for path in targets],
        ),
        selected_rules=rules,
    )


def _prepare_pipeline_workflow1(
    spec: dict[str, Any],
    settings: AdapterSettings,
    run_dir: Path,
    context: dict[str, dict[str, Any]],
) -> tuple[PreparedPhase, dict[str, dict[str, Any]]]:
    stages = _workflow_stages(spec, "workflow1")
    first, last = stages[0], stages[-1]
    input_type = next(iter(first["bindings"].values()))
    source_input = _context_item(context, input_type)
    entry_rule = {
        "raw_paired_reads": "fastp",
        "qc_paired_reads": "bowtie2",
        "clean_run_paired_reads": "merge",
        "clean_sample_paired_reads": "megahit",
        "assembled_contigs": "rename_filter",
    }[input_type]
    exit_rule = {
        "read_qc": "fastp",
        "rrna_filter": "bowtie2",
        "sample_merge": "merge",
        "assembly": "megahit",
        "contig_filter": "rename_filter",
    }[last["id"]]
    records = []
    for record in source_input["records"]:
        files = record["files"]
        if input_type == "assembled_contigs":
            records.append({"sample_id": record["sample_id"], "path": files["fasta"]})
        else:
            records.append(
                {
                    "run_id": record.get("run_id", record["sample_id"]),
                    "sample_id": record["sample_id"],
                    "read1": files["read1"],
                    "read2": files["read2"],
                }
            )
    legacy = {"entry_rule": entry_rule, "exit_rule": exit_rule, "records": records}
    phase, sample_ids = _prepare_workflow1_phase(
        legacy,
        settings,
        run_dir,
        phase_id="upstream",
        phase_rules=_phase_rules(stages),
        fastp_arguments=capability_parameter_arguments(
            spec["options"].get("capability_parameters", []),
            FASTP_CAPABILITY_ID,
        ),
        fastp_values=capability_parameter_values(
            spec["options"].get("capability_parameters", []),
            FASTP_CAPABILITY_ID,
        ),
    )
    run_ids = [record.get("run_id", record["sample_id"]) for record in records]
    produced: dict[str, dict[str, Any]] = {}
    if any(stage["id"] == "read_qc" for stage in stages):
        produced["qc_paired_reads"] = {
            "type": "qc_paired_reads",
            "records": [
                {
                    "run_id": record["run_id"],
                    "sample_id": record["sample_id"],
                    "files": {
                        "read1": str(
                            phase.results_dir
                            / "filter"
                            / record["run_id"]
                            / "01_fastp_r1.fq.gz"
                        ),
                        "read2": str(
                            phase.results_dir
                            / "filter"
                            / record["run_id"]
                            / "01_fastp_r2.fq.gz"
                        ),
                    },
                }
                for record in records
            ],
        }
    if any(stage["id"] == "rrna_filter" for stage in stages):
        produced["clean_run_paired_reads"] = {
            "type": "clean_run_paired_reads",
            "records": [
                {
                    "run_id": run_id,
                    "sample_id": next(
                        record["sample_id"]
                        for record in records
                        if record.get("run_id", record["sample_id"]) == run_id
                    ),
                    "files": {
                        "read1": str(
                            phase.results_dir
                            / "filter"
                            / run_id
                            / "cleanreads_r1.fq.gz"
                        ),
                        "read2": str(
                            phase.results_dir
                            / "filter"
                            / run_id
                            / "cleanreads_r2.fq.gz"
                        ),
                    },
                }
                for run_id in dict.fromkeys(run_ids)
            ],
        }
    if any(stage["id"] == "sample_merge" for stage in stages):
        produced["clean_sample_paired_reads"] = {
            "type": "clean_sample_paired_reads",
            "records": [
                {
                    "sample_id": sample_id,
                    "files": {
                        "read1": str(
                            phase.results_dir
                            / "merge"
                            / sample_id
                            / "cleanreads_r1.fq.gz"
                        ),
                        "read2": str(
                            phase.results_dir
                            / "merge"
                            / sample_id
                            / "cleanreads_r2.fq.gz"
                        ),
                    },
                }
                for sample_id in sample_ids
            ],
        }
    if any(stage["id"] == "assembly" for stage in stages):
        produced["assembled_contigs"] = {
            "type": "assembled_contigs",
            "records": [
                {
                    "sample_id": sample_id,
                    "files": {
                        "fasta": str(
                            phase.results_dir
                            / "assembly"
                            / sample_id
                            / f"{sample_id}.contigs.fa"
                        )
                    },
                }
                for sample_id in sample_ids
            ],
        }
    if any(stage["id"] == "contig_filter" for stage in stages):
        produced["filtered_contigs"] = {
            "type": "filtered_contigs",
            "records": [
                {
                    "sample_id": sample_id,
                    "files": {
                        "fasta": str(
                            phase.results_dir
                            / "assembly"
                            / sample_id
                            / f"{sample_id}.contigs.1kb.renamed.fa"
                        )
                    },
                }
                for sample_id in sample_ids
            ],
        }
    return phase, produced


def _workflow2_targets(stage_id: str, results: Path, viral_type: str) -> list[Path]:
    if stage_id == "virus_sequence_prep":
        return [
            results / "Assembly/orfs.full.faa",
            results / "Assembly/contigs.full.fna",
        ]
    if stage_id == "virus_candidate_search":
        types = ("rna", "dna") if viral_type == "RNA+DNA" else (viral_type.lower(),)
        return [
            results / f"output_{kind}/Iteration_5/hmmsearch.domtblout" for kind in types
        ]
    if stage_id == "virus_candidate_extract":
        return [
            results / "downstream/sequences/coverage.csv",
            results
            / "downstream/sequences/putative_viralcontigs/putative_viralcontigs.fna",
            results
            / "downstream/sequences/putative_markerproteins/putative_markerproteins.faa",
        ]
    if stage_id == "virus_evidence":
        return [
            results / "downstream/cluster/cluster_summary.csv",
            results / "downstream/sequences/checkv/quality_summary.tsv",
            results / "downstream/blast/nr/nr_result.csv",
            results / "downstream/blast/nt/nt_result.csv",
            results / "downstream/blast/vhdb/vhdb_result.csv",
            results / "downstream/blast/hmmsearch_RdRpscan/rdrpscan_result.csv",
        ]
    return [
        results / "summary/sequences/rep_vert_viruses.fna",
        results / "summary/sequences/vert_viruses.fna",
        results / "summary/tables/taxonomy.tsv",
    ]


def _prepare_pipeline_workflow2(
    spec: dict[str, Any],
    settings: AdapterSettings,
    run_dir: Path,
    context: dict[str, dict[str, Any]],
) -> tuple[PreparedPhase, dict[str, dict[str, Any]]]:
    stages = _workflow_stages(spec, "workflow2")
    first, last = stages[0], stages[-1]
    input_type = next(iter(first["bindings"].values()))
    source_input = _context_item(context, input_type)
    viral_type = spec["options"]["viral_type"]

    def configure(workflow_dir: Path, results_dir: Path) -> Path:
        input_path = workflow_dir / "data/contigs_list.txt"
        input_path.parent.mkdir(parents=True, exist_ok=True)
        rename_contigs = input_type in {"contigs_fasta", "assembled_contigs"}
        if input_type in {"filtered_contigs", "contigs_fasta", "assembled_contigs"}:
            records = source_input["records"]
            if rename_contigs:
                lines = [
                    f"{record['sample_id']} "
                    f"{_source(record['files']['fasta'], must_exist=not source_input.get('_generated'))}"
                    for record in records
                ]
            else:
                lines = [
                    str(
                        _source(
                            record["files"]["fasta"],
                            must_exist=not source_input.get("_generated"),
                        )
                    )
                    for record in records
                ]
        elif input_type == "virus_prepared_sequences":
            files = source_input["records"][0]["files"]
            contigs = _source(files["contigs"])
            orfs = _source(files["orfs"])
            _link(contigs, results_dir / "Assembly/contigs.full.fna")
            _link(orfs, results_dir / "Assembly/orfs.full.faa")
            _link(
                contigs,
                results_dir / "Assembly/rename_contigs/input.renamed_contigs.fna",
            )
            _link(orfs, results_dir / "Assembly/getorf/input.orfs.faa")
            lines = [str(contigs)]
            rename_contigs = False
        elif input_type == "virus_candidates":
            files = source_input["records"][0]["files"]
            _link(
                _source(files["contigs"]),
                results_dir
                / "downstream/sequences/putative_viralcontigs/putative_viralcontigs.fna",
            )
            _link(
                _source(files["proteins"]),
                results_dir
                / "downstream/sequences/putative_markerproteins/putative_markerproteins.faa",
            )
            _link(
                _source(files["coverage"]),
                results_dir / "downstream/sequences/coverage.csv",
            )
            _link(
                _source(files["nucl2prot"]),
                results_dir / "downstream/sequences/nucl2prot.tsv",
            )
            lines = [str(_source(files["contigs"]))]
            rename_contigs = False
        else:
            files = source_input["records"][0]["files"]
            _materialize_workspace(files["workspace"], results_dir)
            candidate = results_dir / "Assembly/contigs.full.fna"
            lines = [str(candidate)]
            rename_contigs = False
        input_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return _rewrite_workflow2_config(
            workflow_dir,
            results_dir,
            viral_type=viral_type,
            rename_contigs=rename_contigs,
        )

    phase = _new_phase(
        settings,
        run_dir,
        phase_id="virus-identification",
        workflow_id="workflow2",
        source=settings.workflow2_source,
        targets=_workflow2_targets(
            last["id"], run_dir / "results/virus-identification", viral_type
        ),
        rules=_phase_rules(stages),
        configure=configure,
    )
    workspace_record = {"files": {"workspace": str(phase.results_dir)}}
    produced = {
        "virus_prepared_sequences": {
            "type": "virus_prepared_sequences",
            "records": [
                {
                    "files": {
                        "contigs": str(phase.results_dir / "Assembly/contigs.full.fna"),
                        "orfs": str(phase.results_dir / "Assembly/orfs.full.faa"),
                    }
                }
            ],
        },
        "virus_search_results": {
            "type": "virus_search_results",
            "records": [workspace_record],
        },
        "virus_candidates": {
            "type": "virus_candidates",
            "records": [
                {
                    "files": {
                        "contigs": str(
                            phase.results_dir
                            / "downstream/sequences/putative_viralcontigs/putative_viralcontigs.fna"
                        ),
                        "proteins": str(
                            phase.results_dir
                            / "downstream/sequences/putative_markerproteins/putative_markerproteins.faa"
                        ),
                        "coverage": str(
                            phase.results_dir / "downstream/sequences/coverage.csv"
                        ),
                        "nucl2prot": str(
                            phase.results_dir / "downstream/sequences/nucl2prot.tsv"
                        ),
                    }
                }
            ],
        },
        "virus_evidence_bundle": {
            "type": "virus_evidence_bundle",
            "records": [workspace_record],
        },
        "virus_summary_bundle": {
            "type": "virus_summary_bundle",
            "records": [
                {
                    "files": {
                        "fasta": str(
                            phase.results_dir / "summary/sequences/vert_viruses.fna"
                        ),
                        "table": str(phase.results_dir / "summary/tables/viruses.xlsx"),
                        "taxonomy": str(
                            phase.results_dir / "summary/tables/taxonomy.tsv"
                        ),
                    }
                }
            ],
        },
    }
    return phase, {stage["produces"]: produced[stage["produces"]] for stage in stages}


def _paired_records(item: dict[str, Any]) -> list[dict[str, Any]]:
    return item["records"]


def _contig_records(item: dict[str, Any]) -> list[dict[str, Any]]:
    return item["records"]


def _prepare_pipeline_workflow3(
    spec: dict[str, Any],
    settings: AdapterSettings,
    run_dir: Path,
    context: dict[str, dict[str, Any]],
) -> tuple[PreparedPhase, dict[str, dict[str, Any]]]:
    stages = _workflow_stages(spec, "workflow3")
    first = stages[0]

    def configure(workflow_dir: Path, results_dir: Path) -> Path:
        later = [
            value for value in first["bindings"].values() if value.endswith("_bundle")
        ]
        if later:
            sample_lists = []
            for artifact_type in later:
                bundle = _context_item(context, artifact_type)["records"][0]["files"]
                _materialize_workspace(bundle["workspace"], results_dir)
                sample_lists.append(_source(bundle["sample_list"]))
            if any(
                _sample_ids_from_list(path) != _sample_ids_from_list(sample_lists[0])
                for path in sample_lists[1:]
            ):
                raise ExecutionSpecError("host bundle sample sets must match")
            sample_list = _copy_sample_list(str(sample_lists[0]), workflow_dir)
        else:
            paired_type = next(
                (
                    value
                    for stage in stages
                    for value in stage["bindings"].values()
                    if "paired_reads" in value
                ),
                None,
            )
            contig_type = next(
                (
                    value
                    for stage in stages
                    for value in stage["bindings"].values()
                    if value
                    in {"filtered_contigs", "assembled_contigs", "contigs_fasta"}
                ),
                None,
            )
            paired = (
                _paired_records(_context_item(context, paired_type))
                if paired_type
                else []
            )
            contigs = (
                _contig_records(_context_item(context, contig_type))
                if contig_type
                else []
            )
            paired_by_sample = {record["sample_id"]: record for record in paired}
            contigs_by_sample = {record["sample_id"]: record for record in contigs}
            sample_ids = sorted(set(paired_by_sample) | set(contigs_by_sample))
            lines = []
            for sample_id in sample_ids:
                pair = paired_by_sample.get(sample_id)
                contig = contigs_by_sample.get(sample_id)
                fallback = (pair or contig)["files"]
                read1 = pair["files"]["read1"] if pair else fallback["fasta"]
                read2 = pair["files"]["read2"] if pair else fallback["fasta"]
                fasta = contig["files"]["fasta"] if contig else read1
                lines.append(f"{sample_id} {read1} {read2} {fasta}")
            sample_list = workflow_dir / "data/samples.txt"
            sample_list.parent.mkdir(parents=True, exist_ok=True)
            sample_list.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return _rewrite_top_level(
            workflow_dir / "config/config.yaml",
            {
                "samplelist": f'samplelist: "{sample_list}"',
                "outdir": f'outdir: "{results_dir}"',
            },
        )

    target_map = {
        "host_mapping": ["EukaMarker_Mapping/1.sortedbam/{sample}_sorted.bam"],
        "host_coverage": [
            "EukaMarker_Mapping/2.filteredcoverage_q0/{sample}_coverage.txt"
        ],
        "host_blast": ["EukaMarker_megablast/3.check_milestone/{sample}.done"],
        "host_integrate": ["depths.txt", "blastresult.full.txt"],
        "host_summary": [
            "HostIdentification_result.xlsx",
            "CYTB.fasta",
            "COI.fasta",
            "ND1.fasta",
        ],
    }
    sample_ids: list[str] = []
    for item in context.values():
        sample_ids.extend(
            record.get("sample_id")
            for record in item.get("records", [])
            if record.get("sample_id")
        )
    sample_ids = list(dict.fromkeys(sample_ids)) or ["input"]
    targets: list[Path] = []
    for stage in stages:
        if stage["is_deliverable"] or stage == stages[-1]:
            for template in target_map[stage["id"]]:
                if "{sample}" in template:
                    targets.extend(
                        run_dir
                        / "results/host-identification"
                        / template.format(sample=sample)
                        for sample in sample_ids
                    )
                else:
                    targets.append(run_dir / "results/host-identification" / template)
    phase = _new_phase(
        settings,
        run_dir,
        phase_id="host-identification",
        workflow_id="workflow3",
        source=settings.workflow3_source,
        targets=targets,
        rules=_phase_rules(stages),
        configure=configure,
    )
    workspace = {
        "files": {
            "workspace": str(phase.results_dir),
            "sample_list": str(phase.workflow_dir / "data/samples.txt"),
        }
    }
    produced = {
        "host_alignment_bundle": {
            "type": "host_alignment_bundle",
            "records": [workspace],
        },
        "host_coverage_bundle": {
            "type": "host_coverage_bundle",
            "records": [workspace],
        },
        "host_blast_bundle": {"type": "host_blast_bundle", "records": [workspace]},
        "host_evidence_bundle": {
            "type": "host_evidence_bundle",
            "records": [workspace],
        },
        "host_summary_bundle": {
            "type": "host_summary_bundle",
            "records": [
                {
                    "files": {
                        "report": str(
                            phase.results_dir / "HostIdentification_result.xlsx"
                        )
                    }
                }
            ],
        },
    }
    return phase, {stage["produces"]: produced[stage["produces"]] for stage in stages}


def _prepare_pipeline_workflow4(
    spec: dict[str, Any],
    settings: AdapterSettings,
    run_dir: Path,
    context: dict[str, dict[str, Any]],
) -> tuple[PreparedPhase, dict[str, dict[str, Any]]]:
    stages = _workflow_stages(spec, "workflow4")
    first = stages[0]

    def configure(workflow_dir: Path, results_dir: Path) -> Path:
        input_type = next(iter(first["bindings"].values()))
        item = _context_item(context, input_type)
        if input_type in {"kraken_profiles", "bracken_profiles"}:
            files = item["records"][0]["files"]
            _materialize_workspace(files["workspace"], results_dir)
            sample_list = _copy_sample_list(files["sample_list"], workflow_dir)
        else:
            lines = [
                f"{record['sample_id']} {record['files']['read1']} {record['files']['read2']}"
                for record in item["records"]
            ]
            sample_list = workflow_dir / "data/samples.txt"
            sample_list.parent.mkdir(parents=True, exist_ok=True)
            sample_list.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return _rewrite_top_level(
            workflow_dir / "config/config.yaml",
            {
                "sample_list": f'sample_list: "{sample_list}"',
                "outdir": f'outdir: "{results_dir}"',
            },
        )

    sample_ids = [
        record.get("sample_id")
        for item in context.values()
        for record in item.get("records", [])
        if record.get("sample_id")
    ]
    sample_ids = list(dict.fromkeys(sample_ids)) or ["input"]
    target_map = {
        "microbe_classify": [
            f"results/kraken2/{sample}.report" for sample in sample_ids
        ],
        "microbe_abundance": [
            f"results/bracken/{sample}_{level}.txt"
            for sample in sample_ids
            for level in ("species", "genus", "family")
        ],
        "microbe_summary": [
            f"results/summary/combined_{level}.txt"
            for level in ("species", "genus", "family")
        ],
    }
    targets = [
        run_dir / "results/microbial-profile" / value
        for value in target_map[stages[-1]["id"]]
    ]
    phase = _new_phase(
        settings,
        run_dir,
        phase_id="microbial-profile",
        workflow_id="workflow4",
        source=settings.workflow4_source,
        targets=targets,
        rules=_phase_rules(stages),
        configure=configure,
    )
    workspace = {
        "files": {
            "workspace": str(phase.results_dir),
            "sample_list": str(phase.workflow_dir / "data/samples.txt"),
        }
    }
    produced = {
        "kraken_profiles": {"type": "kraken_profiles", "records": [workspace]},
        "bracken_profiles": {"type": "bracken_profiles", "records": [workspace]},
        "microbe_summary_bundle": {
            "type": "microbe_summary_bundle",
            "records": [{"files": {"workspace": str(phase.results_dir)}}],
        },
    }
    return phase, {stage["produces"]: produced[stage["produces"]] for stage in stages}


def _reference_path(item: dict[str, Any]) -> str:
    files = item["records"][0]["files"]
    return files.get("fasta") or files.get("contigs")


def _prepare_pipeline_workflow5(
    spec: dict[str, Any],
    settings: AdapterSettings,
    run_dir: Path,
    context: dict[str, dict[str, Any]],
) -> tuple[PreparedPhase, dict[str, dict[str, Any]]]:
    stages = _workflow_stages(spec, "workflow5")
    first = stages[0]

    def configure(workflow_dir: Path, results_dir: Path) -> Path:
        if first["id"] == "virus_read_mapping":
            paired_type, reference_type = first["bindings"].values()
            paired = _context_item(context, paired_type)
            reference = _context_item(context, reference_type)
            lines = [
                f"{record['sample_id']} {record['files']['read1']} {record['files']['read2']}"
                for record in paired["records"]
            ]
            sample_list = workflow_dir / "data/samples.txt"
            sample_list.parent.mkdir(parents=True, exist_ok=True)
            sample_list.write_text("\n".join(lines) + "\n", encoding="utf-8")
            reference_path = _reference_path(reference)
        else:
            input_type = next(iter(first["bindings"].values()))
            files = _context_item(context, input_type)["records"][0]["files"]
            _materialize_workspace(files["workspace"], results_dir)
            sample_list = _copy_sample_list(files["sample_list"], workflow_dir)
            reference_path = files.get("reference") or str(
                results_dir / "reference.fna"
            )
            if not Path(reference_path).exists():
                _placeholder(results_dir, Path(reference_path))
        return _rewrite_top_level(
            workflow_dir / "config/config.yaml",
            {
                "sample_list": f'sample_list: "{sample_list}"',
                "reference_fasta": f'reference_fasta: "{reference_path}"',
                "outdir": f'outdir: "{results_dir}"',
            },
        )

    target_map = {
        "virus_read_mapping": [
            "results/summary/cleanreads_count.txt",
            "results/coverage/{sample}.samtools.coverage",
        ],
        "virus_coverage": ["results/coverage/filtered.coverage"],
        "virus_rpm": ["results/summary/rpm.csv", "results/summary/summary.tsv"],
    }
    sample_ids = [
        record.get("sample_id")
        for item in context.values()
        for record in item.get("records", [])
        if record.get("sample_id")
    ]
    sample_ids = list(dict.fromkeys(sample_ids)) or ["input"]
    targets = []
    for template in target_map[stages[-1]["id"]]:
        if "{sample}" in template:
            targets.extend(
                run_dir
                / "results/virus-quantification"
                / template.format(sample=sample)
                for sample in sample_ids
            )
        else:
            targets.append(run_dir / "results/virus-quantification" / template)
    phase = _new_phase(
        settings,
        run_dir,
        phase_id="virus-quantification",
        workflow_id="workflow5",
        source=settings.workflow5_source,
        targets=targets,
        rules=_phase_rules(stages),
        configure=configure,
    )
    if first["id"] == "virus_read_mapping":
        reference_type = list(first["bindings"].values())[1]
        reference = _reference_path(_context_item(context, reference_type))
    else:
        reference = _context_item(context, next(iter(first["bindings"].values())))[
            "records"
        ][0]["files"].get("reference")
    workspace = {
        "files": {
            "workspace": str(phase.results_dir),
            "sample_list": str(phase.workflow_dir / "data/samples.txt"),
            "reference": reference,
        }
    }
    produced = {
        "virus_mapping_bundle": {
            "type": "virus_mapping_bundle",
            "records": [workspace],
        },
        "virus_coverage_bundle": {
            "type": "virus_coverage_bundle",
            "records": [
                {
                    "files": {
                        "workspace": str(phase.results_dir),
                        "sample_list": str(phase.workflow_dir / "data/samples.txt"),
                    }
                }
            ],
        },
        "virus_quantification_bundle": {
            "type": "virus_quantification_bundle",
            "records": [
                {
                    "files": {
                        "rpm": str(phase.results_dir / "results/summary/rpm.csv"),
                        "summary": str(
                            phase.results_dir / "results/summary/summary.tsv"
                        ),
                    }
                }
            ],
        },
    }
    return phase, {stage["produces"]: produced[stage["produces"]] for stage in stages}


def _prepare_pipeline_workflow6(
    spec: dict[str, Any],
    settings: AdapterSettings,
    run_dir: Path,
    context: dict[str, dict[str, Any]],
) -> tuple[PreparedPhase, dict[str, dict[str, Any]]]:
    stages = _workflow_stages(spec, "workflow6")
    first = stages[0]

    def configure(workflow_dir: Path, results_dir: Path) -> Path:
        generated_taxonomy = False
        if first["id"] == "virus_quality":
            virus_type, taxonomy_type = first["bindings"].values()
            input_fasta = _reference_path(_context_item(context, virus_type))
            taxonomy_item = _context_item(context, taxonomy_type)
            taxonomy = taxonomy_item["records"][0]["files"]["taxonomy"]
            generated_taxonomy = bool(taxonomy_item.get("_generated"))
        else:
            input_type = next(iter(first["bindings"].values()))
            files = _context_item(context, input_type)["records"][0]["files"]
            _materialize_workspace(files["workspace"], results_dir)
            input_fasta = str(results_dir / "results/checkv/filtered_viral_gt50.fna")
            taxonomy = files["taxonomy"]
        if not generated_taxonomy:
            taxonomy_source = _source(taxonomy)
            taxonomy = workflow_dir / "data/taxonomy.tsv"
            taxonomy.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(taxonomy_source, taxonomy)
        return _rewrite_top_level(
            workflow_dir / "config/config.yaml",
            {
                "input_fasta": f'input_fasta: "{input_fasta}"',
                "taxonomy_tsv": f'taxonomy_tsv: "{taxonomy}"',
                "outdir": f'outdir: "{results_dir}"',
            },
        )

    target_map = {
        "virus_quality": ["results/checkv/filtered_viral_gt50.fna"],
        "annotation_strategy": [
            "results/summary/strategy.tsv",
            "results/structure/vigor_input.fna",
            "results/structure/prodigal_input.fna",
        ],
        "structure_annotation": ["results/structure/all_predicted_proteins.faa"],
        "functional_annotation": ["results/function/dramv/annotations.tsv"],
        "annotation_delivery": [
            "results/summary/annotation_summary.csv",
            "results/summary/consistency_report.tsv",
            "results/genbank/DONE",
            "results/review/review_ids.txt",
        ],
    }
    targets = [
        run_dir / "results/virus-annotation" / value
        for value in target_map[stages[-1]["id"]]
    ]
    phase = _new_phase(
        settings,
        run_dir,
        phase_id="virus-annotation",
        workflow_id="workflow6",
        source=settings.workflow6_source,
        targets=targets,
        rules=_phase_rules(stages),
        configure=configure,
    )
    workspace = {
        "files": {
            "workspace": str(phase.results_dir),
            "taxonomy": str(phase.workflow_dir / "data/taxonomy.tsv"),
        }
    }
    produced = {
        "virus_quality_bundle": {
            "type": "virus_quality_bundle",
            "records": [workspace],
        },
        "annotation_strategy_bundle": {
            "type": "annotation_strategy_bundle",
            "records": [workspace],
        },
        "structure_annotation_bundle": {
            "type": "structure_annotation_bundle",
            "records": [workspace],
        },
        "functional_annotation_bundle": {
            "type": "functional_annotation_bundle",
            "records": [workspace],
        },
        "annotation_delivery_bundle": {
            "type": "annotation_delivery_bundle",
            "records": [workspace],
        },
    }
    return phase, {stage["produces"]: produced[stage["produces"]] for stage in stages}


PHASE_PREPARERS = {
    "workflow1": _prepare_pipeline_workflow1,
    "workflow2": _prepare_pipeline_workflow2,
    "workflow3": _prepare_pipeline_workflow3,
    "workflow4": _prepare_pipeline_workflow4,
    "workflow5": _prepare_pipeline_workflow5,
    "workflow6": _prepare_pipeline_workflow6,
}


def prepare_execution(
    state: dict[str, Any],
    settings: AdapterSettings | None = None,
    *,
    execution_spec: dict[str, Any] | None = None,
) -> PreparedExecution:
    settings = settings or AdapterSettings.from_env()
    spec = normalize_execution_spec(execution_spec or state.get("execution_spec"))
    if spec["policy"].get("adaptive") and execution_spec is None:
        raise ExecutionSpecError(
            "adaptive execution must select a route before preparation"
        )
    expected_rules = (
        selected_rules(spec)
        if execution_spec is not None
        else validate_plan_rules(spec, state.get("steps") or [])
    )
    run_dir = Path(state["run_dir"]).resolve()
    (run_dir / "logs").mkdir(parents=True, exist_ok=True)
    context = {item["type"]: item for item in spec["inputs"]}
    phases: list[PreparedPhase] = []
    for workflow_id, prepare_phase in PHASE_PREPARERS.items():
        if not _workflow_stages(spec, workflow_id):
            continue
        phase, produced = prepare_phase(spec, settings, run_dir, context)
        phases.append(phase)
        for artifact in produced.values():
            artifact["_generated"] = True
        context.update(produced)
    return PreparedExecution(
        workflow_id="analysis_pipeline",
        phases=tuple(phases),
        seqkit=settings.seqkit,
        selected_rules=expected_rules,
    )

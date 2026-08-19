"""Bounded 5% rapid screen used by adaptive virus analysis."""

from __future__ import annotations

import gzip
import os
import shlex
import subprocess
from pathlib import Path
from typing import Any, Iterator, TextIO

from adaptive_routing import aggregate_screening, parse_kraken_report, screening_metrics
from workflow_adapters import AdapterSettings


SAMPLE_FRACTION = 0.05
SAMPLE_SEED = 17


def _load_simple_yaml(path: Path) -> dict[str, Any]:
    root: dict[str, Any] = {}
    stack: list[tuple[int, dict[str, Any]]] = [(-1, root)]
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.split("#", 1)[0].rstrip()
        if not line.strip() or ":" not in line:
            continue
        indent = len(line) - len(line.lstrip())
        key, raw_value = line.strip().split(":", 1)
        while stack[-1][0] >= indent:
            stack.pop()
        parent = stack[-1][1]
        value = raw_value.strip().strip("\"'")
        if value:
            parent[key] = value
        else:
            child: dict[str, Any] = {}
            parent[key] = child
            stack.append((indent, child))
    return root


def _open_text(path: Path) -> TextIO:
    return (
        gzip.open(path, "rt", encoding="utf-8", errors="replace")
        if path.suffix == ".gz"
        else path.open("r", encoding="utf-8", errors="replace")
    )


def _fastq_ids(path: Path) -> Iterator[str]:
    with _open_text(path) as handle:
        while header := handle.readline():
            sequence = handle.readline()
            plus = handle.readline()
            quality = handle.readline()
            if (
                not sequence
                or not plus.startswith("+")
                or not quality
                or not header.startswith("@")
            ):
                raise ValueError(f"invalid FASTQ record in {path}")
            yield header[1:].split()[0].removesuffix("/1").removesuffix("/2")


def validate_paired_fastq(read1: Path, read2: Path) -> int:
    count = 0
    left = _fastq_ids(read1)
    right = _fastq_ids(read2)
    while True:
        first = next(left, None)
        second = next(right, None)
        if first is None or second is None:
            if first != second:
                raise ValueError("sampled FASTQ mates contain different record counts")
            return count
        if first != second:
            raise ValueError(f"sampled FASTQ mate IDs differ: {first} != {second}")
        count += 1


def _run(command: list[str], log: TextIO) -> None:
    log.write(f"$ {shlex.join(command)}\n")
    log.flush()
    subprocess.run(command, check=True, stdout=log, stderr=subprocess.STDOUT, text=True)


def _rrna_filter(
    bowtie2: str,
    samtools: str,
    database: str,
    read1: Path,
    read2: Path,
    output1: Path,
    output2: Path,
    threads: int,
    log: TextIO,
) -> None:
    aligner_command = [
        bowtie2,
        "--local",
        "--threads",
        str(threads),
        "-1",
        str(read1),
        "-2",
        str(read2),
        "-x",
        database,
    ]
    converter_command = [
        samtools,
        "fastq",
        "-f",
        "12",
        "-1",
        str(output1),
        "-2",
        str(output2),
        "-@",
        str(max(1, threads - 1)),
    ]
    log.write(f"$ {shlex.join(aligner_command)} | {shlex.join(converter_command)}\n")
    log.flush()
    aligner = subprocess.Popen(aligner_command, stdout=subprocess.PIPE, stderr=log)
    assert aligner.stdout is not None
    converter = subprocess.Popen(
        converter_command, stdin=aligner.stdout, stdout=subprocess.DEVNULL, stderr=log
    )
    aligner.stdout.close()
    converter_code = converter.wait()
    aligner_code = aligner.wait()
    if aligner_code or converter_code:
        raise subprocess.CalledProcessError(
            aligner_code or converter_code, aligner_command
        )


def run_rapid_screen(
    spec: dict[str, Any], run_dir: Path, *, threads: int = 4
) -> dict[str, Any]:
    settings = AdapterSettings.from_env()
    workflow1 = Path(
        os.getenv("BIOAGENT_WORKFLOW1_SOURCE", str(settings.workflow1_source))
    )
    workflow4 = Path(
        os.getenv(
            "BIOAGENT_WORKFLOW4_SOURCE",
            "/data/user/guojindan/Projects/Workflows/workflow4_kraken_bracken",
        )
    )
    upstream = _load_simple_yaml(workflow1 / "config/config.yaml")
    kraken = _load_simple_yaml(workflow4 / "config/config.yaml")
    software = upstream.get("software") or {}
    kraken_software = kraken.get("software") or {}
    database = upstream.get("database") or {}
    params = upstream.get("params") or {}
    kraken_params = kraken.get("params") or {}

    kraken_db = os.getenv(
        "BIOAGENT_SCREEN_KRAKEN_DB",
        "/data/user/guojindan/Databases/Kraken/Viral_20260226",
    )
    output_root = run_dir / "results/screening"
    log_path = run_dir / ".bioagent/screening.log"
    sample_buckets: dict[str, dict[str, Any]] = {}
    with log_path.open("w", encoding="utf-8") as log:
        for record in spec["inputs"][0]["data"]["runs"]:
            sample_id = record["sample_id"]
            sample_dir = output_root / record["run_id"]
            sample_dir.mkdir(parents=True, exist_ok=True)
            sampled1 = sample_dir / "sampled_r1.fastq.gz"
            sampled2 = sample_dir / "sampled_r2.fastq.gz"
            _run(
                [
                    settings.seqkit,
                    "sample",
                    "--proportion",
                    str(SAMPLE_FRACTION),
                    "--rand-seed",
                    str(SAMPLE_SEED),
                    "--out-file",
                    str(sampled1),
                    record["read1"],
                ],
                log,
            )
            _run(
                [
                    settings.seqkit,
                    "sample",
                    "--proportion",
                    str(SAMPLE_FRACTION),
                    "--rand-seed",
                    str(SAMPLE_SEED),
                    "--out-file",
                    str(sampled2),
                    record["read2"],
                ],
                log,
            )
            sampled_pairs = validate_paired_fastq(sampled1, sampled2)

            fastp1 = sample_dir / "fastp_r1.fastq.gz"
            fastp2 = sample_dir / "fastp_r2.fastq.gz"
            _run(
                [
                    str(software["fastp"]),
                    *shlex.split(str(params.get("fastp") or "")),
                    "--thread",
                    str(threads),
                    "-i",
                    str(sampled1),
                    "-I",
                    str(sampled2),
                    "-o",
                    str(fastp1),
                    "-O",
                    str(fastp2),
                    "--json",
                    str(sample_dir / "fastp.json"),
                    "--html",
                    str(sample_dir / "fastp.html"),
                ],
                log,
            )
            clean1 = sample_dir / "clean_r1.fastq.gz"
            clean2 = sample_dir / "clean_r2.fastq.gz"
            _rrna_filter(
                str(software["bowtie2"]),
                str(software["samtools"]),
                str(database["rrna_bowtie2"]),
                fastp1,
                fastp2,
                clean1,
                clean2,
                threads,
                log,
            )
            filtered_pairs = validate_paired_fastq(clean1, clean2)
            report = sample_dir / "kraken.report"
            _run(
                [
                    str(kraken_software["kraken2"]),
                    "--db",
                    kraken_db,
                    "--paired",
                    "--gzip-compressed",
                    "--output",
                    str(sample_dir / "kraken.output"),
                    "--report",
                    str(report),
                    "--confidence",
                    str(kraken_params.get("kraken2_confidence") or "0.3"),
                    "--threads",
                    str(threads),
                    str(clean1),
                    str(clean2),
                ],
                log,
            )
            parsed = parse_kraken_report(report)
            bucket = sample_buckets.setdefault(
                sample_id,
                {
                    "sampled_pairs": 0,
                    "post_filter_pairs": 0,
                    "viral_reads": 0,
                    "species": {},
                    "report_paths": [],
                },
            )
            bucket["sampled_pairs"] += sampled_pairs
            bucket["post_filter_pairs"] += filtered_pairs
            bucket["viral_reads"] += int(parsed.get("viral_reads") or 0)
            bucket["report_paths"].append(str(report))
            for species in parsed.get("species") or []:
                key = (species["taxid"], species["name"])
                bucket["species"][key] = bucket["species"].get(key, 0) + int(
                    species["reads"]
                )
    sample_results = []
    for sample_id, bucket in sample_buckets.items():
        metrics = screening_metrics(
            {
                "viral_reads": bucket["viral_reads"],
                "species": [
                    {"taxid": taxid, "name": name, "reads": reads}
                    for (taxid, name), reads in bucket["species"].items()
                ],
            },
            sample_id=sample_id,
            sampled_pairs=bucket["sampled_pairs"],
        )
        metrics["post_filter_read_pairs"] = bucket["post_filter_pairs"]
        metrics["post_filter_ratio"] = (
            bucket["post_filter_pairs"] / bucket["sampled_pairs"]
            if bucket["sampled_pairs"]
            else 0.0
        )
        metrics["report_paths"] = bucket["report_paths"]
        sample_results.append(metrics)
    result = aggregate_screening(sample_results)
    result["database"] = kraken_db
    result["log_path"] = str(log_path)
    return result

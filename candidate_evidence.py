"""Read-only Workflow 2 candidate evidence extraction.

The module deliberately has no persistence or report integration.  Callers receive
validated CandidateEvidenceV2 values; the CLI exposes aggregate diagnostics only.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Literal

from evidence_contract import (
    CandidateEvidenceV2,
    EvidenceIssueV2,
    EvidenceSourceV2,
    ObservationV2,
)


class CandidateEvidenceParseError(ValueError):
    """Raised when a Workflow 2 result cannot be parsed without ambiguity."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class MetricSpec:
    column: str
    metric: str
    source_id: str
    unit: Literal["ratio", "percent", "count", "bp"]
    population: str
    source_group: str
    tool_id: str
    reference_id: str | None = None
    denominator_description: str | None = None
    required: bool = False
    absolute_tolerance: Decimal = Decimal(0)


@dataclass(frozen=True)
class Workflow2EvidenceResult:
    run_id: str
    sources: tuple[EvidenceArtifact, ...]
    candidates: tuple[CandidateEvidenceV2, ...]


@dataclass(frozen=True)
class EvidenceArtifact:
    artifact_type: str
    path: str
    sha256: str
    bytes: int
    rows: int


@dataclass(frozen=True)
class SourceSpec:
    source_id: str
    artifact_type: str
    relative_path: str
    delimiter: str
    key_column: str
    required_columns: tuple[str, ...]


@dataclass(frozen=True)
class ParsedSource:
    artifact: EvidenceArtifact
    rows: dict[str, tuple[int, dict[str, str]]]


METRICS = (
    MetricSpec(
        "qlen",
        "contig_length",
        "nr",
        "bp",
        "candidate_contig",
        "workflow2.nr",
        "blastx",
        "workflow2.nr",
        required=True,
    ),
    MetricSpec(
        "Profile_cov",
        "hmm_profile_coverage",
        "coverage",
        "ratio",
        "best_marker_profile",
        "workflow2.marker_search",
        "hmmsearch",
        "workflow2.rdrpscan_hmm",
        "HMM profile length",
        absolute_tolerance=Decimal("1e-15"),
    ),
    MetricSpec(
        "completeness",
        "checkv_completeness",
        "checkv",
        "percent",
        "candidate_contig",
        "workflow2.checkv",
        "checkv",
        "workflow2.checkv_db",
        "CheckV estimated complete genome or provirus length",
    ),
    MetricSpec(
        "contamination",
        "checkv_contamination",
        "checkv",
        "percent",
        "candidate_contig",
        "workflow2.checkv",
        "checkv",
        "workflow2.checkv_db",
        "CheckV estimated candidate contig length",
    ),
    MetricSpec(
        "nr_pident",
        "nr_amino_acid_identity",
        "nr",
        "percent",
        "best_nr_alignment",
        "workflow2.nr",
        "blastx",
        "workflow2.nr",
        "aligned amino-acid positions",
    ),
    MetricSpec(
        "approval_rate",
        "nr_family_vote_fraction",
        "nr",
        "ratio",
        "classified_nr_hits",
        "workflow2.nr_vote",
        "workflow2.parse_blastresult",
        "workflow2.nr",
        "sum of bitscores for classified hits within 75% of the best classified bitscore",
    ),
    MetricSpec(
        "gene_count",
        "checkv_gene_count",
        "checkv",
        "count",
        "candidate_contig",
        "workflow2.checkv",
        "checkv",
        "workflow2.checkv_db",
    ),
    MetricSpec(
        "viral_genes",
        "checkv_viral_gene_count",
        "checkv",
        "count",
        "candidate_contig",
        "workflow2.checkv",
        "checkv",
        "workflow2.checkv_db",
    ),
    MetricSpec(
        "host_genes",
        "checkv_host_gene_count",
        "checkv",
        "count",
        "candidate_contig",
        "workflow2.checkv",
        "checkv",
        "workflow2.checkv_db",
    ),
)
REQUIRED_COLUMNS = {"qseqid", *(metric.column for metric in METRICS)}
MISSING_VALUES = {"", "-", "na", "n/a", "nan", "none", "null"}
SOURCE_SPECS = (
    SourceSpec(
        "summary",
        "workflow2_virus_candidates",
        "summary/tables/virus_candidates.csv",
        ",",
        "qseqid",
        tuple(sorted(REQUIRED_COLUMNS)),
    ),
    SourceSpec(
        "coverage",
        "workflow2_marker_coverage",
        "downstream/sequences/coverage.csv",
        ",",
        "qseqid",
        ("qseqid", "Profile_cov"),
    ),
    SourceSpec(
        "checkv",
        "workflow2_checkv_quality",
        "downstream/sequences/checkv/quality_summary.tsv",
        "\t",
        "contig_id",
        (
            "contig_id",
            "completeness",
            "contamination",
            "gene_count",
            "viral_genes",
            "host_genes",
        ),
    ),
    SourceSpec(
        "nr",
        "workflow2_nr_summary",
        "downstream/blast/nr/nr_result.csv",
        ",",
        "qseqid",
        ("qseqid", "qlen", "nr_pident", "approval_rate"),
    ),
)


def _under(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _read_stable(path: Path) -> tuple[bytes, str]:
    if path.is_symlink():
        raise CandidateEvidenceParseError(
            "EVIDENCE_SOURCE_INVALID", "candidate table must not be a symbolic link"
        )
    try:
        before = path.stat()
        raw = path.read_bytes()
        after = path.stat()
    except OSError as exc:
        raise CandidateEvidenceParseError(
            "EVIDENCE_SOURCE_UNAVAILABLE", "candidate table is unavailable"
        ) from exc
    identity_before = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    identity_after = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    if identity_before != identity_after or len(raw) != before.st_size:
        raise CandidateEvidenceParseError(
            "EVIDENCE_SOURCE_CHANGED", "candidate table changed while being read"
        )
    return raw, hashlib.sha256(raw).hexdigest()


def _execution_spec(state: dict[str, Any]) -> dict[str, Any]:
    for value in (
        state.get("selected_execution_spec"),
        state.get("execution_spec"),
        (state.get("plan") or {}).get("execution_spec")
        if isinstance(state.get("plan"), dict)
        else None,
    ):
        if isinstance(value, dict):
            return value
    return {}


def _sample_ids(state: dict[str, Any]) -> tuple[str, ...]:
    values: list[str] = []
    for bundle in _execution_spec(state).get("inputs") or []:
        if not isinstance(bundle, dict):
            continue
        records = bundle.get("records")
        if not isinstance(records, list):
            data = bundle.get("data") if isinstance(bundle.get("data"), dict) else {}
            records = [
                *data.get("runs", []),
                *data.get("samples", []),
            ]
        for record in records:
            if isinstance(record, dict) and isinstance(record.get("sample_id"), str):
                sample_id = record["sample_id"].strip()
                if sample_id and sample_id not in values:
                    values.append(sample_id)
    return tuple(sorted(values, key=lambda value: (-len(value), value)))


def _sample_for_candidate(candidate_id: str, samples: tuple[str, ...]) -> str:
    matches = [
        sample
        for sample in samples
        if candidate_id == sample
        or candidate_id.startswith((f"{sample}-", f"{sample}_"))
    ]
    if not matches:
        raise CandidateEvidenceParseError(
            "EVIDENCE_SAMPLE_UNRESOLVED",
            "candidate ID cannot be mapped to a registered sample",
        )
    longest = len(matches[0])
    equally_specific = [sample for sample in matches if len(sample) == longest]
    if len(equally_specific) != 1:
        raise CandidateEvidenceParseError(
            "EVIDENCE_SAMPLE_AMBIGUOUS",
            "candidate ID maps to more than one registered sample",
        )
    return matches[0]


def _attempt(state: dict[str, Any]) -> int | Literal["unknown"]:
    attempts = state.get("attempts")
    if isinstance(attempts, list) and attempts:
        number = attempts[-1].get("number") if isinstance(attempts[-1], dict) else None
        if isinstance(number, int) and number > 0:
            return number
    return "unknown"


def _workflow2_results(state: dict[str, Any], run_dir: Path) -> Path:
    phases = [
        phase
        for phase in state.get("phases") or []
        if isinstance(phase, dict)
        and phase.get("workflow_id") == "workflow2"
        and phase.get("status") == "completed"
    ]
    if len(phases) != 1 or not isinstance(phases[0].get("results_dir"), str):
        raise CandidateEvidenceParseError(
            "EVIDENCE_PHASE_UNAVAILABLE",
            "exactly one completed Workflow 2 phase is required",
        )
    results_dir = Path(phases[0]["results_dir"])
    if not results_dir.is_absolute():
        results_dir = run_dir / results_dir
    try:
        resolved = results_dir.resolve(strict=True)
    except OSError as exc:
        raise CandidateEvidenceParseError(
            "EVIDENCE_PHASE_UNAVAILABLE", "Workflow 2 result directory is unavailable"
        ) from exc
    if not _under(resolved, run_dir):
        raise CandidateEvidenceParseError(
            "EVIDENCE_PATH_OUTSIDE_RUN", "Workflow 2 result directory is outside run root"
        )
    return resolved


def _decimal(raw: Any, *, column: str, required: bool) -> Decimal | None:
    value = str(raw or "").strip()
    if value.lower() in MISSING_VALUES:
        if required:
            raise CandidateEvidenceParseError(
                "EVIDENCE_REQUIRED_VALUE_MISSING", f"required metric {column} is missing"
            )
        return None
    try:
        number = Decimal(value)
    except InvalidOperation as exc:
        raise CandidateEvidenceParseError(
            "EVIDENCE_VALUE_INVALID", f"metric {column} is not numeric"
        ) from exc
    if not number.is_finite():
        raise CandidateEvidenceParseError(
            "EVIDENCE_VALUE_INVALID", f"metric {column} is not finite"
        )
    return number


def _parse_source(spec: SourceSpec, results_dir: Path, run_dir: Path) -> ParsedSource:
    candidate = results_dir / spec.relative_path
    if candidate.is_symlink():
        raise CandidateEvidenceParseError(
            "EVIDENCE_SOURCE_INVALID", "evidence source must not be a symbolic link"
        )
    try:
        path = candidate.resolve(strict=True)
    except OSError as exc:
        raise CandidateEvidenceParseError(
            "EVIDENCE_SOURCE_UNAVAILABLE", "Workflow 2 evidence source is unavailable"
        ) from exc
    if not _under(path, run_dir):
        raise CandidateEvidenceParseError(
            "EVIDENCE_PATH_OUTSIDE_RUN", "evidence source is outside run root"
        )
    raw, source_sha256 = _read_stable(path)
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeError as exc:
        raise CandidateEvidenceParseError(
            "EVIDENCE_SOURCE_INVALID", "evidence source is not UTF-8"
        ) from exc
    reader = csv.DictReader(io.StringIO(text, newline=""), delimiter=spec.delimiter)
    columns = set(reader.fieldnames or [])
    missing_columns = sorted(set(spec.required_columns) - columns)
    if missing_columns:
        raise CandidateEvidenceParseError(
            "EVIDENCE_SCHEMA_INVALID",
            "evidence source is missing required columns: "
            + ", ".join(missing_columns),
        )
    rows: dict[str, tuple[int, dict[str, str]]] = {}
    for row_index, row in enumerate(reader, start=2):
        key = str(row.get(spec.key_column) or "").strip()
        if not key:
            raise CandidateEvidenceParseError(
                "EVIDENCE_CANDIDATE_INVALID", "evidence source candidate ID is missing"
            )
        if key in rows:
            code = (
                "EVIDENCE_CANDIDATE_DUPLICATE"
                if spec.source_id == "summary"
                else "EVIDENCE_PROVENANCE_DUPLICATE"
            )
            raise CandidateEvidenceParseError(
                code, "evidence source candidate ID is duplicated"
            )
        rows[key] = (row_index, row)
    artifact = EvidenceArtifact(
        artifact_type=spec.artifact_type,
        path=path.relative_to(run_dir).as_posix(),
        sha256=source_sha256,
        bytes=len(raw),
        rows=len(rows),
    )
    return ParsedSource(artifact=artifact, rows=rows)


def parse_workflow2_candidate_evidence(state_path: Path) -> Workflow2EvidenceResult:
    """Parse a completed Workflow 2 candidate table without writing any files."""

    try:
        state_path = state_path.resolve(strict=True)
        state = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise CandidateEvidenceParseError(
            "EVIDENCE_STATE_INVALID", "run state is unavailable or invalid"
        ) from exc
    if not isinstance(state, dict) or state_path.parent.name != ".bioagent":
        raise CandidateEvidenceParseError(
            "EVIDENCE_STATE_INVALID", "state must be a run-local .bioagent file"
        )
    run_dir = state_path.parent.parent.resolve()
    registered_run_dir = state.get("run_dir")
    if isinstance(registered_run_dir, str) and Path(registered_run_dir).resolve() != run_dir:
        raise CandidateEvidenceParseError(
            "EVIDENCE_STATE_INVALID", "state run_dir does not match its location"
        )
    run_id = state.get("run_id")
    if not isinstance(run_id, str) or not run_id:
        raise CandidateEvidenceParseError(
            "EVIDENCE_STATE_INVALID", "state run_id is required"
        )
    samples = _sample_ids(state)
    if not samples:
        raise CandidateEvidenceParseError(
            "EVIDENCE_SAMPLE_UNRESOLVED", "execution spec has no registered samples"
        )
    results_dir = _workflow2_results(state, run_dir)
    parsed_sources = {
        spec.source_id: _parse_source(spec, results_dir, run_dir)
        for spec in SOURCE_SPECS
    }
    summary_source = parsed_sources["summary"]
    attempt = _attempt(state)
    candidates: list[CandidateEvidenceV2] = []
    for candidate_id, (_row_index, row) in summary_source.rows.items():
        sample_id = _sample_for_candidate(candidate_id, samples)

        observations: list[ObservationV2] = []
        issues: list[EvidenceIssueV2] = []
        for metric in METRICS:
            source = parsed_sources[metric.source_id]
            source_match = source.rows.get(candidate_id)
            if source_match is None:
                raise CandidateEvidenceParseError(
                    "EVIDENCE_PROVENANCE_MISSING",
                    "candidate is missing from an upstream evidence source",
                )
            source_row_index, source_row = source_match
            final_number = _decimal(
                row.get(metric.column), column=metric.column, required=metric.required
            )
            source_number = _decimal(
                source_row.get(metric.column),
                column=metric.column,
                required=metric.required,
            )
            numbers_match = final_number == source_number
            if final_number is not None and source_number is not None:
                numbers_match = (
                    abs(final_number - source_number) <= metric.absolute_tolerance
                )
            if not numbers_match:
                raise CandidateEvidenceParseError(
                    "EVIDENCE_PROVENANCE_MISMATCH",
                    "candidate summary does not match its upstream evidence source",
                )
            value = (
                None
                if source_number is None
                else int(source_number)
                if source_number == source_number.to_integral_value()
                else float(source_number)
            )
            if value is not None and metric.unit == "ratio" and not 0 <= value <= 1:
                raise CandidateEvidenceParseError(
                    "EVIDENCE_VALUE_INVALID",
                    f"metric {metric.column} must be between 0 and 1",
                )
            if value is not None and metric.unit == "percent" and not 0 <= value <= 100:
                raise CandidateEvidenceParseError(
                    "EVIDENCE_VALUE_INVALID",
                    f"metric {metric.column} must be between 0 and 100",
                )
            evidence_id = f"E-W2-{metric.metric.upper().replace('_', '-')}"
            evidence_source = EvidenceSourceV2(
                artifact_type=source.artifact.artifact_type,
                artifact_path=source.artifact.path,
                locator=(
                    f"row:{source_row_index};column:{metric.column};"
                    f"key:{next(spec.key_column for spec in SOURCE_SPECS if spec.source_id == metric.source_id)}"
                ),
                sha256=source.artifact.sha256,
                tool_id=metric.tool_id,
                tool_version="unknown",
                **(
                    {
                        "reference_id": metric.reference_id,
                        "reference_version": "unknown",
                    }
                    if metric.reference_id
                    else {}
                ),
            )
            observation = ObservationV2(
                evidence_id=evidence_id,
                metric=metric.metric,
                value=value,
                unit=metric.unit,
                population=metric.population,
                source_group=metric.source_group,
                denominator_description=metric.denominator_description,
                source=evidence_source,
                missing_reason=(
                    f"{metric.column} is absent from the Workflow 2 summary row"
                    if value is None
                    else None
                ),
            )
            observations.append(observation)
            if value is None:
                issues.append(
                    EvidenceIssueV2(
                        issue_id=f"MISSING-{metric.metric.upper().replace('_', '-')}",
                        kind="missing",
                        message=f"Workflow 2 did not provide {metric.metric}.",
                        evidence_ids=[evidence_id],
                    )
                )

        issues.append(
            EvidenceIssueV2(
                issue_id="LIMITATION-DEPENDENCY-IDENTITY",
                kind="limitation",
                message=(
                    "Workflow 2 tool and reference versions are not frozen in this "
                    "historical result; unknown identities must not support a formal conclusion."
                ),
                evidence_ids=[item.evidence_id for item in observations],
            )
        )
        candidates.append(
            CandidateEvidenceV2(
                run_id=run_id,
                attempt=attempt,
                report_scope="full_analysis",
                sample_id=sample_id,
                candidate_id=candidate_id,
                candidate_kind="virus_contig",
                observations=observations,
                issues=issues,
            )
        )

    return Workflow2EvidenceResult(
        run_id=run_id,
        sources=tuple(parsed_sources[spec.source_id].artifact for spec in SOURCE_SPECS),
        candidates=tuple(candidates),
    )


def summary(result: Workflow2EvidenceResult) -> dict[str, Any]:
    missing = sum(
        issue.kind == "missing"
        for candidate in result.candidates
        for issue in candidate.issues
    )
    limitations = sum(
        issue.kind == "limitation"
        for candidate in result.candidates
        for issue in candidate.issues
    )
    return {
        "schema_version": 2,
        "run_id": result.run_id,
        "sources": [
            {
                "artifact_type": source.artifact_type,
                "path": source.path,
                "sha256": source.sha256,
                "bytes": source.bytes,
                "rows": source.rows,
            }
            for source in result.sources
        ],
        "candidate_count": len(result.candidates),
        "observation_count": sum(
            len(candidate.observations) for candidate in result.candidates
        ),
        "missing_issue_count": missing,
        "limitation_issue_count": limitations,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", required=True, type=Path)
    parser.add_argument("--summary", action="store_true")
    args = parser.parse_args(argv)
    if not args.summary:
        parser.error("only aggregate --summary output is available")
    try:
        result = parse_workflow2_candidate_evidence(args.state)
    except CandidateEvidenceParseError as exc:
        print(json.dumps({"status": "failed", "code": exc.code}, sort_keys=True))
        return 1
    print(json.dumps(summary(result), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

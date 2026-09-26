from __future__ import annotations

import csv
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from candidate_evidence import (
    METRICS,
    CandidateEvidenceParseError,
    _read_stable,
    main,
    parse_workflow2_candidate_evidence,
    summary,
)


def _row(candidate_id: str = "sample-a-contig-1") -> dict[str, str]:
    return {
        "qseqid": candidate_id,
        "qlen": "1234",
        "Profile_cov": "0.75",
        "completeness": "98.5",
        "contamination": "0.2",
        "nr_pident": "91.2",
        "approval_rate": "0.8",
        "gene_count": "8",
        "viral_genes": "7",
        "host_genes": "1",
    }


def _run(
    root: Path,
    rows: list[dict[str, str]],
    *,
    legacy: bool = False,
    results_dir: Path | None = None,
) -> Path:
    run_dir = root / "analysis-test"
    bioagent = run_dir / ".bioagent"
    bioagent.mkdir(parents=True)
    results = results_dir or run_dir / "results" / "virus-identification"
    table = results / "summary" / "tables" / "virus_candidates.csv"
    table.parent.mkdir(parents=True, exist_ok=True)
    with table.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(
            stream, fieldnames=["qseqid", *(metric.column for metric in METRICS)]
        )
        writer.writeheader()
        writer.writerows(rows)
    records = (
        {"data": {"runs": [{"sample_id": "sample-a"}]}}
        if legacy
        else {"records": [{"sample_id": "sample-a"}]}
    )
    state = {
        "run_id": "analysis-test",
        "run_dir": str(run_dir),
        "execution_spec": {"inputs": [{"id": "primary", **records}]},
        "phases": [
            {
                "id": "classification",
                "workflow_id": "workflow2",
                "status": "completed",
                "results_dir": str(results),
            }
        ],
    }
    state_path = bioagent / "state.json"
    state_path.write_text(json.dumps(state), encoding="utf-8")
    return state_path


def test_parser_builds_provenanced_workflow2_candidate(tmp_path: Path) -> None:
    result = parse_workflow2_candidate_evidence(_run(tmp_path, [_row()]))

    assert result.source_rows == 1
    assert len(result.source_sha256) == 64
    candidate = result.candidates[0]
    assert candidate.sample_id == "sample-a"
    assert candidate.candidate_kind == "virus_contig"
    assert candidate.report_scope == "full_analysis"
    assert len(candidate.observations) == len(METRICS)
    profile = next(
        item for item in candidate.observations if item.metric == "hmm_profile_coverage"
    )
    assert profile.value == 0.75
    assert profile.denominator_description == "HMM profile length"
    assert profile.source is not None
    assert profile.source.artifact_path.endswith("virus_candidates.csv")
    assert profile.source.locator == "row:2;column:Profile_cov;key:qseqid"
    assert {issue.kind for issue in candidate.issues} == {"limitation"}


def test_parser_supports_legacy_runs_and_preserves_missing_values(tmp_path: Path) -> None:
    row = _row()
    row["completeness"] = "-"
    result = parse_workflow2_candidate_evidence(
        _run(tmp_path, [row], legacy=True)
    )

    candidate = result.candidates[0]
    completeness = next(
        item for item in candidate.observations if item.metric == "checkv_completeness"
    )
    assert completeness.value is None
    assert completeness.missing_reason
    assert {issue.kind for issue in candidate.issues} == {"missing", "limitation"}
    aggregate = summary(result)
    assert aggregate["candidate_count"] == 1
    assert aggregate["missing_issue_count"] == 1


@pytest.mark.parametrize(
    ("mutate", "code"),
    [
        (lambda rows: rows.append(dict(rows[0])), "EVIDENCE_CANDIDATE_DUPLICATE"),
        (
            lambda rows: rows[0].update({"qseqid": "unknown-contig"}),
            "EVIDENCE_SAMPLE_UNRESOLVED",
        ),
        (
            lambda rows: rows[0].update({"Profile_cov": "not-a-number"}),
            "EVIDENCE_VALUE_INVALID",
        ),
        (
            lambda rows: rows[0].update({"Profile_cov": "1.1"}),
            "EVIDENCE_VALUE_INVALID",
        ),
    ],
)
def test_parser_fails_closed_on_ambiguous_or_invalid_rows(
    tmp_path: Path, mutate, code: str
) -> None:
    rows = [_row()]
    mutate(rows)

    with pytest.raises(CandidateEvidenceParseError) as raised:
        parse_workflow2_candidate_evidence(_run(tmp_path, rows))

    assert raised.value.code == code


def test_parser_rejects_missing_schema_columns(tmp_path: Path) -> None:
    state_path = _run(tmp_path, [_row()])
    table = (
        state_path.parent.parent
        / "results/virus-identification/summary/tables/virus_candidates.csv"
    )
    table.write_text("qseqid,qlen\nsample-a-contig-1,1234\n", encoding="utf-8")

    with pytest.raises(CandidateEvidenceParseError) as raised:
        parse_workflow2_candidate_evidence(state_path)

    assert raised.value.code == "EVIDENCE_SCHEMA_INVALID"


def test_parser_rejects_results_outside_run(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    state_path = _run(tmp_path, [_row()], results_dir=outside)

    with pytest.raises(CandidateEvidenceParseError) as raised:
        parse_workflow2_candidate_evidence(state_path)

    assert raised.value.code == "EVIDENCE_PATH_OUTSIDE_RUN"


def test_stable_read_rejects_source_change(tmp_path: Path) -> None:
    path = tmp_path / "table.csv"
    path.write_text("value\n", encoding="utf-8")
    current = path.stat()
    before = SimpleNamespace(
        st_dev=current.st_dev,
        st_ino=current.st_ino,
        st_size=current.st_size,
        st_mtime_ns=current.st_mtime_ns,
        st_mode=current.st_mode,
    )
    after = SimpleNamespace(
        st_dev=current.st_dev,
        st_ino=current.st_ino,
        st_size=current.st_size,
        st_mtime_ns=current.st_mtime_ns + 1,
        st_mode=current.st_mode,
    )

    with (
        patch.object(Path, "stat", side_effect=[before, before, after]),
        pytest.raises(CandidateEvidenceParseError) as raised,
    ):
        _read_stable(path)

    assert raised.value.code == "EVIDENCE_SOURCE_CHANGED"


def test_cli_prints_aggregate_only(tmp_path: Path, capsys) -> None:
    state_path = _run(tmp_path, [_row()])

    assert main(["--state", str(state_path), "--summary"]) == 0
    output = json.loads(capsys.readouterr().out)

    assert output["candidate_count"] == 1
    assert "sample-a-contig-1" not in json.dumps(output)

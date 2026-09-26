from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from evidence_contract import CandidateEvidence, CandidateEvidenceV2

FIXTURE = Path(__file__).parents[1] / "benchmarks" / "candidate_evidence_v2.json"


def _fixture() -> dict:
    return json.loads(FIXTURE.read_text())


def _observation(payload: dict, evidence_id: str) -> dict:
    return next(
        item for item in payload["observations"] if item["evidence_id"] == evidence_id
    )


def test_candidate_evidence_v2_fixture_is_small_and_valid() -> None:
    evidence = CandidateEvidenceV2.model_validate(_fixture())

    assert FIXTURE.stat().st_size < 100 * 1024
    assert evidence.report_scope == "sampled_screening"
    assert len(evidence.observations) == 8
    assert {issue.kind for issue in evidence.issues} == {"limitation"}


def test_candidate_evidence_v1_remains_readable() -> None:
    evidence = CandidateEvidence.model_validate(
        {
            "schema_version": 1,
            "run_id": "run-1",
            "sample_id": "sample-1",
            "candidate_id": "sample-1:taxon:1",
            "taxid": "1",
            "name": "root",
            "observations": [
                {
                    "evidence_id": "E-1",
                    "metric": "read_pairs",
                    "value": 0,
                    "unit": "read_pairs",
                    "population": "input",
                    "source_group": "source",
                    "source_path": "artifact.json",
                    "locator": "$.count",
                    "sha256": "0" * 64,
                }
            ],
            "tools": {"reader": "1"},
            "reference_id": "none",
        }
    )

    assert evidence.schema_version == 1
    assert evidence.observations[0].value == 0


def test_v2_distinguishes_zero_from_missing() -> None:
    zero = _fixture()
    _observation(zero, "E-SAMPLED-READS")["value"] = 0
    assert CandidateEvidenceV2.model_validate(zero).observations[0].value == 0

    missing = copy.deepcopy(zero)
    _observation(missing, "E-SAMPLED-READS")["value"] = None
    with pytest.raises(ValidationError, match="missing values require missing_reason"):
        CandidateEvidenceV2.model_validate(missing)

    _observation(missing, "E-SAMPLED-READS")["missing_reason"] = (
        "artifact field absent"
    )
    assert CandidateEvidenceV2.model_validate(missing).observations[0].value is None


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (
            lambda data: _observation(data, "E-SAMPLE-FRACTION").update(
                {"denominator_evidence_id": "E-VIRAL"}
            ),
            "denominator must be the second ratio input",
        ),
        (
            lambda data: (
                _observation(data, "E-SAMPLE-FRACTION")["derivation"].update(
                    {"input_evidence_ids": ["E-CANDIDATE", "E-UNKNOWN"]}
                ),
                _observation(data, "E-SAMPLE-FRACTION").update(
                    {"denominator_evidence_id": "E-UNKNOWN"}
                ),
            ),
            "unknown evidence references",
        ),
        (
            lambda data: _observation(data, "E-SAMPLED-READS")["source"].update(
                {"sha256": "not-a-sha256"}
            ),
            "String should match pattern",
        ),
        (
            lambda data: _observation(data, "E-SAMPLED-READS").update(
                {"formula": "value / 2"}
            ),
            "Extra inputs are not permitted",
        ),
    ],
)
def test_v2_rejects_invalid_provenance_and_arbitrary_formula(mutation, message: str) -> None:
    payload = _fixture()
    mutation(payload)

    with pytest.raises(ValidationError, match=message):
        CandidateEvidenceV2.model_validate(payload)


def test_v2_rejects_duplicate_ids_and_unknown_issue_references() -> None:
    duplicate = _fixture()
    _observation(duplicate, "E-FILTERED")["evidence_id"] = "E-SAMPLED"
    with pytest.raises(ValidationError, match="evidence_id must be unique"):
        CandidateEvidenceV2.model_validate(duplicate)

    unknown_issue = _fixture()
    unknown_issue["issues"][0]["evidence_ids"] = ["E-UNKNOWN"]
    with pytest.raises(ValidationError, match="unknown issue evidence references"):
        CandidateEvidenceV2.model_validate(unknown_issue)


def test_v2_rejects_derivation_cycles() -> None:
    payload = _fixture()
    first = _observation(payload, "E-FILTERED")
    second = _observation(payload, "E-VIRAL")
    first.pop("source")
    second.pop("source")
    first["derivation"] = {"operation": "sum", "input_evidence_ids": ["E-VIRAL"]}
    second["derivation"] = {"operation": "sum", "input_evidence_ids": ["E-FILTERED"]}

    with pytest.raises(ValidationError, match="contains a cycle"):
        CandidateEvidenceV2.model_validate(payload)


def test_v2_can_express_conflict_missing_and_limitation_issues() -> None:
    payload = _fixture()
    payload["issues"] = [
        {
            "issue_id": "ISSUE-CONFLICT",
            "kind": "conflict",
            "message": "two source groups disagree",
            "evidence_ids": ["E-SAMPLED", "E-FILTERED"],
        },
        {
            "issue_id": "ISSUE-MISSING",
            "kind": "missing",
            "message": "a required metric is absent",
            "evidence_ids": [],
        },
        {
            "issue_id": "ISSUE-LIMIT",
            "kind": "limitation",
            "message": "sampled screening only",
            "evidence_ids": ["E-SAMPLED"],
        },
    ]

    evidence = CandidateEvidenceV2.model_validate(payload)
    assert {issue.kind for issue in evidence.issues} == {
        "conflict",
        "missing",
        "limitation",
    }

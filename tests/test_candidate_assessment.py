from __future__ import annotations

import copy
import json
import shutil
from pathlib import Path

import pytest
from test_candidate_evidence_parser import _row, _run

from candidate_assessment import (
    ASSET_ROOT,
    AssessmentError,
    assess_candidate,
    load_engineering_rules,
    load_release_set,
    main,
    parse_and_assess,
    resolve_release_set,
    summary,
)
from candidate_evidence import parse_workflow2_candidate_evidence
from evidence_contract import (
    AssetRelease,
    CandidateEvidenceV2,
    EvidenceIssueV2,
    ReleaseSet,
)
from workflow_catalog import CAPABILITY_MANIFESTS


def _assets(tmp_path: Path) -> Path:
    target = tmp_path / "agent" / "assets" / "m2"
    target.parent.parent.mkdir(parents=True)
    shutil.copytree(ASSET_ROOT, target)
    source = ASSET_ROOT.parent.parent
    for name in (
        "candidate_assessment.py",
        "candidate_evidence.py",
        "evidence_contract.py",
    ):
        shutil.copyfile(source / name, target.parent.parent / name)
    return target


def _state(state_path: Path) -> dict:
    return json.loads(state_path.read_text())


def _complete_state(state: dict, release_set: ReleaseSet) -> tuple[dict, ReleaseSet]:
    state = copy.deepcopy(state)
    manifests = sorted(
        (
            manifest
            for manifest in CAPABILITY_MANIFESTS.values()
            if manifest.id.startswith("workflow2.")
        ),
        key=lambda item: item.id,
    )
    stage_ids = {manifest.stage_id for manifest in manifests}
    state["execution_spec"].update(
        {
            "stages": [
                {"id": stage_id, "workflow_id": "workflow2"}
                for stage_id in sorted(stage_ids)
            ],
            "capability_contract": {
                "manifests": [
                    {
                        "capability_id": manifest.id,
                        "manifest_version": manifest.version,
                    }
                    for manifest in manifests
                ]
            },
        }
    )
    identity = {"sha256": "a" * 64}
    software_ids = {
        dependency
        for manifest in manifests
        for dependency in manifest.software_dependencies
    }
    reference_ids = {
        dependency
        for manifest in manifests
        for dependency in manifest.reference_dependencies
    }
    state["recovery_contract"] = {
        "dependencies_complete": True,
        "last_check": {"blocker_code": None},
        "dependencies": {
            "capability_ids": [manifest.id for manifest in manifests],
            "workflow": identity,
            "parser": identity,
            "software": {name: identity for name in software_ids},
            "references": {name: identity for name in reference_ids},
        },
    }
    releases = list(release_set.releases)
    releases.extend(
        AssetRelease(
            asset_id=reference_id,
            asset_type="reference",
            version="test",
            source="test",
            license="test",
            review_status="approved",
        )
        for reference_id in sorted(reference_ids)
    )
    return state, ReleaseSet(releases=releases)


def test_historical_w2_run_produces_only_not_generated_assessment(
    tmp_path: Path,
) -> None:
    state_path = _run(tmp_path, [{**_row(), "completeness": "-"}])

    result = parse_and_assess(state_path)

    assert result.release_resolution.status == "incomplete"
    assert len(result.assessments) == 1
    assessment = result.assessments[0]
    assert assessment.status == "not_generated"
    assert assessment.draft is None
    assert assessment.provider == assessment.model == "none"
    assert assessment.error_code == "DOMAIN_ASSETS_PENDING"
    assert {finding.rule_id for finding in assessment.findings} == {
        "EVIDENCE_MISSING",
        "DEPENDENCY_IDENTITY_INCOMPLETE",
        "CORRELATED_EVIDENCE",
        "DOMAIN_ASSETS_PENDING",
    }


def test_conflicts_are_preserved_and_unknown_evidence_is_rejected(
    tmp_path: Path,
) -> None:
    evidence = parse_workflow2_candidate_evidence(_run(tmp_path, [_row()]))
    candidate = evidence.candidates[0]
    candidate = candidate.model_copy(
        update={
            "issues": [
                *candidate.issues,
                EvidenceIssueV2(
                    issue_id="CONFLICT-TEST",
                    kind="conflict",
                    message="conflict",
                    evidence_ids=[candidate.observations[0].evidence_id],
                ),
            ]
        }
    )
    release_set = load_release_set()
    resolution = resolve_release_set(_state(_run(tmp_path / "second", [_row()])), release_set)
    rules = load_engineering_rules(ASSET_ROOT, release_set)

    assessment = assess_candidate(candidate, resolution, rules)

    conflict = next(
        finding
        for finding in assessment.findings
        if finding.rule_id == "EVIDENCE_CONFLICT"
    )
    assert conflict.evidence_ids == [candidate.observations[0].evidence_id]
    invalid = CandidateEvidenceV2.model_construct(
        **{key: value for key, value in candidate.__dict__.items() if key != "issues"},
        issues=[
            EvidenceIssueV2(
                issue_id="MISSING-TEST",
                kind="missing",
                message="missing",
                evidence_ids=["unknown-evidence"],
            )
        ],
    )
    with pytest.raises(AssessmentError, match="unknown evidence"):
        assess_candidate(invalid, resolution, rules)


@pytest.mark.parametrize("mutation", ["tamper", "duplicate", "missing"])
def test_release_loader_fails_closed(tmp_path: Path, mutation: str) -> None:
    assets = _assets(tmp_path)
    releases_path = assets / "releases.json"
    if mutation == "tamper":
        (assets / "engineering-rules-v1.json").write_text("{}")
    else:
        releases = json.loads(releases_path.read_text())
        if mutation == "duplicate":
            releases["releases"].append(copy.deepcopy(releases["releases"][0]))
        else:
            releases["releases"] = releases["releases"][:1]
        releases_path.write_text(json.dumps(releases))

    with pytest.raises(AssessmentError) as raised:
        load_release_set(assets)

    assert raised.value.code in {"RELEASE_HASH_MISMATCH", "RELEASE_INVALID"}


def test_release_resolution_distinguishes_complete_incomplete_and_unavailable(
    tmp_path: Path,
) -> None:
    state_path = _run(tmp_path, [_row()])
    state = _state(state_path)
    release_set = load_release_set()

    incomplete = resolve_release_set(state, release_set)

    assert incomplete.status == "incomplete"
    assert "CAPABILITY_IDENTITY_INCOMPLETE" in incomplete.limitation_codes
    unavailable_state = copy.deepcopy(state)
    unavailable_state["recovery_contract"] = {
        "last_check": {"blocker_code": "RECOVERY_CHECK_UNAVAILABLE"},
        "dependencies": {},
    }
    unavailable = resolve_release_set(unavailable_state, release_set)
    assert unavailable.status == "unavailable"
    assert "DEPENDENCY_IDENTITY_UNAVAILABLE" in unavailable.limitation_codes

    complete_state, complete_releases = _complete_state(state, release_set)
    mismatched_state = copy.deepcopy(complete_state)
    mismatched = mismatched_state["execution_spec"]["capability_contract"][
        "manifests"
    ][0]
    mismatched["manifest_version"] = 999
    mismatch = resolve_release_set(mismatched_state, complete_releases)
    assert mismatch.status == "incomplete"
    assert mismatched["capability_id"] in mismatch.missing_dependency_ids

    complete = resolve_release_set(complete_state, complete_releases)
    assert complete.status == "complete"
    assert not complete.missing_dependency_ids
    assert not complete.limitation_codes


def test_checksum_observation_never_promotes_reference_identity(
    tmp_path: Path,
) -> None:
    state = _state(_run(tmp_path, [_row()]))
    state["recovery_contract"] = {
        "dependencies_complete": False,
        "last_check": {"blocker_code": "RECOVERY_DEPENDENCY_IDENTITY_INCOMPLETE"},
        "dependencies": {
            "reference_observations": {
                "workflow2.nr": {
                    "source_checksums": [
                        {"algorithm": "md5", "digest": "0" * 32}
                    ]
                },
                "workflow2.nt": {
                    "source_checksums": [
                        {"algorithm": "md5", "digest": "1" * 32}
                    ]
                },
            }
        },
    }

    resolution = resolve_release_set(state, load_release_set())

    assert resolution.status == "incomplete"
    assert resolution.observed_checksum_ids == ["workflow2.nr", "workflow2.nt"]
    assert {"workflow2.nr", "workflow2.nt"} <= set(
        resolution.missing_dependency_ids
    )


def test_cli_summary_is_aggregate_only(tmp_path: Path, capsys) -> None:
    state_path = _run(tmp_path, [_row()])

    assert main(["--state", str(state_path), "--summary"]) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload == summary(parse_and_assess(state_path))
    assert payload["assessment_count"] == 1
    assert payload["assessment_status_counts"] == {"not_generated": 1}
    serialized = json.dumps(payload)
    assert "sample-a-contig-1" not in serialized
    assert "candidate_id" not in serialized
    assert "observations" not in serialized

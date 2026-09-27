"""Deterministic, read-only Workflow 2 engineering assessments.

This module deliberately cannot produce biological claims.  It converts validated
CandidateEvidenceV2 issues into bounded engineering findings and keeps every
Assessment in ``not_generated`` until governed domain assets are available.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from candidate_evidence import (
    CandidateEvidenceParseError,
    Workflow2EvidenceResult,
    parse_workflow2_candidate_evidence,
)
from candidate_evidence import (
    summary as evidence_summary,
)
from evidence_contract import (
    Assessment,
    CandidateEvidenceV2,
    ReleaseResolution,
    ReleaseSet,
    RuleFinding,
)
from workflow_catalog import CAPABILITY_MANIFESTS

ASSET_ROOT = Path(__file__).parent / "assets" / "m2"
CAPABILITY_RELEASE_ID = "candidate-evidence-v2"
RULE_RELEASE_ID = "b3-engineering-integrity-rules-v1"
RULE_IDS = (
    "EVIDENCE_MISSING",
    "EVIDENCE_CONFLICT",
    "DEPENDENCY_IDENTITY_INCOMPLETE",
    "CORRELATED_EVIDENCE",
    "DOMAIN_ASSETS_PENDING",
)
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class AssessmentError(ValueError):
    """Raised when a deterministic assessment cannot be trusted."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class Workflow2AssessmentResult:
    evidence: Workflow2EvidenceResult
    release_resolution: ReleaseResolution
    assessments: tuple[Assessment, ...]


def _digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _asset_file(root: Path, name: str) -> Path:
    relative = Path(name)
    if relative.is_absolute() or ".." in relative.parts or relative.name != name:
        raise AssessmentError("RELEASE_INVALID", "release file path is invalid")
    try:
        resolved_root = root.resolve(strict=True)
        candidate = root / relative
        if candidate.is_symlink():
            raise AssessmentError("RELEASE_INVALID", "release file cannot be a symlink")
        resolved = candidate.resolve(strict=True)
    except (FileNotFoundError, NotADirectoryError) as exc:
        raise AssessmentError("RELEASE_UNAVAILABLE", "release file is missing") from exc
    except OSError as exc:
        raise AssessmentError(
            "RELEASE_UNAVAILABLE", "release file is temporarily unavailable"
        ) from exc
    if resolved.parent != resolved_root or not resolved.is_file():
        raise AssessmentError("RELEASE_INVALID", "release file escapes its asset root")
    return resolved


def load_release_set(root: Path = ASSET_ROOT) -> ReleaseSet:
    """Load a small governed release set and verify every registered file."""

    release_path = _asset_file(root, "releases.json")
    try:
        release_set = ReleaseSet.model_validate_json(release_path.read_text())
    except OSError as exc:
        raise AssessmentError(
            "RELEASE_UNAVAILABLE", "release registry is temporarily unavailable"
        ) from exc
    except ValueError as exc:
        raise AssessmentError("RELEASE_INVALID", "release registry is invalid") from exc
    by_id = {release.asset_id: release for release in release_set.releases}
    required = {CAPABILITY_RELEASE_ID, RULE_RELEASE_ID}
    if not required <= by_id.keys():
        raise AssessmentError("RELEASE_INVALID", "required engineering releases are missing")
    for release in release_set.releases:
        if release.review_status != "approved":
            raise AssessmentError("RELEASE_INVALID", "engineering release is not approved")
        for name, expected in release.files.items():
            if not SHA256_RE.fullmatch(expected):
                raise AssessmentError("RELEASE_INVALID", "release hash is invalid")
            try:
                actual = _digest(_asset_file(root, name).read_bytes())
            except OSError as exc:
                raise AssessmentError(
                    "RELEASE_UNAVAILABLE", "release file is temporarily unavailable"
                ) from exc
            if actual != expected:
                raise AssessmentError("RELEASE_HASH_MISMATCH", "release file hash mismatch")
    try:
        capability = json.loads(
            _asset_file(root, "candidate-evidence-v2.json").read_text()
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise AssessmentError("RELEASE_INVALID", "capability release is invalid") from exc
    implementation = capability.get("implementation_sha256")
    expected_modules = {
        "candidate_assessment.py",
        "candidate_evidence.py",
        "evidence_contract.py",
    }
    if not isinstance(implementation, dict) or set(implementation) != expected_modules:
        raise AssessmentError("RELEASE_INVALID", "capability implementation is invalid")
    source_root = root.parent.parent
    for name, expected in implementation.items():
        if not isinstance(expected, str) or not SHA256_RE.fullmatch(expected):
            raise AssessmentError("RELEASE_INVALID", "implementation hash is invalid")
        try:
            actual = _digest((source_root / name).read_bytes())
        except OSError as exc:
            raise AssessmentError(
                "RELEASE_UNAVAILABLE", "capability implementation is unavailable"
            ) from exc
        if actual != expected:
            raise AssessmentError(
                "RELEASE_HASH_MISMATCH", "capability implementation hash mismatch"
            )
    return release_set


def load_engineering_rules(
    root: Path, release_set: ReleaseSet
) -> dict[str, dict[str, Any]]:
    release = next(
        item for item in release_set.releases if item.asset_id == RULE_RELEASE_ID
    )
    if set(release.files) != {"engineering-rules-v1.json"}:
        raise AssessmentError("RELEASE_INVALID", "engineering rule release is invalid")
    try:
        raw = json.loads(_asset_file(root, "engineering-rules-v1.json").read_text())
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise AssessmentError("RELEASE_INVALID", "engineering rules are invalid") from exc
    if (
        not isinstance(raw, dict)
        or raw.get("scope") != "engineering_integrity_only"
        or raw.get("review_status") != "approved"
        or not isinstance(raw.get("rules"), list)
    ):
        raise AssessmentError("RELEASE_INVALID", "engineering rule scope is invalid")
    rules = {
        item.get("rule_id"): item
        for item in raw["rules"]
        if isinstance(item, dict) and isinstance(item.get("rule_id"), str)
    }
    if set(rules) != set(RULE_IDS) or len(rules) != len(raw["rules"]):
        raise AssessmentError("RELEASE_INVALID", "engineering rule registry is invalid")
    for rule_id, rule in rules.items():
        if rule.get("severity") not in {"limitation", "missing", "conflict"}:
            raise AssessmentError("RELEASE_INVALID", f"invalid severity for {rule_id}")
        if not isinstance(rule.get("message"), str) or not rule["message"]:
            raise AssessmentError("RELEASE_INVALID", f"missing message for {rule_id}")
    return rules


def _identity_complete(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    if SHA256_RE.fullmatch(str(value.get("sha256") or "")):
        return True
    files = value.get("files")
    return bool(files) and isinstance(files, list) and all(
        isinstance(item, dict)
        and SHA256_RE.fullmatch(str(item.get("sha256") or ""))
        for item in files
    )


def _checksum_observations(dependencies: dict[str, Any]) -> list[str]:
    observations = dependencies.get("reference_observations")
    if not isinstance(observations, dict):
        return []
    observed = []
    for dependency_id, value in observations.items():
        if not isinstance(dependency_id, str) or not isinstance(value, dict):
            continue
        checksums = value.get("source_checksums")
        if isinstance(checksums, list) and checksums and all(
            isinstance(item, dict)
            and item.get("algorithm") in {"md5", "sha256"}
            and isinstance(item.get("digest"), str)
            and bool(
                re.fullmatch(
                    r"[0-9a-f]{32}" if item["algorithm"] == "md5" else r"[0-9a-f]{64}",
                    item["digest"],
                )
            )
            for item in checksums
        ):
            observed.append(dependency_id)
    return sorted(set(observed))


def resolve_release_set(
    state: dict[str, Any], release_set: ReleaseSet
) -> ReleaseResolution:
    """Resolve W2 capability and dependency identity without inspecting databases."""

    workflow2_manifests = sorted(
        (manifest for manifest in CAPABILITY_MANIFESTS.values() if manifest.id.startswith("workflow2.")),
        key=lambda manifest: manifest.id,
    )
    stages = (state.get("execution_spec") or {}).get("stages")
    limitations: set[str] = set()
    if isinstance(stages, list) and stages:
        selected_stage_ids = {
            str(stage.get("id"))
            for stage in stages
            if isinstance(stage, dict) and stage.get("workflow_id") == "workflow2"
        }
        selected = [
            manifest
            for manifest in workflow2_manifests
            if manifest.stage_id in selected_stage_ids
        ]
        if not selected:
            selected = workflow2_manifests
            limitations.add("EXECUTION_SPEC_WORKFLOW2_UNAVAILABLE")
    else:
        selected = workflow2_manifests
        limitations.add("EXECUTION_SPEC_UNAVAILABLE")

    capability_ids = sorted(manifest.id for manifest in selected)
    software_ids = sorted(
        {dependency for manifest in selected for dependency in manifest.software_dependencies}
    )
    reference_ids = sorted(
        {dependency for manifest in selected for dependency in manifest.reference_dependencies}
    )
    missing: set[str] = set()

    capability_contract = (state.get("execution_spec") or {}).get("capability_contract")
    if isinstance(capability_contract, dict):
        frozen = {
            item.get("capability_id"): item.get("manifest_version")
            for item in capability_contract.get("manifests") or []
            if isinstance(item, dict)
        }
        current = {manifest.id: manifest.version for manifest in selected}
        mismatched = {
            capability_id
            for capability_id, version in current.items()
            if frozen.get(capability_id) != version
        }
        if mismatched:
            missing.update(mismatched)
            limitations.add("CAPABILITY_IDENTITY_INCOMPLETE")
    else:
        missing.update(capability_ids)
        limitations.add("CAPABILITY_IDENTITY_INCOMPLETE")

    recovery = state.get("recovery_contract")
    dependencies = recovery.get("dependencies") if isinstance(recovery, dict) else {}
    dependencies = dependencies if isinstance(dependencies, dict) else {}
    blocker = (
        (recovery.get("last_check") or {}).get("blocker_code")
        if isinstance(recovery, dict)
        else None
    )
    unavailable = blocker == "RECOVERY_CHECK_UNAVAILABLE"
    if unavailable:
        limitations.add("DEPENDENCY_IDENTITY_UNAVAILABLE")
    complete_contract = (
        isinstance(recovery, dict)
        and recovery.get("dependencies_complete") is True
        and not blocker
    )
    frozen_capabilities = set(dependencies.get("capability_ids") or [])
    software = dependencies.get("software")
    references = dependencies.get("references")
    software = software if isinstance(software, dict) else {}
    references = references if isinstance(references, dict) else {}
    if not complete_contract:
        missing.update(software_ids)
        missing.update(reference_ids)
        limitations.add("DEPENDENCY_IDENTITY_INCOMPLETE")
    else:
        if not set(capability_ids) <= frozen_capabilities:
            missing.update(set(capability_ids) - frozen_capabilities)
            limitations.add("CAPABILITY_IDENTITY_INCOMPLETE")
        if not _identity_complete(dependencies.get("workflow")):
            missing.add("workflow2.source")
        if not _identity_complete(dependencies.get("parser")):
            missing.add("candidate-evidence-v2.parser")
        missing.update(
            dependency_id
            for dependency_id in software_ids
            if not _identity_complete(software.get(dependency_id))
        )
        missing.update(
            dependency_id
            for dependency_id in reference_ids
            if not _identity_complete(references.get(dependency_id))
        )
        if missing:
            limitations.add("DEPENDENCY_IDENTITY_INCOMPLETE")

    published_references = {
        release.asset_id
        for release in release_set.releases
        if release.asset_type == "reference" and release.review_status == "approved"
    }
    unpublished = set(reference_ids) - published_references
    if unpublished:
        missing.update(unpublished)
        limitations.add("REFERENCE_RELEASES_UNPUBLISHED")

    status = "unavailable" if unavailable else "incomplete" if missing or limitations else "complete"
    return ReleaseResolution(
        status=status,
        release_set=release_set,
        capability_ids=capability_ids,
        software_dependency_ids=software_ids,
        reference_dependency_ids=reference_ids,
        missing_dependency_ids=sorted(missing),
        observed_checksum_ids=_checksum_observations(dependencies),
        limitation_codes=sorted(limitations),
    )


def assess_candidate(
    candidate: CandidateEvidenceV2,
    resolution: ReleaseResolution,
    rules: dict[str, dict[str, Any]],
) -> Assessment:
    """Map evidence integrity issues to findings without creating claims."""

    evidence_ids = {item.evidence_id for item in candidate.observations}
    findings: list[RuleFinding] = []

    def add(rule_id: str, selected_ids: set[str]) -> None:
        unknown = selected_ids - evidence_ids
        if unknown:
            raise AssessmentError(
                "ASSESSMENT_EVIDENCE_INVALID", "finding references unknown evidence"
            )
        rule = rules[rule_id]
        findings.append(
            RuleFinding(
                rule_id=rule_id,
                severity=rule["severity"],
                message=rule["message"],
                evidence_ids=sorted(selected_ids),
            )
        )

    for kind, rule_id in (
        ("missing", "EVIDENCE_MISSING"),
        ("conflict", "EVIDENCE_CONFLICT"),
    ):
        selected_ids = {
            evidence_id
            for issue in candidate.issues
            if issue.kind == kind
            for evidence_id in issue.evidence_ids
        }
        if selected_ids:
            add(rule_id, selected_ids)

    dependency_ids = {
        evidence_id
        for issue in candidate.issues
        if issue.kind == "limitation"
        for evidence_id in issue.evidence_ids
    }
    if resolution.status != "complete" or dependency_ids:
        add("DEPENDENCY_IDENTITY_INCOMPLETE", dependency_ids or evidence_ids)

    groups: dict[str, set[str]] = defaultdict(set)
    for observation in candidate.observations:
        groups[observation.source_group].add(observation.evidence_id)
    correlated = set().union(*(ids for ids in groups.values() if len(ids) > 1))
    if correlated:
        add("CORRELATED_EVIDENCE", correlated)

    add("DOMAIN_ASSETS_PENDING", set())
    return Assessment(
        candidate_id=candidate.candidate_id,
        status="not_generated",
        findings=findings,
        provider="none",
        model="none",
        error_code="DOMAIN_ASSETS_PENDING",
        release_set=resolution.release_set,
    )


def assess_workflow2(
    evidence: Workflow2EvidenceResult,
    state: dict[str, Any],
    *,
    asset_root: Path = ASSET_ROOT,
) -> Workflow2AssessmentResult:
    release_set = load_release_set(asset_root)
    resolution = resolve_release_set(state, release_set)
    rules = load_engineering_rules(asset_root, release_set)
    assessments = tuple(
        assess_candidate(candidate, resolution, rules)
        for candidate in evidence.candidates
    )
    return Workflow2AssessmentResult(evidence, resolution, assessments)


def parse_and_assess(
    state_path: Path, *, asset_root: Path = ASSET_ROOT
) -> Workflow2AssessmentResult:
    try:
        before = state_path.stat()
        state = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise AssessmentError("ASSESSMENT_STATE_INVALID", "run state is invalid") from exc
    evidence = parse_workflow2_candidate_evidence(state_path)
    try:
        after = state_path.stat()
    except OSError as exc:
        raise AssessmentError(
            "ASSESSMENT_STATE_UNAVAILABLE", "run state is temporarily unavailable"
        ) from exc
    if (before.st_ino, before.st_size, before.st_mtime_ns) != (
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ):
        raise AssessmentError("ASSESSMENT_STATE_CHANGED", "run state changed while reading")
    if not isinstance(state, dict) or state.get("run_id") != evidence.run_id:
        raise AssessmentError("ASSESSMENT_STATE_INVALID", "run identity is invalid")
    return assess_workflow2(evidence, state, asset_root=asset_root)


def summary(result: Workflow2AssessmentResult) -> dict[str, Any]:
    payload = evidence_summary(result.evidence)
    finding_counts = Counter(
        finding.rule_id
        for assessment in result.assessments
        for finding in assessment.findings
    )
    status_counts = Counter(assessment.status for assessment in result.assessments)
    payload.update(
        {
            "schema_version": 1,
            "release": {
                "status": result.release_resolution.status,
                "release_ids": sorted(
                    release.asset_id
                    for release in result.release_resolution.release_set.releases
                ),
                "missing_dependency_count": len(
                    result.release_resolution.missing_dependency_ids
                ),
                "observed_checksum_count": len(
                    result.release_resolution.observed_checksum_ids
                ),
                "retryable": result.release_resolution.status == "unavailable",
                "limitation_codes": result.release_resolution.limitation_codes,
            },
            "assessment_count": len(result.assessments),
            "assessment_status_counts": dict(sorted(status_counts.items())),
            "finding_counts": dict(sorted(finding_counts.items())),
        }
    )
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", required=True, type=Path)
    parser.add_argument("--summary", action="store_true")
    args = parser.parse_args(argv)
    if not args.summary:
        parser.error("only aggregate --summary output is available")
    try:
        result = parse_and_assess(args.state)
    except (CandidateEvidenceParseError, AssessmentError) as exc:
        print(
            json.dumps(
                {
                    "status": "failed",
                    "code": exc.code,
                    "retryable": exc.code.endswith("_UNAVAILABLE"),
                },
                sort_keys=True,
            )
        )
        return 1
    print(json.dumps(summary(result), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

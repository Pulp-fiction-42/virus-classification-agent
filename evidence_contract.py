"""Versioned asset and candidate contracts shared by replay and future services."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)


class AssetRelease(Contract):
    asset_id: str
    asset_type: Literal["knowledge", "rules", "reference", "capability", "model"]
    version: str
    source: str
    license: str = "unknown"
    files: dict[str, str] = Field(default_factory=dict)
    compatible_with: list[str] = Field(default_factory=list)
    review_status: Literal["pending", "approved"] = "pending"


class ReleaseSet(Contract):
    schema_version: Literal[1] = 1
    releases: list[AssetRelease]

    @model_validator(mode="after")
    def validate_unique_releases(self) -> ReleaseSet:
        asset_ids = [release.asset_id for release in self.releases]
        if len(set(asset_ids)) != len(asset_ids):
            raise ValueError("release set must select exactly one version per asset")
        return self


class ReleaseResolution(Contract):
    """Release and dependency identity status for a bounded assessment."""

    schema_version: Literal[1] = 1
    status: Literal["complete", "incomplete", "unavailable"]
    release_set: ReleaseSet
    capability_ids: list[str]
    software_dependency_ids: list[str]
    reference_dependency_ids: list[str]
    missing_dependency_ids: list[str] = Field(default_factory=list)
    observed_checksum_ids: list[str] = Field(default_factory=list)
    limitation_codes: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_resolution(self) -> ReleaseResolution:
        for values in (
            self.capability_ids,
            self.software_dependency_ids,
            self.reference_dependency_ids,
            self.missing_dependency_ids,
            self.observed_checksum_ids,
            self.limitation_codes,
        ):
            if values != sorted(set(values)):
                raise ValueError("release resolution lists must be sorted and unique")
        if self.status == "complete" and (
            self.missing_dependency_ids or self.limitation_codes
        ):
            raise ValueError("complete release resolution cannot contain limitations")
        if self.status == "unavailable" and "DEPENDENCY_IDENTITY_UNAVAILABLE" not in self.limitation_codes:
            raise ValueError("unavailable release resolution requires an unavailable limitation")
        return self


class CapabilityManifest(Contract):
    capability_id: str
    version: str
    input_artifacts: list[list[str]]
    output_artifact: str
    parameter_schema: dict[str, Any]
    adapter: str
    parser: str
    resources: dict[str, Any]
    software_dependencies: list[str]
    reference_dependencies: list[str]
    validation_status: str = "code_exists"


class Observation(Contract):
    evidence_id: str
    metric: str
    value: int | float | None
    unit: Literal["read_pairs", "reads", "ratio"]
    denominator: str | None = None
    population: str
    source_group: str
    source_path: str
    locator: str
    sha256: str
    formula: str | None = None


class CandidateEvidence(Contract):
    schema_version: Literal[1] = 1
    run_id: str
    attempt: str = "unknown"
    sample_id: str
    candidate_id: str
    candidate_kind: Literal["screening_taxon"] = "screening_taxon"
    taxid: str
    name: str
    observations: list[Observation]
    tools: dict[str, str]
    reference_id: str
    reference_version: str = "unknown"


class EvidenceSourceV2(Contract):
    """Immutable provenance for an observation read from an artifact."""

    artifact_type: str
    artifact_path: str
    locator: str
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    tool_id: str
    tool_version: str
    reference_id: str | None = None
    reference_version: str | None = None

    @model_validator(mode="after")
    def validate_reference_identity(self) -> EvidenceSourceV2:
        if (self.reference_id is None) != (self.reference_version is None):
            raise ValueError("reference_id and reference_version must be provided together")
        return self


class EvidenceDerivationV2(Contract):
    """A deliberately small allowlist of deterministic evidence operations."""

    operation: Literal["ratio", "sum", "difference", "paired_read_pairs"]
    input_evidence_ids: list[str] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_arity(self) -> EvidenceDerivationV2:
        expected = {"ratio": 2, "difference": 2, "paired_read_pairs": 1}
        if self.operation in expected and len(self.input_evidence_ids) != expected[self.operation]:
            raise ValueError(f"{self.operation} requires exactly {expected[self.operation]} inputs")
        return self


class ObservationV2(Contract):
    evidence_id: str
    metric: str
    value: int | float | None
    unit: Literal["read_pairs", "reads", "ratio", "percent", "count", "bases", "bp", "depth"]
    population: str
    source_group: str
    denominator_evidence_id: str | None = None
    denominator_description: str | None = Field(
        default=None, min_length=1, max_length=240
    )
    source: EvidenceSourceV2 | None = None
    derivation: EvidenceDerivationV2 | None = None
    missing_reason: str | None = None

    @model_validator(mode="after")
    def validate_value_and_origin(self) -> ObservationV2:
        if (self.source is None) == (self.derivation is None):
            raise ValueError("exactly one of source or derivation is required")
        if self.value is None and not self.missing_reason:
            raise ValueError("missing values require missing_reason")
        if self.value is not None and self.missing_reason is not None:
            raise ValueError("present values cannot have missing_reason")

        ratio_unit = self.unit in {"ratio", "percent"}
        if not ratio_unit and (
            self.denominator_evidence_id is not None
            or self.denominator_description is not None
        ):
            raise ValueError("only ratio and percent values may declare a denominator")
        if ratio_unit and self.source is not None and (
            self.denominator_evidence_id is not None
            or self.denominator_description is None
        ):
            raise ValueError(
                "source ratio and percent values require denominator_description only"
            )
        if ratio_unit and self.derivation is not None:
            if (
                self.denominator_evidence_id is None
                or self.denominator_description is not None
                or self.derivation.operation != "ratio"
            ):
                raise ValueError(
                    "derived ratio and percent values require a ratio denominator evidence"
                )
            if self.denominator_evidence_id != self.derivation.input_evidence_ids[1]:
                raise ValueError("denominator must be the second ratio input")
        if self.value is not None and self.unit == "ratio" and not 0 <= self.value <= 1:
            raise ValueError("ratio values must be between 0 and 1")
        if self.value is not None and self.unit == "percent" and not 0 <= self.value <= 100:
            raise ValueError("percent values must be between 0 and 100")
        return self


class EvidenceIssueV2(Contract):
    issue_id: str
    kind: Literal["limitation", "missing", "conflict"]
    message: str
    evidence_ids: list[str] = Field(default_factory=list)


class CandidateEvidenceV2(Contract):
    """Internal evidence contract; it does not imply a biological assessment."""

    schema_version: Literal[2] = 2
    run_id: str
    attempt: int | Literal["unknown"]
    report_scope: Literal["sampled_screening", "full_analysis"]
    sample_id: str
    candidate_id: str
    candidate_kind: Literal[
        "screening_taxon", "virus_contig", "virus_protein", "host_taxon"
    ]
    taxid: str | None = None
    name: str | None = None
    observations: list[ObservationV2] = Field(min_length=1)
    issues: list[EvidenceIssueV2] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_evidence_graph(self) -> CandidateEvidenceV2:
        if self.candidate_kind == "screening_taxon" and self.taxid is None:
            raise ValueError("screening_taxon requires taxid")

        evidence_by_id = {item.evidence_id: item for item in self.observations}
        if len(evidence_by_id) != len(self.observations):
            raise ValueError("evidence_id must be unique")

        dependencies: dict[str, list[str]] = {}
        for item in self.observations:
            referenced_ids: list[str] = []
            if item.denominator_evidence_id is not None:
                referenced_ids.append(item.denominator_evidence_id)
            if item.derivation is not None:
                referenced_ids.extend(item.derivation.input_evidence_ids)
            unknown = set(referenced_ids) - evidence_by_id.keys()
            if unknown:
                raise ValueError(f"unknown evidence references: {sorted(unknown)}")
            if item.evidence_id in referenced_ids:
                raise ValueError("evidence cannot reference itself")
            dependencies[item.evidence_id] = referenced_ids

        visiting: set[str] = set()
        visited: set[str] = set()

        def visit(evidence_id: str) -> None:
            if evidence_id in visiting:
                raise ValueError("evidence derivation graph contains a cycle")
            if evidence_id in visited:
                return
            visiting.add(evidence_id)
            for dependency_id in dependencies[evidence_id]:
                visit(dependency_id)
            visiting.remove(evidence_id)
            visited.add(evidence_id)

        for evidence_id in dependencies:
            visit(evidence_id)

        issue_ids = [issue.issue_id for issue in self.issues]
        if len(set(issue_ids)) != len(issue_ids):
            raise ValueError("issue_id must be unique")
        for issue in self.issues:
            unknown = set(issue.evidence_ids) - evidence_by_id.keys()
            if unknown:
                raise ValueError(f"unknown issue evidence references: {sorted(unknown)}")
        return self


class RuleFinding(Contract):
    rule_id: str
    severity: Literal["limitation", "missing", "conflict"]
    message: str
    evidence_ids: list[str] = Field(default_factory=list)


class ExplanationClaim(Contract):
    text: str = Field(max_length=1200)
    evidence_ids: list[str]
    citation_ids: list[str]


class AssessmentDraft(Contract):
    candidate_id: str
    claims: list[ExplanationClaim] = Field(max_length=8)
    finding_ids: list[str]


class Assessment(Contract):
    schema_version: Literal[1] = 1
    candidate_id: str
    status: Literal["draft", "not_generated"]
    review_status: Literal["pending"] = "pending"
    draft: AssessmentDraft | None = None
    findings: list[RuleFinding]
    provider: str
    model: str
    error_code: str | None = None
    release_set: ReleaseSet

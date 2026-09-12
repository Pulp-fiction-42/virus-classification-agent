"""Versioned asset and candidate contracts shared by replay and future services."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


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

"""Provider-neutral action and retrieval contracts for analysis conversations."""

from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator


GOALS = {
    "contig_filter",
    "virus_summary",
    "host_summary",
    "microbe_summary",
    "virus_rpm",
    "annotation_delivery",
}
VIRAL_TYPES = {"RNA", "DNA", "RNA+DNA"}
ACTION_BLOCK_RE = re.compile(
    r"^\s*```conversation_action\s*([\s\S]*?)```\s*$", re.IGNORECASE
)


class ProposedPatch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    goals: list[str] | None = None
    viral_type: str | None = None

    @model_validator(mode="after")
    def validate_values(self) -> "ProposedPatch":
        if self.goals is not None:
            if not self.goals or len(set(self.goals)) != len(self.goals):
                raise ValueError("goals must be a non-empty unique list")
            if any(goal not in GOALS for goal in self.goals):
                raise ValueError("proposed_patch contains an unsupported goal")
        if self.viral_type is not None and self.viral_type not in VIRAL_TYPES:
            raise ValueError("viral_type must be RNA, DNA, or RNA+DNA")
        return self


class ConversationAction(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action: Literal["respond", "clarify", "propose_patch"]
    message: str = Field(min_length=1, max_length=20_000)
    proposed_patch: ProposedPatch | None = None
    citations: list[str] = Field(default_factory=list, max_length=50)

    @model_validator(mode="after")
    def validate_action(self) -> "ConversationAction":
        patch_is_empty = self.proposed_patch is None or (
            self.proposed_patch.goals is None
            and self.proposed_patch.viral_type is None
        )
        if self.action == "propose_patch" and patch_is_empty:
            raise ValueError("propose_patch requires proposed_patch")
        if self.action != "propose_patch":
            if not patch_is_empty:
                raise ValueError("only propose_patch may include proposed_patch")
            self.proposed_patch = None
        self.citations = []
        return self


class ConversationContractError(ValueError):
    def __init__(self, message: str, *, code: str = "ACTION_SCHEMA_INVALID") -> None:
        super().__init__(message)
        self.code = code


def parse_conversation_action(text: str) -> ConversationAction:
    match = ACTION_BLOCK_RE.fullmatch(text)
    if match is None:
        raise ConversationContractError("conversation action envelope is missing")
    try:
        value = json.loads(match.group(1))
    except json.JSONDecodeError as exc:
        raise ConversationContractError(
            "conversation action JSON is invalid", code="ACTION_JSON_INVALID"
        ) from exc
    return validate_conversation_action(value)


def validate_conversation_action(value: Any) -> ConversationAction:
    """Apply the same strict contract to SDK structured output and text fallback."""

    try:
        return ConversationAction.model_validate(value)
    except ValidationError as exc:
        forbidden = any(error["type"] == "extra_forbidden" for error in exc.errors())
        raise ConversationContractError(
            "conversation action envelope is invalid",
            code="PATCH_FIELD_FORBIDDEN" if forbidden else "ACTION_SCHEMA_INVALID",
        ) from exc


def conversation_action_output_format() -> dict[str, Any]:
    return {
        "type": "json_schema",
        "schema": ConversationAction.model_json_schema(),
    }


def proposal_output(action: ConversationAction) -> dict[str, Any] | None:
    if action.action != "propose_patch" or action.proposed_patch is None:
        return None
    return {
        "type": "analysis_patch_proposal",
        "id": f"analysis_patch_{uuid.uuid4().hex}",
        "status": "pending_registration",
        "patch": action.proposed_patch.model_dump(exclude_none=True),
        "citations": action.citations,
    }


def action_trace_output(
    *,
    action: ConversationAction | None,
    model: str,
    latency_ms: int,
    error_code: str | None = None,
) -> dict[str, Any]:
    return {
        "type": "conversation_action_trace",
        "id": f"conversation_action_{uuid.uuid4().hex}",
        "status": "rejected" if action and error_code else "completed" if action else "failed",
        "schema_valid": action is not None,
        "action": action.action if action else None,
        "model": model,
        "latency_ms": max(0, latency_ms),
        "model_calls": 1,
        **({"error_code": error_code} if error_code else {}),
    }


Corpus = Literal[
    "ictv",
    "workflow_capabilities",
    "artifact_glossary",
    "tools_databases",
    "faq_policy",
]
CitationQuality = Literal["verified", "incomplete", "unverified"]


class Citation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    citation_id: str
    corpus: Corpus
    source: str
    source_version: str | None = None
    locator: str | None = None
    content: str
    retrieved_at: str
    score: float | None = None
    quality: CitationQuality = "unverified"


class RetrievalResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    query: str
    passages: list[Citation] = Field(default_factory=list)
    insufficient: bool = False
    error_code: Literal["KNOWLEDGE_INSUFFICIENT", "RETRIEVAL_UNAVAILABLE"] | None = None


@dataclass(frozen=True)
class RetrievalRequest:
    query: str
    corpora: tuple[Corpus, ...]
    limit: int = 8


class RetrievalAdapter:
    async def retrieve(self, request: RetrievalRequest) -> RetrievalResult:
        raise NotImplementedError


class KnowledgeInsufficientError(RuntimeError):
    code = "KNOWLEDGE_INSUFFICIENT"

    def __init__(self, message: str = "没有足够且可引用的资料回答该问题。") -> None:
        super().__init__(message)

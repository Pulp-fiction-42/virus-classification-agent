"""Bounded structured generation. No domain rules or executable tools live here."""

from __future__ import annotations

import asyncio
import json
import re
import time
import uuid
from importlib.metadata import version
from pathlib import Path
from typing import Any, Literal, Protocol

from jsonschema import Draft202012Validator, ValidationError
from pydantic import BaseModel, ConfigDict, Field

import config

TaskKind = Literal[
    "intent_clarification", "controlled_patch", "citation_qa", "evidence_explanation"
]
ProviderError = Literal[
    "CLOUD_DISABLED",
    "MODEL_UNAVAILABLE",
    "MODEL_TIMEOUT",
    "MODEL_SCHEMA_INVALID",
    "CONTEXT_LIMIT",
    "OUTPUT_LIMIT",
]


class ProviderRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    task_kind: TaskKind
    output_schema: dict[str, Any]
    context: dict[str, Any]
    instructions: str
    model: str = config.MODEL
    conversation_id: str | None = None
    timeout_seconds: float = Field(default=180, gt=0, le=600)
    max_context_chars: int = Field(default=24000, gt=0, le=64000)
    max_output_tokens: int = Field(default=2048, gt=0, le=8192)


class ProviderResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    provider: str
    model: str
    backend_version: str = "not_loaded"
    latency_ms: int = 0
    output: dict[str, Any] | None = None
    error_code: ProviderError | None = None


class Provider(Protocol):
    async def generate(self, request: ProviderRequest) -> ProviderResult: ...


def claude_agent_sdk_version() -> str:
    try:
        return version("claude-agent-sdk")
    except Exception:  # noqa: BLE001 - health must stay available without the SDK
        return "not_installed"


def claude_agent_sdk_ready() -> bool:
    return claude_agent_sdk_version() == config.CLAUDE_AGENT_SDK_VERSION


PRIVATE_KEYS = {
    "path",
    "paths",
    "files",
    "fingerprints",
    "execution_spec",
    "log",
    "logs",
    "sequence",
    "api_key",
    "token",
}
PATH_RE = re.compile(r"(?<!\w)(?:/[\w.~-]+){2,}[^\s\"'<>，。；：]*|[A-Za-z]:\\[^\s]+")
SEQUENCE_RE = re.compile(r"\b[ACGTUNacgtun]{80,}\b")


def safe_context(value: Any) -> Any:
    """Strip private fields and paths before history or context reaches a provider."""
    if isinstance(value, dict):
        return {
            k: safe_context(v)
            for k, v in value.items()
            if k.lower() not in PRIVATE_KEYS
        }
    if isinstance(value, list):
        return [safe_context(v) for v in value]
    if isinstance(value, str):
        return SEQUENCE_RE.sub("[序列已移除]", PATH_RE.sub("[路径已移除]", value))
    return value


class MiniMaxProvider:
    """SDK session identities and transcripts stay inside this adapter's namespace."""

    def __init__(
        self,
        *,
        allow_cloud: bool = False,
        state_dir: Path | None = None,
        client_factory=None,
    ):
        self.allow_cloud = allow_cloud
        self.state_dir = (
            state_dir or config.CLAUDE_STATE_DIR / "providers" / "minimax-m0"
        )
        self.client_factory = client_factory
        self._locks: dict[str, asyncio.Lock] = {}

    async def generate(self, request: ProviderRequest) -> ProviderResult:
        started = time.monotonic()
        sdk_version = claude_agent_sdk_version()
        result = ProviderResult(
            provider="minimax", model=request.model, backend_version=sdk_version
        )
        if not self.allow_cloud:
            result.error_code = "CLOUD_DISABLED"
            return result
        if self.client_factory is None and not claude_agent_sdk_ready():
            result.error_code = "MODEL_UNAVAILABLE"
            return result
        prompt = json.dumps(safe_context(request.context), ensure_ascii=False)
        if (
            len(prompt)
            + len(request.instructions)
            + len(json.dumps(request.output_schema))
            > request.max_context_chars
        ):
            result.error_code = "CONTEXT_LIMIT"
            return result
        key = request.conversation_id or str(uuid.uuid4())
        try:
            uuid.UUID(key)
        except ValueError:
            result.error_code = "MODEL_SCHEMA_INVALID"
            return result
        lock = self._locks.setdefault(key, asyncio.Lock())
        try:
            async with asyncio.timeout(request.timeout_seconds), lock:
                result.output = await self._generate(request, prompt, key)
        except TimeoutError:
            result.error_code = "MODEL_TIMEOUT"
        except (json.JSONDecodeError, ValidationError, ValueError, TypeError):
            result.error_code = "MODEL_SCHEMA_INVALID"
        except Exception:  # noqa: BLE001 - SDK errors must not expose provider details
            result.error_code = "MODEL_UNAVAILABLE"
        finally:
            result.latency_ms = round((time.monotonic() - started) * 1000)
            if request.conversation_id is None and not lock.locked():
                self._locks.pop(key, None)
        if (
            result.output is not None
            and len(json.dumps(result.output, ensure_ascii=False))
            > request.max_output_tokens * 8
        ):
            result.output = None
            result.error_code = "OUTPUT_LIMIT"
        return result

    async def _generate(
        self, request: ProviderRequest, prompt: str, key: str
    ) -> dict[str, Any]:
        from claude_agent_sdk import (
            AssistantMessage,
            ClaudeAgentOptions,
            ClaudeSDKClient,
            ResultMessage,
            ToolUseBlock,
        )

        if not config.MINIMAX_API_KEY and self.client_factory is None:
            raise OSError("model credentials unavailable")
        self.state_dir.mkdir(parents=True, exist_ok=True)
        mapping = self.state_dir / f"{key}.json"
        previous = json.loads(mapping.read_text()) if mapping.exists() else {}
        input_size = (
            len(prompt)
            + len(request.instructions)
            + len(json.dumps(request.output_schema))
        )
        previous_size = previous.get("context_upper_bound", 0)
        resume = previous.get("sdk_session_id")
        if previous_size + input_size > request.max_context_chars:
            # The host supplies bounded history on every call. Start a new SDK
            # session when its accumulated context would exceed that budget;
            # keep old transcripts for audit instead of silently truncating them.
            resume = None
            previous_size = 0
        options = ClaudeAgentOptions(
            system_prompt=request.instructions,
            tools=[],
            allowed_tools=[],
            mcp_servers={},
            setting_sources=[],
            permission_mode="dontAsk",
            max_turns=1,
            resume=resume,
            output_format={"type": "json_schema", "schema": request.output_schema},
            cwd=config.PROJECT_ROOT,
            env={
                "ANTHROPIC_BASE_URL": config.ANTHROPIC_BASE_URL,
                "ANTHROPIC_AUTH_TOKEN": config.MINIMAX_API_KEY,
                "ANTHROPIC_MODEL": config.CLAUDE_MODEL,
                "ANTHROPIC_DEFAULT_SONNET_MODEL": config.CLAUDE_MODEL,
                "ANTHROPIC_DEFAULT_OPUS_MODEL": config.CLAUDE_MODEL,
                "ANTHROPIC_DEFAULT_HAIKU_MODEL": config.CLAUDE_MODEL,
                "CLAUDE_CONFIG_DIR": str(self.state_dir / "sdk"),
                "CLAUDE_CODE_AUTO_COMPACT_WINDOW": "1000000",
                "CLAUDE_CODE_MAX_OUTPUT_TOKENS": str(request.max_output_tokens),
                "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
            },
        )
        client = (self.client_factory or ClaudeSDKClient)(options=options)
        try:
            await client.connect()
            await client.query(prompt)
            final = None
            structured = None
            async for message in client.receive_response():
                if isinstance(message, AssistantMessage):
                    for block in message.content:
                        if (
                            isinstance(block, ToolUseBlock)
                            and block.name == "StructuredOutput"
                        ):
                            if structured is not None:
                                raise ValueError("multiple structured generations")
                            structured = block.input
                if isinstance(message, ResultMessage):
                    final = message
            if final is None:
                raise OSError("generation failed")
            # A single-turn SDK call can terminate at its StructuredOutput tool.
            # Validate that payload directly; do not run a second model turn just
            # to acknowledge it. Every other SDK error remains a failure.
            if final.is_error and not (
                final.subtype == "error_max_turns" and structured is not None
            ):
                raise OSError("generation failed")
            output = (
                final.structured_output
                if final.structured_output is not None
                else structured
            )
            if output is None:
                output = json.loads(final.result or "")
            Draft202012Validator(request.output_schema).validate(output)
            if not isinstance(output, dict):
                raise TypeError("object required")
            if request.conversation_id:
                temp = mapping.with_suffix(".tmp")
                temp.write_text(
                    json.dumps(
                        {
                            "sdk_session_id": final.session_id,
                            "context_upper_bound": previous_size
                            + input_size
                            + request.max_output_tokens * 8,
                        }
                    )
                )
                temp.replace(mapping)
            return output
        finally:
            await client.disconnect()

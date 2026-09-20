"""Host-owned conversation actions, independent of the generation SDK."""

from __future__ import annotations

import re
import uuid
from collections.abc import AsyncIterator

import config
from conversation_context import build_conversation_context
from conversation_contract import (
    ConversationAction,
    ConversationContractError,
    action_trace_output,
    proposal_output,
    validate_conversation_action,
)
from conversation_events import (
    InvalidSessionError,
    OutputItemAdded,
    RuntimeEvent,
    TextDelta,
)
from provider import MiniMaxProvider, Provider, ProviderRequest, safe_context
from runtime_errors import ConversationPublicError, SessionStateError  # noqa: F401

ICTV_INTENT_PATTERN = re.compile(
    r"ictv|病毒分类学|病毒分類學|(?:科|属|屬).{0,8}(?:界定|划分|分类|分類|标准|標準)|"
    r"marker[ _-]?gene|标记基因|標記基因|genome[ _-]?type|基因组类型|[a-z]+viridae",
    re.IGNORECASE,
)
MODEL_UNAVAILABLE_MESSAGE = "对话模型暂时不可用；数据集、方案与执行功能不受影响。"


def _history(messages: list[dict]) -> list[dict[str, str]]:
    history = []
    for item in messages[-8:]:
        if item.get("role") not in {"user", "assistant"}:
            continue
        content = item.get("content")
        if isinstance(content, list):
            content = "\n".join(
                str(part.get("text", ""))
                for part in content
                if isinstance(part, dict) and part.get("type") == "text"
            )
        if isinstance(content, str):
            history.append(
                {"role": item["role"], "content": safe_context(content)[-1600:]}
            )
    return history


class SessionManager:
    """Maintain the HTTP/event boundary; providers own their sessions."""

    def __init__(self, *, provider: Provider | None = None):
        self.provider = provider or MiniMaxProvider(
            allow_cloud=config.MODEL_POLICY == "minimax_explicit"
        )

    async def ensure_session(self, chat_id: str | None) -> str | None:
        """Use persistent SDK sessions only for durable Open WebUI UUIDs."""
        try:
            return str(uuid.UUID(chat_id or ""))
        except (ValueError, AttributeError):
            return None

    async def stream_turn(
        self,
        messages: list[dict],
        chat_id: str | None,
        routing_request: dict | None = None,
        dataset_context: dict | None = None,
        plan_context: dict | None = None,
    ) -> AsyncIterator[RuntimeEvent]:
        conversation_id = await self.ensure_session(chat_id)
        history = _history(messages)
        latest = next(
            (item["content"] for item in reversed(history) if item["role"] == "user"),
            "",
        )
        if not latest:
            raise InvalidSessionError("Request does not contain a user message")
        # Project-authorized chat retrieval belongs to B2. The M0 draft corpus
        # is deliberately available only through the isolated replay CLI.
        if ICTV_INTENT_PATTERN.search(latest):
            raise ConversationPublicError(
                "KNOWLEDGE_UNAVAILABLE",
                "当前对话尚未接入经审核的分类知识，请使用开发回放查看待审核资料。",
            )
        request = ProviderRequest(
            task_kind="intent_clarification",
            output_schema=ConversationAction.model_json_schema(),
            instructions=config.SYSTEM_PROMPT,
            context=safe_context(
                {
                    "history": history,
                    "analysis": build_conversation_context(
                        routing=routing_request,
                        dataset=dataset_context,
                        plan=plan_context,
                    ),
                }
            ),
            conversation_id=conversation_id,
            timeout_seconds=config.CONVERSATION_TIMEOUT_SECONDS,
        )
        result = await self.provider.generate(request)
        action = None
        error = result.error_code
        if error is None:
            try:
                action = validate_conversation_action(result.output)
            except ConversationContractError as exc:
                error = exc.code
        yield OutputItemAdded(
            action_trace_output(
                action=action,
                model=result.model,
                latency_ms=result.latency_ms,
                error_code=error,
            )
        )
        if error:
            raise ConversationPublicError(error, MODEL_UNAVAILABLE_MESSAGE)
        assert action is not None
        yield TextDelta(action.message)
        item = proposal_output(action)
        if item:
            yield OutputItemAdded(item)

    async def stream_ephemeral_turn(
        self, messages: list[dict]
    ) -> AsyncIterator[RuntimeEvent]:
        result = await self.provider.generate(
            ProviderRequest(
                task_kind="intent_clarification",
                output_schema={
                    "type": "object",
                    "properties": {"message": {"type": "string"}},
                    "required": ["message"],
                    "additionalProperties": False,
                },
                instructions="根据所提供的对话完成标题或摘要任务。仅返回 message 字段，不调用工具，不加入新的事实。",
                context={"history": _history(messages)},
                timeout_seconds=config.CONVERSATION_TIMEOUT_SECONDS,
            )
        )
        if result.error_code:
            raise ConversationPublicError(result.error_code, MODEL_UNAVAILABLE_MESSAGE)
        yield TextDelta(result.output["message"])

    async def close_all(self) -> None:
        # Each bounded provider call closes its SDK connection in finally.
        return None

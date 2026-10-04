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
    ConversationReply,
    RetrievalRequest,
    action_trace_output,
    proposal_output,
    validate_conversation_action,
    validate_conversation_reply,
)
from conversation_events import (
    InvalidSessionError,
    OutputItemAdded,
    RuntimeEvent,
    TextDelta,
)
from knowledge import (
    ENGINEERING_CORPORA,
    MAX_ONLINE_PASSAGE_CHARS,
    MAX_ONLINE_PASSAGES,
    MAX_ONLINE_QUERY_CHARS,
    engineering_knowledge_intent,
    load_engineering_store,
    online_citation,
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

    def __init__(self, *, provider: Provider | None = None, knowledge_store=None):
        self.provider = provider or MiniMaxProvider(
            allow_cloud=config.MODEL_POLICY == "minimax_explicit"
        )
        self.knowledge_store = knowledge_store or load_engineering_store()

    def knowledge_status(self) -> dict:
        return {
            "status": "ready",
            "provider": "sqlite_fts5",
            "release_id": self.knowledge_store.release_id,
            "snapshot_id": self.knowledge_store.snapshot_id,
            "scope": self.knowledge_store.release_scope,
        }

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
        analysis: bool = False,
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
        retrieval = None
        if analysis and engineering_knowledge_intent(latest):
            retrieval = await self.knowledge_store.retrieve(
                RetrievalRequest(
                    query=latest[:MAX_ONLINE_QUERY_CHARS],
                    corpora=ENGINEERING_CORPORA,
                    limit=MAX_ONLINE_PASSAGES,
                )
            )
            if retrieval.error_code:
                message = (
                    "工程知识检索暂时不可用，请稍后重试。"
                    if retrieval.error_code == "RETRIEVAL_UNAVAILABLE"
                    else "没有足够且可引用的工程资料回答该问题。"
                )
                raise ConversationPublicError(retrieval.error_code, message)
        output_schema = (
            ConversationAction.model_json_schema()
            if analysis
            else ConversationReply.model_json_schema()
        )
        request = ProviderRequest(
            task_kind="intent_clarification",
            output_schema=output_schema,
            instructions=(
                config.SYSTEM_PROMPT if analysis else config.DASHBOARD_SYSTEM_PROMPT
            )
            + (
                "\n本轮提供的是工程契约检索结果，不是生物学审核。必须使用 respond，"
                "citations 必须列出至少一个且只能使用 context.knowledge.passages 中的 citation_id。"
                if retrieval
                else ""
            ),
            context=safe_context(
                {
                    "history": history,
                    **(
                        {
                            "analysis": build_conversation_context(
                                routing=routing_request,
                                dataset=dataset_context,
                                plan=plan_context,
                            )
                        }
                        if analysis
                        else {}
                    ),
                    **(
                        {
                            "knowledge": {
                                "scope": "engineering_contract",
                                "snapshot_id": retrieval.snapshot_id,
                                "passages": [
                                    {
                                        "citation_id": item.citation_id,
                                        "content": item.content[
                                            :MAX_ONLINE_PASSAGE_CHARS
                                        ],
                                        "source": item.source,
                                        "source_version": item.source_version,
                                        "locator": item.locator,
                                    }
                                    for item in retrieval.passages
                                ],
                            }
                        }
                        if retrieval
                        else {}
                    ),
                }
            ),
            conversation_id=conversation_id,
            timeout_seconds=config.CONVERSATION_TIMEOUT_SECONDS,
            max_context_chars=64000 if analysis else 24000,
        )
        result = await self.provider.generate(request)
        if not analysis:
            if result.error_code:
                raise ConversationPublicError(
                    result.error_code, MODEL_UNAVAILABLE_MESSAGE
                )
            try:
                reply = validate_conversation_reply(result.output)
            except ConversationContractError as exc:
                raise ConversationPublicError(
                    exc.code, MODEL_UNAVAILABLE_MESSAGE
                ) from exc
            yield TextDelta(reply.message)
            return

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
        if retrieval is not None:
            registered = {item.citation_id: item for item in retrieval.passages}
            if action.action != "respond" or not action.citations:
                raise ConversationPublicError(
                    "CITATION_INVALID", "工程知识回答缺少有效引用。"
                )
            try:
                cited = [registered[citation_id] for citation_id in action.citations]
                for citation in cited:
                    if citation.snapshot_id != retrieval.snapshot_id:
                        raise ValueError("citation snapshot mismatch")
                    self.knowledge_store.verify(citation)
            except (KeyError, ValueError) as exc:
                raise ConversationPublicError(
                    "CITATION_INVALID", "工程知识回答包含无效引用。"
                ) from exc
            yield OutputItemAdded(
                {
                    "type": "analysis_knowledge_citations",
                    "id": f"analysis_knowledge_{uuid.uuid4().hex}",
                    "schema_version": 1,
                    "status": "completed",
                    "release_id": self.knowledge_store.release_id,
                    "snapshot_id": retrieval.snapshot_id,
                    "scope": "engineering_contract",
                    "query": latest[:MAX_ONLINE_QUERY_CHARS],
                    "citations": [online_citation(citation) for citation in cited],
                }
            )
        elif action.citations:
            raise ConversationPublicError(
                "CITATION_INVALID", "回答引用不属于本轮受控检索。"
            )
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
                output_schema=ConversationReply.model_json_schema(),
                instructions="根据所提供的对话完成标题或摘要任务。仅返回 message 字段，不调用工具，不加入新的事实。",
                context={"history": _history(messages)},
                timeout_seconds=config.CONVERSATION_TIMEOUT_SECONDS,
            )
        )
        if result.error_code:
            raise ConversationPublicError(result.error_code, MODEL_UNAVAILABLE_MESSAGE)
        try:
            reply = validate_conversation_reply(result.output)
        except ConversationContractError as exc:
            raise ConversationPublicError(exc.code, MODEL_UNAVAILABLE_MESSAGE) from exc
        yield TextDelta(reply.message)

    async def close_all(self) -> None:
        # Each bounded provider call closes its SDK connection in finally.
        self.knowledge_store.close()

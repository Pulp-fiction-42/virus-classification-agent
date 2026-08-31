"""Claude Agent SDK runtime and Open WebUI chat session management."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
import uuid
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    from claude_agent_sdk import (
        AssistantMessage,
        ClaudeAgentOptions,
        ClaudeSDKClient,
        ResultMessage,
        StreamEvent,
        ToolResultBlock,
        ToolUseBlock,
        UserMessage,
    )
except (
    ModuleNotFoundError
):  # pragma: no cover - exercised by local tests without the SDK installed

    class ClaudeAgentOptions:
        """Small test fallback for environments without claude-agent-sdk installed."""

        def __init__(self, **kwargs: Any) -> None:
            self.allowed_tools = []
            self.disallowed_tools = []
            for key, value in kwargs.items():
                setattr(self, key, value)

    class ClaudeSDKClient:  # type: ignore[no-redef]
        def __init__(self, options: ClaudeAgentOptions | None = None) -> None:
            self.options = options

        async def connect(self, prompt: str | None = None) -> None:
            raise RuntimeError("claude-agent-sdk is not installed")

        async def query(self, prompt: str, session_id: str = "default") -> None:
            raise RuntimeError("claude-agent-sdk is not installed")

        async def receive_response(self) -> AsyncIterator[Any]:
            raise RuntimeError("claude-agent-sdk is not installed")
            yield None

        async def set_permission_mode(self, mode: str) -> None:
            raise RuntimeError("claude-agent-sdk is not installed")

        async def disconnect(self) -> None:
            return None

    @dataclass
    class ResultMessage:  # type: ignore[no-redef]
        session_id: str = "default"
        is_error: bool = False
        result: str | None = None
        errors: list[str] | None = None
        subtype: str = "success"
        structured_output: Any | None = None

    @dataclass
    class StreamEvent:  # type: ignore[no-redef]
        event: dict[str, Any]

    @dataclass
    class ToolResultBlock:  # type: ignore[no-redef]
        tool_use_id: str
        content: str | list[dict[str, Any]] | None = None
        is_error: bool | None = None

    @dataclass
    class UserMessage:  # type: ignore[no-redef]
        content: str | list[Any]

    @dataclass
    class ToolUseBlock:  # type: ignore[no-redef]
        id: str
        name: str
        input: dict[str, Any]

    @dataclass
    class AssistantMessage:  # type: ignore[no-redef]
        content: list[Any]
        model: str = ""


import config
from conversation_events import (
    InvalidSessionError,
    OutputItemAdded,
    RuntimeEvent,
    TextDelta,
)
from conversation_contract import (
    ConversationContractError,
    action_trace_output,
    conversation_action_output_format,
    parse_conversation_action,
    proposal_output,
    validate_conversation_action,
)
from conversation_context import build_conversation_context
from runtime_errors import ConversationPublicError, SessionStateError


log = logging.getLogger(__name__)
SDK_PLAN_PERMISSION_MODE = "dontAsk"

PLAN_PROMPT_PREFIX = """当前是 BioAgent 咨询对话。
分析目标、病毒类型、路由模式与执行方案由界面控制器和确定性 planner 管理。
你只能解释当前数据集与方案、提出一个明确追问，或提出受控 goals/viral_type patch。
你不能修改数据集、模式、步骤、资源或执行状态，也不能启动分析。
必须按运行时提供的 JSON Schema 返回一个 action。
仅当运行时不支持 structured output 时，才输出一个 conversation_action fenced JSON，且不得在控制块外输出文字。"""


MODEL_TIMEOUT_MESSAGE = "对话模型请求超时，请重试；数据集、方案与执行功能不受影响。"
MODEL_UNAVAILABLE_MESSAGE = (
    "对话模型暂时不可用，请稍后重试；数据集、方案与执行功能不受影响。"
)
ACTION_INVALID_MESSAGE = "对话模型返回了无效的结构化结果，请重试；方案未发生变更。"
KNOWLEDGE_PATCH_MESSAGE = (
    "知识查询不能形成方案修改建议，请分开提出修改要求；方案未发生变更。"
)
KNOWLEDGE_UNAVAILABLE_MESSAGE = "当前无法完成经验证的 ICTV 查询，请稍后重试。"

ICTV_INTENT_PATTERN = re.compile(
    r"(?:\bictv\b|病毒分类学|病毒分類學|"
    r"(?:病毒)?(?:科|属|屬).{0,8}(?:界定|划分|劃分|分类|分類|判定|标准|標準)|"
    r"(?:marker[ _-]?gene|标记基因|標記基因)|"
    r"(?:genome[ _-]?type|基因组类型|基因組類型)|"
    r"\b[a-z][a-z0-9-]*viridae\b)",
    re.IGNORECASE,
)


class MarkdownTextStream:
    """Preserve Markdown paragraph boundaries between assistant messages."""

    def __init__(self) -> None:
        self.has_text = False
        self.pending_message_boundary = False
        self.trailing_newlines = 0

    def push(self, event: dict[str, Any]) -> str | None:
        if event.get("type") == "message_start":
            self.pending_message_boundary = self.has_text
            return None

        if event.get("type") != "content_block_delta":
            return None

        delta = event.get("delta") or {}
        if delta.get("type") != "text_delta" or not delta.get("text"):
            return None

        text = delta["text"]
        if self.pending_message_boundary:
            leading_newlines = len(text) - len(text.lstrip("\n"))
            missing_newlines = max(0, 2 - self.trailing_newlines - leading_newlines)
            text = "\n" * missing_newlines + text
            self.pending_message_boundary = False

        self.has_text = True
        text_without_newlines = text.rstrip("\n")
        if text_without_newlines:
            self.trailing_newlines = len(text) - len(text_without_newlines)
        else:
            self.trailing_newlines += len(text)
        self.trailing_newlines = min(2, self.trailing_newlines)
        return text


@dataclass
class ManagedSession:
    client: Any
    lock: asyncio.Lock
    last_used_at: float
    bootstrapped: bool
    claude_session_id: str | None


class SessionManager:
    """Keeps one ClaudeSDKClient per Open WebUI chat ID."""

    def __init__(
        self,
        project_root: Path = config.PROJECT_ROOT,
        config_dir: Path = config.CLAUDE_STATE_DIR,
        *,
        sdk_config_dir: Path = config.CLAUDE_RUNTIME_CONFIG_DIR,
        client_factory: Callable[[ClaudeAgentOptions], Any] = ClaudeSDKClient,
        idle_timeout_seconds: float = config.SESSION_IDLE_TIMEOUT_SECONDS,
        analysis_run_root: Path = config.ANALYSIS_RUN_ROOT,
    ) -> None:
        self.project_root = project_root.resolve()
        self.config_dir = config_dir.resolve()
        self.analysis_run_root = analysis_run_root.resolve()
        self.config_dir.mkdir(parents=True, exist_ok=True)
        self.projects_dir = self.config_dir / "projects"
        self.projects_dir.mkdir(parents=True, exist_ok=True)
        self.chat_map_dir = self.config_dir / "bioagent" / "chat-map"
        self.chat_map_dir.mkdir(parents=True, exist_ok=True)
        self.sdk_config_dir = sdk_config_dir.resolve()
        self._prepare_sdk_config_dir()
        self.client_factory = client_factory
        self.idle_timeout_seconds = idle_timeout_seconds
        self._sessions: dict[str, ManagedSession] = {}
        self._sessions_lock = asyncio.Lock()

        # SDK inspection helpers read this process variable, while the CLI
        # subprocess receives the same value through ClaudeAgentOptions.env.
        os.environ["CLAUDE_CONFIG_DIR"] = str(self.sdk_config_dir)

    def _prepare_sdk_config_dir(self) -> None:
        """Keep SDK-generated runtime files outside the persistent state tree."""
        self.sdk_config_dir.mkdir(parents=True, exist_ok=True)
        projects_link = self.sdk_config_dir / "projects"
        if projects_link.is_symlink():
            if projects_link.resolve() == self.projects_dir:
                return
            projects_link.unlink()
        if projects_link.exists():
            raise SessionStateError(
                f"Claude runtime projects path must be a symlink: {projects_link}"
            )
        projects_link.symlink_to(self.projects_dir, target_is_directory=True)

    async def stream_turn(
        self,
        messages: list[dict],
        chat_id: str | None,
        routing_request: dict[str, Any] | None = None,
        dataset_context: dict[str, Any] | None = None,
        plan_context: dict[str, Any] | None = None,
    ) -> AsyncIterator[RuntimeEvent]:
        normalized_chat_id = self._validate_chat_id(chat_id)
        session = await self._get_conversation_session(normalized_chat_id)

        async with session.lock:
            user_prompt = self._prompt_for_session(messages, session)
            ictv_intent = self._has_ictv_intent(self._latest_user_message(messages))
            prompt = self._prefixed_prompt(
                user_prompt,
                routing_request=routing_request,
                dataset_context=dataset_context,
                plan_context=plan_context,
            )
            session.last_used_at = time.time()
            model_started_at = time.monotonic()
            try:
                async with asyncio.timeout(config.CONVERSATION_TIMEOUT_SECONDS):
                    (
                        response_parts,
                        knowledge_attempted,
                        knowledge_succeeded,
                        structured_output,
                    ) = await self._collect_response(
                        normalized_chat_id, session, prompt
                    )
            except TimeoutError as exc:
                error_code = "KNOWLEDGE_UNAVAILABLE" if ictv_intent else "MODEL_TIMEOUT"
                error_message = (
                    KNOWLEDGE_UNAVAILABLE_MESSAGE
                    if ictv_intent
                    else MODEL_TIMEOUT_MESSAGE
                )
                yield OutputItemAdded(
                    action_trace_output(
                        action=None,
                        model=config.MODEL,
                        latency_ms=round((time.monotonic() - model_started_at) * 1000),
                        error_code=error_code,
                    )
                )
                raise ConversationPublicError(error_code, error_message) from exc
            except Exception as exc:
                log.debug("BioAgent conversation model request failed", exc_info=True)
                error_code = (
                    "KNOWLEDGE_UNAVAILABLE" if ictv_intent else "MODEL_UNAVAILABLE"
                )
                error_message = (
                    KNOWLEDGE_UNAVAILABLE_MESSAGE
                    if ictv_intent
                    else MODEL_UNAVAILABLE_MESSAGE
                )
                yield OutputItemAdded(
                    action_trace_output(
                        action=None,
                        model=config.MODEL,
                        latency_ms=round((time.monotonic() - model_started_at) * 1000),
                        error_code=error_code,
                    )
                )
                raise ConversationPublicError(error_code, error_message) from exc

            try:
                action = (
                    validate_conversation_action(structured_output)
                    if structured_output is not None
                    else parse_conversation_action("".join(response_parts))
                )
            except ConversationContractError as exc:
                yield OutputItemAdded(
                    action_trace_output(
                        action=None,
                        model=config.MODEL,
                        latency_ms=round((time.monotonic() - model_started_at) * 1000),
                        error_code=exc.code,
                    )
                )
                raise ConversationPublicError(exc.code, ACTION_INVALID_MESSAGE) from exc
            if ictv_intent and not knowledge_succeeded:
                yield OutputItemAdded(
                    action_trace_output(
                        action=None,
                        model=config.MODEL,
                        latency_ms=round((time.monotonic() - model_started_at) * 1000),
                        error_code="KNOWLEDGE_UNAVAILABLE",
                    )
                )
                raise ConversationPublicError(
                    "KNOWLEDGE_UNAVAILABLE", KNOWLEDGE_UNAVAILABLE_MESSAGE
                )

            item = proposal_output(action)
            if item and knowledge_attempted:
                yield OutputItemAdded(
                    action_trace_output(
                        action=action,
                        model=config.MODEL,
                        latency_ms=round((time.monotonic() - model_started_at) * 1000),
                        error_code="KNOWLEDGE_PATCH_FORBIDDEN",
                    )
                )
                raise ConversationPublicError(
                    "KNOWLEDGE_PATCH_FORBIDDEN", KNOWLEDGE_PATCH_MESSAGE
                )
            yield TextDelta(action.message)
            yield OutputItemAdded(
                action_trace_output(
                    action=action,
                    model=config.MODEL,
                    latency_ms=round((time.monotonic() - model_started_at) * 1000),
                )
            )
            if item:
                yield OutputItemAdded(item)

            session.bootstrapped = True
            session.last_used_at = time.time()

    async def _collect_response(
        self,
        chat_id: str,
        session: ManagedSession,
        prompt: str,
    ) -> tuple[list[str], bool, bool, Any | None]:
        await session.client.query(prompt)
        result_seen = False
        text_stream = MarkdownTextStream()
        response_parts: list[str] = []
        knowledge_tool_ids: set[str] = set()
        knowledge_attempted = False
        knowledge_succeeded = False
        structured_output: Any | None = None
        async for message in session.client.receive_response():
            if isinstance(message, StreamEvent):
                tool_id = self._knowledge_tool_id(message.event)
                if tool_id is not None:
                    knowledge_attempted = True
                    knowledge_tool_ids.add(tool_id)
                text = text_stream.push(message.event)
                if text:
                    response_parts.append(text)
            elif isinstance(message, AssistantMessage):
                for block in message.content:
                    if isinstance(block, ToolUseBlock) and self._is_knowledge_tool_name(
                        block.name
                    ):
                        knowledge_attempted = True
                        knowledge_tool_ids.add(block.id)
            elif isinstance(message, UserMessage) and isinstance(message.content, list):
                for block in message.content:
                    if (
                        isinstance(block, ToolResultBlock)
                        and block.tool_use_id in knowledge_tool_ids
                        and block.is_error is not True
                    ):
                        knowledge_succeeded = True
            elif isinstance(message, ResultMessage):
                result_seen = True
                structured_output = getattr(message, "structured_output", None)
                self._record_result(chat_id, session, message)
        if not result_seen:
            raise SessionStateError("Claude Agent SDK ended without a ResultMessage")
        return (
            response_parts,
            knowledge_attempted,
            knowledge_succeeded,
            structured_output,
        )

    async def stream_ephemeral_turn(
        self, messages: list[dict]
    ) -> AsyncIterator[RuntimeEvent]:
        """Run an Open WebUI background task without persistent chat state."""
        options = self._build_options(
            permission_mode=SDK_PLAN_PERMISSION_MODE,
            structured_output=False,
        )
        client = self.client_factory(options)
        result_seen = False

        try:
            await client.connect()
            await client.query(self._render_history(messages))

            text_stream = MarkdownTextStream()
            async for message in client.receive_response():
                if isinstance(message, StreamEvent):
                    text = text_stream.push(message.event)
                    if text:
                        yield TextDelta(text)
                elif isinstance(message, ResultMessage):
                    result_seen = True
                    if message.is_error:
                        detail = (
                            message.result
                            or "; ".join(message.errors or [])
                            or message.subtype
                        )
                        raise SessionStateError(
                            f"Claude Agent SDK request failed: {detail}"
                        )

            if not result_seen:
                raise SessionStateError(
                    "Claude Agent SDK ended without a ResultMessage"
                )
        finally:
            await client.disconnect()

    async def ensure_session(self, chat_id: str | None) -> None:
        normalized_chat_id = self._validate_chat_id(chat_id)
        await self._get_conversation_session(normalized_chat_id)

    async def _get_conversation_session(self, chat_id: str) -> ManagedSession:
        try:
            async with asyncio.timeout(config.CONVERSATION_TIMEOUT_SECONDS):
                return await self._get_or_create_session(chat_id)
        except TimeoutError as exc:
            raise ConversationPublicError(
                "MODEL_TIMEOUT", MODEL_TIMEOUT_MESSAGE
            ) from exc
        except ConversationPublicError:
            raise
        except Exception as exc:
            log.debug(
                "BioAgent conversation session initialization failed",
                exc_info=True,
            )
            raise ConversationPublicError(
                "MODEL_UNAVAILABLE", MODEL_UNAVAILABLE_MESSAGE
            ) from exc

    async def close_idle_sessions(self) -> None:
        now = time.time()
        stale_chat_ids: list[str] = []
        async with self._sessions_lock:
            for chat_id, session in self._sessions.items():
                if now - session.last_used_at >= self.idle_timeout_seconds:
                    stale_chat_ids.append(chat_id)
            stale_sessions = [self._sessions.pop(chat_id) for chat_id in stale_chat_ids]

        for session in stale_sessions:
            await self._disconnect_session(session)

    async def close_all(self) -> None:
        async with self._sessions_lock:
            sessions = list(self._sessions.values())
            self._sessions.clear()

        for session in sessions:
            await self._disconnect_session(session)

    def transcript_path(self, session_id: str) -> Path:
        project_name = re.sub(r"[^A-Za-z0-9]", "-", str(self.project_root))
        return self.projects_dir / project_name / f"{session_id}.jsonl"

    async def _get_or_create_session(self, chat_id: str) -> ManagedSession:
        await self.close_idle_sessions()

        async with self._sessions_lock:
            session = self._sessions.get(chat_id)
            if session:
                return session

            session = await self._create_session(chat_id)
            self._sessions[chat_id] = session
            return session

    async def _create_session(self, chat_id: str) -> ManagedSession:
        claude_session_id = self._load_claude_session_id(chat_id)
        if claude_session_id:
            transcript = self.transcript_path(claude_session_id)
            if not transcript.exists():
                raise SessionStateError(
                    f"Cannot resume missing Claude transcript for Open WebUI chat: {chat_id}"
                )
            self._validate_transcript(transcript)

        options = self._build_options(
            permission_mode=SDK_PLAN_PERMISSION_MODE,
            resume=claude_session_id,
        )
        client = self.client_factory(options)
        await client.connect()
        return ManagedSession(
            client=client,
            lock=asyncio.Lock(),
            last_used_at=time.time(),
            bootstrapped=bool(claude_session_id),
            claude_session_id=claude_session_id,
        )

    def _build_options(
        self,
        *,
        permission_mode: str,
        resume: str | None = None,
        structured_output: bool = True,
    ) -> ClaudeAgentOptions:
        knowledge_enabled = bool(config.TAXONAGENT_MCP_URL)
        return ClaudeAgentOptions(
            system_prompt=config.SYSTEM_PROMPT,
            tools=[],
            allowed_tools=["mcp__taxonagent__*"] if knowledge_enabled else [],
            mcp_servers=(
                {
                    "taxonagent": {
                        "type": "http",
                        "url": config.TAXONAGENT_MCP_URL,
                    },
                }
                if knowledge_enabled
                else {}
            ),
            permission_mode=permission_mode,
            resume=resume,
            model=config.MODEL,
            include_partial_messages=True,
            output_format=(
                conversation_action_output_format() if structured_output else None
            ),
            cwd=self.project_root,
            setting_sources=[],
            env={
                "ANTHROPIC_BASE_URL": config.ANTHROPIC_BASE_URL,
                "ANTHROPIC_API_KEY": config.MINIMAX_API_KEY,
                "ANTHROPIC_AUTH_TOKEN": config.MINIMAX_API_KEY,
                "MINIMAX_API_KEY": config.MINIMAX_API_KEY,
                "CLAUDE_CONFIG_DIR": str(self.sdk_config_dir),
                "BIOAGENT_RUN_ROOT": str(self.analysis_run_root),
            },
        )

    def _prompt_for_session(self, messages: list[dict], session: ManagedSession) -> str:
        if session.bootstrapped:
            return self._latest_user_message(messages)
        return self._render_history(messages)

    @staticmethod
    def _prefixed_prompt(
        prompt: str,
        routing_request: dict[str, Any] | None = None,
        dataset_context: dict[str, Any] | None = None,
        plan_context: dict[str, Any] | None = None,
    ) -> str:
        context = build_conversation_context(
            routing=routing_request,
            dataset=dataset_context,
            plan=plan_context,
        )
        return (
            f"{PLAN_PROMPT_PREFIX}\n\nSAFE ANALYSIS CONTEXT (read-only):\n"
            f"{json.dumps(context, ensure_ascii=False)}\n\nUSER REQUEST:\n{prompt}"
        )

    @staticmethod
    def _validate_chat_id(chat_id: str | None) -> str:
        if not chat_id:
            raise InvalidSessionError("Missing X-OpenWebUI-Chat-Id header")
        if SessionManager._is_ephemeral_chat(chat_id):
            return chat_id
        try:
            parsed = uuid.UUID(chat_id)
        except ValueError as exc:
            raise InvalidSessionError("X-OpenWebUI-Chat-Id must be a UUID") from exc
        if str(parsed) != chat_id.lower():
            raise InvalidSessionError("X-OpenWebUI-Chat-Id must be a canonical UUID")
        return str(parsed)

    @staticmethod
    def _is_ephemeral_chat(chat_id: str | None) -> bool:
        return bool(
            chat_id and (chat_id.startswith("local:") or chat_id.startswith("channel:"))
        )

    @classmethod
    def _render_history(cls, messages: list[dict]) -> str:
        rendered = []
        for message in messages:
            content = cls._content_as_text(message.get("content"))
            if cls._is_runtime_control_message(
                str(message.get("role", "user")), content
            ):
                continue
            if content:
                rendered.append(
                    f"{str(message.get('role', 'user')).upper()}: {content}"
                )
        if not rendered:
            raise InvalidSessionError("Request does not contain a usable message")
        return "\n\n".join(rendered)

    @classmethod
    def _latest_user_message(cls, messages: list[dict]) -> str:
        for message in reversed(messages):
            if message.get("role") == "user":
                content = cls._content_as_text(message.get("content"))
                if content:
                    return content
        raise InvalidSessionError("Request does not contain a user message")

    @staticmethod
    def _is_runtime_control_message(role: str, content: str) -> bool:
        return False

    @staticmethod
    def _knowledge_tool_id(event: dict[str, Any]) -> str | None:
        if event.get("type") != "content_block_start":
            return None
        block = event.get("content_block")
        if not isinstance(block, dict) or block.get("type") not in {
            "tool_use",
            "server_tool_use",
        }:
            return None
        name = str(block.get("name") or "")
        if not SessionManager._is_knowledge_tool_name(name):
            return None
        tool_id = block.get("id")
        return str(tool_id) if tool_id else "__taxonagent_without_id__"

    @staticmethod
    def _is_knowledge_tool_name(name: str) -> bool:
        return name.startswith("mcp__taxonagent__") or name == "ictv_family_info"

    @staticmethod
    def _has_ictv_intent(message: str) -> bool:
        return bool(ICTV_INTENT_PATTERN.search(message))

    @staticmethod
    def _content_as_text(content: object) -> str:
        if isinstance(content, str):
            return content.strip()
        if isinstance(content, list):
            parts = []
            for item in content:
                if isinstance(item, dict) and item.get("type") == "text":
                    text = item.get("text")
                    if isinstance(text, str) and text.strip():
                        parts.append(text.strip())
            return "\n".join(parts)
        return ""

    @staticmethod
    def _validate_transcript(path: Path) -> None:
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
            if not lines:
                raise ValueError("empty transcript")
            for line in lines:
                if line.strip():
                    json.loads(line)
        except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
            raise SessionStateError(
                f"Cannot resume corrupt transcript: {path}"
            ) from exc

    def _record_result(
        self, chat_id: str, session: ManagedSession, result: ResultMessage
    ) -> None:
        if result.is_error:
            detail = result.result or "; ".join(result.errors or []) or result.subtype
            raise SessionStateError(f"Claude Agent SDK request failed: {detail}")

        result_session_id = getattr(result, "session_id", None)
        if not result_session_id:
            return

        if session.claude_session_id is None:
            session.claude_session_id = result_session_id
            self._save_claude_session_id(chat_id, result_session_id)
            return

        if result_session_id != session.claude_session_id:
            raise SessionStateError(
                "Claude Agent SDK returned a session ID that does not match "
                "the stored Claude session ID"
            )

    def _chat_map_path(self, chat_id: str) -> Path:
        safe_name = re.sub(r"[^A-Za-z0-9_.-]", "-", chat_id)
        return self.chat_map_dir / f"{safe_name}.json"

    def _load_claude_session_id(self, chat_id: str) -> str | None:
        path = self._chat_map_path(chat_id)
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise SessionStateError(
                f"Cannot read Claude session mapping: {path}"
            ) from exc

        if data.get("openwebui_chat_id") != chat_id:
            raise SessionStateError(
                f"Claude session mapping does not match Open WebUI chat: {path}"
            )
        claude_session_id = data.get("claude_session_id")
        if not isinstance(claude_session_id, str) or not claude_session_id:
            raise SessionStateError(
                f"Claude session mapping is missing claude_session_id: {path}"
            )
        return claude_session_id

    def _save_claude_session_id(self, chat_id: str, claude_session_id: str) -> None:
        path = self._chat_map_path(chat_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        now = time.time()
        created_at = now
        if path.exists():
            try:
                existing = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(existing.get("created_at"), (int, float)):
                    created_at = existing["created_at"]
            except (OSError, UnicodeError, json.JSONDecodeError):
                pass
        payload = {
            "openwebui_chat_id": chat_id,
            "claude_session_id": claude_session_id,
            "created_at": created_at,
            "updated_at": now,
        }
        tmp_path = path.with_suffix(".json.tmp")
        tmp_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        tmp_path.replace(path)

    @staticmethod
    async def _disconnect_session(session: ManagedSession) -> None:
        async with session.lock:
            await session.client.disconnect()

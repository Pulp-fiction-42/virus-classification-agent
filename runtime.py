"""Claude Agent SDK runtime and Open WebUI chat session management."""

from __future__ import annotations

import asyncio
import json
import os
import re
import time
import uuid
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    from claude_agent_sdk import ClaudeAgentOptions, ClaudeSDKClient, ResultMessage, StreamEvent
except ModuleNotFoundError:  # pragma: no cover - exercised by local tests without the SDK installed

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

    @dataclass
    class StreamEvent:  # type: ignore[no-redef]
        event: dict[str, Any]

import config


PLAN_MODE = "plan"
EXECUTE_MODE = "execute"
SDK_PLAN_PERMISSION_MODE = "plan"
SDK_EXECUTE_PERMISSION_MODE = "bypassPermissions"

PLAN_PROMPT_PREFIX = ""

EXECUTE_PROMPT_PREFIX = """当前是 execute 模式。遵循之前计划模式下生成的计划执行。

执行回合只返回普通 Markdown 状态或结果说明；不要输出 analysis_scheme fenced block，也不要输出任何卡片 JSON。
真实 Snakemake 运行开始后，需要写入 /data/runs/<run_id>/.bioagent/state.json，并将 Snakemake stdout/stderr 记录到 /data/runs/<run_id>/.bioagent/execution.log。"""


class SessionError(RuntimeError):
    """Base error for an SDK-backed chat session."""


class InvalidSessionError(SessionError):
    """Raised when Open WebUI does not provide a usable persistent chat ID."""


class SessionStateError(SessionError):
    """Raised when an SDK session cannot be safely used."""


@dataclass
class ManagedSession:
    client: Any
    mode: str
    lock: asyncio.Lock
    last_used_at: float
    bootstrapped: bool


class SessionManager:
    """Keeps one ClaudeSDKClient per Open WebUI chat ID."""

    def __init__(
        self,
        project_root: Path = config.PROJECT_ROOT,
        config_dir: Path = config.CLAUDE_CONFIG_DIR,
        *,
        client_factory: Callable[[ClaudeAgentOptions], Any] = ClaudeSDKClient,
        idle_timeout_seconds: float = config.SESSION_IDLE_TIMEOUT_SECONDS,
    ) -> None:
        self.project_root = project_root.resolve()
        self.config_dir = config_dir.resolve()
        self.config_dir.mkdir(parents=True, exist_ok=True)
        self.client_factory = client_factory
        self.idle_timeout_seconds = idle_timeout_seconds
        self._sessions: dict[str, ManagedSession] = {}
        self._sessions_lock = asyncio.Lock()

        # SDK inspection helpers read this process variable, while the CLI
        # subprocess receives the same value through ClaudeAgentOptions.env.
        os.environ["CLAUDE_CONFIG_DIR"] = str(self.config_dir)

    async def set_mode(self, chat_id: str | None, mode: str) -> None:
        if mode not in {PLAN_MODE, EXECUTE_MODE}:
            raise InvalidSessionError("Mode must be either plan or execute")

        normalized_chat_id = self._validate_chat_id(chat_id)
        session = await self._get_or_create_session(normalized_chat_id)
        permission_mode = self._sdk_permission_mode(mode)

        async with session.lock:
            await session.client.set_permission_mode(permission_mode)
            session.mode = mode
            session.last_used_at = time.time()

    async def stream_turn(self, messages: list[dict], chat_id: str | None) -> AsyncIterator[str]:
        normalized_chat_id = self._validate_chat_id(chat_id)
        session = await self._get_or_create_session(normalized_chat_id)

        async with session.lock:
            prompt = self._prefixed_prompt(session.mode, self._prompt_for_session(messages, session))
            session.last_used_at = time.time()
            await session.client.query(prompt, session_id=normalized_chat_id)

            result_seen = False
            async for message in session.client.receive_response():
                if isinstance(message, StreamEvent):
                    event = message.event
                    if event.get("type") != "content_block_delta":
                        continue
                    delta = event.get("delta", {})
                    if delta.get("type") == "text_delta" and delta.get("text"):
                        yield delta["text"]
                elif isinstance(message, ResultMessage):
                    result_seen = True
                    self._validate_result(normalized_chat_id, message)

            if not result_seen:
                raise SessionStateError("Claude Agent SDK ended without a ResultMessage")

            session.bootstrapped = True
            session.last_used_at = time.time()

    async def ensure_session(self, chat_id: str | None) -> None:
        normalized_chat_id = self._validate_chat_id(chat_id)
        await self._get_or_create_session(normalized_chat_id)

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

    def current_mode(self, chat_id: str | None) -> str:
        normalized_chat_id = self._validate_chat_id(chat_id)
        session = self._sessions.get(normalized_chat_id)
        return session.mode if session else PLAN_MODE

    def transcript_path(self, session_id: str) -> Path:
        project_name = re.sub(r"[^A-Za-z0-9]", "-", str(self.project_root))
        return self.config_dir / "projects" / project_name / f"{session_id}.jsonl"

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
        transcript = self.transcript_path(chat_id)
        if transcript.exists():
            self._validate_transcript(transcript)

        # The SDK can only switch back to bypassPermissions if the subprocess
        # was launched with that capability. We immediately set the active mode
        # to plan before accepting any user turn, so new chats still default to
        # read-only plan behavior.
        options = self._build_options(permission_mode=SDK_EXECUTE_PERMISSION_MODE)
        client = self.client_factory(options)
        await client.connect()
        await client.set_permission_mode(SDK_PLAN_PERMISSION_MODE)
        return ManagedSession(
            client=client,
            mode=PLAN_MODE,
            lock=asyncio.Lock(),
            last_used_at=time.time(),
            bootstrapped=transcript.exists(),
        )

    def _build_options(self, *, permission_mode: str) -> ClaudeAgentOptions:
        return ClaudeAgentOptions(
            system_prompt=config.SYSTEM_PROMPT,
            tools={"type": "preset", "preset": "claude_code"},
            permission_mode=permission_mode,
            model=config.MODEL,
            include_partial_messages=True,
            cwd=self.project_root,
            add_dirs=config.EXTRA_DIRS,
            setting_sources=["project"],
            skills="all",
            env={
                "ANTHROPIC_BASE_URL": config.ANTHROPIC_BASE_URL,
                "ANTHROPIC_AUTH_TOKEN": config.MINIMAX_API_KEY,
                "MINIMAX_API_KEY": config.MINIMAX_API_KEY,
                "CLAUDE_CONFIG_DIR": str(self.config_dir),
            },
        )

    def _prompt_for_session(self, messages: list[dict], session: ManagedSession) -> str:
        if session.bootstrapped:
            return self._latest_user_message(messages)
        return self._render_history(messages)

    @staticmethod
    def _prefixed_prompt(mode: str, prompt: str) -> str:
        if mode == PLAN_MODE:
            return prompt
        return f"{EXECUTE_PROMPT_PREFIX}\n\nUSER REQUEST:\n{prompt}"

    @staticmethod
    def _sdk_permission_mode(mode: str) -> str:
        return SDK_PLAN_PERMISSION_MODE if mode == PLAN_MODE else SDK_EXECUTE_PERMISSION_MODE

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
        return bool(chat_id and (chat_id.startswith("local:") or chat_id.startswith("channel:")))

    @classmethod
    def _render_history(cls, messages: list[dict]) -> str:
        rendered = []
        for message in messages:
            content = cls._content_as_text(message.get("content"))
            if cls._is_runtime_control_message(str(message.get("role", "user")), content):
                continue
            if content:
                rendered.append(f"{str(message.get('role', 'user')).upper()}: {content}")
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
        normalized = content.strip()
        if role == "user" and normalized in {"/plan", "/execute"}:
            return True
        if role == "assistant" and re.fullmatch(r"已切换到 (plan|execute) mode", normalized):
            return True
        return False

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
            raise SessionStateError(f"Cannot resume corrupt transcript: {path}") from exc

    @staticmethod
    def _validate_result(expected_session_id: str, result: ResultMessage) -> None:
        if result.is_error:
            detail = result.result or "; ".join(result.errors or []) or result.subtype
            raise SessionStateError(f"Claude Agent SDK request failed: {detail}")
        result_session_id = getattr(result, "session_id", None)
        if result_session_id and result_session_id != expected_session_id:
            raise SessionStateError(
                "Claude Agent SDK returned a session ID that does not match the Open WebUI chat ID"
            )

    @staticmethod
    async def _disconnect_session(session: ManagedSession) -> None:
        async with session.lock:
            await session.client.disconnect()

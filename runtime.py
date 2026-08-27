"""Claude Agent SDK runtime and Open WebUI chat session management."""

from __future__ import annotations

import asyncio
import json
import os
import re
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    from claude_agent_sdk import (
        AssistantMessage,
        ClaudeAgentOptions,
        ClaudeSDKClient,
        ResultMessage,
        StreamEvent,
        ThinkingBlock,
        ToolUseBlock,
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

    @dataclass
    class ThinkingBlock:  # type: ignore[no-redef]
        thinking: str
        signature: str = ""

    @dataclass
    class ToolUseBlock:  # type: ignore[no-redef]
        id: str
        name: str
        input: dict[str, Any]

    @dataclass
    class AssistantMessage:  # type: ignore[no-redef]
        content: list[Any]
        model: str = ""
        uuid: str | None = None
        message_id: str | None = None
        parent_tool_use_id: str | None = None

    @dataclass
    class StreamEvent:  # type: ignore[no-redef]
        event: dict[str, Any]


import config
from resource_calibration import ResourceCalibrator
from pipeline_spec import (
    canonical_steps,
    ExecutionSpecError,
    normalize_execution_spec,
    normalize_plan_execution_spec,
    validate_plan_rules,
)


PLAN_MODE = "plan"
EXECUTE_MODE = "execute"
SDK_PLAN_PERMISSION_MODE = "dontAsk"
SDK_EXECUTE_PERMISSION_MODE = "bypassPermissions"

PLAN_PROMPT_PREFIX = """当前是 BioAgent 方案模式，不是 Claude Code Plan Mode。
不要调用 EnterPlanMode 或 ExitPlanMode，不要创建或修改任何 plan 文件。
如果用户要求生成、重新生成或调整分析方案，必须在回复末尾输出 analysis_plan 控制块；不得只返回文字方案。
execution_spec.inputs 只能引用 AUTHORITATIVE ANALYSIS DATASET 中已有的 id 和 type；不要输出路径、records、文件角色、run_id 或 sample_id。"""
ANALYSIS_PLAN_BLOCK_RE = re.compile(r"```analysis_plan\s*([\s\S]*?)```", re.IGNORECASE)


class SessionError(RuntimeError):
    """Base error for an SDK-backed chat session."""


class InvalidSessionError(SessionError):
    """Raised when Open WebUI does not provide a usable persistent chat ID."""


class SessionStateError(SessionError):
    """Raised when an SDK session cannot be safely used."""


@dataclass(frozen=True)
class TextDelta:
    text: str


@dataclass(frozen=True)
class OutputItemAdded:
    item: dict[str, Any]


RuntimeEvent = TextDelta | OutputItemAdded


class TrailingControlBlockFilter:
    """Streams visible text while collecting one trailing control block."""

    def __init__(self, marker: str) -> None:
        self.marker = marker
        self.buffer = ""
        self.hidden = ""
        self.collecting = False

    def push(self, text: str) -> list[str]:
        if self.collecting:
            self.hidden += text
            return []

        self.buffer += text
        marker_index = self.buffer.find(self.marker)
        if marker_index >= 0:
            visible = self.buffer[:marker_index]
            self.hidden = self.buffer[marker_index:]
            self.buffer = ""
            self.collecting = True
            return [visible] if visible else []

        keep = len(self.marker) - 1
        if len(self.buffer) <= keep:
            return []

        visible = self.buffer[:-keep]
        self.buffer = self.buffer[-keep:]
        return [visible] if visible else []

    def finish(self) -> tuple[list[str], str]:
        if self.collecting:
            return [], self.hidden
        visible = [self.buffer] if self.buffer else []
        self.buffer = ""
        return visible, ""


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
    mode: str
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
        resource_calibrator: ResourceCalibrator | None = None,
        runner_launcher: Callable[[Path, Path], int] | None = None,
    ) -> None:
        self.project_root = project_root.resolve()
        self.config_dir = config_dir.resolve()
        self.analysis_run_root = analysis_run_root.resolve()
        self.config_dir.mkdir(parents=True, exist_ok=True)
        self.projects_dir = self.config_dir / "projects"
        self.projects_dir.mkdir(parents=True, exist_ok=True)
        self.chat_map_dir = self.config_dir / "bioagent" / "chat-map"
        self.chat_map_dir.mkdir(parents=True, exist_ok=True)
        self.agent_trace_dir = self.config_dir / "bioagent" / "agent-traces"
        self.agent_trace_dir.mkdir(parents=True, exist_ok=True)
        self.sdk_config_dir = sdk_config_dir.resolve()
        self._prepare_sdk_config_dir()
        self.analysis_plan_dir = self.config_dir / "bioagent" / "analysis-plans"
        self.analysis_plan_dir.mkdir(parents=True, exist_ok=True)
        self.resource_history_path = (
            self.config_dir / "bioagent" / "resource-history.sqlite3"
        )
        self.resource_calibrator = resource_calibrator or ResourceCalibrator(
            self.resource_history_path,
            self.analysis_run_root,
        )
        self._resource_backfill = threading.Thread(
            target=self.resource_calibrator.backfill,
            name="bioagent-resource-backfill",
            daemon=True,
        )
        self._resource_backfill.start()
        self.client_factory = client_factory
        self.runner_launcher = runner_launcher or self._launch_runner_process
        self.idle_timeout_seconds = idle_timeout_seconds
        self._sessions: dict[str, ManagedSession] = {}
        self._sessions_lock = asyncio.Lock()
        self._execution_lock = asyncio.Lock()

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

    async def stream_turn(
        self,
        messages: list[dict],
        chat_id: str | None,
        routing_request: dict[str, Any] | None = None,
        dataset_context: dict[str, Any] | None = None,
    ) -> AsyncIterator[RuntimeEvent]:
        normalized_chat_id = self._validate_chat_id(chat_id)
        session = await self._get_or_create_session(normalized_chat_id)

        async with session.lock:
            if session.mode == EXECUTE_MODE:
                async for event in self.start_execution(
                    normalized_chat_id, None, dataset_context
                ):
                    yield event
                session.last_used_at = time.time()
                return

            user_prompt = self._prompt_for_session(messages, session)
            prompt = self._prefixed_prompt(
                session.mode,
                user_prompt,
                routing_request=routing_request,
                dataset_context=dataset_context,
            )
            session.last_used_at = time.time()
            await session.client.query(prompt)

            result_seen = False
            control_filter = TrailingControlBlockFilter("```analysis_plan")
            text_stream = MarkdownTextStream()
            async for message in session.client.receive_response():
                if isinstance(message, StreamEvent):
                    text = text_stream.push(message.event)
                    if text:
                        for visible_text in control_filter.push(text):
                            yield TextDelta(visible_text)
                elif isinstance(message, AssistantMessage):
                    self._record_assistant_trace(normalized_chat_id, message)
                elif isinstance(message, ResultMessage):
                    result_seen = True
                    self._record_result(normalized_chat_id, session, message)

            if not result_seen:
                raise SessionStateError(
                    "Claude Agent SDK ended without a ResultMessage"
                )

            visible_parts, hidden_text = control_filter.finish()
            for visible_text in visible_parts:
                yield TextDelta(visible_text)
            trailing_text, analysis_plan = self._extract_analysis_plan(
                hidden_text,
                routing_request=routing_request,
                dataset_context=dataset_context,
            )
            if trailing_text:
                yield TextDelta(f"\n\n{trailing_text}")
            if analysis_plan:
                self._save_analysis_plan(normalized_chat_id, analysis_plan)
                yield OutputItemAdded(analysis_plan)

            session.bootstrapped = True
            session.last_used_at = time.time()

    async def start_execution(
        self,
        chat_id: str | None,
        assistant_message_id: str | None,
        dataset_context: dict[str, Any] | None = None,
    ) -> AsyncIterator[RuntimeEvent]:
        normalized_chat_id = self._validate_chat_id(chat_id)
        async with self._execution_lock:
            plan = self._load_analysis_plan(normalized_chat_id)
            if not plan or plan.get("ready_to_execute") is not True:
                yield TextDelta("请先生成可执行分析方案，再确认启动分析。")
                return

            try:
                self._validate_execution_dataset(plan, dataset_context)
            except SessionStateError as exc:
                yield TextDelta(str(exc))
                return

            try:
                execution_spec = normalize_execution_spec(plan.get("execution_spec"))
                validate_plan_rules(execution_spec, plan.get("steps") or [])
            except ExecutionSpecError:
                yield TextDelta("当前方案缺少有效执行区段，请在方案模式重新生成方案。")
                return
            plan["execution_spec"] = execution_spec

            run = plan.get("run") if isinstance(plan.get("run"), dict) else {}
            state_path = Path(str(run.get("state_path", ""))).resolve()
            if state_path.exists():
                _, state = self._read_run_state(plan)
                if (
                    state.get("run_id") != run.get("run_id")
                    or state.get("openwebui_chat_id") != normalized_chat_id
                ):
                    raise SessionStateError("现有分析状态与当前方案不匹配。")
                yield TextDelta("该分析任务已经启动，不能重复创建运行。")
                return

            state = self._write_initial_run_state(
                plan, normalized_chat_id, assistant_message_id
            )
            try:
                runner_log = state_path.parent / "runner-process.log"
                self.runner_launcher(state_path, runner_log)
            except Exception as exc:
                state["execution"].update(
                    {
                        "status": "failed",
                        "ok": False,
                        "returncode": -1,
                        "error": f"无法启动分析进程：{exc}",
                        "finished_at": datetime.now()
                        .astimezone()
                        .isoformat(timespec="seconds"),
                    }
                )
                state["technical_report"].update(
                    {"status": "unavailable", "available": False}
                )
                state["report"].update({"status": "unavailable", "available": False})
                self._write_json_atomic(state_path, state)
                yield TextDelta(f"分析任务启动失败：{exc}")
                yield OutputItemAdded(self._analysis_run_item(plan, "failed"))
                return

            yield TextDelta("分析任务已启动，进度与结果将在状态区自动更新。")
            yield OutputItemAdded(self._analysis_run_item(plan, "running"))

    async def stream_ephemeral_turn(
        self, messages: list[dict]
    ) -> AsyncIterator[RuntimeEvent]:
        """Run an Open WebUI background task without persistent chat state."""
        options = self._build_options(permission_mode=SDK_PLAN_PERMISSION_MODE)
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

        # The SDK can only switch back to bypassPermissions if the subprocess
        # was launched with that capability. We immediately set the active mode
        # to plan before accepting any user turn, so new chats still default to
        # read-only plan behavior.
        options = self._build_options(
            permission_mode=SDK_EXECUTE_PERMISSION_MODE,
            resume=claude_session_id,
        )
        client = self.client_factory(options)
        await client.connect()
        await client.set_permission_mode(SDK_PLAN_PERMISSION_MODE)
        return ManagedSession(
            client=client,
            mode=PLAN_MODE,
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
    ) -> ClaudeAgentOptions:
        return ClaudeAgentOptions(
            system_prompt=config.SYSTEM_PROMPT,
            tools={"type": "preset", "preset": "claude_code"},
            allowed_tools=["Read", "Glob", "Grep", "mcp__taxonagent__*"],
            mcp_servers={
                "taxonagent": {
                    "type": "http",
                    "url": config.TAXONAGENT_MCP_URL,
                },
            },
            permission_mode=permission_mode,
            resume=resume,
            model=config.MODEL,
            thinking={
                "type": "enabled",
                "budget_tokens": 20_000,
                "display": "summarized",
            },
            include_partial_messages=True,
            cwd=self.project_root,
            add_dirs=config.EXTRA_DIRS,
            setting_sources=["project"],
            skills="all",
            env={
                "ANTHROPIC_BASE_URL": config.ANTHROPIC_BASE_URL,
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
        mode: str,
        prompt: str,
        routing_request: dict[str, Any] | None = None,
        dataset_context: dict[str, Any] | None = None,
    ) -> str:
        if mode != PLAN_MODE:
            raise SessionStateError("执行模式不应调用 Claude")
        routing_text = (
            "\n\nANALYSIS ROUTING UI SETTINGS (authoritative):\n"
            + json.dumps(routing_request, ensure_ascii=False)
            if routing_request
            else ""
        )
        dataset_text = (
            "\n\nAUTHORITATIVE ANALYSIS DATASET (paths are resolved by runtime):\n"
            + json.dumps(dataset_context.get("summary"), ensure_ascii=False)
            if isinstance(dataset_context, dict)
            and isinstance(dataset_context.get("summary"), dict)
            else "\n\nAUTHORITATIVE ANALYSIS DATASET: none"
        )
        return f"{PLAN_PROMPT_PREFIX}{routing_text}{dataset_text}\n\nUSER REQUEST:\n{prompt}"

    @staticmethod
    def _sdk_permission_mode(mode: str) -> str:
        return (
            SDK_PLAN_PERMISSION_MODE
            if mode == PLAN_MODE
            else SDK_EXECUTE_PERMISSION_MODE
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
        normalized = content.strip()
        if role == "user" and normalized in {"/plan", "/execute"}:
            return True
        if role == "assistant" and re.fullmatch(
            r"已切换到 (plan|execute) mode", normalized
        ):
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

    def _extract_analysis_plan(
        self,
        text: str,
        *,
        routing_request: dict[str, Any] | None = None,
        dataset_context: dict[str, Any] | None = None,
    ) -> tuple[str, dict[str, Any] | None]:
        matches = list(ANALYSIS_PLAN_BLOCK_RE.finditer(text))
        visible = ANALYSIS_PLAN_BLOCK_RE.sub("", text).strip()
        if not matches:
            return visible, None

        for match in reversed(matches):
            try:
                raw_plan = json.loads(match.group(1))
            except json.JSONDecodeError:
                continue
            plan = self._build_analysis_plan(
                raw_plan,
                routing_request=routing_request,
                dataset_context=dataset_context,
            )
            if plan:
                return visible, plan
        return visible, None

    def _build_analysis_plan(
        self,
        raw_plan: Any,
        *,
        routing_request: dict[str, Any] | None = None,
        dataset_context: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        if not isinstance(raw_plan, dict):
            return None

        execution_spec = None
        execution_error = None
        raw_execution_spec = self._apply_analysis_routing(
            raw_plan.get("execution_spec"), routing_request
        )
        try:
            raw_execution_spec = self._bind_dataset_inputs(
                raw_execution_spec, dataset_context
            )
            execution_spec = normalize_plan_execution_spec(raw_execution_spec, [])
        except ExecutionSpecError as exc:
            execution_error = exc
        steps = canonical_steps(execution_spec) if execution_spec else []

        run_id = self._new_run_id()
        run_dir = self.analysis_run_root / run_id
        is_deep = bool(
            execution_spec and execution_spec["policy"].get("requested_mode") == "deep"
        )
        plan: dict[str, Any] = {
            "type": "analysis_plan",
            "id": f"analysis_plan_{run_id}",
            "status": "completed",
            "title": raw_plan.get("title")
            if isinstance(raw_plan.get("title"), str)
            else "分析方案",
            "ready_to_execute": (
                raw_plan.get("ready_to_execute") is not False
                and execution_spec is not None
                and not is_deep
                and isinstance(dataset_context, dict)
                and dataset_context.get("status") == "ready"
            ),
            "run": {
                "run_id": run_id,
                "run_dir": str(run_dir),
                "state_path": str(run_dir / ".bioagent" / "state.json"),
                "log_path": str(run_dir / ".bioagent" / "execution.log"),
            },
            "steps": steps,
            **(
                {"dataset": dataset_context}
                if isinstance(dataset_context, dict)
                else {}
            ),
        }
        if execution_spec is not None:
            plan["execution_spec"] = execution_spec
            plan["routing"] = {
                **execution_spec["policy"],
                "available_routes": ["A", "B"]
                if execution_spec["policy"].get("adaptive")
                else [],
            }
            try:
                plan["resource_estimate"] = self.resource_calibrator.estimate(
                    execution_spec
                )
            except Exception:
                pass
            if is_deep:
                plan["blocking_reason"] = (
                    "深度探索路径暂未开放，请选择智能、紧急或标准模式。"
                )
            elif dataset_context.get("status") != "ready":
                plan["blocking_reason"] = self._dataset_blocking_reason(
                    str(dataset_context.get("status") or "missing")
                )
        elif execution_error is not None:
            plan["blocking_reason"] = self._execution_blocking_reason(execution_error)
        return plan

    @staticmethod
    def _bind_dataset_inputs(
        raw_execution_spec: Any,
        dataset_context: dict[str, Any] | None,
    ) -> Any:
        if not isinstance(dataset_context, dict):
            raise ExecutionSpecError("missing authoritative analysis dataset")
        artifacts = dataset_context.get("artifacts")
        if not isinstance(artifacts, list) or not artifacts:
            raise ExecutionSpecError("analysis dataset has no assigned artifacts")
        if not isinstance(raw_execution_spec, dict):
            return raw_execution_spec

        by_id = {
            artifact.get("id"): artifact
            for artifact in artifacts
            if isinstance(artifact, dict) and isinstance(artifact.get("id"), str)
        }
        requested = raw_execution_spec.get("inputs")
        references = (
            requested if isinstance(requested, list) and requested else artifacts
        )
        bound_inputs = []
        for reference in references:
            if not isinstance(reference, dict):
                raise ExecutionSpecError("dataset input references must be objects")
            artifact = by_id.get(reference.get("id"))
            if artifact is None or reference.get("type") != artifact.get("type"):
                raise ExecutionSpecError("unknown analysis dataset artifact reference")
            records = []
            for record in artifact.get("records", []):
                if not isinstance(record, dict):
                    raise ExecutionSpecError("analysis dataset records must be objects")
                files = record.get("files")
                if not isinstance(files, dict):
                    raise ExecutionSpecError(
                        "analysis dataset record files are missing"
                    )
                records.append(
                    {
                        key: value
                        for key, value in record.items()
                        if key not in {"record_id", "files"}
                    }
                    | {
                        "files": {
                            role: value.get("path")
                            for role, value in files.items()
                            if isinstance(value, dict)
                        }
                    }
                )
            bound_inputs.append(
                {"id": artifact["id"], "type": artifact["type"], "records": records}
            )
        return {**raw_execution_spec, "inputs": bound_inputs}

    @staticmethod
    def _dataset_blocking_reason(status: str) -> str:
        return {
            "checking": "数据集正在执行全量预检，完成前不能启动分析。",
            "needs_review": "数据集的配对或样本关系需要人工确认。",
            "invalid": "数据集未通过格式或配对预检。",
            "stale": "数据集文件在预检后发生变化，请建立新 revision。",
            "missing": "请先建立并选择数据集。",
        }.get(status, "数据集尚未满足执行条件。")

    def _validate_execution_dataset(
        self,
        plan: dict[str, Any],
        current: dict[str, Any] | None,
    ) -> None:
        planned = plan.get("dataset")
        if not isinstance(planned, dict) or not isinstance(current, dict):
            raise SessionStateError("当前方案没有可验证的数据集，请重新建立方案。")
        if planned.get("id") != current.get("id") or planned.get(
            "revision"
        ) != current.get("revision"):
            raise SessionStateError(
                "当前数据集 revision 与方案不一致，请重新生成方案。"
            )
        if current.get("status") != "ready":
            raise SessionStateError(
                self._dataset_blocking_reason(str(current.get("status")))
            )
        if planned.get("artifacts") != current.get("artifacts"):
            raise SessionStateError("数据集内容与方案快照不一致，请重新生成方案。")
        for artifact in current.get("artifacts", []):
            for record in artifact.get("records", []):
                for value in (record.get("files") or {}).values():
                    if not isinstance(value, dict) or not isinstance(
                        value.get("path"), str
                    ):
                        raise SessionStateError("数据集缺少有效文件指纹。")
                    path = Path(value["path"]).resolve()
                    if not self._is_under_analysis_root(path):
                        raise SessionStateError("数据集文件不在 BIOAGENT_RUN_ROOT 内。")
                    try:
                        stat = path.stat()
                    except OSError as exc:
                        raise SessionStateError(
                            "数据集文件已不可用，请重新预检。"
                        ) from exc
                    actual = {
                        "device": stat.st_dev,
                        "inode": stat.st_ino,
                        "size": stat.st_size,
                        "mtime_ns": stat.st_mtime_ns,
                    }
                    if actual != value.get("fingerprint"):
                        raise SessionStateError(
                            "数据集文件在预检后发生变化，请建立新 revision。"
                        )

    @staticmethod
    def _apply_analysis_routing(
        raw_execution_spec: Any,
        routing_request: dict[str, Any] | None,
    ) -> Any:
        if not routing_request or not isinstance(raw_execution_spec, dict):
            return raw_execution_spec
        return {**raw_execution_spec, "policy": routing_request}

    @staticmethod
    def _execution_blocking_reason(error: ExecutionSpecError) -> str:
        message = str(error)
        if "missing execution_spec" in message:
            return "方案缺少可执行流程信息，请补充输入后重新生成方案。"
        if "input" in message or "path" in message:
            return "输入文件信息与流程起点不匹配，请检查输入后重新生成方案。"
        return "流程区段与方案步骤不一致，请重新生成方案。"

    @staticmethod
    def _new_run_id() -> str:
        return f"analysis-{datetime.now().strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:8]}"

    def _analysis_plan_path(self, chat_id: str) -> Path:
        safe_name = re.sub(r"[^A-Za-z0-9_.-]", "-", chat_id)
        return self.analysis_plan_dir / f"{safe_name}.json"

    def _load_analysis_plan(self, chat_id: str) -> dict[str, Any] | None:
        path = self._analysis_plan_path(chat_id)
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise SessionStateError(
                f"Cannot read analysis plan reservation: {path}"
            ) from exc
        return data if isinstance(data, dict) else None

    def _save_analysis_plan(self, chat_id: str, plan: dict[str, Any]) -> None:
        payload = {
            **plan,
            "openwebui_chat_id": chat_id,
            "updated_at": time.time(),
        }
        self._write_json_atomic(self._analysis_plan_path(chat_id), payload)

    def _write_initial_run_state(
        self,
        plan: dict[str, Any],
        chat_id: str,
        assistant_message_id: str | None,
    ) -> dict[str, Any]:
        run = plan.get("run") if isinstance(plan.get("run"), dict) else {}
        run_dir = Path(str(run.get("run_dir", ""))).resolve()
        if not self._is_under_analysis_root(run_dir):
            raise SessionStateError(
                f"分析运行目录不在 BIOAGENT_RUN_ROOT 内：{run_dir}；root={self.analysis_run_root}"
            )

        state_path = run_dir / ".bioagent" / "state.json"
        requested_state_path = Path(str(run.get("state_path", ""))).resolve()
        if requested_state_path != state_path:
            raise SessionStateError("分析状态路径与运行目录不匹配。")
        if state_path.exists():
            raise SessionStateError("当前分析方案已经启动，请重新生成方案后再执行。")

        log_path = Path(
            str(run.get("log_path", run_dir / ".bioagent" / "execution.log"))
        ).resolve()
        if not self._is_under_analysis_root(log_path):
            raise SessionStateError(
                f"分析日志不在 BIOAGENT_RUN_ROOT 内：{log_path}；root={self.analysis_run_root}"
            )

        try:
            execution_spec = normalize_execution_spec(plan.get("execution_spec"))
            validate_plan_rules(execution_spec, plan.get("steps") or [])
        except ExecutionSpecError as exc:
            raise SessionStateError(f"分析方案执行区段无效：{exc}") from exc

        state = {
            "version": 3,
            "run_id": run.get("run_id"),
            "run_dir": str(run_dir),
            "openwebui_chat_id": chat_id,
            "assistant_message_id": assistant_message_id,
            "title": plan.get("title"),
            "dataset": plan.get("dataset"),
            "plan": plan,
            "execution_spec": execution_spec,
            "steps": [
                {
                    "id": step["id"],
                    "label": step["label"],
                    **({"tool": step["tool"]} if step.get("tool") else {}),
                    **({"rules": step["rules"]} if step.get("rules") else {}),
                    **(
                        {"depends_on": step["depends_on"]}
                        if isinstance(step.get("depends_on"), list)
                        else {}
                    ),
                    **({"produces": step["produces"]} if step.get("produces") else {}),
                    "is_deliverable": step.get("is_deliverable") is True,
                    "status": "pending",
                }
                for step in plan.get("steps", [])
                if isinstance(step, dict) and step.get("id") and step.get("label")
            ],
            "resource_estimate": plan.get("resource_estimate"),
            **(
                {"routing": plan["routing"]}
                if isinstance(plan.get("routing"), dict)
                else {}
            ),
            "resource_history_db": str(self.resource_history_path),
            "execution": {
                "started": True,
                "status": "running",
                "ok": None,
                "returncode": None,
                "current_step_id": None,
                "log": str(log_path),
            },
            "technical_report": {
                "status": "collecting",
                "available": False,
                "telemetry": str(run_dir / ".bioagent" / "telemetry.json"),
                "telemetry_version": 3,
                "agent_trace": str(run_dir / ".bioagent" / "agent-trace.jsonl"),
            },
            "report": {
                "status": "collecting",
                "available": False,
                "schema_version": 4,
                "telemetry": str(run_dir / ".bioagent" / "telemetry.json"),
            },
        }
        try:
            self.analysis_run_root.mkdir(parents=True, exist_ok=True)
            bioagent_dir = run_dir / ".bioagent"
            bioagent_dir.mkdir(parents=True, exist_ok=True)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            log_path.touch(exist_ok=True)
            self._snapshot_agent_trace(chat_id, bioagent_dir / "agent-trace.jsonl")
            self._write_json_atomic(state_path, state)
        except OSError as exc:
            raise SessionStateError(
                f"无法创建分析运行目录：{run_dir}，请检查 BIOAGENT_RUN_ROOT 写权限"
            ) from exc
        return state

    def _launch_runner_process(self, state_path: Path, log_path: Path) -> int:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("ab") as runner_log:
            process = subprocess.Popen(
                [
                    sys.executable,
                    str(self.project_root / "analysis_runner.py"),
                    "--state",
                    str(state_path),
                ],
                cwd=self.project_root,
                stdin=subprocess.DEVNULL,
                stdout=runner_log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                close_fds=True,
            )
        threading.Thread(
            target=process.wait,
            name=f"bioagent-runner-{process.pid}",
            daemon=True,
        ).start()
        return process.pid

    @staticmethod
    def _analysis_run_item(plan: dict[str, Any], status: str) -> dict[str, Any]:
        run = plan.get("run") if isinstance(plan.get("run"), dict) else {}
        return {
            "type": "analysis_run",
            "id": f"analysis_run_{run.get('run_id')}",
            "status": status,
            "run": {
                key: run.get(key)
                for key in ("run_id", "run_dir", "state_path", "log_path")
            },
        }

    def _read_run_state(self, plan: dict[str, Any]) -> tuple[Path, dict[str, Any]]:
        run = plan.get("run") if isinstance(plan.get("run"), dict) else {}
        state_path = Path(str(run.get("state_path", ""))).resolve()
        if not self._is_under_analysis_root(state_path):
            raise ValueError("Analysis state is outside BIOAGENT_RUN_ROOT")
        state = json.loads(state_path.read_text(encoding="utf-8"))
        if not isinstance(state, dict):
            raise ValueError("Analysis state must be a JSON object")
        return state_path, state

    def _is_under_analysis_root(self, path: Path) -> bool:
        try:
            path.resolve().relative_to(self.analysis_run_root)
            return True
        except ValueError:
            return False

    @staticmethod
    def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_suffix(f"{path.suffix}.tmp")
        tmp_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        tmp_path.replace(path)

    def _agent_trace_path(self, chat_id: str) -> Path:
        safe_name = re.sub(r"[^A-Za-z0-9_.-]", "-", chat_id)
        return self.agent_trace_dir / f"{safe_name}.jsonl"

    @classmethod
    def _redact_trace_value(cls, value: Any, key: str = "") -> Any:
        if re.search(r"token|secret|password|api[_-]?key|authorization", key, re.I):
            return "[REDACTED]"
        if isinstance(value, dict):
            return {
                str(item_key): cls._redact_trace_value(item, str(item_key))
                for item_key, item in value.items()
            }
        if isinstance(value, list):
            return [cls._redact_trace_value(item) for item in value]
        if isinstance(value, str):
            return re.sub(
                r"(?i)(bearer\s+|(?:api[_-]?key|token|secret|password)\s*[=:]\s*)\S+",
                r"\1[REDACTED]",
                value,
            )
        return value

    def _record_assistant_trace(self, chat_id: str, message: AssistantMessage) -> None:
        message_id = (
            getattr(message, "uuid", None)
            or getattr(message, "message_id", None)
            or str(uuid.uuid4())
        )
        records = []
        for index, block in enumerate(message.content):
            event_id = f"{message_id}:{index}"
            if isinstance(block, ThinkingBlock) and block.thinking.strip():
                records.append(
                    {
                        "version": 1,
                        "id": event_id,
                        "type": "thinking_summary",
                        "timestamp": datetime.now()
                        .astimezone()
                        .isoformat(timespec="seconds"),
                        "message_id": message_id,
                        "thinking": block.thinking.strip(),
                    }
                )
            elif isinstance(block, ToolUseBlock):
                records.append(
                    {
                        "version": 1,
                        "id": event_id,
                        "type": "tool_use",
                        "timestamp": datetime.now()
                        .astimezone()
                        .isoformat(timespec="seconds"),
                        "message_id": message_id,
                        "tool": block.name,
                        "input": self._redact_trace_value(block.input),
                    }
                )
        if not records:
            return

        path = self._agent_trace_path(chat_id)
        existing_ids: set[str] = set()
        if path.is_file():
            try:
                for line in path.read_text(encoding="utf-8").splitlines():
                    if line.strip():
                        value = json.loads(line)
                        if isinstance(value, dict) and isinstance(value.get("id"), str):
                            existing_ids.add(value["id"])
            except (OSError, UnicodeError, json.JSONDecodeError) as exc:
                raise SessionStateError(f"Cannot read agent trace: {path}") from exc
        new_records = [record for record in records if record["id"] not in existing_ids]
        if not new_records:
            return
        try:
            with path.open("a", encoding="utf-8") as stream:
                for record in new_records:
                    stream.write(json.dumps(record, ensure_ascii=False) + "\n")
        except OSError as exc:
            raise SessionStateError(f"Cannot write agent trace: {path}") from exc

    def _snapshot_agent_trace(self, chat_id: str, destination: Path) -> None:
        source = self._agent_trace_path(chat_id)
        content = source.read_text(encoding="utf-8") if source.is_file() else ""
        destination.write_text(content, encoding="utf-8")

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

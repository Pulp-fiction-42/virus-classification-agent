"""Claude Agent SDK runtime and Open WebUI chat session management."""

from __future__ import annotations

import asyncio
import html
import json
import os
import re
import time
import uuid
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

try:
    from claude_agent_sdk import (
        ClaudeAgentOptions,
        ClaudeSDKClient,
        ResultMessage,
        StreamEvent,
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
    class StreamEvent:  # type: ignore[no-redef]
        event: dict[str, Any]


import config


PLAN_MODE = "plan"
EXECUTE_MODE = "execute"
SDK_PLAN_PERMISSION_MODE = "dontAsk"
SDK_EXECUTE_PERMISSION_MODE = "bypassPermissions"

PLAN_PROMPT_PREFIX = """当前是 BioAgent 方案模式，不是 Claude Code Plan Mode。
不要调用 EnterPlanMode 或 ExitPlanMode，不要创建或修改任何 plan 文件。
如果用户要求生成、重新生成或调整分析方案，必须在回复末尾输出 analysis_plan 控制块；不得只返回文字方案。"""
ANALYSIS_PLAN_BLOCK_RE = re.compile(r"```analysis_plan\s*([\s\S]*?)```", re.IGNORECASE)
DIAGNOSTIC_REPORT_BLOCK_RE = re.compile(
    r"```diagnostic_report\s*([\s\S]*?)```", re.IGNORECASE
)
DIAGNOSTIC_REPORT_FILENAME = "diagnostic-opinion.html"

EXECUTE_PROMPT_PREFIX = """当前是 execute 模式。遵循之前计划模式下生成的计划执行。

执行回合先返回普通 Markdown 状态或结果说明；不要输出 analysis_plan fenced block，也不要输出任何卡片 JSON。
runtime 已经创建 run_dir 和 .bioagent/state.json。必须使用注入的 run_id、run_dir、state_path、log_path 和 steps；不要创建第二个 run 目录。
每次运行 Snakemake 都必须显式传入 --directory "<run_dir>/workflow"、--snakefile "<run_dir>/workflow/Snakefile" 和绝对 configfile 路径；不得只依赖 cd，否则会污染 agent 工作目录。
每个阶段开始或结束时更新 state.json 中的 steps[].status 和 execution.current_step_id。
步骤状态只使用 pending、running、completed、failed；execution 状态只使用 running、completed、failed。
不要后台运行；命令结束前必须写入 execution.status、execution.ok、execution.returncode。
只有执行成功时，必须在回复最后追加一个 diagnostic_report fenced block，内容是无样式的 <article class="diagnostic-report"> HTML fragment；失败时不得输出该控制块。"""

DIAGNOSTIC_REPORT_STYLES = """
@page {
  size: A4;
  margin: 18mm;
}

:root {
  color: #202124;
  background: #ffffff;
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "Noto Sans SC", sans-serif;
  font-size: 15px;
  line-height: 1.65;
}

* {
  box-sizing: border-box;
}

body {
  margin: 0;
  background: #f5f6f7;
}

.diagnostic-report {
  width: min(210mm, 100%);
  min-height: 297mm;
  margin: 24px auto;
  padding: 18mm;
  background: #ffffff;
  box-shadow: 0 8px 28px rgb(0 0 0 / 8%);
}

header {
  padding-bottom: 18px;
  border-bottom: 2px solid #30343b;
}

h1, h2, h3, p {
  margin-top: 0;
}

h1 {
  margin-bottom: 8px;
  font-size: 28px;
  letter-spacing: 0.08em;
}

h2 {
  margin: 28px 0 12px;
  padding-bottom: 6px;
  border-bottom: 1px solid #d9dce1;
  font-size: 18px;
}

h3 {
  margin: 20px 0 8px;
  font-size: 16px;
}

p, ul, ol, dl, table {
  margin-bottom: 12px;
}

ul, ol {
  padding-left: 1.5em;
}

dt {
  color: #5f6368;
  font-size: 13px;
}

dd {
  margin: 0 0 10px;
  font-weight: 600;
}

table {
  width: 100%;
  border-collapse: collapse;
  font-size: 13px;
}

th, td {
  padding: 8px 10px;
  border: 1px solid #d9dce1;
  text-align: left;
  vertical-align: top;
}

th {
  background: #f1f3f4;
  font-weight: 600;
}

code {
  overflow-wrap: anywhere;
  font-family: "SFMono-Regular", Consolas, monospace;
  font-size: 0.9em;
}

.review-status, .note {
  color: #5f6368;
}

section, table, tr {
  break-inside: avoid;
}

@media print {
  body {
    background: #ffffff;
  }

  .diagnostic-report {
    width: auto;
    min-height: auto;
    margin: 0;
    padding: 0;
    box-shadow: none;
  }
}
""".strip()


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


class DiagnosticReportFragmentValidator(HTMLParser):
    """Accept the small semantic HTML subset used by diagnostic reports."""

    allowed_tags = {
        "article",
        "header",
        "section",
        "h1",
        "h2",
        "h3",
        "p",
        "ul",
        "ol",
        "li",
        "dl",
        "dt",
        "dd",
        "table",
        "thead",
        "tbody",
        "tr",
        "th",
        "td",
        "strong",
        "em",
        "small",
        "code",
        "br",
    }
    void_tags = {"br"}

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.stack: list[str] = []
        self.root_count = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self._validate_tag(tag, attrs)
        if not self.stack:
            if tag != "article" or self.root_count:
                raise ValueError("Report fragment must contain one article root")
            classes = dict(attrs).get("class", "") or ""
            if "diagnostic-report" not in classes.split():
                raise ValueError("Report article must use class diagnostic-report")
            self.root_count += 1
        elif tag == "article":
            raise ValueError("Nested report articles are not allowed")
        if tag not in self.void_tags:
            self.stack.append(tag)

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag not in self.void_tags or not self.stack:
            raise ValueError("Only br may be self-closing inside the report")
        self._validate_tag(tag, attrs)

    def handle_endtag(self, tag: str) -> None:
        if tag in self.void_tags or not self.stack or self.stack[-1] != tag:
            raise ValueError(f"Unbalanced report tag: {tag}")
        self.stack.pop()

    def handle_data(self, data: str) -> None:
        if not self.stack and data.strip():
            raise ValueError("Report content must stay inside the article root")

    def handle_comment(self, data: str) -> None:
        raise ValueError("Report comments are not allowed")

    def handle_decl(self, decl: str) -> None:
        raise ValueError("Report fragment must not contain a document declaration")

    def handle_pi(self, data: str) -> None:
        raise ValueError("Report processing instructions are not allowed")

    def finish(self) -> None:
        self.close()
        if self.stack or self.root_count != 1:
            raise ValueError("Report fragment must contain one balanced article root")

    def _validate_tag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag not in self.allowed_tags:
            raise ValueError(f"Report tag is not allowed: {tag}")
        if any(name != "class" for name, _value in attrs):
            raise ValueError("Only class attributes are allowed in reports")


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
        self.analysis_plan_dir = self.config_dir / "bioagent" / "analysis-plans"
        self.analysis_plan_dir.mkdir(parents=True, exist_ok=True)
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
        self, messages: list[dict], chat_id: str | None
    ) -> AsyncIterator[RuntimeEvent]:
        normalized_chat_id = self._validate_chat_id(chat_id)
        session = await self._get_or_create_session(normalized_chat_id)

        async with session.lock:
            user_prompt = self._prompt_for_session(messages, session)
            plan = None
            if session.mode == EXECUTE_MODE:
                plan = self._load_analysis_plan(normalized_chat_id)
                if not plan or plan.get("ready_to_execute") is not True:
                    yield TextDelta("请先生成可执行分析方案，再确认启动分析。")
                    return
                self._write_initial_run_state(plan)

            try:
                prompt = self._prefixed_prompt(session.mode, user_prompt, plan)
                session.last_used_at = time.time()
                await session.client.query(prompt)

                result_seen = False
                control_filter = TrailingControlBlockFilter(
                    "```analysis_plan"
                    if session.mode == PLAN_MODE
                    else "```diagnostic_report"
                )
                async for message in session.client.receive_response():
                    if isinstance(message, StreamEvent):
                        event = message.event
                        if event.get("type") != "content_block_delta":
                            continue
                        delta = event.get("delta", {})
                        if delta.get("type") == "text_delta" and delta.get("text"):
                            for visible_text in control_filter.push(delta["text"]):
                                yield TextDelta(visible_text)
                    elif isinstance(message, ResultMessage):
                        result_seen = True
                        self._record_result(normalized_chat_id, session, message)

                if not result_seen:
                    raise SessionStateError(
                        "Claude Agent SDK ended without a ResultMessage"
                    )
            except Exception:
                if plan:
                    self._fail_missing_terminal_state(plan)
                raise

            visible_parts, hidden_text = control_filter.finish()
            for visible_text in visible_parts:
                yield TextDelta(visible_text)

            if session.mode == PLAN_MODE:
                trailing_text, analysis_plan = self._extract_analysis_plan(hidden_text)
                if trailing_text:
                    yield TextDelta(f"\n\n{trailing_text}")
                if analysis_plan:
                    self._save_analysis_plan(normalized_chat_id, analysis_plan)
                    yield OutputItemAdded(analysis_plan)
            elif plan:
                self._fail_missing_terminal_state(plan)
                trailing_text, report_fragment = self._extract_diagnostic_report(
                    hidden_text
                )
                if trailing_text:
                    yield TextDelta(f"\n\n{trailing_text}")
                if self._run_succeeded(plan):
                    if report_fragment:
                        try:
                            report_item = self._save_diagnostic_report(
                                plan, report_fragment
                            )
                        except (OSError, UnicodeError, ValueError) as exc:
                            self._mark_report_failed(plan, str(exc))
                            yield TextDelta(
                                "\n\n> 诊断意见书生成失败，请检查本次分析输出。"
                            )
                        else:
                            yield OutputItemAdded(report_item)
                    else:
                        self._mark_report_failed(
                            plan, "Missing or invalid diagnostic_report block"
                        )
                        yield TextDelta(
                            "\n\n> 诊断意见书生成失败，请检查本次分析输出。"
                        )

            session.bootstrapped = True
            session.last_used_at = time.time()

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

            async for message in client.receive_response():
                if isinstance(message, StreamEvent):
                    event = message.event
                    if event.get("type") != "content_block_delta":
                        continue
                    delta = event.get("delta", {})
                    if delta.get("type") == "text_delta" and delta.get("text"):
                        yield TextDelta(delta["text"])
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
        mode: str, prompt: str, plan: dict[str, Any] | None = None
    ) -> str:
        if mode == PLAN_MODE:
            return f"{PLAN_PROMPT_PREFIX}\n\nUSER REQUEST:\n{prompt}"
        plan_json = json.dumps(plan, ensure_ascii=False, indent=2) if plan else "{}"
        return (
            f"{EXECUTE_PROMPT_PREFIX}\n\n"
            f"RUNTIME ANALYSIS PLAN:\n```json\n{plan_json}\n```\n\n"
            f"USER REQUEST:\n{prompt}"
        )

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

    def _extract_analysis_plan(self, text: str) -> tuple[str, dict[str, Any] | None]:
        matches = list(ANALYSIS_PLAN_BLOCK_RE.finditer(text))
        visible = ANALYSIS_PLAN_BLOCK_RE.sub("", text).strip()
        if not matches:
            return visible, None

        for match in reversed(matches):
            try:
                raw_plan = json.loads(match.group(1))
            except json.JSONDecodeError:
                continue
            plan = self._build_analysis_plan(raw_plan)
            if plan:
                return visible, plan
        return visible, None

    @staticmethod
    def _extract_diagnostic_report(text: str) -> tuple[str, str | None]:
        matches = list(DIAGNOSTIC_REPORT_BLOCK_RE.finditer(text))
        visible = DIAGNOSTIC_REPORT_BLOCK_RE.sub("", text).strip()
        if not matches:
            return visible, None

        fragment = matches[-1].group(1).strip()
        return visible, fragment or None

    def _build_analysis_plan(self, raw_plan: Any) -> dict[str, Any] | None:
        if not isinstance(raw_plan, dict) or not isinstance(
            raw_plan.get("steps"), list
        ):
            return None

        steps: list[dict[str, Any]] = []
        for raw_step in raw_plan["steps"]:
            if not isinstance(raw_step, dict):
                continue
            label = raw_step.get("label")
            if not isinstance(label, str) or not label.strip():
                continue
            step: dict[str, Any] = {
                "id": f"step_{len(steps) + 1}",
                "label": label.strip(),
            }
            tool = raw_step.get("tool")
            if isinstance(tool, str) and tool.strip():
                step["tool"] = tool.strip()
            rules = raw_step.get("rules")
            if isinstance(rules, list):
                normalized_rules = [
                    rule.strip()
                    for rule in rules
                    if isinstance(rule, str) and rule.strip()
                ]
                if normalized_rules:
                    step["rules"] = normalized_rules
            steps.append(step)

        if not steps:
            return None

        run_id = self._new_run_id()
        run_dir = self.analysis_run_root / run_id
        resource_estimate = raw_plan.get("resource_estimate")
        plan: dict[str, Any] = {
            "type": "analysis_plan",
            "id": f"analysis_plan_{run_id}",
            "status": "completed",
            "title": raw_plan.get("title")
            if isinstance(raw_plan.get("title"), str)
            else "分析方案",
            "ready_to_execute": raw_plan.get("ready_to_execute") is not False,
            "run": {
                "run_id": run_id,
                "run_dir": str(run_dir),
                "state_path": str(run_dir / ".bioagent" / "state.json"),
                "log_path": str(run_dir / ".bioagent" / "execution.log"),
            },
            "steps": steps,
        }
        if isinstance(resource_estimate, dict):
            plan["resource_estimate"] = {
                "eta": resource_estimate.get("eta"),
                "compute": resource_estimate.get("compute")
                or resource_estimate.get("resource"),
                "confidence": resource_estimate.get("confidence"),
            }
        return plan

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

    def _write_initial_run_state(self, plan: dict[str, Any]) -> None:
        run = plan.get("run") if isinstance(plan.get("run"), dict) else {}
        run_dir = Path(str(run.get("run_dir", ""))).resolve()
        if not self._is_under_analysis_root(run_dir):
            raise SessionStateError(
                f"分析运行目录不在 BIOAGENT_RUN_ROOT 内：{run_dir}；root={self.analysis_run_root}"
            )

        log_path = Path(
            str(run.get("log_path", run_dir / ".bioagent" / "execution.log"))
        ).resolve()
        if not self._is_under_analysis_root(log_path):
            raise SessionStateError(
                f"分析日志不在 BIOAGENT_RUN_ROOT 内：{log_path}；root={self.analysis_run_root}"
            )

        state = {
            "version": 1,
            "run_id": run.get("run_id"),
            "run_dir": str(run_dir),
            "title": plan.get("title"),
            "steps": [
                {
                    "id": step["id"],
                    "label": step["label"],
                    **({"tool": step["tool"]} if step.get("tool") else {}),
                    **({"rules": step["rules"]} if step.get("rules") else {}),
                    "status": "pending",
                }
                for step in plan.get("steps", [])
                if isinstance(step, dict) and step.get("id") and step.get("label")
            ],
            "resource_estimate": plan.get("resource_estimate"),
            "execution": {
                "started": True,
                "status": "running",
                "ok": None,
                "returncode": None,
                "current_step_id": None,
                "log": str(log_path),
            },
        }
        try:
            self.analysis_run_root.mkdir(parents=True, exist_ok=True)
            bioagent_dir = run_dir / ".bioagent"
            bioagent_dir.mkdir(parents=True, exist_ok=True)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            log_path.touch(exist_ok=True)
            self._write_json_atomic(bioagent_dir / "state.json", state)
        except OSError as exc:
            raise SessionStateError(
                f"无法创建分析运行目录：{run_dir}，请检查 BIOAGENT_RUN_ROOT 写权限"
            ) from exc

    def _fail_missing_terminal_state(self, plan: dict[str, Any]) -> None:
        run = plan.get("run") if isinstance(plan.get("run"), dict) else {}
        state_path = Path(str(run.get("state_path", ""))).resolve()
        if not self._is_under_analysis_root(state_path):
            return

        try:
            state = json.loads(state_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            state = {
                "version": 1,
                "run_id": run.get("run_id"),
                "run_dir": run.get("run_dir"),
                "steps": plan.get("steps", []),
                "execution": {},
            }

        execution = state.setdefault("execution", {})
        if isinstance(execution, dict) and isinstance(execution.get("ok"), bool):
            return

        if isinstance(execution, dict):
            execution["started"] = True
            execution["status"] = "failed"
            execution["ok"] = False
            execution["returncode"] = -1
            execution["error"] = "Agent turn ended without terminal execution state"

        steps = state.get("steps")
        if isinstance(steps, list):
            for step in steps:
                if isinstance(step, dict) and step.get("status") == "running":
                    step["status"] = "failed"
                    break

        self._write_json_atomic(state_path, state)

    def _run_succeeded(self, plan: dict[str, Any]) -> bool:
        _state_path, state = self._read_run_state(plan)
        execution = state.get("execution")
        return bool(
            isinstance(execution, dict)
            and execution.get("status") == "completed"
            and execution.get("ok") is True
        )

    def _save_diagnostic_report(
        self, plan: dict[str, Any], fragment: str
    ) -> dict[str, Any]:
        validator = DiagnosticReportFragmentValidator()
        validator.feed(fragment)
        validator.finish()

        state_path, state = self._read_run_state(plan)
        execution = state.get("execution")
        if not (
            isinstance(execution, dict)
            and execution.get("status") == "completed"
            and execution.get("ok") is True
        ):
            raise ValueError("Diagnostic reports require a successful run")

        run = plan.get("run") if isinstance(plan.get("run"), dict) else {}
        run_id = str(run.get("run_id") or state.get("run_id") or "")
        run_dir = Path(str(run.get("run_dir") or state.get("run_dir") or "")).resolve()
        if not run_id or not self._is_under_analysis_root(run_dir):
            raise ValueError("Diagnostic report run is outside BIOAGENT_RUN_ROOT")

        title_base = str(plan.get("title") or state.get("title") or "分析结果")
        title = f"{title_base} · 诊断意见书"
        report_path = run_dir / DIAGNOSTIC_REPORT_FILENAME
        generated_at = int(time.time())
        document = self._render_diagnostic_report(title, fragment)
        self._write_text_atomic(report_path, document)

        state["report"] = {
            "status": "completed",
            "title": title,
            "path": str(report_path),
            "generated_at": generated_at,
        }
        self._write_json_atomic(state_path, state)

        return {
            "type": "diagnostic_report",
            "id": f"diagnostic_report_{run_id}",
            "status": "completed",
            "title": title,
            "run": {
                "run_id": run_id,
                "run_dir": str(run_dir),
            },
            "filename": DIAGNOSTIC_REPORT_FILENAME,
            "generated_at": generated_at,
        }

    def _mark_report_failed(self, plan: dict[str, Any], error: str) -> None:
        try:
            state_path, state = self._read_run_state(plan)
            state["report"] = {"status": "failed", "error": error}
            self._write_json_atomic(state_path, state)
        except (OSError, UnicodeError, json.JSONDecodeError, ValueError):
            return

    def _read_run_state(self, plan: dict[str, Any]) -> tuple[Path, dict[str, Any]]:
        run = plan.get("run") if isinstance(plan.get("run"), dict) else {}
        state_path = Path(str(run.get("state_path", ""))).resolve()
        if not self._is_under_analysis_root(state_path):
            raise ValueError("Analysis state is outside BIOAGENT_RUN_ROOT")
        state = json.loads(state_path.read_text(encoding="utf-8"))
        if not isinstance(state, dict):
            raise ValueError("Analysis state must be a JSON object")
        return state_path, state

    @staticmethod
    def _render_diagnostic_report(title: str, fragment: str) -> str:
        return (
            "<!doctype html>\n"
            '<html lang="zh-CN">\n'
            "<head>\n"
            '<meta charset="utf-8">\n'
            '<meta name="viewport" content="width=device-width, initial-scale=1">\n'
            f"<title>{html.escape(title)}</title>\n"
            f"<style>\n{DIAGNOSTIC_REPORT_STYLES}\n</style>\n"
            "</head>\n"
            f"<body>\n{fragment.strip()}\n</body>\n"
            "</html>\n"
        )

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

    @staticmethod
    def _write_text_atomic(path: Path, content: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_suffix(f"{path.suffix}.tmp")
        tmp_path.write_text(content, encoding="utf-8")
        tmp_path.replace(path)

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

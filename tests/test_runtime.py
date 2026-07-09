from __future__ import annotations

import json
import tempfile
import unittest
import uuid
from dataclasses import dataclass
from pathlib import Path

import config
import runtime
from runtime import (
    EXECUTE_MODE,
    PLAN_MODE,
    EXECUTE_PROMPT_PREFIX,
    PLAN_PROMPT_PREFIX,
    InvalidSessionError,
    SessionManager,
    SessionStateError,
)


MESSAGES = [
    {"role": "user", "content": "first question"},
    {"role": "assistant", "content": "first answer"},
    {"role": "user", "content": "follow-up question"},
]


@dataclass
class FakeStreamEvent:
    event: dict


@dataclass
class FakeResultMessage:
    session_id: str = "default"
    is_error: bool = False
    result: str | None = None
    errors: list[str] | None = None
    subtype: str = "success"


class FakeClient:
    instances = []

    def __init__(self, options):
        self.options = options
        self.connected = False
        self.disconnected = False
        self.permission_modes = []
        self.queries = []
        FakeClient.instances.append(self)

    async def connect(self):
        self.connected = True

    async def query(self, prompt, session_id=None):
        self.queries.append((prompt, session_id))

    async def receive_response(self):
        session_id = self.options.resume or "sdk-session-1"
        yield FakeStreamEvent(
            {
                "type": "content_block_delta",
                "delta": {"type": "text_delta", "text": "chunk"},
            }
        )
        yield FakeResultMessage(session_id=session_id, is_error=False, subtype="success")

    async def set_permission_mode(self, mode):
        self.permission_modes.append(mode)

    async def disconnect(self):
        self.disconnected = True


class SessionManagerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.real_stream_event = runtime.StreamEvent
        self.real_result_message = runtime.ResultMessage
        runtime.StreamEvent = FakeStreamEvent
        runtime.ResultMessage = FakeResultMessage
        FakeClient.instances = []
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name) / "agent"
        root.mkdir()
        self.manager = SessionManager(
            root,
            root / ".claude" / "sessions",
            sdk_config_dir=root / "sdk-runtime",
            client_factory=FakeClient,
            idle_timeout_seconds=60 * 60,
        )
        self.chat_id = str(uuid.uuid4())

    def tearDown(self):
        runtime.StreamEvent = self.real_stream_event
        runtime.ResultMessage = self.real_result_message
        self.temp_dir.cleanup()

    async def test_new_chat_defaults_to_plan_mode(self):
        await self.manager.ensure_session(self.chat_id)

        self.assertEqual(self.manager.current_mode(self.chat_id), PLAN_MODE)
        self.assertEqual(FakeClient.instances[0].options.permission_mode, "bypassPermissions")
        self.assertEqual(FakeClient.instances[0].permission_modes, ["plan"])

    def test_sdk_runtime_projects_link_targets_persistent_transcripts(self):
        projects_link = self.manager.sdk_config_dir / "projects"

        self.assertTrue(projects_link.is_symlink())
        self.assertEqual(projects_link.resolve(), self.manager.projects_dir)

    async def test_plan_mode_calls_sdk_permission_mode(self):
        await self.manager.set_mode(self.chat_id, PLAN_MODE)

        self.assertEqual(FakeClient.instances[0].permission_modes[-1], "plan")

    async def test_execute_mode_calls_sdk_bypass_permissions(self):
        await self.manager.set_mode(self.chat_id, EXECUTE_MODE)

        self.assertEqual(FakeClient.instances[0].permission_modes[-1], "bypassPermissions")
        self.assertEqual(self.manager.current_mode(self.chat_id), EXECUTE_MODE)

    async def test_mode_switch_does_not_query_client(self):
        await self.manager.set_mode(self.chat_id, EXECUTE_MODE)

        self.assertEqual(FakeClient.instances[0].queries, [])

    async def test_same_chat_id_reuses_managed_session(self):
        first = [chunk async for chunk in self.manager.stream_turn(MESSAGES, self.chat_id)]
        second = [chunk async for chunk in self.manager.stream_turn(MESSAGES, self.chat_id)]

        self.assertEqual(first, ["chunk"])
        self.assertEqual(second, ["chunk"])
        self.assertEqual(len(FakeClient.instances), 1)
        self.assertEqual(len(FakeClient.instances[0].queries), 2)

    async def test_first_turn_persists_claude_session_mapping(self):
        [chunk async for chunk in self.manager.stream_turn(MESSAGES, self.chat_id)]

        mapping = json.loads(self.manager._chat_map_path(self.chat_id).read_text(encoding="utf-8"))
        self.assertEqual(mapping["openwebui_chat_id"], self.chat_id)
        self.assertEqual(mapping["claude_session_id"], "sdk-session-1")
        self.assertIsNone(FakeClient.instances[0].queries[0][1])

    async def test_follow_up_uses_stored_claude_session_id(self):
        [chunk async for chunk in self.manager.stream_turn(MESSAGES, self.chat_id)]
        [chunk async for chunk in self.manager.stream_turn(MESSAGES, self.chat_id)]

        self.assertIsNone(FakeClient.instances[0].queries[0][1])
        self.assertIsNone(FakeClient.instances[0].queries[1][1])

    async def test_missing_or_invalid_chat_id_is_rejected(self):
        with self.assertRaises(InvalidSessionError):
            await self.manager.ensure_session(None)
        with self.assertRaises(InvalidSessionError):
            await self.manager.ensure_session("not-a-uuid")

    async def test_corrupt_transcript_is_rejected(self):
        claude_session_id = "sdk-corrupt-session"
        self.manager._save_claude_session_id(self.chat_id, claude_session_id)
        transcript = self.manager.transcript_path(claude_session_id)
        transcript.parent.mkdir(parents=True)
        transcript.write_text("not-json\n", encoding="utf-8")

        with self.assertRaises(SessionStateError):
            await self.manager.ensure_session(self.chat_id)

    async def test_plan_prompt_passthrough_without_runtime_prefix(self):
        await self.manager.set_mode(self.chat_id, PLAN_MODE)
        chunks = [chunk async for chunk in self.manager.stream_turn(MESSAGES, self.chat_id)]

        prompt, session_id = FakeClient.instances[0].queries[0]
        self.assertEqual(chunks, ["chunk"])
        self.assertIsNone(session_id)
        self.assertEqual(prompt, "USER: first question\n\nASSISTANT: first answer\n\nUSER: follow-up question")
        self.assertNotIn("USER REQUEST:", prompt)

    async def test_execute_prompt_contains_plan_execution_instruction(self):
        await self.manager.set_mode(self.chat_id, EXECUTE_MODE)
        [chunk async for chunk in self.manager.stream_turn(MESSAGES, self.chat_id)]

        prompt, _ = FakeClient.instances[0].queries[0]
        self.assertIn(EXECUTE_PROMPT_PREFIX, prompt)
        self.assertIn("遵循之前计划模式下生成的计划执行", prompt)
        self.assertIn("不要输出 analysis_scheme", prompt)
        self.assertIn(".bioagent/state.json", prompt)
        self.assertIn(".bioagent/execution.log", prompt)
        self.assertIn("USER REQUEST:", prompt)

    async def test_existing_transcript_sends_latest_user_message_only(self):
        claude_session_id = "sdk-existing-session"
        self.manager._save_claude_session_id(self.chat_id, claude_session_id)
        transcript = self.manager.transcript_path(claude_session_id)
        transcript.parent.mkdir(parents=True)
        transcript.write_text(json.dumps({"type": "user"}) + "\n", encoding="utf-8")

        [chunk async for chunk in self.manager.stream_turn(MESSAGES, self.chat_id)]

        prompt, session_id = FakeClient.instances[0].queries[0]
        self.assertIsNone(session_id)
        self.assertEqual(FakeClient.instances[0].options.resume, claude_session_id)
        self.assertIn("follow-up question", prompt)
        self.assertNotIn("first answer", prompt)

    async def test_recreated_manager_resumes_via_client_options(self):
        [chunk async for chunk in self.manager.stream_turn(MESSAGES, self.chat_id)]
        transcript = self.manager.transcript_path("sdk-session-1")
        transcript.parent.mkdir(parents=True, exist_ok=True)
        transcript.write_text(json.dumps({"type": "user"}) + "\n", encoding="utf-8")
        await self.manager.close_all()

        root = self.manager.project_root
        recreated = SessionManager(
            root,
            self.manager.config_dir,
            sdk_config_dir=self.manager.sdk_config_dir,
            client_factory=FakeClient,
            idle_timeout_seconds=60 * 60,
        )
        [chunk async for chunk in recreated.stream_turn(MESSAGES, self.chat_id)]

        resumed_client = FakeClient.instances[-1]
        self.assertEqual(resumed_client.options.resume, "sdk-session-1")
        self.assertIsNone(resumed_client.queries[0][1])
        await recreated.close_all()

    async def test_ephemeral_turn_does_not_create_chat_mapping(self):
        chunks = [chunk async for chunk in self.manager.stream_ephemeral_turn(MESSAGES)]

        self.assertEqual(chunks, ["chunk"])
        self.assertFalse(self.manager._chat_map_path(self.chat_id).exists())
        self.assertTrue(FakeClient.instances[-1].disconnected)

    async def test_bootstrap_history_filters_mode_switch_messages(self):
        messages = [
            {"role": "user", "content": "classify this sample"},
            {"role": "assistant", "content": "working"},
            {"role": "user", "content": "/execute"},
            {"role": "assistant", "content": "已切换到 execute mode"},
            {"role": "user", "content": "run workflow1"},
        ]

        [chunk async for chunk in self.manager.stream_turn(messages, self.chat_id)]

        prompt, _ = FakeClient.instances[0].queries[0]
        self.assertIn("classify this sample", prompt)
        self.assertIn("run workflow1", prompt)
        self.assertNotIn("/execute", prompt)
        self.assertNotIn("已切换到 execute mode", prompt)

    async def test_sdk_options_load_all_skills_and_all_builtin_tools(self):
        await self.manager.ensure_session(self.chat_id)
        options = FakeClient.instances[0].options

        self.assertEqual(options.skills, "all")
        self.assertEqual(options.setting_sources, ["project"])
        self.assertEqual(options.tools, {"type": "preset", "preset": "claude_code"})
        self.assertEqual(options.permission_mode, "bypassPermissions")
        self.assertEqual(options.env["CLAUDE_CONFIG_DIR"], str(self.manager.sdk_config_dir))
        self.assertEqual(options.allowed_tools, [])
        self.assertEqual(options.disallowed_tools, [])
        self.assertEqual(options.system_prompt, config.SYSTEM_PROMPT)

    def test_system_prompt_contains_analysis_scheme_contract(self):
        self.assertIn("analysis_scheme", config.SYSTEM_PROMPT)
        self.assertIn("```analysis_scheme", config.SYSTEM_PROMPT)
        self.assertIn('"ready_to_execute"', config.SYSTEM_PROMPT)
        self.assertIn('"steps"', config.SYSTEM_PROMPT)
        self.assertIn("Do not create run directories", config.SYSTEM_PROMPT)
        self.assertIn(".bioagent/state.json", config.SYSTEM_PROMPT)
        self.assertIn(".bioagent/execution.log", config.SYSTEM_PROMPT)

    def test_gatekeeper_skill_contains_minimal_analysis_contract(self):
        skill_path = (
            Path(__file__).resolve().parents[1]
            / ".claude"
            / "skills"
            / "bioinformatics-workflow-gatekeeper"
            / "SKILL.md"
        )
        skill = skill_path.read_text(encoding="utf-8")

        self.assertIn("analysis_scheme", skill)
        self.assertIn("Plan mode must not create run directories", skill)
        self.assertIn("Do not output `analysis_scheme` in execute mode", skill)
        self.assertIn("${RUN_DIR}/.bioagent/state.json", skill)
        self.assertIn("${RUN_DIR}/.bioagent/execution.log", skill)


if __name__ == "__main__":
    unittest.main()

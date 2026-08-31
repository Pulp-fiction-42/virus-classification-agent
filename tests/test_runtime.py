from __future__ import annotations

import asyncio
import tempfile
import unittest
import uuid
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import patch

import runtime
from runtime import OutputItemAdded, SessionManager, SessionStateError, TextDelta


@dataclass
class FakeStreamEvent:
    event: dict


@dataclass
class FakeResultMessage:
    session_id: str = "sdk-session"
    is_error: bool = False
    result: str | None = None
    errors: list[str] | None = None
    subtype: str = "success"
    structured_output: object | None = None


@dataclass
class FakeToolResultBlock:
    tool_use_id: str
    content: str | None = None
    is_error: bool | None = None


@dataclass
class FakeUserMessage:
    content: list[object]


class FakeClient:
    response = """```conversation_action
{"action":"respond","message":"固定回答","citations":[]}
```"""
    tool_name = None
    tool_result = False
    tool_error = False
    hang = False
    fail = False
    structured_output = None

    def __init__(self, options):
        self.options = options
        self.queries = []
        self.disconnected = False

    async def connect(self):
        return None

    async def query(self, prompt, session_id=None):
        self.queries.append(prompt)

    async def receive_response(self):
        if self.hang:
            await asyncio.Future()
        if self.fail:
            raise OSError("provider detail must stay private")
        if self.tool_name:
            yield FakeStreamEvent(
                {
                    "type": "content_block_start",
                    "content_block": {
                        "type": "tool_use",
                        "id": "tool-1",
                        "name": self.tool_name,
                    },
                }
            )
            if self.tool_result:
                yield FakeUserMessage(
                    [
                        FakeToolResultBlock(
                            "tool-1",
                            content="verified result",
                            is_error=self.tool_error,
                        )
                    ]
                )
        yield FakeStreamEvent(
            {
                "type": "content_block_delta",
                "delta": {"type": "text_delta", "text": self.response},
            }
        )
        yield FakeResultMessage(structured_output=self.structured_output)

    async def disconnect(self):
        self.disconnected = True


class SessionManagerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        FakeClient.response = """```conversation_action
{"action":"respond","message":"固定回答","citations":[]}
```"""
        FakeClient.tool_name = None
        FakeClient.tool_result = False
        FakeClient.tool_error = False
        FakeClient.hang = False
        FakeClient.fail = False
        FakeClient.structured_output = None
        self.real_stream_event = runtime.StreamEvent
        self.real_result_message = runtime.ResultMessage
        self.real_tool_result_block = runtime.ToolResultBlock
        self.real_user_message = runtime.UserMessage
        runtime.StreamEvent = FakeStreamEvent
        runtime.ResultMessage = FakeResultMessage
        runtime.ToolResultBlock = FakeToolResultBlock
        runtime.UserMessage = FakeUserMessage
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.manager = SessionManager(
            root,
            root / "state",
            sdk_config_dir=root / "sdk",
            client_factory=FakeClient,
            analysis_run_root=root / "runs",
        )
        self.chat_id = str(uuid.uuid4())

    def tearDown(self):
        runtime.StreamEvent = self.real_stream_event
        runtime.ResultMessage = self.real_result_message
        runtime.ToolResultBlock = self.real_tool_result_block
        runtime.UserMessage = self.real_user_message
        self.temp_dir.cleanup()

    async def test_valid_action_is_exposed_as_text(self):
        events = [
            event
            async for event in self.manager.stream_turn(
                [{"role": "user", "content": "解释方案"}],
                self.chat_id,
                dataset_context={
                    "summary": {"status": "ready"},
                    "artifacts": [{"files": {"/private": "must-not-leak"}}],
                },
                plan_context={
                    "goals": ["virus_summary"],
                    "options": {"viral_type": "RNA"},
                    "steps": [{"id": "virus", "label": "病毒识别", "tool": "/private/tool"}],
                    "execution_spec": {"inputs": ["/private/input.fastq"]},
                    "resource_estimate": {"runner": "/private/runner"},
                },
            )
        ]
        self.assertEqual(
            [event.text for event in events if isinstance(event, TextDelta)],
            ["固定回答"],
        )
        prompt = self.manager._sessions[self.chat_id].client.queries[0]
        self.assertIn('"status": "ready"', prompt)
        self.assertIn('"viral_type": "RNA"', prompt)
        self.assertNotIn("/private", prompt)
        self.assertNotIn("execution_spec", prompt)
        self.assertNotIn("resource_estimate", prompt)

    async def test_patch_action_emits_control_item(self):
        FakeClient.response = """```conversation_action
{"action":"propose_patch","message":"建议调整病毒类型。","proposed_patch":{"viral_type":"RNA"},"citations":[]}
```"""
        events = [
            event
            async for event in self.manager.stream_turn(
                [{"role": "user", "content": "只分析 RNA"}],
                self.chat_id,
            )
        ]
        item = next(
            event.item
            for event in events
            if isinstance(event, OutputItemAdded)
            and event.item.get("type") == "analysis_patch_proposal"
        )
        self.assertEqual(item["type"], "analysis_patch_proposal")
        self.assertEqual(item["patch"], {"viral_type": "RNA"})

    async def test_structured_output_takes_priority_over_text_fallback(self):
        FakeClient.response = "not a conversation_action fence"
        FakeClient.structured_output = {
            "action": "propose_patch",
            "message": "建议增加病毒汇总。",
            "proposed_patch": {"goals": ["virus_summary"]},
            "citations": [],
        }
        events = [
            event
            async for event in self.manager.stream_turn(
                [{"role": "user", "content": "增加病毒汇总"}], self.chat_id
            )
        ]
        item = next(
            event.item
            for event in events
            if isinstance(event, OutputItemAdded)
            and event.item.get("type") == "analysis_patch_proposal"
        )
        self.assertEqual(item["patch"], {"goals": ["virus_summary"]})

    async def test_structured_output_rejects_patch_alias(self):
        FakeClient.structured_output = {
            "action": "propose_patch",
            "message": "建议调整。",
            "patch": {"viral_type": "RNA"},
            "citations": [],
        }
        with self.assertRaisesRegex(SessionStateError, "无效的结构化结果"):
            async for _event in self.manager.stream_turn(
                [{"role": "user", "content": "只分析 RNA"}], self.chat_id
            ):
                pass

    async def test_invalid_model_output_fails_without_state_change(self):
        FakeClient.response = "普通文本"
        with self.assertRaises(SessionStateError):
            async for _event in self.manager.stream_turn(
                [{"role": "user", "content": "修改模式"}],
                self.chat_id,
            ):
                pass

    async def test_knowledge_tool_turn_cannot_form_patch(self):
        FakeClient.tool_name = "mcp__taxonagent__ictv_family_info"
        FakeClient.tool_result = True
        FakeClient.response = """```conversation_action
{"action":"propose_patch","message":"查询后建议调整。","proposed_patch":{"viral_type":"RNA"},"citations":[]}
```"""
        with self.assertRaisesRegex(SessionStateError, "知识查询不能形成方案修改建议"):
            async for _event in self.manager.stream_turn(
                [{"role": "user", "content": "查询 ICTV 并调整方案"}],
                self.chat_id,
            ):
                pass

    async def test_general_turn_does_not_require_knowledge_tool(self):
        events = [
            event
            async for event in self.manager.stream_turn(
                [{"role": "user", "content": "解释这个质控方案"}], self.chat_id
            )
        ]
        self.assertEqual(
            [event.text for event in events if isinstance(event, TextDelta)],
            ["固定回答"],
        )

    async def test_ictv_turn_requires_successful_matching_tool_result(self):
        cases = [
            (None, False, False),
            ("mcp__taxonagent__ictv_family_info", False, False),
            ("mcp__taxonagent__ictv_family_info", True, True),
        ]
        for tool_name, tool_result, tool_error in cases:
            with self.subTest(tool_name=tool_name, tool_error=tool_error):
                FakeClient.tool_name = tool_name
                FakeClient.tool_result = tool_result
                FakeClient.tool_error = tool_error
                manager = SessionManager(
                    Path(self.temp_dir.name),
                    Path(self.temp_dir.name) / f"state-{tool_name}-{tool_result}-{tool_error}",
                    sdk_config_dir=Path(self.temp_dir.name)
                    / f"sdk-{tool_name}-{tool_result}-{tool_error}",
                    client_factory=FakeClient,
                    analysis_run_root=Path(self.temp_dir.name) / "runs",
                )
                emitted = []
                with self.assertRaisesRegex(
                    SessionStateError, "当前无法完成经验证的 ICTV 查询"
                ):
                    async for event in manager.stream_turn(
                        [{"role": "user", "content": "请查询 ICTV 的分类规则"}],
                        str(uuid.uuid4()),
                    ):
                        emitted.append(event)
                self.assertFalse(any(isinstance(event, TextDelta) for event in emitted))
                self.assertFalse(
                    any(
                        isinstance(event, OutputItemAdded)
                        and event.item.get("type") == "analysis_patch_proposal"
                        for event in emitted
                    )
                )

    async def test_ictv_turn_accepts_successful_tool_result(self):
        FakeClient.tool_name = "mcp__taxonagent__ictv_family_info"
        FakeClient.tool_result = True
        events = [
            event
            async for event in self.manager.stream_turn(
                [{"role": "user", "content": "ICTV 如何界定这个病毒科？"}], self.chat_id
            )
        ]
        self.assertEqual(
            [event.text for event in events if isinstance(event, TextDelta)],
            ["固定回答"],
        )

    async def test_ictv_transport_failure_uses_knowledge_error(self):
        FakeClient.fail = True
        with self.assertRaisesRegex(
            SessionStateError, "当前无法完成经验证的 ICTV 查询"
        ):
            async for _event in self.manager.stream_turn(
                [{"role": "user", "content": "ICTV 如何分类？"}], self.chat_id
            ):
                pass

    async def test_timeout_emits_failed_trace_then_uses_fixed_error(self):
        FakeClient.hang = True
        events = []
        with patch.object(runtime.config, "CONVERSATION_TIMEOUT_SECONDS", 0.01):
            with self.assertRaisesRegex(SessionStateError, "对话模型请求超时"):
                async for event in self.manager.stream_turn(
                    [{"role": "user", "content": "解释方案"}],
                    self.chat_id,
                ):
                    events.append(event)
        trace = next(event.item for event in events if isinstance(event, OutputItemAdded))
        self.assertEqual(trace["error_code"], "MODEL_TIMEOUT")

    async def test_provider_failure_uses_fixed_safe_error(self):
        FakeClient.fail = True
        with self.assertRaisesRegex(
            SessionStateError, "对话模型暂时不可用.*数据集、方案与执行功能不受影响"
        ) as raised:
            async for _event in self.manager.stream_turn(
                [{"role": "user", "content": "解释方案"}], self.chat_id
            ):
                pass
        self.assertNotIn("provider detail", str(raised.exception))

    async def test_session_connect_failure_uses_fixed_safe_error(self):
        class ConnectFailureClient(FakeClient):
            async def connect(self):
                raise OSError("private initialize error")

        root = Path(self.temp_dir.name)
        manager = SessionManager(
            root,
            root / "connect-state",
            sdk_config_dir=root / "connect-sdk",
            client_factory=ConnectFailureClient,
            analysis_run_root=root / "runs",
        )
        with self.assertRaisesRegex(SessionStateError, "对话模型暂时不可用") as raised:
            await manager.ensure_session(str(uuid.uuid4()))
        self.assertNotIn("private initialize", str(raised.exception))

    def test_model_has_only_taxonagent_tool_and_no_reasoning_budget(self):
        options = self.manager._build_options(permission_mode="dontAsk")
        self.assertEqual(options.tools, [])
        self.assertEqual(options.allowed_tools, ["mcp__taxonagent__*"])
        self.assertEqual(options.setting_sources, [])
        self.assertIsNone(options.thinking)
        self.assertEqual(options.add_dirs, [])
        self.assertIsNone(options.skills)
        self.assertEqual(options.output_format["type"], "json_schema")
        self.assertEqual(
            options.output_format["schema"]["properties"]["action"]["enum"],
            ["respond", "clarify", "propose_patch"],
        )
        self.assertIn("ANTHROPIC_API_KEY", options.env)
        self.assertIn("ANTHROPIC_AUTH_TOKEN", options.env)
        self.assertEqual(options.env["ANTHROPIC_API_KEY"], options.env["ANTHROPIC_AUTH_TOKEN"])

    def test_ephemeral_options_do_not_force_conversation_schema(self):
        options = self.manager._build_options(
            permission_mode="dontAsk", structured_output=False
        )
        self.assertIsNone(options.output_format)

    def test_taxonagent_can_be_disabled_without_exposing_other_tools(self):
        with patch.object(runtime.config, "TAXONAGENT_MCP_URL", ""):
            options = self.manager._build_options(permission_mode="dontAsk")
        self.assertEqual(options.allowed_tools, [])
        self.assertEqual(options.mcp_servers, {})


if __name__ == "__main__":
    unittest.main()

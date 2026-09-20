from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
import uuid
from pathlib import Path
from typing import ClassVar
from unittest.mock import patch

from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock, ToolUseBlock

from conversation_contract import ConversationAction
from provider import MiniMaxProvider, ProviderRequest


class FakeClient:
    instances: ClassVar[list] = []
    output: ClassVar[dict] = {"message": "说明"}
    fail = False
    hang = False
    malformed = False
    structured_turn_limit = False
    plain_text: ClassVar[str | None] = None
    plain_text_turn_limit = False
    assistant_model = "test"
    assistant_error = None

    def __init__(self, *, options):
        self.options = options
        self.queries = []
        self.closed = False
        self.instances.append(self)

    async def connect(self):
        if self.fail:
            raise OSError("private vendor error")

    async def query(self, prompt):
        self.queries.append(prompt)

    async def receive_response(self):
        if self.hang:
            await asyncio.Future()
        if self.plain_text is not None:
            yield AssistantMessage(
                content=[TextBlock(text=self.plain_text)],
                model=self.assistant_model,
                error=self.assistant_error,
            )
        if self.structured_turn_limit:
            yield AssistantMessage(
                content=[
                    ToolUseBlock(
                        id="structured", name="StructuredOutput", input=self.output
                    )
                ],
                model="test",
            )
        is_error = bool(
            self.structured_turn_limit
            or self.plain_text_turn_limit
            or self.assistant_error
        )
        subtype = (
            "error_during_execution"
            if self.assistant_error
            else "error_max_turns"
            if is_error
            else "success"
        )
        yield ResultMessage(
            subtype=subtype,
            duration_ms=1,
            duration_api_ms=1,
            is_error=is_error,
            num_turns=1,
            session_id="private-sdk-id",
            structured_output=(
                None
                if self.malformed
                or self.structured_turn_limit
                or self.plain_text is not None
                else self.output
            ),
            result="{invalid" if self.malformed else None,
        )

    async def disconnect(self):
        self.closed = True


class ProviderTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        FakeClient.instances = []
        FakeClient.output = {"message": "说明"}
        FakeClient.fail = FakeClient.hang = FakeClient.malformed = False
        FakeClient.structured_turn_limit = False
        FakeClient.plain_text = None
        FakeClient.plain_text_turn_limit = False
        FakeClient.assistant_model = "test"
        FakeClient.assistant_error = None
        self.provider = MiniMaxProvider(
            allow_cloud=True, state_dir=self.root, client_factory=FakeClient
        )

    def request(self, **kwargs):
        output_schema = kwargs.pop(
            "output_schema",
            {
                "type": "object",
                "properties": {"message": {"type": "string"}},
                "required": ["message"],
                "additionalProperties": False,
            },
        )
        return ProviderRequest(
            task_kind=kwargs.pop("task_kind", "evidence_explanation"),
            instructions="仅输出结构化结果",
            context={"evidence": "有限上下文"},
            output_schema=output_schema,
            **kwargs,
        )

    async def test_four_tasks_use_same_bounded_tool_free_transport(self):
        for kind in [
            "intent_clarification",
            "controlled_patch",
            "citation_qa",
            "evidence_explanation",
        ]:
            result = await self.provider.generate(self.request(task_kind=kind))
            self.assertIsNone(result.error_code)
            client = FakeClient.instances[-1]
            self.assertEqual(len(client.queries), 1)
            self.assertEqual(client.options.tools, [])
            self.assertEqual(client.options.allowed_tools, [])
            self.assertEqual(client.options.mcp_servers, {})
            self.assertEqual(client.options.max_turns, 1)
            self.assertIsNone(client.options.model)
            self.assertEqual(
                client.options.env["ANTHROPIC_BASE_URL"],
                "https://api.minimax.cn/anthropic",
            )
            self.assertEqual(
                client.options.env["ANTHROPIC_MODEL"], "MiniMax-M3[1m]"
            )
            self.assertEqual(
                client.options.env["ANTHROPIC_DEFAULT_SONNET_MODEL"],
                "MiniMax-M3[1m]",
            )
            self.assertEqual(
                client.options.env["CLAUDE_CODE_AUTO_COMPACT_WINDOW"], "1000000"
            )
            self.assertNotIn("ANTHROPIC_API_KEY", client.options.env)
            self.assertTrue(client.closed)
            self.assertNotIn("private-sdk-id", result.model_dump_json())

    async def test_unpinned_sdk_is_unavailable_before_network(self):
        with patch("provider.claude_agent_sdk_ready", return_value=False):
            result = await MiniMaxProvider(allow_cloud=True).generate(self.request())
        self.assertEqual(result.error_code, "MODEL_UNAVAILABLE")

    async def test_cloud_disabled_does_not_touch_sdk_or_network(self):
        with patch("socket.socket", side_effect=AssertionError("network forbidden")):
            result = await MiniMaxProvider(
                state_dir=self.root, client_factory=FakeClient
            ).generate(self.request())
        self.assertEqual(result.error_code, "CLOUD_DISABLED")
        self.assertEqual(FakeClient.instances, [])

    async def test_single_turn_structured_payload_requires_schema_validation(self):
        FakeClient.structured_turn_limit = True
        result = await self.provider.generate(self.request())
        self.assertIsNone(result.error_code)
        self.assertEqual(len(FakeClient.instances[-1].queries), 1)
        FakeClient.output = {"shell": "forbidden"}
        result = await self.provider.generate(self.request())
        self.assertEqual(result.error_code, "MODEL_SCHEMA_INVALID")

    async def test_plain_text_turn_is_validated_as_message_output(self):
        FakeClient.plain_text = "自然语言回答"
        FakeClient.plain_text_turn_limit = True

        result = await self.provider.generate(self.request())

        self.assertIsNone(result.error_code)
        self.assertEqual(result.output, {"message": "自然语言回答"})
        self.assertEqual(len(FakeClient.instances[-1].queries), 1)

    async def test_plain_text_does_not_bypass_analysis_action_schema(self):
        FakeClient.plain_text = "我已经修改方案。"
        FakeClient.plain_text_turn_limit = True

        result = await self.provider.generate(
            self.request(output_schema=ConversationAction.model_json_schema())
        )

        self.assertEqual(result.error_code, "MODEL_SCHEMA_INVALID")
        self.assertIsNone(result.output)

    async def test_synthetic_or_api_error_text_is_not_a_reply(self):
        for model, error in (("<synthetic>", None), ("test", "rate_limit")):
            with self.subTest(model=model, error=error):
                FakeClient.plain_text = "上游错误"
                FakeClient.plain_text_turn_limit = True
                FakeClient.assistant_model = model
                FakeClient.assistant_error = error
                result = await self.provider.generate(self.request())
                self.assertEqual(result.error_code, "MODEL_UNAVAILABLE")

    async def test_context_limit_rejected_before_connect(self):
        result = await self.provider.generate(self.request(max_context_chars=10))
        self.assertEqual(result.error_code, "CONTEXT_LIMIT")
        self.assertEqual(FakeClient.instances, [])

    async def test_schema_json_timeout_and_transport_errors(self):
        FakeClient.output = {"shell": "bad"}
        result = await self.provider.generate(self.request())
        self.assertEqual(result.error_code, "MODEL_SCHEMA_INVALID")
        FakeClient.malformed = True
        result = await self.provider.generate(self.request())
        self.assertEqual(result.error_code, "MODEL_SCHEMA_INVALID")
        FakeClient.malformed = False
        FakeClient.hang = True
        result = await self.provider.generate(self.request(timeout_seconds=0.01))
        self.assertEqual(result.error_code, "MODEL_TIMEOUT")
        self.assertTrue(FakeClient.instances[-1].closed)
        FakeClient.hang = False
        FakeClient.fail = True
        result = await self.provider.generate(self.request())
        self.assertEqual(result.error_code, "MODEL_UNAVAILABLE")
        self.assertNotIn("private vendor", result.model_dump_json())

    async def test_resume_is_private_and_new_namespace_does_not_read_old_sdk_map(self):
        key = str(uuid.uuid4())
        await self.provider.generate(self.request(conversation_id=key))
        self.assertEqual(
            json.loads((self.root / f"{key}.json").read_text())["sdk_session_id"],
            "private-sdk-id",
        )
        await self.provider.generate(self.request(conversation_id=key))
        self.assertEqual(FakeClient.instances[-1].options.resume, "private-sdk-id")
        self.assertIn(
            str(self.root), FakeClient.instances[-1].options.env["CLAUDE_CONFIG_DIR"]
        )

    async def test_accumulated_context_starts_new_session_and_keeps_old_mapping_auditable(
        self,
    ):
        key = str(uuid.uuid4())
        mapping = self.root / f"{key}.json"
        mapping.write_text(
            json.dumps({"sdk_session_id": "old-session", "context_upper_bound": 24000})
        )
        await self.provider.generate(self.request(conversation_id=key))
        self.assertIsNone(FakeClient.instances[-1].options.resume)
        self.assertIn("context_upper_bound", json.loads(mapping.read_text()))

    async def test_output_budget_rejects_oversized_result(self):
        FakeClient.output = {"message": "oversized result"}
        result = await self.provider.generate(self.request(max_output_tokens=1))
        self.assertEqual(result.error_code, "OUTPUT_LIMIT")
        self.assertIsNone(result.output)

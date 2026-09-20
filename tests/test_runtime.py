from __future__ import annotations

import unittest
import uuid

import config
from conversation_events import OutputItemAdded, TextDelta
from provider import ProviderResult
from runtime import SessionManager
from runtime_errors import ConversationPublicError


class FakeProvider:
    def __init__(self, output=None, error=None):
        self.output = output or {
            "action": "respond",
            "message": "固定回答",
            "citations": [],
        }
        self.error = error
        self.requests = []

    async def generate(self, request):
        self.requests.append(request)
        return ProviderResult(
            provider="fake",
            model="fake-model",
            output=self.output,
            error_code=self.error,
        )


class RuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def turn(self, provider, message="解释方案", **kwargs):
        return [
            event
            async for event in SessionManager(provider=provider).stream_turn(
                [{"role": "user", "content": message}],
                str(uuid.uuid4()),
                analysis=True,
                **kwargs,
            )
        ]

    async def test_dashboard_uses_message_only_contract_without_action_events(self):
        provider = FakeProvider({"message": "普通对话回复"})
        events = [
            event
            async for event in SessionManager(provider=provider).stream_turn(
                [{"role": "user", "content": "解释宏基因组学"}],
                "local:dashboard:socket:session",
            )
        ]

        self.assertEqual(events, [TextDelta("普通对话回复")])
        request = provider.requests[0]
        self.assertEqual(set(request.output_schema["properties"]), {"message"})
        self.assertEqual(request.instructions, config.DASHBOARD_SYSTEM_PROMPT)
        self.assertNotIn("analysis", request.context)

    async def test_dashboard_rejects_analysis_action_fields(self):
        provider = FakeProvider()
        with self.assertRaises(ConversationPublicError) as raised:
            async for _event in SessionManager(provider=provider).stream_turn(
                [{"role": "user", "content": "你好"}], "local:dashboard:test"
            ):
                pass
        self.assertEqual(raised.exception.code, "ACTION_SCHEMA_INVALID")

    async def test_action_and_controlled_patch_keep_event_contract(self):
        for output in [
            {"action": "respond", "message": "固定回答", "citations": []},
            {"action": "clarify", "message": "请确认目标", "citations": []},
            {
                "action": "propose_patch",
                "message": "建议调整",
                "proposed_patch": {"viral_type": "RNA"},
                "citations": [],
            },
        ]:
            provider = FakeProvider(output)
            events = await self.turn(provider)
            self.assertEqual(len(provider.requests), 1)
            self.assertTrue(
                any(
                    isinstance(e, TextDelta) and e.text == output["message"]
                    for e in events
                )
            )
            traces = [e.item for e in events if isinstance(e, OutputItemAdded)]
            self.assertTrue(traces[0]["schema_valid"])
            self.assertEqual(
                any(t["type"] == "analysis_patch_proposal" for t in traces),
                output["action"] == "propose_patch",
            )

    async def test_host_rejects_execution_fields(self):
        provider = FakeProvider(
            {
                "action": "propose_patch",
                "message": "执行",
                "proposed_patch": {"mode": "emergency"},
            }
        )
        with self.assertRaises(ConversationPublicError):
            await self.turn(provider)

    async def test_context_is_bounded_and_removes_paths_and_sequences(self):
        provider = FakeProvider()
        await self.turn(
            provider,
            "/private/dataset/file.fastq " + "ACGT" * 100,
            dataset_context={
                "summary": {
                    "status": "ready",
                    "artifacts": [{"files": {"read1": "/data/private"}}],
                }
            },
        )
        context = str(provider.requests[0].context)
        self.assertNotIn("/private", context)
        self.assertNotIn("/data", context)
        self.assertNotIn("ACGT" * 100, context)
        self.assertNotIn("'files':", context)

    async def test_unreviewed_replay_knowledge_is_not_enabled_in_chat(self):
        provider = FakeProvider()
        with self.assertRaises(ConversationPublicError) as raised:
            await self.turn(provider, "ICTV 的分类标准是什么")
        self.assertEqual(raised.exception.code, "KNOWLEDGE_UNAVAILABLE")
        self.assertEqual(provider.requests, [])

    async def test_provider_errors_keep_deterministic_controls_available(self):
        for code in [
            "CLOUD_DISABLED",
            "MODEL_TIMEOUT",
            "MODEL_UNAVAILABLE",
            "MODEL_SCHEMA_INVALID",
            "CONTEXT_LIMIT",
        ]:
            with self.assertRaises(ConversationPublicError) as raised:
                await self.turn(FakeProvider(error=code))
            self.assertEqual(raised.exception.code, code)
            self.assertIn("方案与执行功能不受影响", str(raised.exception))

    async def test_only_uuid_chat_ids_use_persistent_provider_sessions(self):
        provider = FakeProvider()
        manager = SessionManager(provider=provider)
        chat_id = str(uuid.uuid4())
        self.assertEqual(await manager.ensure_session(chat_id), chat_id)
        self.assertIsNone(await manager.ensure_session(None))
        self.assertIsNone(await manager.ensure_session("local:socket-id"))
        self.assertIsNone(await manager.ensure_session("../invalid"))
        self.assertEqual(provider.requests, [])

    async def test_missing_or_local_chat_id_uses_ephemeral_provider_session(self):
        for chat_id in (None, "local:socket-id"):
            provider = FakeProvider({"message": "固定回答"})
            events = [
                event
                async for event in SessionManager(provider=provider).stream_turn(
                    [{"role": "user", "content": "解释方案"}], chat_id
                )
            ]
            self.assertTrue(any(isinstance(event, TextDelta) for event in events))
            self.assertIsNone(provider.requests[0].conversation_id)

    async def test_background_task_has_no_persistent_session(self):
        provider = FakeProvider({"message": "标题"})
        events = [
            event
            async for event in SessionManager(provider=provider).stream_ephemeral_turn(
                [{"role": "user", "content": "总结"}]
            )
        ]
        self.assertEqual(events[0].text, "标题")
        self.assertIsNone(provider.requests[0].conversation_id)

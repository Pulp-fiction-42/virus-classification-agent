from __future__ import annotations

import json
import unittest
import uuid
from unittest.mock import AsyncMock, patch

import config
from conversation_contract import RetrievalResult
from conversation_events import OutputItemAdded, TextDelta
from provider import MiniMaxProvider, ProviderResult
from runtime import SessionManager
from runtime_errors import ConversationPublicError
from workflow_catalog import capability_catalog


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
        self.assertEqual(request.max_context_chars, 24000)
        self.assertNotIn("analysis", request.context)

    async def test_analysis_catalog_dataset_and_plan_fit_provider_budget(self):
        provider = MiniMaxProvider(allow_cloud=True)
        output = {"action": "respond", "message": "请检查建议。", "citations": []}
        message = (
            "请只对当前双端测序数据进行序列质控，将最短保留读长改为80bp，"
            "其他设置保持当前默认值。请说明这个调整的含义，以及执行前还需要"
            "确认哪些事项，先给出建议供我检查，不要自动开始计算，也不要扩大分析范围。"
        )
        with (
            patch("provider.claude_agent_sdk_ready", return_value=True),
            patch.object(
                provider, "_generate", new_callable=AsyncMock, return_value=output
            ) as generate,
        ):
            events = await self.turn(
                provider,
                message,
                routing_request={"mode": "standard"},
                dataset_context={
                    "summary": {
                        "dataset_id": "ds-test",
                        "revision": 1,
                        "status": "ready",
                        "artifacts": [
                            {
                                "id": "primary",
                                "type": "raw_paired_reads",
                                "records": [
                                    {
                                        "record_id": "rec-1",
                                        "run_id": "run-1",
                                        "sample_id": "sample-1",
                                    }
                                ],
                            }
                        ],
                        "checks": {},
                        "issues": [],
                    }
                },
                plan_context={
                    "title": "序列质控方案",
                    "summary": "对当前双端测序数据进行质控。",
                    "goals": ["read_qc"],
                    "ready_to_execute": True,
                    "options": {"viral_type": "RNA+DNA"},
                    "steps": [
                        {
                            "id": "read_qc",
                            "label": "序列质控",
                            "depends_on": [],
                            "is_deliverable": True,
                        }
                    ],
                },
            )

        generate.assert_awaited_once()
        request, prompt, _key = generate.call_args.args
        self.assertEqual(request.max_context_chars, 64000)
        self.assertEqual(
            request.context["analysis"]["capability_catalog"], capability_catalog()
        )
        total_chars = (
            len(prompt)
            + len(request.instructions)
            + len(json.dumps(request.output_schema))
        )
        self.assertGreater(total_chars, 24000)
        self.assertLessEqual(total_chars, request.max_context_chars)
        self.assertIn(TextDelta(output["message"]), events)

    async def test_analysis_still_rejects_oversized_context_before_generation(self):
        provider = MiniMaxProvider(allow_cloud=True)
        with (
            patch("provider.claude_agent_sdk_ready", return_value=True),
            patch.object(provider, "_generate", new_callable=AsyncMock) as generate,
            self.assertRaises(ConversationPublicError) as raised,
        ):
            await self.turn(provider, plan_context={"summary": "摘要" * 32000})

        self.assertEqual(raised.exception.code, "CONTEXT_LIMIT")
        generate.assert_not_awaited()

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

    async def test_engineering_answer_requires_registered_citations(self):
        provider = FakeProvider(
            {
                "action": "respond",
                "message": "fastp 的参数来自已发布工程契约。",
                "citations": ["ENG-K08"],
            }
        )
        events = await self.turn(provider, "fastp 有哪些可控参数")
        items = [event.item for event in events if isinstance(event, OutputItemAdded)]
        citation_item = next(
            item for item in items if item["type"] == "analysis_knowledge_citations"
        )

        self.assertEqual(citation_item["schema_version"], 1)
        self.assertEqual(citation_item["scope"], "engineering_contract")
        self.assertEqual(citation_item["citations"][0]["citation_id"], "ENG-K08")
        self.assertNotIn("/Users/", str(citation_item))
        request = provider.requests[0]
        self.assertEqual(request.context["knowledge"]["scope"], "engineering_contract")
        self.assertLessEqual(len(request.context["knowledge"]["passages"]), 4)

    async def test_engineering_answer_rejects_missing_and_forged_citations(self):
        for citations in ([], ["forged-citation"]):
            provider = FakeProvider(
                {
                    "action": "respond",
                    "message": "无效引用回答。",
                    "citations": citations,
                }
            )
            with (
                self.subTest(citations=citations),
                self.assertRaises(ConversationPublicError) as raised,
            ):
                await self.turn(provider, "fastp 有哪些可控参数")
            self.assertEqual(raised.exception.code, "CITATION_INVALID")

    async def test_unknown_engineering_knowledge_fails_before_model(self):
        class EmptyStore:
            async def retrieve(self, request):
                return RetrievalResult(
                    query=request.query,
                    corpora=list(request.corpora),
                    insufficient=True,
                    error_code="KNOWLEDGE_INSUFFICIENT",
                )

        provider = FakeProvider()
        with self.assertRaises(ConversationPublicError) as raised:
            async for _event in SessionManager(
                provider=provider, knowledge_store=EmptyStore()
            ).stream_turn(
                [{"role": "user", "content": "unknown_tool.fastp 的参数是什么"}],
                str(uuid.uuid4()),
                analysis=True,
            ):
                pass
        self.assertEqual(raised.exception.code, "KNOWLEDGE_INSUFFICIENT")
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

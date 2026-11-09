from __future__ import annotations

import asyncio
import unittest
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from conversation_events import OutputItemAdded, TextDelta
from provider import ProviderResult
from runtime import SessionManager
from runtime_errors import ConversationPublicError
from taxonagent_adapter import MAX_TAXONAGENT_CHARS, TaxonAgentClient


class TaxonAgentTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.client = TaxonAgentClient("http://127.0.0.1:8765/mcp")
        self.session = SimpleNamespace(
            list_tools=AsyncMock(
                return_value=SimpleNamespace(
                    tools=[SimpleNamespace(name="taxonagent_query")]
                )
            ),
            call_tool=AsyncMock(
                return_value=SimpleNamespace(
                    isError=False,
                    content=[SimpleNamespace(type="text", text="Taxonomy evidence")],
                )
            ),
        )

        @asynccontextmanager
        async def session():
            yield self.session

        self.client._session = session

    async def test_read_only_query_and_bounded_result(self):
        self.session.call_tool.return_value.content[0].text = "x" * 20000
        values = await self.client.fetch("Coronaviridae?", 1)
        self.session.call_tool.assert_awaited_once_with(
            "taxonagent_query", {"question": "Coronaviridae?"}
        )
        self.assertEqual(len(values[0]["content"]), MAX_TAXONAGENT_CHARS)
        self.assertNotIn("source_version", values[0])
        self.assertEqual((await self.client.status())["status"], "ready")

    async def test_health_uses_tool_list_and_cache_without_query(self):
        self.assertEqual((await self.client.status())["status"], "ready")
        self.assertEqual((await self.client.status())["status"], "ready")
        self.session.list_tools.assert_awaited_once()
        self.session.call_tool.assert_not_awaited()

    async def test_disabled_missing_tool_and_connection_failure(self):
        self.assertEqual((await TaxonAgentClient("").status())["status"], "disabled")
        self.session.list_tools.return_value.tools = []
        self.assertEqual((await self.client.status())["status"], "unavailable")
        self.client._health = (0, "ready")
        self.session.list_tools.side_effect = OSError("connection refused")
        self.assertEqual((await self.client.status())["status"], "unavailable")

    async def test_tool_error_empty_answer_and_timeout(self):
        self.session.call_tool.return_value.isError = True
        with self.assertRaises(ValueError):
            await self.client.fetch("Coronaviridae?", 1)
        self.session.call_tool.return_value.isError = False
        for value in ("", "   ", "(no answer — output budget exhausted)"):
            self.session.call_tool.return_value.content[0].text = value
            with self.assertRaises(ValueError):
                await self.client.fetch("Coronaviridae?", 1)

        async def slow(*args, **kwargs):
            await asyncio.Event().wait()

        self.client.timeout = 0.01
        self.session.call_tool.side_effect = slow
        with self.assertRaises(TimeoutError):
            await self.client.fetch("Coronaviridae?", 1)
        self.assertEqual((await self.client.status())["status"], "unavailable")

    async def turn(self, output, *, fail=False):
        taxonagent = SimpleNamespace(
            url="http://127.0.0.1:8765/mcp",
            fetch=AsyncMock(
                return_value=[
                    {
                        "citation_id": "taxonagent-test",
                        "content": "Grounded classification evidence",
                    }
                ]
            ),
        )
        if fail:
            taxonagent.fetch.side_effect = OSError("unavailable")
        provider = SimpleNamespace(
            generate=AsyncMock(
                return_value=ProviderResult(
                    provider="fake", model="fake", output=output
                )
            )
        )
        self.provider = provider
        manager = SessionManager(provider=provider, taxonagent=taxonagent)
        return [
            event
            async for event in manager.stream_turn(
                [
                    {
                        "role": "user",
                        "content": "What is the classification standard of Coronaviridae?",
                    }
                ],
                None,
                analysis=True,
                plan_context={"title": "private plan"},
            )
        ]

    async def test_analysis_answer_preserves_source_without_fabricated_review(self):
        events = await self.turn(
            {
                "action": "respond",
                "message": "分类回答",
                "citations": ["taxonagent-test"],
            }
        )
        self.assertIn(TextDelta("分类回答"), events)
        item = next(
            event.item
            for event in events
            if isinstance(event, OutputItemAdded)
            and event.item["type"] == "analysis_knowledge_citations"
        )
        self.assertEqual(item["scope"], "taxonagent")
        source = item["citations"][0]
        self.assertIsNone(source["source_version"])
        self.assertIsNone(source["locator"])
        self.assertEqual(source["quality"], "incomplete")
        self.assertEqual(source["review_status"], "pending")
        request = self.provider.generate.call_args.args[0]
        self.assertEqual(request.task_kind, "citation_qa")
        self.assertNotIn("analysis", request.context)

    async def test_rejects_unregistered_sources_and_plan_changes(self):
        for output in (
            {"action": "respond", "message": "no source", "citations": []},
            {"action": "respond", "message": "forged", "citations": ["other"]},
            {
                "action": "propose_patch",
                "message": "change plan",
                "proposed_patch": {"goals": ["assembly"]},
            },
        ):
            with self.assertRaises(ConversationPublicError) as raised:
                await self.turn(output)
            self.assertEqual(raised.exception.code, "CITATION_INVALID")
        events = await self.turn({"action": "clarify", "message": "请说明分类层级。"})
        self.assertIn(TextDelta("请说明分类层级。"), events)

    async def test_retrieval_failure_never_calls_model(self):
        with self.assertRaises(ConversationPublicError) as raised:
            await self.turn({"action": "respond", "message": "not used"}, fail=True)
        self.assertEqual(raised.exception.code, "RETRIEVAL_UNAVAILABLE")
        self.provider.generate.assert_not_awaited()

    async def test_transport_ignores_shell_proxy(self):
        real_client = TaxonAgentClient("http://127.0.0.1:8765/mcp", timeout=0.05)
        with (
            patch(
                "taxonagent_adapter.httpx.AsyncClient",
                side_effect=OSError("stop before network"),
            ) as client,
            self.assertRaises(OSError),
        ):
            await real_client.fetch("Coronaviridae?", 1)
        self.assertIs(client.call_args.kwargs["trust_env"], False)

from __future__ import annotations

import json
import unittest

from starlette.requests import Request

import adapter
from adapter import ChatRequest, error_sse
from runtime import OutputItemAdded, SessionStateError, TextDelta


def request_with_headers(**headers: str) -> Request:
    raw_headers = [
        (key.replace("_", "-").lower().encode(), value.encode())
        for key, value in headers.items()
    ]
    return Request({"type": "http", "headers": raw_headers})


async def response_text(response) -> str:
    chunks = []
    async for chunk in response.body_iterator:
        chunks.append(chunk.decode() if isinstance(chunk, bytes) else chunk)
    return "".join(chunks)


class FakeSessions:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.persistent_calls = []
        self.ephemeral_calls = []
        self.ensure_calls = []

    async def ensure_session(self, chat_id):
        self.ensure_calls.append(chat_id)

    async def stream_turn(self, messages, chat_id):
        self.persistent_calls.append((messages, chat_id))
        if self.fail:
            raise SessionStateError(
                "无法创建分析运行目录：/tmp/run，请检查 BIOAGENT_RUN_ROOT 写权限"
            )
        yield TextDelta("persistent")
        yield OutputItemAdded({"type": "analysis_plan", "id": "plan-test"})

    async def stream_ephemeral_turn(self, messages):
        self.ephemeral_calls.append(messages)
        if self.fail:
            raise SessionStateError("task failed")
        yield TextDelta("ephemeral")

    async def set_mode(self, chat_id, mode):
        return None

    async def close_all(self):
        return None


class AdapterTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.original_sessions = adapter.sessions

    def tearDown(self):
        adapter.sessions = self.original_sessions

    async def test_metadata_task_uses_ephemeral_session(self):
        fake = FakeSessions()
        adapter.sessions = fake

        response = await adapter.chat(
            ChatRequest(
                messages=[{"role": "user", "content": "generate title"}],
                metadata={"task": "title_generation"},
            ),
            request_with_headers(x_openwebui_chat_id="chat-id"),
        )

        body = await response_text(response)
        self.assertIn("ephemeral", body)
        self.assertEqual(len(fake.ephemeral_calls), 1)
        self.assertEqual(fake.persistent_calls, [])
        self.assertEqual(fake.ensure_calls, [])

    async def test_task_header_uses_ephemeral_session(self):
        fake = FakeSessions()
        adapter.sessions = fake

        response = await adapter.chat(
            ChatRequest(messages=[{"role": "user", "content": "generate tags"}]),
            request_with_headers(x_openwebui_task="tags_generation"),
        )

        body = await response_text(response)
        self.assertIn("ephemeral", body)
        self.assertEqual(len(fake.ephemeral_calls), 1)

    async def test_persistent_stream_includes_structured_plan(self):
        fake = FakeSessions()
        adapter.sessions = fake
        chat_id = "11111111-1111-1111-1111-111111111111"

        response = await adapter.chat(
            ChatRequest(messages=[{"role": "user", "content": "plan analysis"}]),
            request_with_headers(x_openwebui_chat_id=chat_id),
        )

        body = await response_text(response)
        self.assertIn("persistent", body)
        self.assertIn('"type": "response.output_item.added"', body)
        self.assertIn('"type": "analysis_plan"', body)
        self.assertNotIn('"type": "diagnostic_report"', body)
        self.assertEqual(fake.ensure_calls, [chat_id])
        self.assertEqual(len(fake.persistent_calls), 1)
        self.assertEqual(fake.ephemeral_calls, [])

    async def test_stream_failure_returns_openai_error_and_done(self):
        adapter.sessions = FakeSessions(fail=True)

        response = await adapter.chat(
            ChatRequest(
                messages=[{"role": "user", "content": "generate title"}],
                metadata={"task": "title_generation"},
            ),
            request_with_headers(),
        )

        body = await response_text(response)
        payload = json.loads(
            next(
                line.removeprefix("data: ")
                for line in body.splitlines()
                if line.startswith("data: {")
            )
        )
        self.assertEqual(payload["object"], "error")
        self.assertEqual(payload["error"]["type"], "SessionStateError")
        self.assertEqual(payload["error"]["message"], "task failed")
        self.assertTrue(body.rstrip().endswith("data: [DONE]"))

    def test_error_sse_is_openai_compatible(self):
        payload = error_sse(SessionStateError("failed"), "chatcmpl-test")
        data = json.loads(payload.removeprefix("data: ").strip())

        self.assertEqual(data["id"], "chatcmpl-test")
        self.assertEqual(data["object"], "error")
        self.assertEqual(data["error"]["type"], "SessionStateError")


if __name__ == "__main__":
    unittest.main()

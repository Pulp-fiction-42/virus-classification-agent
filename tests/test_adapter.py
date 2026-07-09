from __future__ import annotations

import json
import unittest

from starlette.requests import Request

import adapter
from adapter import ChatRequest
from runtime import SessionStateError


def request_with_headers(**headers: str) -> Request:
    raw_headers = [(key.replace("_", "-").lower().encode(), value.encode()) for key, value in headers.items()]
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
            raise SessionStateError("resume failed")
        yield "persistent"

    async def stream_ephemeral_turn(self, messages):
        self.ephemeral_calls.append(messages)
        if self.fail:
            raise SessionStateError("task failed")
        yield "ephemeral"


class AdapterTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.real_sessions = adapter.sessions

    def tearDown(self):
        adapter.sessions = self.real_sessions

    async def test_task_metadata_uses_ephemeral_session(self):
        fake = FakeSessions()
        adapter.sessions = fake
        request = request_with_headers(x_openwebui_chat_id="5bf59cc7-26fc-49e4-b4d3-16fbed175204")
        response = await adapter.chat(
            ChatRequest(
                messages=[{"role": "user", "content": "generate title"}],
                metadata={"task": "title_generation"},
            ),
            request,
        )

        body = await response_text(response)
        self.assertIn("ephemeral", body)
        self.assertIn("data: [DONE]", body)
        self.assertEqual(len(fake.ephemeral_calls), 1)
        self.assertEqual(fake.persistent_calls, [])
        self.assertEqual(fake.ensure_calls, [])

    async def test_normal_chat_uses_persistent_session(self):
        fake = FakeSessions()
        adapter.sessions = fake
        chat_id = "5bf59cc7-26fc-49e4-b4d3-16fbed175204"
        response = await adapter.chat(
            ChatRequest(messages=[{"role": "user", "content": "hello"}]),
            request_with_headers(x_openwebui_chat_id=chat_id),
        )

        body = await response_text(response)
        self.assertIn("persistent", body)
        self.assertEqual(fake.ensure_calls, [chat_id])
        self.assertEqual(len(fake.persistent_calls), 1)
        self.assertEqual(fake.ephemeral_calls, [])

    async def test_stream_failure_returns_error_event_and_done(self):
        adapter.sessions = FakeSessions(fail=True)
        response = await adapter.chat(
            ChatRequest(
                messages=[{"role": "user", "content": "generate title"}],
                metadata={"task": "title_generation"},
            ),
            request_with_headers(),
        )

        body = await response_text(response)
        events = [line.removeprefix("data: ") for line in body.splitlines() if line.startswith("data: {")]
        payload = json.loads(events[0])
        self.assertEqual(payload["error"]["message"], "task failed")
        self.assertIn("data: [DONE]", body)


if __name__ == "__main__":
    unittest.main()

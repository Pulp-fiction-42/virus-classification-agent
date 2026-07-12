from __future__ import annotations

import json
import unittest
import uuid

import adapter
from adapter import error_sse
from fastapi.testclient import TestClient
from runtime import SessionStateError


class BrokenStreamingSessions:
    async def ensure_session(self, chat_id):
        return None

    async def stream_turn(self, messages, chat_id):
        raise SessionStateError("无法创建分析运行目录：/tmp/run，请检查 BIOAGENT_RUN_ROOT 写权限")
        yield None

    async def close_all(self):
        return None


class AdapterTests(unittest.TestCase):
    def test_error_sse_is_openai_compatible_and_done_safe(self):
        payload = error_sse(
            SessionStateError("无法创建分析运行目录：/tmp/run，请检查 BIOAGENT_RUN_ROOT 写权限"),
            "chatcmpl-test",
        )
        data = json.loads(payload.removeprefix("data: ").strip())

        self.assertEqual(data["id"], "chatcmpl-test")
        self.assertEqual(data["object"], "error")
        self.assertEqual(data["error"]["type"], "SessionStateError")
        self.assertIn("BIOAGENT_RUN_ROOT", data["error"]["message"])

    def test_streaming_exception_yields_error_and_done(self):
        original_sessions = adapter.sessions
        adapter.sessions = BrokenStreamingSessions()
        try:
            with TestClient(adapter.app) as client:
                with client.stream(
                    "POST",
                    "/v1/chat/completions",
                    headers={"X-OpenWebUI-Chat-Id": str(uuid.uuid4())},
                    json={
                        "messages": [{"role": "user", "content": "确认启动分析"}],
                        "stream": True,
                    },
                ) as response:
                    body = "".join(response.iter_text())
        finally:
            adapter.sessions = original_sessions

        self.assertIn('"object": "error"', body)
        self.assertIn("BIOAGENT_RUN_ROOT", body)
        self.assertTrue(body.rstrip().endswith("data: [DONE]"))


if __name__ == "__main__":
    unittest.main()

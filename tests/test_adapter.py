from __future__ import annotations

import json
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import adapter
from adapter import (
    CandidateAssessmentsRequest,
    ChatRequest,
    CompilePlanRequest,
    ResumeRunRequest,
    StartRunRequest,
    error_sse,
)
from deterministic_planner import PlannerError
from fastapi import HTTPException
from pydantic import ValidationError
from remote_client import RemoteConnectionError, RemoteExecutionError
from runtime import SessionStateError, TextDelta
from runtime_errors import ConversationPublicError
from starlette.requests import Request


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
    def __init__(
        self,
        *,
        fail: bool = False,
        text_parts: list[str] | None = None,
        error: Exception | None = None,
        ensure_error: Exception | None = None,
    ) -> None:
        self.fail = fail
        self.text_parts = text_parts or ["persistent"]
        self.error = error
        self.ensure_error = ensure_error
        self.persistent_calls = []
        self.ephemeral_calls = []
        self.ensure_calls = []

    async def ensure_session(self, chat_id):
        self.ensure_calls.append(chat_id)
        if self.ensure_error:
            raise self.ensure_error

    async def stream_turn(
        self,
        messages,
        chat_id,
        routing=None,
        dataset_context=None,
        plan_context=None,
        analysis=False,
    ):
        self.persistent_calls.append(
            (messages, chat_id, routing, dataset_context, plan_context, analysis)
        )
        if self.fail:
            raise SessionStateError(
                "无法创建分析运行目录：/tmp/run，请检查 BIOAGENT_RUN_ROOT 写权限"
            )
        if self.error:
            raise self.error
        for text in self.text_parts:
            yield TextDelta(text)

    async def stream_ephemeral_turn(self, messages):
        self.ephemeral_calls.append(messages)
        if self.fail:
            raise SessionStateError("task failed")
        if self.error:
            raise self.error
        yield TextDelta("ephemeral")

    async def close_all(self):
        return None


class FakePlanner:
    def __init__(self, *, fail: bool = False, resume_error: Exception | None = None):
        self.fail = fail
        self.resume_error = resume_error
        self.compile_calls = []
        self.start_calls = []
        self.resume_calls = []

    def compile(self, **kwargs):
        self.compile_calls.append(kwargs)
        if self.fail:
            raise PlannerError("invalid structured plan")
        return {"planner_version": 1, **kwargs}

    async def start(self, **kwargs):
        self.start_calls.append(kwargs)
        if self.fail:
            raise PlannerError("persisted plan mismatch")
        return {"type": "analysis_run", "status": "running"}

    async def resume(self, **kwargs):
        self.resume_calls.append(kwargs)
        if self.resume_error:
            raise self.resume_error
        if self.fail:
            raise PlannerError("persisted plan mismatch")
        return {"type": "analysis_run", "status": "queued"}


class AdapterTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.original_sessions = adapter.sessions
        self.original_planner = adapter.planner
        adapter.sessions = None

    def tearDown(self):
        adapter.sessions = self.original_sessions
        adapter.planner = self.original_planner

    async def test_compile_api_is_structured_and_model_free(self):
        fake = FakePlanner()
        adapter.planner = fake
        result = await adapter.compile_analysis_plan(
            CompilePlanRequest(
                dataset={"id": "ds", "revision": 1, "status": "ready"},
                goals=["virus_summary"],
                options={"viral_type": "RNA"},
                routing={"mode": "standard"},
            )
        )
        self.assertEqual(result["planner_version"], 1)
        self.assertEqual(fake.compile_calls[0]["goals"], ["virus_summary"])

    async def test_candidate_assessment_api_forwards_only_bounded_selection(self):
        with patch.object(
            adapter,
            "_remote_call",
            AsyncMock(return_value={"status": "not_generated"}),
        ) as remote:
            result = await adapter.remote_candidate_assessments(
                CandidateAssessmentsRequest(
                    run_dir="/runs/analysis-one",
                    candidate_ids=["candidate-one"],
                )
            )

        self.assertEqual(result, {"status": "not_generated"})
        remote.assert_awaited_once_with(
            "candidate_assessments",
            run_dir="/runs/analysis-one",
            candidate_ids=["candidate-one"],
        )
        with self.assertRaises(ValidationError):
            CandidateAssessmentsRequest(
                run_dir="/runs/analysis-one",
                candidate_ids=[str(index) for index in range(6)],
            )

    async def test_candidate_assessment_unavailable_error_is_retryable(self):
        worker = SimpleNamespace(
            invoke=AsyncMock(
                side_effect=RemoteExecutionError(
                    "ASSESSMENT_RUN_UNAVAILABLE: run state is unavailable"
                )
            )
        )
        with (
            patch.object(adapter, "_remote_required", return_value=worker),
            self.assertRaises(HTTPException) as raised,
        ):
            await adapter._remote_call(
                "candidate_assessments",
                run_dir="/runs/analysis-one",
                candidate_ids=["candidate-one"],
            )

        self.assertEqual(raised.exception.status_code, 503)
        self.assertEqual(
            raised.exception.detail,
            {
                "code": "ASSESSMENT_RUN_UNAVAILABLE",
                "message": "run state is unavailable",
                "retryable": True,
            },
        )

    async def test_health_separates_deterministic_model_and_knowledge(self):
        with (
            patch.object(adapter.config, "MINIMAX_API_KEY", ""),
            patch.object(adapter.config, "TAXONAGENT_MCP_URL", ""),
            patch.object(adapter, "claude_agent_sdk_version", return_value="0.2.103"),
            patch.object(adapter, "claude_agent_sdk_ready", return_value=True),
        ):
            result = await adapter.analysis_health()
        self.assertEqual(result["deterministic"]["status"], "ready")
        self.assertEqual(result["conversation_model"]["status"], "unavailable")
        self.assertEqual(result["conversation_model"]["id"], "BioAgent")
        self.assertEqual(result["conversation_model"]["sdk_version"], "0.2.103")
        self.assertEqual(
            result["conversation_model"]["required_sdk_version"], "0.2.103"
        )
        self.assertEqual(result["knowledge"]["status"], "ready")
        self.assertEqual(result["knowledge"]["provider"], "sqlite_fts5")
        self.assertEqual(result["knowledge"]["release_id"], "engineering-knowledge-v4")
        self.assertEqual(result["knowledge"]["scope"], "engineering_contract")

    async def test_health_rejects_unpinned_sdk(self):
        with (
            patch.object(adapter.config, "MODEL_POLICY", "minimax_explicit"),
            patch.object(adapter.config, "MINIMAX_API_KEY", "configured"),
            patch.object(adapter, "claude_agent_sdk_version", return_value="0.2.152"),
            patch.object(adapter, "claude_agent_sdk_ready", return_value=False),
        ):
            result = await adapter.analysis_health()
        self.assertEqual(result["conversation_model"]["status"], "unavailable")

    async def test_capability_catalog_is_public_and_path_free(self):
        result = await adapter.analysis_capabilities()
        self.assertEqual(result["schema_version"], 2)
        self.assertIn("read_qc", {item["id"] for item in result["deliverables"]})
        self.assertEqual(result["tools"][0]["id"], "fastp")
        self.assertEqual(result["tools"][0]["version"], "1.3.2")
        self.assertNotIn("executable", result["tools"][0])
        self.assertEqual(result["workflow_skills"][0]["id"], "bioagent-w1-read-qc")
        fastp = result["capabilities"][0]
        self.assertEqual(fastp["id"], "workflow1.read_qc.fastp")
        self.assertEqual(fastp["version"], 1)
        workflow3 = [
            item
            for item in result["capabilities"]
            if item["id"].startswith("workflow3.")
        ]
        self.assertEqual(
            [item["id"] for item in workflow3],
            [
                "workflow3.host_mapping.bowtie2",
                "workflow3.host_coverage.samtools",
                "workflow3.host_blast.blastn",
                "workflow3.host_integrate.posix",
                "workflow3.host_summary.python",
            ],
        )
        self.assertEqual(
            [item["output_artifact"] for item in workflow3],
            [
                "host_alignment_bundle",
                "host_coverage_bundle",
                "host_blast_bundle",
                "host_evidence_bundle",
                "host_summary_bundle",
            ],
        )
        self.assertEqual(workflow3[0]["resources"], {"threads": 12})
        self.assertEqual(
            workflow3[0]["reference_dependencies"],
            ["workflow3.eukamarker_bowtie2"],
        )
        self.assertEqual(
            workflow3[-1]["reference_dependencies"],
            [
                "workflow3.eukamarker_taxonomy",
                "workflow3.eukamarker_taxonomy_chinese",
            ],
        )
        self.assertTrue(
            all(item["validation_status"] == "dry_run_validated" for item in workflow3)
        )
        workflow4_to_6 = [
            item
            for item in result["capabilities"]
            if item["id"].startswith(("workflow4.", "workflow5.", "workflow6."))
        ]
        self.assertEqual(len(workflow4_to_6), 11)
        self.assertEqual(
            next(
                item
                for item in workflow4_to_6
                if item["id"] == "workflow4.microbe_classify.kraken2"
            )["reference_dependencies"],
            ["workflow4.kraken2_standard_20240112"],
        )
        self.assertEqual(
            next(
                item
                for item in workflow4_to_6
                if item["id"] == "workflow5.virus_read_mapping.bowtie2"
            )["reference_dependencies"],
            [],
        )
        self.assertEqual(
            next(
                item
                for item in workflow4_to_6
                if item["id"] == "workflow6.functional_annotation.dramv"
            )["reference_dependencies"],
            ["workflow6.dramv_data", "workflow6.dram_config"],
        )
        self.assertTrue(
            all(
                item["validation_status"] == "dry_run_validated"
                for item in workflow4_to_6
            )
        )
        self.assertNotIn("path", json.dumps(result).lower())

    async def test_start_api_forwards_backend_owned_run_reference(self):
        fake = FakePlanner()
        adapter.planner = fake
        result = await adapter.start_analysis_run(
            StartRunRequest(
                plan={"goals": ["virus_summary"]},
                dataset={"id": "ds", "revision": 1, "status": "ready"},
                chat_id="00000000-0000-0000-0000-000000000001",
                plan_id="pl_test",
                plan_revision=3,
                run_id="analysis-fixed",
                queue_context={"project_id": "project-test", "project_priority": "high"},
            )
        )
        self.assertEqual(result["status"], "running")
        self.assertEqual(fake.start_calls[0]["run_id"], "analysis-fixed")
        self.assertEqual(fake.start_calls[0]["queue_context"]["project_priority"], "high")

    def test_start_api_rejects_extra_queue_context_fields(self):
        with self.assertRaises(ValidationError):
            StartRunRequest(
                plan={"goals": ["virus_summary"]},
                dataset={"id": "ds", "revision": 1, "status": "ready"},
                chat_id="00000000-0000-0000-0000-000000000001",
                plan_id="pl_test",
                plan_revision=3,
                run_id="analysis-fixed",
                queue_context={
                    "project_id": "project-test",
                    "project_priority": "high",
                    "browser_priority": "high",
                },
            )

    async def test_resume_api_forwards_only_authoritative_run_context(self):
        fake = FakePlanner()
        adapter.planner = fake
        result = await adapter.resume_analysis_run(
            ResumeRunRequest(
                plan={"goals": ["read_qc"]},
                dataset={"id": "ds", "revision": 1, "status": "ready"},
                chat_id="00000000-0000-0000-0000-000000000001",
                plan_id="pl_test",
                plan_revision=3,
                run_id="analysis-fixed",
            )
        )
        self.assertEqual(result["status"], "queued")
        self.assertEqual(fake.resume_calls[0]["run_id"], "analysis-fixed")
        self.assertNotIn("queue_context", fake.resume_calls[0])

    async def test_resume_api_maps_temporary_verification_failure_to_retryable_503(self):
        adapter.planner = FakePlanner(
            resume_error=RemoteConnectionError("Cyclone unavailable")
        )
        request = ResumeRunRequest(
            plan={},
            dataset={},
            chat_id="00000000-0000-0000-0000-000000000001",
            plan_id="pl_test",
            plan_revision=3,
            run_id="analysis-fixed",
        )

        with self.assertRaises(HTTPException) as raised:
            await adapter.resume_analysis_run(request)

        self.assertEqual(raised.exception.status_code, 503)
        self.assertEqual(
            raised.exception.detail["code"], "RECOVERY_CHECK_UNAVAILABLE"
        )
        self.assertTrue(raised.exception.detail["retryable"])

    async def test_resume_api_preserves_stable_worker_rejection_code(self):
        adapter.planner = FakePlanner(
            resume_error=RemoteExecutionError(
                "RECOVERY_WORKER_CHANGED: worker version changed"
            )
        )
        request = ResumeRunRequest(
            plan={},
            dataset={},
            chat_id="00000000-0000-0000-0000-000000000001",
            plan_id="pl_test",
            plan_revision=3,
            run_id="analysis-fixed",
        )

        with self.assertRaises(HTTPException) as raised:
            await adapter.resume_analysis_run(request)

        self.assertEqual(raised.exception.status_code, 409)
        self.assertEqual(raised.exception.detail["code"], "RECOVERY_WORKER_CHANGED")
        self.assertFalse(raised.exception.detail["retryable"])

    async def test_resume_api_maps_worker_verification_unavailable_to_503(self):
        adapter.planner = FakePlanner(
            resume_error=RemoteExecutionError(
                "RECOVERY_CHECK_UNAVAILABLE: fingerprint temporarily unavailable"
            )
        )
        request = ResumeRunRequest(
            plan={},
            dataset={},
            chat_id="00000000-0000-0000-0000-000000000001",
            plan_id="pl_test",
            plan_revision=3,
            run_id="analysis-fixed",
        )

        with self.assertRaises(HTTPException) as raised:
            await adapter.resume_analysis_run(request)

        self.assertEqual(raised.exception.status_code, 503)
        self.assertEqual(
            raised.exception.detail["code"], "RECOVERY_CHECK_UNAVAILABLE"
        )
        self.assertTrue(raised.exception.detail["retryable"])

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

    async def test_persistent_stream_is_conversation_only(self):
        fake = FakeSessions()
        adapter.sessions = fake
        chat_id = "11111111-1111-1111-1111-111111111111"

        response = await adapter.chat(
            ChatRequest(messages=[{"role": "user", "content": "plan analysis"}]),
            request_with_headers(x_openwebui_chat_id=chat_id),
        )

        body = await response_text(response)
        self.assertIn("persistent", body)
        self.assertNotIn('"type": "response.output_item.added"', body)
        self.assertNotIn('"type": "analysis_plan"', body)
        self.assertEqual(fake.ensure_calls, [chat_id])
        self.assertEqual(len(fake.persistent_calls), 1)
        self.assertEqual(fake.ephemeral_calls, [])
        self.assertFalse(fake.persistent_calls[0][5])

    async def test_missing_chat_id_uses_one_turn_provider_session(self):
        fake = FakeSessions()
        adapter.sessions = fake

        response = await adapter.chat(
            ChatRequest(messages=[{"role": "user", "content": "first message"}]),
            request_with_headers(),
        )

        body = await response_text(response)
        self.assertIn("persistent", body)
        self.assertEqual(fake.ensure_calls, [None])
        self.assertEqual(fake.persistent_calls[0][1], None)
        self.assertEqual(fake.ephemeral_calls, [])

    async def test_execute_metadata_cannot_start_runner(self):
        fake = FakeSessions()
        adapter.sessions = fake
        chat_id = "11111111-1111-1111-1111-111111111111"
        routing = {"mode": "emergency"}

        response = await adapter.chat(
            ChatRequest(
                messages=[{"role": "user", "content": "确认启动分析"}],
                metadata={
                    "chat_meta": {
                        "source": "analysis",
                        "analysis_mode": "execute",
                        "analysis_routing": routing,
                    }
                },
            ),
            request_with_headers(
                x_openwebui_chat_id=chat_id,
                x_openwebui_message_id="assistant-message",
            ),
        )
        body = await response_text(response)

        self.assertEqual(fake.ensure_calls, [chat_id])
        self.assertEqual(len(fake.persistent_calls), 1)
        self.assertTrue(fake.persistent_calls[0][5])
        self.assertNotIn('"type": "analysis_run"', body)

    async def test_dataset_context_is_forwarded_as_read_only_chat_context(self):
        fake = FakeSessions()
        adapter.sessions = fake
        chat_id = "11111111-1111-1111-1111-111111111111"
        dataset = {"id": "dataset-1", "revision": 2, "status": "ready"}

        plan_response = await adapter.chat(
            ChatRequest(
                messages=[{"role": "user", "content": "plan"}],
                metadata={
                    "chat_meta": {
                        "source": "analysis",
                        "analysis_dataset": dataset,
                    }
                },
            ),
            request_with_headers(x_openwebui_chat_id=chat_id),
        )
        await response_text(plan_response)
        self.assertEqual(fake.persistent_calls[0][3], dataset)
        self.assertTrue(fake.persistent_calls[0][5])

    async def test_persisted_plan_summary_is_forwarded_as_read_only_chat_context(self):
        fake = FakeSessions()
        adapter.sessions = fake
        chat_id = "11111111-1111-1111-1111-111111111111"
        summary = {"goals": ["virus_summary"], "ready_to_execute": True}

        response = await adapter.chat(
            ChatRequest(
                messages=[{"role": "user", "content": "explain this plan"}],
                metadata={
                    "chat_meta": {
                        "source": "analysis",
                        "analysis_plan_summary": summary,
                    }
                },
            ),
            request_with_headers(x_openwebui_chat_id=chat_id),
        )
        await response_text(response)
        self.assertEqual(fake.persistent_calls[0][4], summary)
        self.assertTrue(fake.persistent_calls[0][5])

    async def test_streaming_and_non_streaming_preserve_markdown_boundaries(self):
        fake = FakeSessions(text_parts=["开始检查。", "\n\n## 检查完成"])
        adapter.sessions = fake
        chat_id = "11111111-1111-1111-1111-111111111111"
        messages = [{"role": "user", "content": "check"}]

        streaming_response = await adapter.chat(
            ChatRequest(messages=messages),
            request_with_headers(x_openwebui_chat_id=chat_id),
        )
        streaming_body = await response_text(streaming_response)
        streamed_text = "".join(
            json.loads(line.removeprefix("data: "))["choices"][0]["delta"].get(
                "content", ""
            )
            for line in streaming_body.splitlines()
            if line.startswith("data: {")
            and '"object": "chat.completion.chunk"' in line
        )

        non_streaming_response = await adapter.chat(
            ChatRequest(messages=messages, stream=False),
            request_with_headers(x_openwebui_chat_id=chat_id),
        )
        non_streamed_text = json.loads(non_streaming_response.body)["choices"][0][
            "message"
        ]["content"]

        expected = "开始检查。\n\n## 检查完成"
        self.assertEqual(streamed_text, expected)
        self.assertEqual(non_streamed_text, expected)

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
        self.assertEqual(payload["error"]["message"], adapter.CONVERSATION_ERROR)
        self.assertTrue(body.rstrip().endswith("data: [DONE]"))

    async def test_public_conversation_errors_keep_distinct_safe_messages(self):
        cases = {
            "ACTION_SCHEMA_INVALID": "对话模型返回了无效的结构化结果，请重试；方案未发生变更。",
            "MODEL_TIMEOUT": "对话模型请求超时，请重试；数据集、方案与执行功能不受影响。",
            "MODEL_UNAVAILABLE": adapter.CONVERSATION_ERROR,
            "KNOWLEDGE_PATCH_FORBIDDEN": "知识查询不能形成方案修改建议，请分开提出修改要求；方案未发生变更。",
            "KNOWLEDGE_UNAVAILABLE": "当前无法完成经验证的 ICTV 查询，请稍后重试。",
        }
        for code, message in cases.items():
            with self.subTest(code=code):
                adapter.sessions = FakeSessions(
                    error=ConversationPublicError(code, message)
                )
                response = await adapter.chat(
                    ChatRequest(messages=[{"role": "user", "content": "test"}]),
                    request_with_headers(
                        x_openwebui_chat_id="11111111-1111-1111-1111-111111111111"
                    ),
                )
                body = await response_text(response)
                payload = json.loads(
                    next(
                        line.removeprefix("data: ")
                        for line in body.splitlines()
                        if line.startswith("data: {")
                    )
                )
                self.assertEqual(payload["error"]["code"], code)
                self.assertEqual(payload["error"]["message"], message)

    async def test_session_initialize_error_is_safe_and_typed(self):
        adapter.sessions = FakeSessions(
            ensure_error=ConversationPublicError(
                "MODEL_UNAVAILABLE", adapter.CONVERSATION_ERROR
            )
        )
        with self.assertRaises(HTTPException) as raised:
            await adapter.chat(
                ChatRequest(messages=[{"role": "user", "content": "test"}]),
                request_with_headers(
                    x_openwebui_chat_id="11111111-1111-1111-1111-111111111111"
                ),
            )
        self.assertEqual(raised.exception.status_code, 503)
        self.assertEqual(raised.exception.detail["code"], "MODEL_UNAVAILABLE")

    def test_error_sse_is_openai_compatible(self):
        payload = error_sse(SessionStateError("failed"), "chatcmpl-test")
        data = json.loads(payload.removeprefix("data: ").strip())

        self.assertEqual(data["id"], "chatcmpl-test")
        self.assertEqual(data["object"], "error")
        self.assertEqual(data["error"]["type"], "SessionStateError")


if __name__ == "__main__":
    unittest.main()

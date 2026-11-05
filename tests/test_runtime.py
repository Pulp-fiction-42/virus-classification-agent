from __future__ import annotations

import json
import tempfile
import unittest
import uuid
from unittest.mock import patch
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
    OutputItemAdded,
    SessionManager,
    SessionStateError,
    TextDelta,
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
    response_text_parts = ["chunk"]
    result_session_id = None
    before_result = None

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
        session_id = self.result_session_id or self.options.resume or "sdk-session-1"
        for text in self.response_text_parts:
            yield FakeStreamEvent(
                {
                    "type": "content_block_delta",
                    "delta": {"type": "text_delta", "text": text},
                }
            )
        if type(self).before_result:
            type(self).before_result()
        yield FakeResultMessage(
            session_id=session_id, is_error=False, subtype="success"
        )

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
        FakeClient.response_text_parts = ["chunk"]
        FakeClient.result_session_id = None
        FakeClient.before_result = None
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name) / "agent"
        root.mkdir()
        self.run_root = Path(self.temp_dir.name) / "runs"
        self.manager = SessionManager(
            root,
            root / ".claude" / "sessions",
            sdk_config_dir=root / "sdk-runtime",
            client_factory=FakeClient,
            idle_timeout_seconds=60 * 60,
            analysis_run_root=self.run_root,
            allow_legacy_execution=True,  # Explicit fixture for historical SDK execution tests.
            planning_backend="legacy",  # Historical transport/router regression only.
        )
        self.chat_id = str(uuid.uuid4())

    def tearDown(self):
        runtime.StreamEvent = self.real_stream_event
        runtime.ResultMessage = self.real_result_message
        self.temp_dir.cleanup()

    @staticmethod
    def texts(events):
        return [event.text for event in events if isinstance(event, TextDelta)]

    def ready_plan(self):
        run_id = "analysis-20260709-120000-testabcd"
        run_dir = self.run_root / run_id
        return {
            "type": "analysis_plan",
            "id": f"analysis_plan_{run_id}",
            "status": "completed",
            "title": "测序质控与分类方案",
            "ready_to_execute": True,
            "run": {
                "run_id": run_id,
                "run_dir": str(run_dir),
                "state_path": str(run_dir / ".bioagent" / "state.json"),
                "log_path": str(run_dir / ".bioagent" / "execution.log"),
            },
            "steps": [
                {"id": "step_1", "label": "质控", "tool": "fastp", "rules": ["fastp"]},
                {
                    "id": "step_2",
                    "label": "分类",
                    "tool": "Kraken2",
                    "rules": ["classify", "summarize"],
                },
            ],
        }

    @staticmethod
    def finish_run(plan, *, ok=True):
        state_path = Path(plan["run"]["state_path"])
        state = json.loads(state_path.read_text(encoding="utf-8"))
        for step in state["steps"]:
            step["status"] = "completed" if ok else "failed"
        state["execution"].update(
            {
                "status": "completed" if ok else "failed",
                "ok": ok,
                "returncode": 0 if ok else 1,
                "current_step_id": None,
            }
        )
        state_path.write_text(
            json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )

    async def test_new_chat_defaults_to_plan_mode(self):
        await self.manager.ensure_session(self.chat_id)

        self.assertEqual(self.manager.current_mode(self.chat_id), PLAN_MODE)
        self.assertEqual(
            FakeClient.instances[0].options.permission_mode, "bypassPermissions"
        )
        self.assertEqual(FakeClient.instances[0].permission_modes, ["dontAsk"])

    def test_sdk_runtime_projects_link_targets_persistent_transcripts(self):
        projects_link = self.manager.sdk_config_dir / "projects"

        self.assertTrue(
            projects_link.is_symlink() or projects_link.resolve() == self.manager.projects_dir
        )
        self.assertEqual(projects_link.resolve(), self.manager.projects_dir)

    async def test_plan_mode_calls_sdk_permission_mode(self):
        await self.manager.set_mode(self.chat_id, PLAN_MODE)

        self.assertEqual(FakeClient.instances[0].permission_modes[-1], "dontAsk")

    async def test_execute_mode_calls_sdk_bypass_permissions(self):
        await self.manager.set_mode(self.chat_id, EXECUTE_MODE)

        self.assertEqual(
            FakeClient.instances[0].permission_modes[-1], "bypassPermissions"
        )
        self.assertEqual(self.manager.current_mode(self.chat_id), EXECUTE_MODE)

    async def test_mode_switch_does_not_query_client(self):
        await self.manager.set_mode(self.chat_id, EXECUTE_MODE)

        self.assertEqual(FakeClient.instances[0].queries, [])

    async def test_same_chat_id_reuses_managed_session(self):
        first = [
            chunk async for chunk in self.manager.stream_turn(MESSAGES, self.chat_id)
        ]
        second = [
            chunk async for chunk in self.manager.stream_turn(MESSAGES, self.chat_id)
        ]

        self.assertEqual(self.texts(first), ["chunk"])
        self.assertEqual(self.texts(second), ["chunk"])
        self.assertEqual(len(FakeClient.instances), 1)
        self.assertEqual(len(FakeClient.instances[0].queries), 2)

    async def test_first_turn_persists_claude_session_mapping(self):
        [chunk async for chunk in self.manager.stream_turn(MESSAGES, self.chat_id)]

        mapping = json.loads(
            self.manager._chat_map_path(self.chat_id).read_text(encoding="utf-8")
        )
        self.assertEqual(mapping["openwebui_chat_id"], self.chat_id)
        self.assertEqual(mapping["claude_session_id"], "sdk-session-1")
        self.assertIsNone(FakeClient.instances[0].queries[0][1])

    async def test_follow_up_uses_stored_claude_session_id(self):
        [chunk async for chunk in self.manager.stream_turn(MESSAGES, self.chat_id)]
        [chunk async for chunk in self.manager.stream_turn(MESSAGES, self.chat_id)]

        self.assertIsNone(FakeClient.instances[0].queries[0][1])
        self.assertIsNone(FakeClient.instances[0].queries[1][1])

    async def test_resumed_sdk_session_rejects_rotated_session_id(self):
        old_session_id = "sdk-session-old"
        self.manager._save_claude_session_id(self.chat_id, old_session_id)
        transcript = self.manager.transcript_path(old_session_id)
        transcript.parent.mkdir(parents=True)
        transcript.write_text("{}\n", encoding="utf-8")
        FakeClient.result_session_id = "sdk-session-new"

        with self.assertRaises(SessionStateError):
            [event async for event in self.manager.stream_turn(MESSAGES, self.chat_id)]

        mapping = json.loads(
            self.manager._chat_map_path(self.chat_id).read_text(encoding="utf-8")
        )
        self.assertIsNone(FakeClient.instances[0].queries[0][1])
        self.assertEqual(mapping["claude_session_id"], old_session_id)

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

    async def test_plan_prompt_requires_analysis_plan_without_claude_plan_mode(self):
        await self.manager.set_mode(self.chat_id, PLAN_MODE)
        chunks = [
            chunk async for chunk in self.manager.stream_turn(MESSAGES, self.chat_id)
        ]

        prompt, session_id = FakeClient.instances[0].queries[0]
        self.assertEqual(self.texts(chunks), ["chunk"])
        self.assertIsNone(session_id)
        self.assertIn(PLAN_PROMPT_PREFIX, prompt)
        self.assertIn("USER REQUEST:", prompt)
        self.assertIn("follow-up question", prompt)
        self.assertIn("不要调用 EnterPlanMode 或 ExitPlanMode", prompt)

    async def test_plan_block_becomes_hidden_output_item(self):
        FakeClient.response_text_parts = [
            "方案如下。",
            """
```analysis_plan
{
  "version": 1,
  "title": "测序质控与分类方案",
  "ready_to_execute": true,
  "steps": [
    {"label": "质控", "tool": "fastp", "rules": ["fastp"]},
    {"label": "分类", "tool": "Kraken2", "rules": ["classify", "summarize"]}
  ],
  "resource_estimate": {
    "eta": "约 1 小时",
    "compute": "4 核",
    "confidence": "估算"
  }
}
```
""",
        ]

        events = [
            event async for event in self.manager.stream_turn(MESSAGES, self.chat_id)
        ]

        self.assertEqual("".join(self.texts(events)).strip(), "方案如下。")
        output_items = [
            event.item for event in events if isinstance(event, OutputItemAdded)
        ]
        self.assertEqual(len(output_items), 1)
        item = output_items[0]
        self.assertEqual(item["type"], "analysis_plan")
        self.assertEqual(item["title"], "测序质控与分类方案")
        # Model-authored steps alone cannot authorize an executable plan.
        self.assertFalse(item["ready_to_execute"])
        self.assertEqual([step["label"] for step in item["steps"]], ["质控", "分类"])
        self.assertEqual(item["steps"][0]["rules"], ["fastp"])
        self.assertEqual(item["steps"][1]["rules"], ["classify", "summarize"])
        self.assertTrue(item["run"]["run_dir"].startswith(str(self.run_root)))
        self.assertFalse(Path(item["run"]["state_path"]).exists())
        saved = json.loads(
            self.manager._analysis_plan_path(self.chat_id).read_text(encoding="utf-8")
        )
        self.assertEqual(saved["run"]["run_id"], item["run"]["run_id"])

    async def test_split_plan_block_marker_does_not_leak(self):
        FakeClient.response_text_parts = [
            "方案会按阶段执行。\n``",
            "`analysis",
            """_plan
{
  "version": 1,
  "title": "拆分控制块",
  "ready_to_execute": true,
  "steps": [{"label": "质控"}]
}
```
""",
        ]

        events = [
            event async for event in self.manager.stream_turn(MESSAGES, self.chat_id)
        ]
        text = "".join(self.texts(events))
        output_items = [
            event.item for event in events if isinstance(event, OutputItemAdded)
        ]

        self.assertEqual(text.strip(), "方案会按阶段执行。")
        self.assertNotIn("analysis_plan", text)
        self.assertEqual(len(output_items), 1)
        self.assertEqual(output_items[0]["title"], "拆分控制块")

    async def test_text_after_plan_block_is_preserved_without_leaking_control_block(
        self,
    ):
        FakeClient.response_text_parts = [
            "还需要补充输入文件。\n",
            """```analysis_plan
{
  "version": 1,
  "title": "待补充输入",
  "ready_to_execute": false,
  "steps": [{"label": "质控"}]
}
```
请提供成对的测序文件路径。
""",
        ]

        events = [
            event async for event in self.manager.stream_turn(MESSAGES, self.chat_id)
        ]
        text = "".join(self.texts(events))
        output_items = [
            event.item for event in events if isinstance(event, OutputItemAdded)
        ]

        self.assertIn("还需要补充输入文件。", text)
        self.assertIn("请提供成对的测序文件路径。", text)
        self.assertNotIn("analysis_plan", text)
        self.assertEqual(len(output_items), 1)
        self.assertFalse(output_items[0]["ready_to_execute"])

    async def test_updated_plan_replaces_saved_reservation(self):
        FakeClient.response_text_parts = [
            """第一版。
```analysis_plan
{"title":"完整流程","ready_to_execute":true,"steps":[{"label":"质控"},{"label":"组装"}]}
```
"""
        ]
        [event async for event in self.manager.stream_turn(MESSAGES, self.chat_id)]

        FakeClient.response_text_parts = [
            """已按要求移除组装。
```analysis_plan
{"title":"质控流程","ready_to_execute":true,"steps":[{"label":"宽松质控"}]}
```
"""
        ]
        events = [
            event async for event in self.manager.stream_turn(MESSAGES, self.chat_id)
        ]
        saved = json.loads(
            self.manager._analysis_plan_path(self.chat_id).read_text(encoding="utf-8")
        )
        output_items = [
            event.item for event in events if isinstance(event, OutputItemAdded)
        ]

        self.assertEqual([step["label"] for step in saved["steps"]], ["宽松质控"])
        self.assertEqual(output_items[0]["run"]["run_id"], saved["run"]["run_id"])

    async def test_execute_without_ready_plan_does_not_query_client(self):
        await self.manager.set_mode(self.chat_id, EXECUTE_MODE)

        events = [
            event async for event in self.manager.stream_turn(MESSAGES, self.chat_id)
        ]

        self.assertEqual(
            self.texts(events), ["请先生成可执行分析方案，再确认启动分析。"]
        )
        self.assertEqual(FakeClient.instances[0].queries, [])

    async def test_execute_creates_state_and_marks_missing_terminal_state_failed(self):
        plan = self.ready_plan()
        self.manager._save_analysis_plan(self.chat_id, plan)
        await self.manager.set_mode(self.chat_id, EXECUTE_MODE)

        [event async for event in self.manager.stream_turn(MESSAGES, self.chat_id)]

        state = json.loads(Path(plan["run"]["state_path"]).read_text(encoding="utf-8"))
        self.assertEqual(state["run_id"], plan["run"]["run_id"])
        self.assertEqual(
            [step["status"] for step in state["steps"]], ["pending", "pending"]
        )
        self.assertEqual(state["steps"][0]["rules"], ["fastp"])
        self.assertEqual(state["steps"][1]["rules"], ["classify", "summarize"])
        self.assertEqual(state["execution"]["status"], "failed")
        self.assertIs(state["execution"]["ok"], False)
        self.assertEqual(state["execution"]["returncode"], -1)

    async def test_execute_followup_preserves_completed_run_for_report_recovery(self):
        plan = self.ready_plan()
        self.manager._save_analysis_plan(self.chat_id, plan)
        self.manager._write_initial_run_state(plan)
        self.finish_run(plan)
        state_path = Path(plan["run"]["state_path"])
        state = json.loads(state_path.read_text(encoding="utf-8"))
        state["results_dir"] = str(Path(plan["run"]["run_dir"]) / "results")
        state_path.write_text(json.dumps(state), encoding="utf-8")
        await self.manager.set_mode(self.chat_id, EXECUTE_MODE)
        FakeClient.response_text_parts = [
            '读取已有结果。\n```diagnostic_report\n'
            '<article class="diagnostic-report"><h1>已有结果</h1></article>\n```'
        ]

        events = [event async for event in self.manager.stream_turn(MESSAGES, self.chat_id)]
        saved = json.loads(state_path.read_text(encoding="utf-8"))

        self.assertEqual(saved["execution"], state["execution"])
        self.assertEqual(saved["steps"], state["steps"])
        self.assertEqual(saved["results_dir"], state["results_dir"])
        self.assertEqual(saved["report"]["status"], "completed")
        self.assertTrue(any(isinstance(e, OutputItemAdded) for e in events))

    def test_existing_failed_run_is_not_reset_before_retry(self):
        plan = self.ready_plan()
        self.manager._write_initial_run_state(plan)
        self.finish_run(plan, ok=False)
        state_path = Path(plan["run"]["state_path"])
        before = state_path.read_bytes()

        self.manager._write_initial_run_state(plan)

        self.assertEqual(state_path.read_bytes(), before)

    def test_corrupt_existing_run_state_is_not_overwritten(self):
        plan = self.ready_plan()
        self.manager._write_initial_run_state(plan)
        state_path = Path(plan["run"]["state_path"])
        state_path.write_text("not json", encoding="utf-8")

        with self.assertRaisesRegex(SessionStateError, "Cannot read existing"):
            self.manager._write_initial_run_state(plan)

        self.assertEqual(state_path.read_text(encoding="utf-8"), "not json")

    async def test_execute_create_state_error_is_clear(self):
        blocked_root = Path(self.temp_dir.name) / "blocked-run-root"
        blocked_root.write_text("not a directory", encoding="utf-8")
        root = Path(self.temp_dir.name) / "blocked-agent"
        root.mkdir()
        manager = SessionManager(
            root,
            root / ".claude" / "sessions",
            sdk_config_dir=root / "sdk-runtime",
            client_factory=FakeClient,
            idle_timeout_seconds=60 * 60,
            analysis_run_root=blocked_root,
            allow_legacy_execution=True,
            planning_backend="legacy",
        )
        run_id = "analysis-20260709-120000-blocked1"
        run_dir = blocked_root / run_id
        plan = self.ready_plan()
        plan["run"] = {
            "run_id": run_id,
            "run_dir": str(run_dir),
            "state_path": str(run_dir / ".bioagent" / "state.json"),
            "log_path": str(run_dir / ".bioagent" / "execution.log"),
        }
        manager._save_analysis_plan(self.chat_id, plan)
        await manager.set_mode(self.chat_id, EXECUTE_MODE)

        with self.assertRaises(SessionStateError) as error:
            [event async for event in manager.stream_turn(MESSAGES, self.chat_id)]

        self.assertIn("无法创建分析运行目录", str(error.exception))
        self.assertIn("BIOAGENT_RUN_ROOT", str(error.exception))
        self.assertEqual(FakeClient.instances[-1].queries, [])

    async def test_execute_prompt_contains_plan_execution_instruction(self):
        self.manager._save_analysis_plan(self.chat_id, self.ready_plan())
        await self.manager.set_mode(self.chat_id, EXECUTE_MODE)
        [chunk async for chunk in self.manager.stream_turn(MESSAGES, self.chat_id)]

        prompt, _ = FakeClient.instances[0].queries[0]
        self.assertIn(EXECUTE_PROMPT_PREFIX, prompt)
        self.assertIn("遵循之前计划模式下生成的计划执行", prompt)
        self.assertIn("不要输出 analysis_plan", prompt)
        self.assertIn("测序质控与分类方案", prompt)
        self.assertIn("analysis-20260709-120000-testabcd", prompt)
        self.assertIn(".bioagent/state.json", prompt)
        self.assertIn(".bioagent/execution.log", prompt)
        self.assertIn('--directory "<run_dir>/workflow"', prompt)
        self.assertIn('--snakefile "<run_dir>/workflow/Snakefile"', prompt)
        self.assertIn("USER REQUEST:", prompt)

    async def test_successful_execute_writes_styled_diagnostic_report(self):
        plan = self.ready_plan()
        self.manager._save_analysis_plan(self.chat_id, plan)
        await self.manager.set_mode(self.chat_id, EXECUTE_MODE)
        FakeClient.before_result = lambda: self.finish_run(plan)
        FakeClient.response_text_parts = [
            "分析完成。\n``",
            "`diagnostic_",
            """report
<article class="diagnostic-report">
  <header><h1>诊断意见书</h1><p class="review-status">待专家审核</p></header>
  <section><h2>实际分析范围</h2><ul><li>质控</li><li>分类</li></ul></section>
  <section><h2>诊断意见</h2><p><strong>结果</strong>来自本次分析。</p></section>
</article >
```
""",
        ]

        events = [
            event async for event in self.manager.stream_turn(MESSAGES, self.chat_id)
        ]

        visible = "".join(self.texts(events))
        reports = [
            event.item
            for event in events
            if isinstance(event, OutputItemAdded)
            and event.item.get("type") == "diagnostic_report"
        ]
        report_path = Path(plan["run"]["run_dir"]) / "diagnostic-opinion.html"
        document = report_path.read_text(encoding="utf-8")
        state = json.loads(Path(plan["run"]["state_path"]).read_text(encoding="utf-8"))

        self.assertEqual(visible.strip(), "分析完成。")
        self.assertNotIn("diagnostic_report", visible)
        self.assertEqual(len(reports), 1)
        self.assertEqual(reports[0]["run"]["run_id"], plan["run"]["run_id"])
        self.assertEqual(reports[0]["filename"], "diagnostic-opinion.html")
        self.assertIn("<!doctype html>", document)
        self.assertIn('<html lang="zh-CN">', document)
        self.assertIn("@page", document)
        self.assertIn('<article class="diagnostic-report">', document)
        self.assertEqual(state["report"]["status"], "completed")
        self.assertEqual(state["report"]["path"], str(report_path))
        self.assertIn("方法、软件与引用", document)
        self.assertTrue((report_path.parent / "analysis-provenance.json").is_file())
        self.assertTrue((report_path.parent / "references.bib").is_file())

    async def test_successful_execute_rejects_unsafe_report_fragment(self):
        plan = self.ready_plan()
        self.manager._save_analysis_plan(self.chat_id, plan)
        await self.manager.set_mode(self.chat_id, EXECUTE_MODE)
        FakeClient.before_result = lambda: self.finish_run(plan)
        FakeClient.response_text_parts = [
            """分析完成。
```diagnostic_report
<article class="diagnostic-report"><h1>诊断意见书</h1><script>alert(1)</script></article>
```
"""
        ]

        events = [
            event async for event in self.manager.stream_turn(MESSAGES, self.chat_id)
        ]

        state = json.loads(Path(plan["run"]["state_path"]).read_text(encoding="utf-8"))
        report_path = Path(plan["run"]["run_dir"]) / "diagnostic-opinion.html"
        self.assertIn("诊断意见书生成失败", "".join(self.texts(events)))
        self.assertEqual(state["report"]["status"], "failed")
        self.assertFalse(report_path.exists())
        self.assertFalse(
            any(
                isinstance(event, OutputItemAdded)
                and event.item.get("type") == "diagnostic_report"
                for event in events
            )
        )

    async def test_successful_execute_marks_missing_report_failed(self):
        plan = self.ready_plan()
        self.manager._save_analysis_plan(self.chat_id, plan)
        await self.manager.set_mode(self.chat_id, EXECUTE_MODE)
        FakeClient.before_result = lambda: self.finish_run(plan)
        FakeClient.response_text_parts = ["分析完成，但没有报告控制块。"]

        events = [
            event async for event in self.manager.stream_turn(MESSAGES, self.chat_id)
        ]

        state = json.loads(Path(plan["run"]["state_path"]).read_text(encoding="utf-8"))
        self.assertIn("诊断意见书生成失败", "".join(self.texts(events)))
        self.assertEqual(state["report"]["status"], "failed")

    async def test_failed_execute_discards_report_block(self):
        plan = self.ready_plan()
        self.manager._save_analysis_plan(self.chat_id, plan)
        await self.manager.set_mode(self.chat_id, EXECUTE_MODE)
        FakeClient.before_result = lambda: self.finish_run(plan, ok=False)
        FakeClient.response_text_parts = [
            """分析失败。
```diagnostic_report
<article class="diagnostic-report"><h1>不应生成</h1></article>
```
"""
        ]

        events = [
            event async for event in self.manager.stream_turn(MESSAGES, self.chat_id)
        ]

        visible = "".join(self.texts(events))
        report_path = Path(plan["run"]["run_dir"]) / "diagnostic-opinion.html"
        self.assertEqual(visible.strip(), "分析失败。")
        self.assertNotIn("不应生成", visible)
        self.assertFalse(report_path.exists())
        self.assertFalse(
            any(
                isinstance(event, OutputItemAdded)
                and event.item.get("type") == "diagnostic_report"
                for event in events
            )
        )

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
            planning_backend="legacy",
        )
        [chunk async for chunk in recreated.stream_turn(MESSAGES, self.chat_id)]

        resumed_client = FakeClient.instances[-1]
        self.assertEqual(resumed_client.options.resume, "sdk-session-1")
        self.assertIsNone(resumed_client.queries[0][1])
        await recreated.close_all()

    async def test_ephemeral_turn_does_not_create_chat_mapping(self):
        events = [event async for event in self.manager.stream_ephemeral_turn(MESSAGES)]

        self.assertEqual(self.texts(events), ["chunk"])
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

    async def test_sdk_provider_uses_configured_model_effort_and_auth(self):
        with (
            patch.object(config, "MODEL", "qwen3.8-27b"),
            patch.object(config, "MODEL_EFFORT", "xhigh"),
            patch.object(config, "ANTHROPIC_BASE_URL", "http://dgx.example:8000"),
            patch.object(config, "PROVIDER_AUTH_TOKEN", "provider-test-token"),
        ):
            options = self.manager._build_options(
                permission_mode=runtime.SDK_PLAN_PERMISSION_MODE
            )
        self.assertEqual(options.model, "qwen3.8-27b")
        self.assertEqual(options.effort, "xhigh")
        self.assertEqual(options.env["ANTHROPIC_BASE_URL"], "http://dgx.example:8000")
        self.assertEqual(options.env["ANTHROPIC_AUTH_TOKEN"], "provider-test-token")
        self.assertNotIn("MINIMAX_API_KEY", options.env)

    async def test_sdk_options_load_all_skills_and_all_builtin_tools(self):
        await self.manager.ensure_session(self.chat_id)
        options = FakeClient.instances[0].options

        self.assertEqual(options.skills, "all")
        self.assertEqual(options.setting_sources, ["project"])
        self.assertEqual(options.tools, {"type": "preset", "preset": "claude_code"})
        self.assertEqual(options.permission_mode, "bypassPermissions")
        self.assertEqual(
            options.env["CLAUDE_CONFIG_DIR"], str(self.manager.sdk_config_dir)
        )
        self.assertEqual(
            options.allowed_tools,
            ["Read", "Glob", "Grep", "mcp__taxonagent__*"],
        )
        self.assertEqual(options.disallowed_tools, [])
        self.assertEqual(options.system_prompt, config.SYSTEM_PROMPT)
        self.assertEqual(options.env["BIOAGENT_RUN_ROOT"], str(self.run_root))

    async def test_sdk_options_register_taxonagent_mcp_server(self):
        await self.manager.ensure_session(self.chat_id)
        options = FakeClient.instances[0].options

        self.assertEqual(
            options.mcp_servers,
            {
                "taxonagent": {
                    "type": "http",
                    "url": config.TAXONAGENT_MCP_URL,
                }
            },
        )

    def test_system_prompt_contains_analysis_plan_contract(self):
        legacy_contract = "analysis" + "_scheme"
        self.assertNotIn(legacy_contract, config.SYSTEM_PROMPT)
        self.assertIn("analysis_plan", config.SYSTEM_PROMPT)
        self.assertIn("```analysis_plan", config.SYSTEM_PROMPT)
        self.assertIn('"ready_to_execute"', config.SYSTEM_PROMPT)
        self.assertIn('"steps"', config.SYSTEM_PROMPT)
        self.assertIn("Do not create run directories", config.SYSTEM_PROMPT)
        self.assertIn("parameter defaults never block execution", config.SYSTEM_PROMPT)
        self.assertIn("final content in the response", config.SYSTEM_PROMPT)
        self.assertIn(".bioagent/state.json", config.SYSTEM_PROMPT)
        self.assertIn(".bioagent/execution.log", config.SYSTEM_PROMPT)
        self.assertIn("diagnostic_report", config.SYSTEM_PROMPT)
        self.assertIn('<article class="diagnostic-report">', config.SYSTEM_PROMPT)
        self.assertIn("omitting empty sections", config.SYSTEM_PROMPT)
        self.assertIn("ICTV virus taxonomy", config.SYSTEM_PROMPT)
        self.assertIn("ictv_family_info", config.SYSTEM_PROMPT)

    async def test_backend_overrides_model_scope_and_preserves_stage_ids(self):
        r1, r2 = self.manager.project_root / "demo_R1.fq", self.manager.project_root / "demo_R2.fq"
        r1.touch()
        r2.touch()
        FakeClient.response_text_parts = ['自动加跑组装。```analysis_plan\n' + json.dumps({
            "ready_to_execute": True, "steps": [{"label": "组装", "rules": ["megahit"]}]
        }) + '\n```']
        events = [event async for event in self.manager.stream_turn([
            {"role": "user", "content": f"只做质控。{r1} {r2}"}
        ], self.chat_id)]
        plan = [event.item for event in events if isinstance(event, OutputItemAdded)][0]
        self.assertTrue(plan["ready_to_execute"])
        self.assertEqual([step["id"] for step in plan["steps"]], ["read_qc"])
        self.assertEqual(plan["steps"][0]["rules"], ["fastp"])
        self.assertNotIn("自动加跑组装", "".join(self.texts(events)))
        self.assertIn("DETERMINISTIC PLANNING CONTEXT", FakeClient.instances[0].queries[0][0])

    async def test_missing_dependencies_persist_even_without_model_control_block(self):
        r1, r2 = self.manager.project_root / "demo_R1.fq", self.manager.project_root / "demo_R2.fq"
        r1.touch()
        r2.touch()
        FakeClient.response_text_parts = ["立即执行。"]
        events = [event async for event in self.manager.stream_turn([
            {"role": "user", "content": f"W2只提取病毒候选，后面的证据分析先不做。{r1} {r2}"}
        ], self.chat_id)]
        plan = self.manager._load_analysis_plan(self.chat_id)
        self.assertFalse(plan["ready_to_execute"])
        self.assertIn("assembly", plan["planning"]["missing_dependencies"])
        self.assertFalse(self.run_root.exists())
        self.assertIn("contigs", "".join(self.texts(events)))
        self.assertNotIn("立即执行", "".join(self.texts(events)))
        await self.manager.set_mode(self.chat_id, EXECUTE_MODE)
        blocked = [event async for event in self.manager.stream_turn([
            {"role": "user", "content": "确认启动分析"}
        ], self.chat_id)]
        self.assertIn("请先生成", "".join(self.texts(blocked)))
        self.assertFalse(self.run_root.exists())

    async def test_runtime_multiturn_dependency_consent_and_parameter_change(self):
        r1, r2 = self.manager.project_root / "demo_R1.fq", self.manager.project_root / "demo_R2.fq"
        r1.touch()
        r2.touch()
        requests = [f"W2只提取病毒候选。{r1} {r2}", "同意补跑W1", "fastp最短长度改成八十"]
        for request in requests:
            [event async for event in self.manager.stream_turn([{"role": "user", "content": request}], self.chat_id)]
        plan = self.manager._load_analysis_plan(self.chat_id)
        self.assertTrue(plan["ready_to_execute"])
        self.assertEqual(plan["planning"]["goals"], ["virus_candidate_extract"])
        self.assertEqual(plan["parameters"], {"fastp.length_required": 80})
        self.assertEqual(plan["plan_revision"], 3)
        self.assertEqual(plan["scope_patch"], {})

    async def test_ambiguous_new_scope_invalidates_previous_ready_card(self):
        self.manager._save_analysis_plan(self.chat_id, self.ready_plan())
        [event async for event in self.manager.stream_turn([
            {"role": "user", "content": "只做W2"}
        ], self.chat_id)]
        plan = self.manager._load_analysis_plan(self.chat_id)
        self.assertFalse(plan["ready_to_execute"])
        self.assertEqual(plan["planning"]["ambiguous_workflows"], ["workflow2"])

    async def test_edit_in_execute_mode_returns_to_planning_without_starting_run(self):
        r1, r2 = self.manager.project_root / "demo_R1.fq", self.manager.project_root / "demo_R2.fq"
        r1.touch()
        r2.touch()
        [event async for event in self.manager.stream_turn([
            {"role": "user", "content": f"只做质控。{r1} {r2}"}
        ], self.chat_id)]
        previous = self.manager._load_analysis_plan(self.chat_id)
        await self.manager.set_mode(self.chat_id, EXECUTE_MODE)
        [event async for event in self.manager.stream_turn([
            {"role": "user", "content": "fastp最短长度改成80"}
        ], self.chat_id)]
        updated = self.manager._load_analysis_plan(self.chat_id)
        self.assertEqual(self.manager.current_mode(self.chat_id), PLAN_MODE)
        self.assertEqual(updated["planning"]["goals"], previous["planning"]["goals"])
        self.assertEqual(updated["parameters"], {"fastp.length_required": 80})
        self.assertFalse(self.run_root.exists())

    async def test_legacy_plan_cannot_start_in_default_runtime(self):
        self.manager.allow_legacy_execution = False
        self.manager._save_analysis_plan(self.chat_id, self.ready_plan())
        await self.manager.set_mode(self.chat_id, EXECUTE_MODE)
        events = [event async for event in self.manager.stream_turn([
            {"role": "user", "content": "确认启动分析"}
        ], self.chat_id)]
        self.assertIn("旧版方案", "".join(self.texts(events)))
        self.assertFalse(self.run_root.exists())

    def test_gatekeeper_skill_contains_minimal_analysis_contract(self):
        skill_path = (
            Path(__file__).resolve().parents[1]
            / ".claude"
            / "skills"
            / "bioinformatics-workflow-gatekeeper"
            / "SKILL.md"
        )
        skill = skill_path.read_text(encoding="utf-8")

        legacy_contract = "analysis" + "_scheme"
        self.assertNotIn(legacy_contract, skill)
        self.assertIn("analysis_plan", skill)
        self.assertIn("Plan mode must not create run directories", skill)
        self.assertIn("Do not output `analysis_plan` in execute mode", skill)
        self.assertIn("technical choices are not user requirements", skill)
        self.assertIn("Never execute a stage omitted from the latest plan", skill)
        self.assertIn("${RUN_DIR}/.bioagent/state.json", skill)
        self.assertIn("${RUN_DIR}/.bioagent/execution.log", skill)
        self.assertIn('--directory "${RUN_DIR}/workflow"', skill)
        self.assertIn('--snakefile "${RUN_DIR}/workflow/Snakefile"', skill)
        self.assertIn('--configfile "${RUN_DIR}/workflow/config/config.yaml"', skill)
        self.assertNotIn('cd "${RUN_DIR}/workflow"', skill)
        self.assertIn("diagnostic_report", skill)
        self.assertIn('<article class="diagnostic-report">', skill)
        self.assertIn("Quality-control or assembly results alone", skill)


if __name__ == "__main__":
    unittest.main()

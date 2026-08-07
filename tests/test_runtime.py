from __future__ import annotations

import json
import tempfile
import unittest
import uuid
from dataclasses import dataclass
from pathlib import Path

import config
import runtime
from runtime import (
    EXECUTE_MODE,
    PLAN_MODE,
    EXECUTE_PROMPT_PREFIX,
    PLAN_PROMPT_PREFIX,
    AgentTraceRecorder,
    InvalidSessionError,
    MarkdownTextStream,
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
    response_events = None
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
        if self.response_events is not None:
            for event in self.response_events:
                yield FakeStreamEvent(event)
        else:
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
        FakeClient.response_events = None
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
                    "label": "去 rRNA",
                    "tool": "Bowtie2",
                    "rules": ["bowtie2"],
                },
            ],
            "execution_spec": {
                "adapter": "workflow1",
                "entry_rule": "fastp",
                "exit_rule": "bowtie2",
                "inputs": [
                    {
                        "id": "primary",
                        "type": "raw_paired_reads",
                        "data": {
                            "runs": [
                                {
                                    "run_id": "run",
                                    "sample_id": "sample",
                                    "read1": "/data/input_r1.fastq.gz",
                                    "read2": "/data/input_r2.fastq.gz",
                                }
                            ]
                        },
                    }
                ],
            },
        }

    @staticmethod
    def assistant_text_events(*messages: list[str]) -> list[dict]:
        events = []
        for parts in messages:
            events.append(
                {
                    "type": "message_start",
                    "message": {"role": "assistant"},
                }
            )
            for text in parts:
                events.append(
                    {
                        "type": "content_block_delta",
                        "delta": {"type": "text_delta", "text": text},
                    }
                )
            events.append({"type": "message_stop"})
        return events

    @staticmethod
    def normalized_text(events: list[dict]) -> str:
        stream = MarkdownTextStream()
        return "".join(filter(None, (stream.push(event) for event in events)))

    def test_markdown_text_stream_separates_tool_surrounded_messages(self):
        events = [
            {
                "type": "message_start",
                "message": {"role": "assistant"},
            },
            {
                "type": "content_block_delta",
                "delta": {"type": "text_delta", "text": "已确认"},
            },
            {
                "type": "content_block_delta",
                "delta": {"type": "text_delta", "text": "启动分析。"},
            },
            {
                "type": "content_block_start",
                "content_block": {"type": "tool_use", "name": "Bash"},
            },
            {"type": "message_stop"},
            {
                "type": "message_start",
                "message": {"role": "assistant"},
            },
            {
                "type": "content_block_delta",
                "delta": {"type": "thinking_delta", "thinking": "总结结果"},
            },
            {
                "type": "content_block_delta",
                "delta": {"type": "text_delta", "text": "## 执行摘要"},
            },
        ]

        self.assertEqual(
            self.normalized_text(events),
            "已确认启动分析。\n\n## 执行摘要",
        )

    def test_markdown_text_stream_preserves_existing_boundary_newlines(self):
        cases = [
            ("第一段。", "第二段。", "第一段。\n\n第二段。"),
            ("第一段。\n", "\n第二段。", "第一段。\n\n第二段。"),
            ("第一段。\n\n", "第二段。", "第一段。\n\n第二段。"),
            ("第一段。", "\n\n第二段。", "第一段。\n\n第二段。"),
        ]

        for first, second, expected in cases:
            with self.subTest(first=first, second=second):
                events = self.assistant_text_events([first], [second])
                self.assertEqual(self.normalized_text(events), expected)

    def test_markdown_text_stream_handles_tool_only_and_multiple_messages(self):
        events = self.assistant_text_events(
            [],
            ["第一段。"],
            [],
            ["第二段。"],
            ["第三", "段。"],
        )

        self.assertEqual(
            self.normalized_text(events),
            "第一段。\n\n第二段。\n\n第三段。",
        )

    async def test_persistent_and_ephemeral_streams_share_markdown_boundaries(self):
        FakeClient.response_events = self.assistant_text_events(
            ["开始检查。"],
            ["## 检查完成"],
        )

        persistent = [
            event async for event in self.manager.stream_turn(MESSAGES, self.chat_id)
        ]
        ephemeral = [
            event async for event in self.manager.stream_ephemeral_turn(MESSAGES)
        ]

        self.assertEqual(
            "".join(self.texts(persistent)),
            "开始检查。\n\n## 检查完成",
        )
        self.assertEqual(
            "".join(self.texts(ephemeral)),
            "开始检查。\n\n## 检查完成",
        )

    def test_agent_trace_records_tool_calls_and_redacts_secrets(self):
        path = Path(self.temp_dir.name) / "agent-trace.jsonl"
        recorder = AgentTraceRecorder(path)
        recorder.consume(
            {
                "type": "content_block_start",
                "index": 1,
                "content_block": {"type": "tool_use", "id": "tool-1", "name": "Bash"},
            }
        )
        recorder.consume(
            {
                "type": "content_block_delta",
                "index": 1,
                "delta": {
                    "type": "input_json_delta",
                    "partial_json": json.dumps(
                        {
                            "command": 'WEBUI_SECRET_KEY=hidden curl -H "Authorization: Bearer abc"',
                            "api_key": "also-hidden",
                        }
                    ),
                },
            }
        )
        recorder.consume({"type": "content_block_stop", "index": 1})

        event = json.loads(path.read_text(encoding="utf-8"))
        serialized = json.dumps(event)
        self.assertEqual(event["tool"], "Bash")
        self.assertNotIn("hidden", serialized)
        self.assertNotIn("abc", serialized)
        self.assertGreaterEqual(serialized.count("[REDACTED]"), 3)

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
        state["report"] = {
            "status": "ready" if ok else "unavailable",
            "available": ok,
            "schema_version": 1,
            "telemetry": str(state_path.parent / "telemetry.json"),
        }
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

        self.assertTrue(projects_link.is_symlink())
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
    {"label": "去 rRNA", "tool": "Bowtie2", "rules": ["bowtie2"]}
  ],
  "execution_spec": {
    "adapter": "workflow1",
    "entry_rule": "fastp",
    "exit_rule": "bowtie2",
    "inputs": [{
      "id": "primary",
      "type": "raw_paired_reads",
      "data": {"runs": [{
        "run_id": "run",
        "sample_id": "sample",
        "read1": "/data/input_r1.fastq.gz",
        "read2": "/data/input_r2.fastq.gz"
      }]}
    }]
  },
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
        self.assertTrue(item["ready_to_execute"])
        self.assertEqual(
            [step["label"] for step in item["steps"]],
            ["质控", "去 rRNA"],
        )
        self.assertEqual(item["steps"][0]["rules"], ["fastp"])
        self.assertEqual(item["steps"][1]["rules"], ["bowtie2"])
        self.assertEqual(item["execution_spec"]["exit_rule"], "bowtie2")
        self.assertEqual(item["resource_estimate"]["version"], 1)
        self.assertEqual(item["resource_estimate"]["confidence"], "low")
        self.assertNotIn("eta", item["resource_estimate"])
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

    async def test_message_boundary_before_plan_block_stays_hidden(self):
        FakeClient.response_events = self.assistant_text_events(
            ["方案如下。"],
            [
                "```analysis",
                """_plan
{
  "version": 1,
  "title": "跨回合控制块",
  "ready_to_execute": false,
  "steps": [{"label": "等待输入"}]
}
```
""",
            ],
        )

        events = [
            event async for event in self.manager.stream_turn(MESSAGES, self.chat_id)
        ]
        text = "".join(self.texts(events))
        output_items = [
            event.item for event in events if isinstance(event, OutputItemAdded)
        ]

        self.assertEqual(text.strip(), "方案如下。")
        self.assertNotIn("analysis_plan", text)
        self.assertEqual(len(output_items), 1)
        self.assertEqual(output_items[0]["title"], "跨回合控制块")

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

    async def test_execute_rejects_legacy_plan_without_execution_spec(self):
        plan = self.ready_plan()
        plan.pop("execution_spec")
        self.manager._save_analysis_plan(self.chat_id, plan)
        await self.manager.set_mode(self.chat_id, EXECUTE_MODE)

        events = [
            event async for event in self.manager.stream_turn(MESSAGES, self.chat_id)
        ]

        self.assertEqual(
            self.texts(events),
            ["当前方案缺少有效执行区段，请在 plan 模式重新生成方案。"],
        )
        self.assertFalse(Path(plan["run"]["state_path"]).exists())
        self.assertEqual(FakeClient.instances[0].queries, [])

    def test_invalid_typed_input_makes_generated_plan_not_executable(self):
        raw_plan = {
            "title": "错误输入",
            "ready_to_execute": True,
            "steps": [{"label": "样本合并", "rules": ["merge"]}],
            "execution_spec": {
                "adapter": "workflow1",
                "entry_rule": "merge",
                "exit_rule": "merge",
                "inputs": [
                    {
                        "id": "primary",
                        "type": "raw_paired_reads",
                        "data": {"runs": []},
                    }
                ],
            },
        }

        plan = self.manager._build_analysis_plan(raw_plan)

        self.assertIsNotNone(plan)
        self.assertFalse(plan["ready_to_execute"])
        self.assertNotIn("execution_spec", plan)
        self.assertIn("输入文件信息", plan["blocking_reason"])

    def test_missing_workflow1_boundaries_are_inferred_from_latest_plan_shape(self):
        raw_plan = {
            "title": "病毒宏基因组上游流程",
            "ready_to_execute": True,
            "steps": [
                {"label": "质控", "rules": ["fastp"]},
                {"label": "去 rRNA", "rules": ["bowtie2"]},
                {"label": "样本合并", "rules": ["merge"]},
                {"label": "组装", "rules": ["megahit"]},
                {"label": "重命名过滤", "rules": ["rename_filter"]},
            ],
            "execution_spec": {
                "adapter": "workflow1",
                "viral_type": "RNA+DNA",
                "inputs": self.ready_plan()["execution_spec"]["inputs"],
            },
        }

        plan = self.manager._build_analysis_plan(raw_plan)

        self.assertIsNotNone(plan)
        self.assertTrue(plan["ready_to_execute"])
        self.assertEqual(plan["execution_spec"]["entry_rule"], "fastp")
        self.assertEqual(plan["execution_spec"]["exit_rule"], "rename_filter")
        self.assertNotIn("viral_type", plan["execution_spec"])
        self.assertNotIn("blocking_reason", plan)

    def test_invalid_workflow1_rules_expose_blocking_reason(self):
        raw_plan = {
            "title": "不连续流程",
            "ready_to_execute": True,
            "steps": [
                {"label": "质控", "rules": ["fastp"]},
                {"label": "组装", "rules": ["megahit"]},
            ],
            "execution_spec": {
                "adapter": "workflow1",
                "inputs": self.ready_plan()["execution_spec"]["inputs"],
            },
        }

        plan = self.manager._build_analysis_plan(raw_plan)

        self.assertIsNotNone(plan)
        self.assertFalse(plan["ready_to_execute"])
        self.assertIn("流程区段", plan["blocking_reason"])

    def test_resource_estimation_failure_does_not_block_valid_plan(self):
        class BrokenCalibrator:
            def estimate(self, _spec):
                raise RuntimeError("sqlite unavailable")

        self.manager.resource_calibrator = BrokenCalibrator()
        source = self.ready_plan()
        raw_plan = {
            "title": source["title"],
            "ready_to_execute": True,
            "steps": source["steps"],
            "execution_spec": source["execution_spec"],
        }

        plan = self.manager._build_analysis_plan(raw_plan)

        self.assertIsNotNone(plan)
        self.assertTrue(plan["ready_to_execute"])
        self.assertIn("execution_spec", plan)
        self.assertNotIn("resource_estimate", plan)

    async def test_execute_creates_state_and_marks_missing_terminal_state_failed(self):
        plan = self.ready_plan()
        self.manager._save_analysis_plan(self.chat_id, plan)
        await self.manager.set_mode(self.chat_id, EXECUTE_MODE)

        [event async for event in self.manager.stream_turn(MESSAGES, self.chat_id)]

        state = json.loads(Path(plan["run"]["state_path"]).read_text(encoding="utf-8"))
        self.assertEqual(state["run_id"], plan["run"]["run_id"])
        self.assertEqual(state["version"], 3)
        self.assertEqual(state["openwebui_chat_id"], self.chat_id)
        self.assertEqual(state["plan"]["title"], plan["title"])
        self.assertEqual(
            [step["status"] for step in state["steps"]], ["pending", "pending"]
        )
        self.assertEqual(state["steps"][0]["rules"], ["fastp"])
        self.assertEqual(state["steps"][1]["rules"], ["bowtie2"])
        self.assertEqual(state["execution_spec"], plan["execution_spec"])
        self.assertEqual(state["execution"]["status"], "failed")
        self.assertIs(state["execution"]["ok"], False)
        self.assertEqual(state["execution"]["returncode"], -1)
        self.assertTrue(state["technical_report"]["available"])
        self.assertEqual(state["technical_report"]["status"], "ready")

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
        self.assertIn("RUNTIME RUNNER COMMAND:", prompt)
        self.assertIn(
            f'--state "{self.ready_plan()["run"]["state_path"]}"',
            prompt,
        )
        self.assertIn("analysis_runner.py", prompt)
        self.assertIn("不得直接调用 Snakemake", prompt)
        self.assertIn("USER REQUEST:", prompt)

    async def test_successful_execute_leaves_report_available_via_state(self):
        plan = self.ready_plan()
        self.manager._save_analysis_plan(self.chat_id, plan)
        await self.manager.set_mode(self.chat_id, EXECUTE_MODE)
        FakeClient.before_result = lambda: self.finish_run(plan)
        FakeClient.response_text_parts = ["分析完成，没有 HTML 控制块。"]

        events = [
            event async for event in self.manager.stream_turn(MESSAGES, self.chat_id)
        ]

        visible = "".join(self.texts(events))
        report_path = Path(plan["run"]["run_dir"]) / "diagnostic-opinion.html"
        state = json.loads(Path(plan["run"]["state_path"]).read_text(encoding="utf-8"))

        self.assertEqual(visible.strip(), "分析完成，没有 HTML 控制块。")
        self.assertFalse(
            any(
                isinstance(event, OutputItemAdded)
                and event.item.get("type") == "diagnostic_report"
                for event in events
            )
        )
        self.assertFalse(report_path.exists())
        self.assertEqual(state["report"]["status"], "ready")
        self.assertTrue(state["report"]["available"])

    async def test_successful_execute_does_not_surface_report_state_in_chat(self):
        plan = self.ready_plan()
        self.manager._save_analysis_plan(self.chat_id, plan)
        await self.manager.set_mode(self.chat_id, EXECUTE_MODE)
        FakeClient.before_result = lambda: self.finish_run(plan, ok=True)
        FakeClient.response_text_parts = ["分析完成。"]

        def finish_without_report():
            self.finish_run(plan, ok=True)
            state_path = Path(plan["run"]["state_path"])
            state = json.loads(state_path.read_text(encoding="utf-8"))
            state["report"]["available"] = False
            state_path.write_text(json.dumps(state), encoding="utf-8")

        FakeClient.before_result = finish_without_report

        events = [
            event async for event in self.manager.stream_turn(MESSAGES, self.chat_id)
        ]

        self.assertEqual("".join(self.texts(events)).strip(), "分析完成。")
        self.assertFalse(
            any(
                isinstance(event, OutputItemAdded)
                and event.item.get("type") == "diagnostic_report"
                for event in events
            )
        )

    async def test_failed_execute_does_not_emit_report(self):
        plan = self.ready_plan()
        self.manager._save_analysis_plan(self.chat_id, plan)
        await self.manager.set_mode(self.chat_id, EXECUTE_MODE)
        FakeClient.before_result = lambda: self.finish_run(plan, ok=False)
        FakeClient.response_text_parts = ["分析失败。"]

        events = [
            event async for event in self.manager.stream_turn(MESSAGES, self.chat_id)
        ]

        visible = "".join(self.texts(events))
        report_path = Path(plan["run"]["run_dir"]) / "diagnostic-opinion.html"
        self.assertEqual(visible.strip(), "分析失败。")
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
        self.assertIn('"execution_spec"', config.SYSTEM_PROMPT)
        self.assertIn('"viral_type"', config.SYSTEM_PROMPT)
        self.assertIn("workflow1 entry_rule and exit_rule", config.SYSTEM_PROMPT)
        self.assertIn("Do not create run directories", config.SYSTEM_PROMPT)
        self.assertIn("parameter defaults never block execution", config.SYSTEM_PROMPT)
        self.assertIn("final content in the response", config.SYSTEM_PROMPT)
        self.assertIn(".bioagent/state.json", config.SYSTEM_PROMPT)
        self.assertIn(".bioagent/execution.log", config.SYSTEM_PROMPT)
        self.assertNotIn("```diagnostic_report", config.SYSTEM_PROMPT)
        self.assertIn(
            "deterministically renders the diagnostic report", config.SYSTEM_PROMPT
        )
        self.assertIn("ICTV virus taxonomy", config.SYSTEM_PROMPT)
        self.assertIn("ictv_family_info", config.SYSTEM_PROMPT)

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
        self.assertIn("execution_spec", skill)
        self.assertIn("one contiguous segment", skill)
        self.assertIn("clean_paired_reads", skill)
        self.assertIn("${RUN_DIR}/.bioagent/state.json", skill)
        self.assertIn("${RUN_DIR}/.bioagent/execution.log", skill)
        self.assertIn("RUNTIME RUNNER COMMAND", skill)
        self.assertIn("analysis_runner.py", skill)
        self.assertIn("Do not edit it manually", skill)
        self.assertNotIn("rsync -a", skill)
        self.assertNotIn("```diagnostic_report", skill)
        self.assertIn("generates the diagnostic report deterministically", skill)
        self.assertIn("Quality-control or assembly results alone", skill)


if __name__ == "__main__":
    unittest.main()

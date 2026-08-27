from __future__ import annotations

import asyncio
import json
import tempfile
import time
import unittest
import uuid
from dataclasses import dataclass
from pathlib import Path

import config
import runtime
from pipeline_spec import normalize_execution_spec, selected_rules
from runtime import (
    EXECUTE_MODE,
    PLAN_MODE,
    PLAN_PROMPT_PREFIX,
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
        self.input_dir = self.run_root / "inputs"
        self.input_dir.mkdir(parents=True)
        self.read1 = self.input_dir / "sample_R1.fastq.gz"
        self.read2 = self.input_dir / "sample_R2.fastq.gz"
        self.contigs = self.input_dir / "contigs.fasta"
        self.read1.write_bytes(b"read-one")
        self.read2.write_bytes(b"read-two")
        self.contigs.write_text(">contig\nACGT\n", encoding="utf-8")
        self.launches = []

        def launch(state_path, log_path):
            self.launches.append((state_path, log_path))
            state = json.loads(state_path.read_text(encoding="utf-8"))
            state["execution"]["pid"] = 4321
            state_path.write_text(json.dumps(state), encoding="utf-8")
            return 4321

        self.manager = SessionManager(
            root,
            root / ".claude" / "sessions",
            sdk_config_dir=root / "sdk-runtime",
            client_factory=FakeClient,
            idle_timeout_seconds=60 * 60,
            analysis_run_root=self.run_root,
            runner_launcher=launch,
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
                {
                    "id": "read_qc",
                    "label": "序列质控",
                    "tool": "fastp",
                    "rules": ["fastp"],
                },
                {
                    "id": "rrna_filter",
                    "label": "去除 rRNA",
                    "tool": "Bowtie2",
                    "rules": ["bowtie2"],
                },
            ],
            "execution_spec": {
                "version": 2,
                "adapter": "analysis_pipeline",
                "inputs": [
                    {
                        "id": "primary",
                        "type": "raw_paired_reads",
                        "records": [
                            {
                                "run_id": "run",
                                "sample_id": "sample",
                                "files": {
                                    "read1": str(self.read1),
                                    "read2": str(self.read2),
                                },
                            }
                        ],
                    }
                ],
                "deliverables": ["rrna_filter"],
                "options": {"viral_type": "RNA+DNA"},
                "policy": {"mode": "standard"},
            },
            "dataset": self.dataset_context(),
        }

    def dataset_context(self, *, status="ready", artifact_type="raw_paired_reads"):
        def source(path):
            stat = path.stat()
            return {
                "path": str(path),
                "fingerprint": {
                    "device": stat.st_dev,
                    "inode": stat.st_ino,
                    "size": stat.st_size,
                    "mtime_ns": stat.st_mtime_ns,
                },
            }

        paired = artifact_type == "raw_paired_reads"
        record = {
            "record_id": "rec-1",
            "sample_id": "sample",
            "files": (
                {"read1": source(self.read1), "read2": source(self.read2)}
                if paired
                else {"fasta": source(self.contigs)}
            ),
        }
        if paired:
            record["run_id"] = "run"
        artifacts = [{"id": "primary", "type": artifact_type, "records": [record]}]
        return {
            "id": "dataset-test",
            "revision": 1,
            "status": status,
            "check_version": "fastq-v1",
            "summary": {
                "id": "dataset-test",
                "revision": 1,
                "status": status,
                "artifacts": [
                    {
                        "id": "primary",
                        "type": artifact_type,
                        "records": [
                            {
                                **({"run_id": "run"} if paired else {}),
                                "sample_id": "sample",
                            }
                        ],
                    }
                ],
            },
            "artifacts": artifacts,
            "checks": {"quick": "passed", "full": "passed"},
            "issues": [],
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
  "version": 2,
  "title": "测序质控与分类方案",
  "ready_to_execute": true,
  "steps": [
    {"label": "质控", "tool": "fastp", "rules": ["fastp"]},
    {"label": "去 rRNA", "tool": "Bowtie2", "rules": ["bowtie2"]}
  ],
  "execution_spec": {
    "version": 2,
    "adapter": "analysis_pipeline",
    "inputs": [{
      "id": "primary",
      "type": "raw_paired_reads",
      "records": [{
        "run_id": "run",
        "sample_id": "sample",
        "files": {
          "read1": "/data/input_r1.fastq.gz",
          "read2": "/data/input_r2.fastq.gz"
        }
      }]
    }],
    "deliverables": ["rrna_filter"],
    "options": {"viral_type": "RNA+DNA"},
    "policy": {"mode": "standard"}
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
            event
            async for event in self.manager.stream_turn(
                MESSAGES, self.chat_id, dataset_context=self.dataset_context()
            )
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
            ["序列质控", "去除 rRNA"],
        )
        self.assertEqual(item["steps"][0]["rules"], ["fastp"])
        self.assertEqual(item["steps"][1]["rules"], ["bowtie2"])
        self.assertEqual(item["execution_spec"]["adapter"], "analysis_pipeline")
        self.assertEqual(item["execution_spec"]["deliverables"], ["rrna_filter"])
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
            event
            async for event in self.manager.stream_turn(
                MESSAGES, self.chat_id, dataset_context=self.dataset_context()
            )
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
{"title":"完整流程","ready_to_execute":true,"execution_spec":{"version":2,"adapter":"analysis_pipeline","inputs":[{"id":"primary","type":"raw_paired_reads","records":[{"run_id":"run","sample_id":"sample","files":{"read1":"/data/r1.fq.gz","read2":"/data/r2.fq.gz"}}]}],"deliverables":["assembly"],"policy":{"mode":"standard"}}}
```
"""
        ]
        [
            event
            async for event in self.manager.stream_turn(
                MESSAGES, self.chat_id, dataset_context=self.dataset_context()
            )
        ]

        FakeClient.response_text_parts = [
            """已按要求移除组装。
```analysis_plan
{"title":"质控流程","ready_to_execute":true,"execution_spec":{"version":2,"adapter":"analysis_pipeline","inputs":[{"id":"primary","type":"raw_paired_reads","records":[{"run_id":"run","sample_id":"sample","files":{"read1":"/data/r1.fq.gz","read2":"/data/r2.fq.gz"}}]}],"deliverables":["read_qc"],"policy":{"mode":"standard"}}}
```
"""
        ]
        events = [
            event
            async for event in self.manager.stream_turn(
                MESSAGES, self.chat_id, dataset_context=self.dataset_context()
            )
        ]
        saved = json.loads(
            self.manager._analysis_plan_path(self.chat_id).read_text(encoding="utf-8")
        )
        output_items = [
            event.item for event in events if isinstance(event, OutputItemAdded)
        ]

        self.assertEqual([step["label"] for step in saved["steps"]], ["序列质控"])
        self.assertEqual(output_items[0]["run"]["run_id"], saved["run"]["run_id"])

    async def test_execute_without_ready_plan_does_not_query_client(self):
        await self.manager.set_mode(self.chat_id, EXECUTE_MODE)

        events = [
            event
            async for event in self.manager.stream_turn(
                MESSAGES, self.chat_id, dataset_context=self.dataset_context()
            )
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
            event
            async for event in self.manager.stream_turn(
                MESSAGES, self.chat_id, dataset_context=plan["dataset"]
            )
        ]

        self.assertEqual(
            self.texts(events),
            ["当前方案缺少有效执行区段，请在方案模式重新生成方案。"],
        )
        self.assertFalse(Path(plan["run"]["state_path"]).exists())
        self.assertEqual(FakeClient.instances[0].queries, [])

    def test_model_records_and_paths_are_replaced_by_dataset(self):
        raw_plan = {
            "title": "错误输入",
            "ready_to_execute": True,
            "steps": [{"label": "样本合并", "rules": ["merge"]}],
            "execution_spec": {
                "version": 2,
                "adapter": "analysis_pipeline",
                "inputs": [
                    {
                        "id": "primary",
                        "type": "raw_paired_reads",
                        "records": [],
                    }
                ],
                "deliverables": ["sample_merge"],
                "policy": {"mode": "standard"},
            },
        }

        plan = self.manager._build_analysis_plan(
            raw_plan, dataset_context=self.dataset_context()
        )

        self.assertIsNotNone(plan)
        self.assertTrue(plan["ready_to_execute"])
        self.assertEqual(
            plan["execution_spec"]["inputs"],
            self.ready_plan()["execution_spec"]["inputs"],
        )

    def test_runtime_compiles_canonical_stages_without_model_steps(self):
        raw_plan = {
            "title": "病毒宏基因组上游流程",
            "ready_to_execute": True,
            "execution_spec": {
                "version": 2,
                "adapter": "analysis_pipeline",
                "inputs": self.ready_plan()["execution_spec"]["inputs"],
                "deliverables": ["contig_filter"],
                "options": {"viral_type": "RNA+DNA"},
                "policy": {"mode": "standard"},
            },
        }

        plan = self.manager._build_analysis_plan(
            raw_plan, dataset_context=self.dataset_context()
        )

        self.assertIsNotNone(plan)
        self.assertTrue(plan["ready_to_execute"])
        self.assertEqual(
            [step["id"] for step in plan["steps"]],
            ["read_qc", "rrna_filter", "sample_merge", "assembly", "contig_filter"],
        )
        self.assertEqual(plan["execution_spec"]["adapter"], "analysis_pipeline")
        self.assertNotIn("blocking_reason", plan)

    def test_unknown_dataset_artifact_reference_is_rejected(self):
        raw_plan = {
            "ready_to_execute": True,
            "execution_spec": {
                "version": 2,
                "adapter": "analysis_pipeline",
                "inputs": [{"id": "invented", "type": "raw_paired_reads"}],
                "deliverables": ["read_qc"],
                "policy": {"mode": "standard"},
            },
        }

        plan = self.manager._build_analysis_plan(
            raw_plan, dataset_context=self.dataset_context()
        )

        self.assertFalse(plan["ready_to_execute"])
        self.assertNotIn("execution_spec", plan)

    def test_checking_dataset_creates_provisional_plan(self):
        raw_plan = {
            "ready_to_execute": True,
            "execution_spec": {
                "version": 2,
                "adapter": "analysis_pipeline",
                "inputs": [{"id": "primary", "type": "raw_paired_reads"}],
                "deliverables": ["read_qc"],
                "policy": {"mode": "standard"},
            },
        }

        plan = self.manager._build_analysis_plan(
            raw_plan, dataset_context=self.dataset_context(status="checking")
        )

        self.assertFalse(plan["ready_to_execute"])
        self.assertIn("全量预检", plan["blocking_reason"])

    def test_dataset_prompt_exposes_summary_without_paths(self):
        dataset = self.dataset_context()

        prompt = self.manager._prefixed_prompt(
            PLAN_MODE, "分析数据", dataset_context=dataset
        )

        self.assertIn("dataset-test", prompt)
        self.assertNotIn(str(self.read1), prompt)
        self.assertNotIn(str(self.read2), prompt)

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

        plan = self.manager._build_analysis_plan(
            raw_plan, dataset_context=self.dataset_context()
        )

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

        plan = self.manager._build_analysis_plan(
            raw_plan, dataset_context=self.dataset_context()
        )

        self.assertIsNotNone(plan)
        self.assertTrue(plan["ready_to_execute"])
        self.assertIn("execution_spec", plan)
        self.assertNotIn("resource_estimate", plan)

    def test_ui_routing_overrides_model_and_deep_plan_is_unready(self):
        execution_spec = {
            **self.ready_plan()["execution_spec"],
            "deliverables": ["virus_summary"],
        }
        raw_plan = {
            "title": "自适应病毒分析",
            "ready_to_execute": True,
            "execution_spec": execution_spec,
        }

        emergency = self.manager._build_analysis_plan(
            raw_plan,
            routing_request={"mode": "emergency", "deadline_seconds": 18_000},
            dataset_context=self.dataset_context(),
        )
        deep = self.manager._build_analysis_plan(
            raw_plan,
            routing_request={"mode": "deep"},
            dataset_context=self.dataset_context(),
        )

        self.assertEqual(emergency["routing"]["requested_mode"], "emergency")
        self.assertEqual(emergency["routing"]["effective_mode"], "emergency")
        self.assertEqual(emergency["routing"]["deadline_seconds"], 18_000)
        self.assertEqual(emergency["routing"]["sample_fraction"], 0.05)
        self.assertEqual(emergency["routing"]["available_routes"], ["A", "B"])
        self.assertTrue(emergency["ready_to_execute"])
        self.assertFalse(deep["ready_to_execute"])
        self.assertIn("暂未开放", deep["blocking_reason"])

    def test_ui_routing_upgrades_raw_classification_to_adaptive(self):
        execution_spec = {
            **self.ready_plan()["execution_spec"],
            "deliverables": ["virus_summary"],
        }
        raw_plan = {
            "title": "病毒分类分析",
            "ready_to_execute": True,
            "execution_spec": execution_spec,
        }

        for routing in (
            {"mode": "auto"},
            {"mode": "emergency", "deadline_seconds": 900},
            {"mode": "standard"},
        ):
            with self.subTest(routing=routing):
                plan = self.manager._build_analysis_plan(
                    raw_plan,
                    routing_request=routing,
                    dataset_context=self.dataset_context(),
                )

                self.assertTrue(plan["ready_to_execute"])
                self.assertEqual(
                    plan["execution_spec"]["adapter"],
                    "analysis_pipeline",
                )
                self.assertEqual(plan["routing"]["requested_mode"], routing["mode"])
                if routing["mode"] == "standard":
                    self.assertEqual(plan["steps"][0]["id"], "read_qc")
                else:
                    self.assertEqual(
                        [step["id"] for step in plan["steps"][:2]],
                        ["adaptive_screen", "adaptive_route"],
                    )
                self.assertEqual(
                    tuple(
                        rule for step in plan["steps"] for rule in step.get("rules", [])
                    ),
                    selected_rules(plan["execution_spec"]),
                )

    def test_ui_routing_applies_to_reads_and_falls_back_for_contigs(self):
        workflow1 = self.ready_plan()
        workflow1_plan = self.manager._build_analysis_plan(
            {
                "title": workflow1["title"],
                "ready_to_execute": True,
                "steps": workflow1["steps"],
                "execution_spec": workflow1["execution_spec"],
            },
            routing_request={"mode": "auto"},
            dataset_context=self.dataset_context(),
        )
        contigs_spec = {
            "version": 2,
            "adapter": "analysis_pipeline",
            "inputs": [
                {
                    "id": "primary",
                    "type": "contigs_fasta",
                    "records": [
                        {
                            "sample_id": "sample",
                            "files": {"fasta": "/data/input_contigs.fasta"},
                        }
                    ],
                }
            ],
            "deliverables": ["virus_summary"],
            "options": {"viral_type": "RNA"},
            "policy": {"mode": "standard"},
        }
        contigs_plan = self.manager._build_analysis_plan(
            {
                "title": "Contigs 病毒分类",
                "ready_to_execute": True,
                "execution_spec": contigs_spec,
            },
            routing_request={"mode": "auto"},
            dataset_context=self.dataset_context(artifact_type="contigs_fasta"),
        )

        self.assertEqual(
            workflow1_plan["execution_spec"]["adapter"], "analysis_pipeline"
        )
        self.assertTrue(workflow1_plan["routing"]["adaptive"])
        self.assertEqual(
            contigs_plan["execution_spec"]["adapter"],
            "analysis_pipeline",
        )
        self.assertFalse(contigs_plan["routing"]["adaptive"])
        self.assertEqual(contigs_plan["routing"]["effective_mode"], "standard")

    async def test_execute_starts_runner_and_returns_structured_run(self):
        plan = self.ready_plan()
        self.manager._save_analysis_plan(self.chat_id, plan)

        events = [
            event
            async for event in self.manager.start_execution(
                self.chat_id, "assistant-message-1", plan["dataset"]
            )
        ]

        state = json.loads(Path(plan["run"]["state_path"]).read_text(encoding="utf-8"))
        self.assertEqual(state["run_id"], plan["run"]["run_id"])
        self.assertEqual(state["version"], 3)
        self.assertEqual(state["openwebui_chat_id"], self.chat_id)
        self.assertEqual(state["assistant_message_id"], "assistant-message-1")
        self.assertEqual(state["plan"]["title"], plan["title"])
        self.assertEqual(state["dataset"], plan["dataset"])
        self.assertEqual(
            [step["status"] for step in state["steps"]], ["pending", "pending"]
        )
        self.assertEqual(state["steps"][0]["rules"], ["fastp"])
        self.assertEqual(state["steps"][1]["rules"], ["bowtie2"])
        self.assertEqual(
            state["execution_spec"], normalize_execution_spec(plan["execution_spec"])
        )
        self.assertEqual(state["execution"]["status"], "running")
        self.assertEqual(state["execution"]["pid"], 4321)
        self.assertEqual(state["report"]["schema_version"], 4)
        self.assertTrue(Path(state["technical_report"]["agent_trace"]).is_file())
        self.assertEqual(len(self.launches), 1)
        self.assertEqual(FakeClient.instances, [])
        self.assertIn("分析任务已启动", "".join(self.texts(events)))
        run_item = next(
            event.item for event in events if isinstance(event, OutputItemAdded)
        )
        self.assertEqual(run_item["type"], "analysis_run")
        self.assertEqual(run_item["status"], "running")
        self.assertEqual(run_item["run"]["state_path"], plan["run"]["state_path"])

    async def test_execute_rejects_changed_dataset_fingerprint(self):
        plan = self.ready_plan()
        self.manager._save_analysis_plan(self.chat_id, plan)
        self.read1.write_bytes(b"changed")

        events = [
            event
            async for event in self.manager.start_execution(
                self.chat_id, "assistant-message-1", plan["dataset"]
            )
        ]

        self.assertIn("预检后发生变化", "".join(self.texts(events)))
        self.assertFalse(Path(plan["run"]["state_path"]).exists())

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
            runner_launcher=lambda *_: 1,
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
        events = [
            event
            async for event in manager.start_execution(
                self.chat_id, "message", plan["dataset"]
            )
        ]

        self.assertIn("不在 BIOAGENT_RUN_ROOT", "".join(self.texts(events)))
        self.assertEqual(FakeClient.instances, [])

    async def test_execute_rejects_mismatched_state_without_overwriting_it(self):
        plan = self.ready_plan()
        state_path = Path(plan["run"]["state_path"])
        state_path.parent.mkdir(parents=True)
        state_path.write_text('{"sentinel": true}\n', encoding="utf-8")
        self.manager._save_analysis_plan(self.chat_id, plan)
        with self.assertRaises(SessionStateError) as error:
            [
                event
                async for event in self.manager.start_execution(
                    self.chat_id, "message", plan["dataset"]
                )
            ]

        self.assertIn("不匹配", str(error.exception))
        self.assertEqual(
            json.loads(state_path.read_text(encoding="utf-8")),
            {"sentinel": True},
        )
        self.assertEqual(FakeClient.instances, [])

    async def test_execute_retry_returns_existing_status_without_relaunch(self):
        plan = self.ready_plan()
        self.manager._save_analysis_plan(self.chat_id, plan)
        [
            event
            async for event in self.manager.start_execution(
                self.chat_id, "first", plan["dataset"]
            )
        ]
        events = [
            event
            async for event in self.manager.start_execution(
                self.chat_id, "second", plan["dataset"]
            )
        ]

        self.assertEqual(len(self.launches), 1)
        self.assertIn("已经启动", "".join(self.texts(events)))

    async def test_execute_launch_error_is_terminal_and_visible(self):
        plan = self.ready_plan()
        self.manager._save_analysis_plan(self.chat_id, plan)

        def fail_launch(*_):
            raise OSError("fork failed")

        self.manager.runner_launcher = fail_launch
        events = [
            event
            async for event in self.manager.start_execution(
                self.chat_id, "message", plan["dataset"]
            )
        ]

        state = json.loads(Path(plan["run"]["state_path"]).read_text(encoding="utf-8"))
        self.assertEqual(state["execution"]["status"], "failed")
        self.assertFalse(state["execution"]["ok"])
        self.assertIn("fork failed", state["execution"]["error"])
        self.assertIn("启动失败", "".join(self.texts(events)))
        item = next(
            event.item for event in events if isinstance(event, OutputItemAdded)
        )
        self.assertEqual(item["status"], "failed")

    async def test_short_lived_runner_completes_after_immediate_response(self):
        root = Path(self.temp_dir.name) / "subprocess-agent"
        root.mkdir()
        (root / "analysis_runner.py").write_text(
            """import json, os, sys
from pathlib import Path
path = Path(sys.argv[sys.argv.index('--state') + 1])
state = json.loads(path.read_text())
state['execution'].update({'pid': os.getpid(), 'status': 'completed', 'ok': True, 'returncode': 0})
temporary = path.with_suffix('.json.tmp')
temporary.write_text(json.dumps(state))
temporary.replace(path)
""",
            encoding="utf-8",
        )
        manager = SessionManager(
            root,
            root / ".claude" / "sessions",
            sdk_config_dir=root / "sdk-runtime",
            client_factory=FakeClient,
            analysis_run_root=self.run_root,
        )
        plan = self.ready_plan()
        manager._save_analysis_plan(self.chat_id, plan)

        events = [
            event
            async for event in manager.start_execution(
                self.chat_id, "message-id", plan["dataset"]
            )
        ]
        self.assertIn("分析任务已启动", "".join(self.texts(events)))

        state_path = Path(plan["run"]["state_path"])
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            state = json.loads(state_path.read_text(encoding="utf-8"))
            if state["execution"]["status"] == "completed":
                break
            await asyncio.sleep(0.01)
        self.assertEqual(state["execution"]["status"], "completed")
        self.assertTrue(state["execution"]["ok"])

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
        self.assertEqual(
            options.thinking,
            {
                "type": "enabled",
                "budget_tokens": 20_000,
                "display": "summarized",
            },
        )

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

    def test_assistant_thinking_and_tools_are_deduplicated_and_redacted(self):
        message = runtime.AssistantMessage(
            content=[
                runtime.ThinkingBlock("核对输入与交付目标", "signature-not-stored"),
                runtime.ToolUseBlock(
                    "tool-1",
                    "Read",
                    {"path": "/data/input", "api_key": "secret-value"},
                ),
            ],
            model="MiniMax-M3",
            uuid="message-1",
        )

        self.manager._record_assistant_trace(self.chat_id, message)
        self.manager._record_assistant_trace(self.chat_id, message)

        lines = (
            self.manager._agent_trace_path(self.chat_id)
            .read_text(encoding="utf-8")
            .splitlines()
        )
        records = [json.loads(line) for line in lines]
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["type"], "thinking_summary")
        self.assertNotIn("signature", records[0])
        self.assertEqual(records[1]["input"]["api_key"], "[REDACTED]")

    def test_empty_thinking_block_does_not_create_trace(self):
        message = runtime.AssistantMessage(
            content=[runtime.ThinkingBlock("", "signature-not-stored")],
            model="MiniMax-M3",
            uuid="message-empty",
        )

        self.manager._record_assistant_trace(self.chat_id, message)

        self.assertFalse(self.manager._agent_trace_path(self.chat_id).exists())

    def test_system_prompt_contains_analysis_plan_contract(self):
        legacy_contract = "analysis" + "_scheme"
        self.assertNotIn(legacy_contract, config.SYSTEM_PROMPT)
        self.assertIn("analysis_plan", config.SYSTEM_PROMPT)
        self.assertIn("```analysis_plan", config.SYSTEM_PROMPT)
        self.assertIn('"ready_to_execute"', config.SYSTEM_PROMPT)
        self.assertIn("Do not include model-authored steps", config.SYSTEM_PROMPT)
        self.assertIn('"execution_spec"', config.SYSTEM_PROMPT)
        self.assertIn('"viral_type"', config.SYSTEM_PROMPT)
        self.assertIn("analysis_pipeline version 2", config.SYSTEM_PROMPT)
        self.assertIn("Do not create run directories", config.SYSTEM_PROMPT)
        self.assertIn("Multiple deliverables are allowed", config.SYSTEM_PROMPT)
        self.assertIn("final analysis_plan fenced block", config.SYSTEM_PROMPT)
        self.assertNotIn("RUNTIME RUNNER COMMAND", config.SYSTEM_PROMPT)
        self.assertNotIn("```diagnostic_report", config.SYSTEM_PROMPT)
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
        self.assertIn("analysis_pipeline` version 2", skill)
        self.assertIn("Multiple endpoints are allowed", skill)
        self.assertIn("${RUN_DIR}/.bioagent/state.json", skill)
        self.assertIn("${RUN_DIR}/.bioagent/execution.log", skill)
        self.assertIn("runtime-provided runner command", skill)
        self.assertIn("analysis_runner.py", skill)
        self.assertIn("Do not edit it manually", skill)
        self.assertNotIn("rsync -a", skill)
        self.assertNotIn("```diagnostic_report", skill)
        self.assertIn("generates the diagnostic report deterministically", skill)
        self.assertIn("Quality-control or assembly results alone", skill)


if __name__ == "__main__":
    unittest.main()

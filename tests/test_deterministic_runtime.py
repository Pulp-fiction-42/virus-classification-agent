from __future__ import annotations

import json
import tempfile
import unittest
import uuid
from pathlib import Path

from deterministic_runtime import DeterministicRuntime
from pipeline_spec import canonical_steps, normalize_execution_spec
from runtime_errors import SessionStateError


class FakeCalibrator:
    def backfill(self) -> None:
        return None

    def estimate(self, _spec):
        return {}


class DeterministicRuntimeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.project_root = root / "agent"
        self.run_root = root / "runs"
        self.input_root = root / "inputs"
        self.project_root.mkdir()
        self.input_root.mkdir(parents=True)
        self.read1 = self.input_root / "sample_R1.fastq"
        self.read2 = self.input_root / "sample_R2.fastq"
        self.read1.write_text("@a/1\nAC\n+\nII\n", encoding="utf-8")
        self.read2.write_text("@a/2\nAC\n+\nII\n", encoding="utf-8")
        self.launches: list[Path] = []

        def launch(state_path: Path, _log_path: Path) -> int:
            self.launches.append(state_path)
            return 1234

        self.runtime = DeterministicRuntime(
            self.project_root,
            root / "state",
            analysis_run_root=self.run_root,
            analysis_data_root=self.input_root,
            resource_calibrator=FakeCalibrator(),
            runner_launcher=launch,
        )
        self.chat_id = str(uuid.uuid4())

    def tearDown(self):
        self.temp_dir.cleanup()

    @staticmethod
    def _file(path: Path) -> dict:
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

    def plan(self) -> dict:
        dataset = {
            "id": "ds-test",
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
                            "sample_id": "sample",
                            "files": {
                                "read1": self._file(self.read1),
                                "read2": self._file(self.read2),
                            },
                        }
                    ],
                }
            ],
        }
        spec = normalize_execution_spec(
            {
                "version": 2,
                "adapter": "analysis_pipeline",
                "inputs": [
                    {
                        "id": "primary",
                        "type": "raw_paired_reads",
                        "records": [
                            {
                                "run_id": "run-1",
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
            }
        )
        run_id = "analysis-20260830-120000-deadbeef"
        run_dir = self.run_root / run_id
        return {
            "title": "确定性方案",
            "dataset": dataset,
            "goals": ["contig_filter"],
            "options": {"viral_type": "RNA+DNA"},
            "planner_version": 1,
            "plan_ref": {"id": "pl_test", "revision": 1},
            "decision_trace": [{"kind": "plan_compiled"}],
            "execution_spec": spec,
            "steps": canonical_steps(spec),
            "run": {
                "run_id": run_id,
                "run_dir": str(run_dir),
                "state_path": str(run_dir / ".bioagent" / "state.json"),
                "log_path": str(run_dir / ".bioagent" / "execution.log"),
            },
        }

    async def test_launch_is_idempotent_and_persists_decision_trace(self):
        plan = self.plan()
        first = await self.runtime.launch_deterministic_plan(plan, self.chat_id)
        second = await self.runtime.launch_deterministic_plan(plan, self.chat_id)
        state = json.loads(Path(plan["run"]["state_path"]).read_text(encoding="utf-8"))

        self.assertEqual(first["status"], "running")
        self.assertEqual(second["status"], "running")
        self.assertEqual(len(self.launches), 1)
        self.assertEqual(state["plan_ref"], {"id": "pl_test", "revision": 1})
        self.assertEqual(state["decision_trace"][0]["kind"], "plan_compiled")
        self.assertTrue(Path(state["technical_report"]["agent_trace"]).is_file())

    async def test_changed_file_is_rejected_before_state_creation(self):
        plan = self.plan()
        self.read1.write_text("changed", encoding="utf-8")
        with self.assertRaisesRegex(SessionStateError, "预检后发生变化"):
            await self.runtime.launch_deterministic_plan(plan, self.chat_id)
        self.assertFalse(Path(plan["run"]["state_path"]).exists())
        self.assertEqual(self.launches, [])

    async def test_input_outside_data_root_is_rejected(self):
        plan = self.plan()
        outside = self.run_root / "outside.fastq"
        outside.parent.mkdir(parents=True, exist_ok=True)
        outside.write_text("@a/1\nAC\n+\nII\n", encoding="utf-8")
        record = plan["dataset"]["artifacts"][0]["records"][0]
        record["files"]["read1"] = self._file(outside)
        with self.assertRaisesRegex(SessionStateError, "BIOAGENT_DATA_ROOT"):
            await self.runtime.launch_deterministic_plan(plan, self.chat_id)

    async def test_mismatched_existing_state_is_not_overwritten(self):
        plan = self.plan()
        state_path = Path(plan["run"]["state_path"])
        state_path.parent.mkdir(parents=True)
        state_path.write_text('{"sentinel":true}', encoding="utf-8")
        with self.assertRaisesRegex(SessionStateError, "不匹配"):
            await self.runtime.launch_deterministic_plan(plan, self.chat_id)
        self.assertEqual(json.loads(state_path.read_text(encoding="utf-8")), {"sentinel": True})

    async def test_launch_failure_is_terminal_and_visible(self):
        plan = self.plan()

        def fail(_state_path: Path, _log_path: Path) -> int:
            raise OSError("fork failed")

        self.runtime.runner_launcher = fail
        with self.assertRaisesRegex(SessionStateError, "启动失败"):
            await self.runtime.launch_deterministic_plan(plan, self.chat_id)
        state = json.loads(Path(plan["run"]["state_path"]).read_text(encoding="utf-8"))
        self.assertEqual(state["execution"]["status"], "failed")
        self.assertIn("fork failed", state["execution"]["error"])


if __name__ == "__main__":
    unittest.main()

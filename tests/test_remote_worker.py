import base64
import hashlib
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import remote_worker


class RemoteWorkerPathTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.data = root / "data"
        self.external = root / "external"
        self.outside = root / "outside"
        self.runs = root / "runs"
        for directory in (self.data, self.external, self.outside, self.runs):
            directory.mkdir()
        self.settings = {
            "data_root": str(self.data),
            "run_root": str(self.runs),
            "allowed_roots": [str(self.data), str(self.external)],
            "python": "/python",
            "tmux": "/tmux",
            "tmux_socket": "bioagent-worker",
        }

    def tearDown(self):
        self.temp.cleanup()

    def test_allowed_symlink_keeps_logical_path_and_tracks_link_and_target(self):
        target = self.external / "sample.fastq"
        target.write_text("@r\nA\n+\n!\n", encoding="utf-8")
        logical = self.data / "sample.fastq"
        logical.symlink_to(target)

        source = remote_worker.resolve_source(str(logical), self.settings)

        self.assertEqual(source["path"], str(logical))
        self.assertEqual(source["canonical_path"], str(target.resolve()))
        self.assertTrue(source["is_symlink"])
        self.assertEqual(len(source["fingerprint"]["links"]), 1)
        self.assertTrue(remote_worker._same_fingerprint(source, self.settings))

        replacement = self.external / "replacement.fastq"
        replacement.write_text("@r\nAA\n+\n!!\n", encoding="utf-8")
        logical.unlink()
        logical.symlink_to(replacement)
        self.assertFalse(remote_worker._same_fingerprint(source, self.settings))

    def test_directory_symlink_chain_is_part_of_fingerprint(self):
        nested = self.external / "nested"
        nested.mkdir()
        target = nested / "reads.fastq"
        target.write_text("@r\nA\n+\n!\n", encoding="utf-8")
        logical_dir = self.data / "linked"
        logical_dir.symlink_to(nested, target_is_directory=True)

        source = remote_worker.resolve_source(
            str(logical_dir / target.name), self.settings
        )

        self.assertTrue(source["is_symlink"])
        self.assertEqual(source["fingerprint"]["links"][0]["path"], str(logical_dir))

    def test_broken_and_outside_symlinks_are_rejected(self):
        broken = self.data / "broken"
        broken.symlink_to(self.external / "missing")
        with self.assertRaises(remote_worker.WorkerError):
            remote_worker.resolve_source(str(broken), self.settings)

        target = self.outside / "secret"
        target.write_text("no", encoding="utf-8")
        escaped = self.data / "escaped"
        escaped.symlink_to(target)
        with self.assertRaises(remote_worker.WorkerError):
            remote_worker.resolve_source(str(escaped), self.settings)

    def test_quick_preflight_and_stale_detection(self):
        left = self.data / "sample_1.fastq"
        right = self.data / "sample_2.fastq"
        left.write_text("@read/1\nAC\n+\n!!\n", encoding="utf-8")
        right.write_text("@read/2\nTG\n+\n!!\n", encoding="utf-8")
        sources = [
            remote_worker.resolve_source(str(left), self.settings),
            remote_worker.resolve_source(str(right), self.settings),
        ]
        artifacts = [
            {
                "id": "primary",
                "type": "raw_paired_reads",
                "records": [
                    {
                        "record_id": "one",
                        "files": {"read1": sources[0], "read2": sources[1]},
                    }
                ],
            }
        ]
        result = remote_worker.preflight(
            {
                "settings": self.settings,
                "sources": sources,
                "artifacts": artifacts,
                "full": False,
            }
        )
        self.assertEqual(result["status"], "ready")
        self.assertEqual(result["details"]["record_count"], 1)

        left.write_text("@changed/1\nAC\n+\n!!\n", encoding="utf-8")
        stale = remote_worker.preflight(
            {
                "settings": self.settings,
                "sources": sources,
                "artifacts": artifacts,
                "full": False,
            }
        )
        self.assertEqual(stale["status"], "stale")


class RemoteWorkerRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.runs = self.root / "runs"
        self.runs.mkdir()
        self.settings = {
            "data_root": str(self.root / "data"),
            "run_root": str(self.runs),
            "allowed_roots": [str(self.root / "data")],
            "python": "/python",
            "tmux": "/tmux",
            "tmux_socket": "bioagent-worker",
        }
        Path(self.settings["data_root"]).mkdir()

    def tearDown(self):
        self.temp.cleanup()

    def _state(self, run_id: str, status: str) -> Path:
        path = self.runs / run_id / ".bioagent" / "state.json"
        path.parent.mkdir(parents=True)
        path.write_text(
            json.dumps(
                {
                    "version": 3,
                    "run_id": run_id,
                    "run_dir": str(path.parent.parent),
                    "worker_version": "abc",
                    "execution": {"status": status},
                    "technical_report": {},
                }
            ),
            encoding="utf-8",
        )
        return path

    @staticmethod
    def _queued_state(run_id, priority, routing, enqueued_at):
        return {
            "run_id": run_id,
            "execution": {"status": "queued", "queued_at": enqueued_at},
            "queue": {
                "schema_version": 1,
                "policy_version": 1,
                "project_id": f"project-{run_id}",
                "project_priority": priority,
                "routing_priority": routing,
                "enqueued_at": enqueued_at,
                "waiting_stage": None,
                "waiting_reason_codes": [],
                "last_checked_at": None,
            },
        }

    def test_queue_order_uses_aging_then_project_and_emergency_priority(self):
        now = 2_000_000_000.0
        old = self._queued_state(
            "analysis-old", "low", "normal", "2020-01-01T00:00:00+00:00"
        )
        high = self._queued_state(
            "analysis-high", "high", "normal", "2033-05-18T03:33:00+00:00"
        )
        urgent = self._queued_state(
            "analysis-urgent", "high", "emergency", "2033-05-18T03:34:00+00:00"
        )
        values = sorted(
            [high, old, urgent],
            key=lambda state: remote_worker._queue_sort_key(
                state, now=now, aging_seconds=24 * 3600
            ),
        )
        self.assertEqual(
            [value["run_id"] for value in values],
            ["analysis-old", "analysis-urgent", "analysis-high"],
        )

    def test_legacy_queue_uses_normal_priority_and_original_queued_time(self):
        state = {
            "run_id": "analysis-legacy",
            "execution": {
                "status": "queued",
                "queued_at": "2026-09-24T10:00:00+08:00",
            },
        }
        queue = remote_worker._queue_record(state)
        self.assertEqual(queue["project_priority"], "normal")
        self.assertEqual(queue["enqueued_at"], "2026-09-24T10:00:00+08:00")

    def test_diagnostic_admission_waits_then_becomes_ready(self):
        state = self._queued_state(
            "analysis-wait", "normal", "normal", "2033-05-18T03:33:00+00:00"
        )
        state["diagnostic"] = {"admission_blocks_remaining": 1}
        first = remote_worker._admission_snapshot(state, self.runs)
        second = remote_worker._admission_snapshot(state, self.runs)
        self.assertFalse(first["admission_ok"])
        self.assertEqual(first["reason_codes"], ["DIAGNOSTIC_RESOURCE_WAIT"])
        self.assertEqual(first["stage"], "full_analysis")
        self.assertTrue(second["admission_ok"])

    def test_duplicate_diagnostic_launch_keeps_original_queue_record(self):
        request = {
            "settings": self.settings,
            "run_id": "analysis-idempotent",
            "worker_version": "abc",
            "duration_seconds": 1,
            "queue_context": {
                "project_id": "project-one",
                "project_priority": "high",
            },
        }
        with patch("remote_worker._ensure_scheduler") as scheduler:
            remote_worker.diagnostic_launch(request)
            state_path = self.runs / "analysis-idempotent/.bioagent/state.json"
            first = json.loads(state_path.read_text(encoding="utf-8"))["queue"]
            remote_worker.diagnostic_launch(
                {
                    **request,
                    "queue_context": {
                        "project_id": "project-other",
                        "project_priority": "low",
                    },
                }
            )
        second = json.loads(state_path.read_text(encoding="utf-8"))["queue"]
        self.assertEqual(first, second)
        self.assertEqual(scheduler.call_count, 2)

    def test_launch_revalidates_execution_spec_before_queueing(self):
        with self.assertRaisesRegex(
            remote_worker.WorkerError, "execution spec is invalid"
        ):
            remote_worker.launch(
                {
                    "settings": self.settings,
                    "run_id": "analysis-forged",
                    "worker_version": "abc",
                    "chat_id": "chat",
                    "dataset": {"status": "ready", "artifacts": []},
                    "plan": {
                        "execution_spec": {
                            "version": 3,
                            "adapter": "analysis_pipeline",
                            "options": {
                                "capability_parameters": [
                                    {
                                        "capability_id": "workflow1.read_qc.fastp",
                                        "manifest_version": 1,
                                        "preset_id": "workflow_default",
                                        "values": {"raw_cli": "--output /tmp/x"},
                                    }
                                ]
                            },
                        },
                        "steps": [],
                    },
                }
            )

    def test_missing_queued_session_is_relaunched(self):
        run_id = "analysis-queued"
        self._state(run_id, "queued")
        with (
            patch("remote_worker._scheduler_session_exists", return_value=False),
            patch("remote_worker._ensure_scheduler") as start,
        ):
            result = remote_worker.snapshot(
                {"settings": self.settings, "run_id": run_id, "worker_version": "abc"}
            )
        self.assertTrue(result["found"])
        start.assert_called_once()

    def test_missing_running_process_is_interrupted_without_rerun(self):
        run_id = "analysis-running"
        path = self._state(run_id, "running")
        with patch("remote_worker._runner_alive", return_value=False):
            self.assertIsNone(remote_worker._running_process(self.runs))
        persisted = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(persisted["execution"]["status"], "interrupted")

    def test_live_runner_is_reconciled_without_duplicate_start(self):
        run_id = "analysis-live"
        path = self._state(run_id, "running")
        supervisor = {
            "run_id": run_id,
            "status": "running",
            "runner_pid": 321,
            "boot_id": remote_worker._boot_id(),
        }
        (path.parent / "supervisor.json").write_text(
            json.dumps(supervisor), encoding="utf-8"
        )
        with (
            patch("remote_worker._runner_alive", return_value=True),
            patch("remote_worker._run_candidate") as start,
        ):
            running = remote_worker._running_process(self.runs)
        self.assertEqual(running, (path, json.loads(path.read_text()), supervisor))
        start.assert_not_called()

    def test_scheduler_skips_resource_wait_and_runs_next_candidate(self):
        first_path = self._state("analysis-waiting", "queued")
        second_path = self._state("analysis-ready", "queued")
        first = json.loads(first_path.read_text(encoding="utf-8"))
        second = json.loads(second_path.read_text(encoding="utf-8"))
        args = type(
            "Args",
            (),
            {
                "run_root": str(self.runs),
                "aging_hours": 24,
                "tmux": "/tmux",
                "tmux_socket": "bioagent-worker",
                "version": "abc",
            },
        )()
        with (
            patch("remote_worker._running_process", return_value=None),
            patch(
                "remote_worker._queued_states",
                side_effect=[[(first_path, first), (second_path, second)], []],
            ),
            patch("remote_worker._legacy_session_exists", return_value=False),
            patch(
                "remote_worker._admission_snapshot",
                side_effect=[
                    {
                        "admission_ok": False,
                        "reason_codes": ["MEMORY_RESERVE_LOW"],
                        "stage": "full_analysis",
                    },
                    {"admission_ok": True, "reason_codes": [], "stage": "full_analysis"},
                ],
            ),
            patch("remote_worker._run_candidate", return_value=0) as run_candidate,
        ):
            remote_worker.schedule(args)

        waiting = json.loads(first_path.read_text(encoding="utf-8"))
        self.assertEqual(waiting["execution"]["status"], "waiting_resources")
        self.assertEqual(waiting["queue"]["waiting_reason_codes"], ["MEMORY_RESERVE_LOW"])
        run_candidate.assert_called_once_with(second_path, second, version="abc")

    def test_scheduler_stops_and_adopts_legacy_queued_session(self):
        state_path = self._state("analysis-legacy-session", "queued")
        state = json.loads(state_path.read_text(encoding="utf-8"))
        args = type(
            "Args",
            (),
            {
                "run_root": str(self.runs),
                "aging_hours": 24,
                "tmux": "/tmux",
                "tmux_socket": "bioagent-worker",
                "version": "abc",
            },
        )()
        with (
            patch("remote_worker._running_process", return_value=None),
            patch(
                "remote_worker._queued_states",
                side_effect=[[(state_path, state)], []],
            ),
            patch("remote_worker._legacy_session_exists", return_value=True),
            patch("remote_worker._stop_legacy_session", return_value=True) as stop,
            patch(
                "remote_worker._admission_snapshot",
                return_value={
                    "admission_ok": True,
                    "reason_codes": [],
                    "stage": "full_analysis",
                },
            ),
            patch("remote_worker._run_candidate", return_value=0) as run_candidate,
        ):
            remote_worker.schedule(args)

        stop.assert_called_once_with(
            {"tmux": "/tmux", "tmux_socket": "bioagent-worker"},
            "analysis-legacy-session",
        )
        run_candidate.assert_called_once()

    def test_snapshot_selects_preliminary_telemetry(self):
        run_id = "analysis-reports"
        path = self._state(run_id, "completed")
        preliminary = path.parent / "preliminary-telemetry.json"
        final = path.parent / "telemetry.json"
        preliminary.write_text(
            json.dumps({"version": 3, "report": "preliminary"}), encoding="utf-8"
        )
        final.write_text(
            json.dumps({"version": 3, "report": "final"}), encoding="utf-8"
        )
        state = json.loads(path.read_text(encoding="utf-8"))
        state["reports"] = {
            "preliminary": {"telemetry": str(preliminary), "available": True},
            "final": {"telemetry": str(final), "available": True},
        }
        path.write_text(json.dumps(state), encoding="utf-8")

        with patch("remote_worker._scheduler_session_exists", return_value=False):
            result = remote_worker.snapshot(
                {
                    "settings": self.settings,
                    "run_id": run_id,
                    "worker_version": "abc",
                    "report_kind": "preliminary",
                }
            )

        self.assertEqual(result["telemetry"]["report"], "preliminary")

    def test_delivery_artifact_requires_registered_hash_matched_file(self):
        run_id = "analysis-delivery"
        state_path = self._state(run_id, "completed")
        run_dir = state_path.parent.parent
        report = run_dir / "reports" / "standard" / "report.html"
        report.parent.mkdir(parents=True)
        report.write_bytes(b"<html>delivery</html>")
        state = json.loads(state_path.read_text(encoding="utf-8"))
        state["standard_delivery"] = {
            "schema_version": 1,
            "status": "ready",
            "artifacts": {
                "html": {
                    "path": "reports/standard/report.html",
                    "bytes": report.stat().st_size,
                    "sha256": hashlib.sha256(report.read_bytes()).hexdigest(),
                    "media_type": "text/html; charset=utf-8",
                }
            },
        }
        state_path.write_text(json.dumps(state), encoding="utf-8")

        result = remote_worker.delivery_artifact(
            {"settings": self.settings, "run_dir": str(run_dir), "format": "html"}
        )
        self.assertEqual(
            base64.b64decode(result["content_base64"]), report.read_bytes()
        )

        report.write_bytes(b"tampered")
        with self.assertRaises(remote_worker.WorkerError):
            remote_worker.delivery_artifact(
                {"settings": self.settings, "run_dir": str(run_dir), "format": "html"}
            )

        outside_run = self.root / "outside" / "analysis-escaped"
        outside_state = outside_run / ".bioagent" / "state.json"
        outside_report = outside_run / "reports" / "standard" / "report.html"
        outside_state.parent.mkdir(parents=True)
        outside_report.parent.mkdir(parents=True)
        outside_report.write_bytes(b"<html>outside</html>")
        outside_state.write_text(
            json.dumps(
                {
                    "standard_delivery": {
                        "schema_version": 1,
                        "status": "ready",
                        "artifacts": {
                            "html": {
                                "path": "reports/standard/report.html",
                                "bytes": outside_report.stat().st_size,
                                "sha256": hashlib.sha256(
                                    outside_report.read_bytes()
                                ).hexdigest(),
                            }
                        },
                    }
                }
            ),
            encoding="utf-8",
        )
        escaped_run = self.runs / "analysis-escaped"
        escaped_run.symlink_to(outside_run, target_is_directory=True)
        with self.assertRaises(remote_worker.WorkerError):
            remote_worker.delivery_artifact(
                {
                    "settings": self.settings,
                    "run_dir": str(escaped_run),
                    "format": "html",
                }
            )

        state["standard_delivery"]["artifacts"]["html"]["path"] = (
            "../outside/report.html"
        )
        state_path.write_text(json.dumps(state), encoding="utf-8")
        with self.assertRaises(remote_worker.WorkerError):
            remote_worker.delivery_artifact(
                {"settings": self.settings, "run_dir": str(run_dir), "format": "html"}
            )

    def test_private_scheduler_session_receives_runner_environment(self):
        self.settings["runner_environment"] = {"BIOAGENT_SNAKEMAKE": "/tools/snakemake"}
        completed = subprocess.CompletedProcess([], 0, stdout=b"", stderr=b"")
        with (
            patch("remote_worker._scheduler_session_exists", return_value=False),
            patch("remote_worker.subprocess.run", return_value=completed) as run,
        ):
            remote_worker._ensure_scheduler(self.settings, "abc")
        command = run.call_args.args[0]
        self.assertEqual(command[:4], ["/tmux", "-L", "bioagent-worker", "new-session"])
        self.assertIn("bioagent-scheduler-", command[6])
        self.assertIn("BIOAGENT_SNAKEMAKE=/tools/snakemake", command[-1])

    def test_diagnostic_run_uses_single_private_scheduler(self):
        completed = subprocess.CompletedProcess([], 0, stdout=b"", stderr=b"")
        with (
            patch("remote_worker._scheduler_session_exists", return_value=False),
            patch("remote_worker.subprocess.run", return_value=completed) as run,
        ):
            remote_worker._ensure_scheduler(self.settings, "abc")
        self.assertIn(" schedule ", f" {run.call_args.args[0][-1]} ")

    def test_toolchain_reports_each_unavailable_tool_without_hiding_workflows(self):
        runner_environment = {
            "BIOAGENT_SNAKEMAKE": "/tools/snakemake",
            "BIOAGENT_SEQKIT": "/tools/seqkit",
            "BIOAGENT_REPORT_PYTHON": "/tools/report-python",
            "BIOAGENT_RSCRIPT": "/tools/Rscript",
            "BIOAGENT_WEASYPRINT": "/tools/weasyprint",
        }
        for index in range(1, 7):
            workflow = self.root / f"workflow{index}"
            workflow.mkdir()
            (workflow / "Snakefile").touch()
            runner_environment[f"BIOAGENT_WORKFLOW{index}_SOURCE"] = str(workflow)
        self.settings["runner_environment"] = runner_environment

        def run(command, **_kwargs):
            if command[0] in {"/python", "/tools/snakemake"}:
                raise subprocess.TimeoutExpired(command, 30)
            return subprocess.CompletedProcess(command, 0, "version 1\n", "")

        with patch("remote_worker._run_captured", side_effect=run):
            result = remote_worker.toolchain({"settings": self.settings})

        self.assertFalse(result["ok"])
        self.assertEqual(result["python"]["error"], "timeout")
        self.assertEqual(result["snakemake"]["error"], "timeout")
        self.assertTrue(result["tmux"]["available"])
        self.assertTrue(result["seqkit"]["available"])
        self.assertTrue(result["report_python"]["available"])
        self.assertTrue(result["rscript"]["available"])
        self.assertTrue(result["weasyprint"]["available"])
        self.assertTrue(all(item["available"] for item in result["workflows"].values()))


if __name__ == "__main__":
    unittest.main()

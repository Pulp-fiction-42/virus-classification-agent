from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from pipeline_spec import normalize_execution_spec
from resource_calibration import ProcessResourceMonitor, ResourceCalibrator
from workflow_adapters import AdapterSettings


class ResourceCalibrationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.run_root = self.root / "runs"
        self.run_root.mkdir()
        self.workflow1 = self.root / "workflow1"
        self.workflow2 = self.root / "workflow2"
        for workflow in (self.workflow1, self.workflow2):
            (workflow / "config").mkdir(parents=True)
        (self.workflow1 / "config/config.yaml").write_text(
            'database:\n  rrna_bowtie2: "/db/rrna/2026"\n', encoding="utf-8"
        )
        (self.workflow2 / "config/config.yaml").write_text(
            'database:\n  nr: "/db/nr/20260420"\n', encoding="utf-8"
        )
        self.seqkit = self.root / "seqkit"
        self.counter = self.root / "seqkit-calls"
        self.seqkit.write_text(
            "#!/bin/sh\n"
            f"printf 'x\\n' >> {self.counter}\n"
            "printf 'file\\tformat\\ttype\\tnum_seqs\\tsum_len\\n'\n"
            "printf '%s\\tFASTQ\\tDNA\\t100\\t10000\\n' \"$5\"\n",
            encoding="utf-8",
        )
        self.seqkit.chmod(0o755)
        self.settings = AdapterSettings(
            workflow1_source=self.workflow1,
            workflow2_source=self.workflow2,
            snakemake="snakemake",
            seqkit=str(self.seqkit),
            cores=8,
        )
        self.calibrator = ResourceCalibrator(
            self.root / "resource-history.sqlite3", self.run_root, self.settings
        )
        self.read1 = self.root / "R1.fastq.gz"
        self.read2 = self.root / "R2.fastq.gz"
        self.read1.write_bytes(b"reads1")
        self.read2.write_bytes(b"reads2")

    def tearDown(self):
        self.temporary.cleanup()

    def workflow1_spec(self) -> dict:
        return normalize_execution_spec(
            {
                "version": 3,
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
                "policy": {"mode": "standard"},
            }
        )

    def classification_spec(self) -> dict:
        raw = self.workflow1_spec()
        return normalize_execution_spec(
            {
                **raw,
                "deliverables": ["virus_summary"],
                "options": {"viral_type": "RNA"},
                "policy": {"mode": "standard"},
            }
        )

    def record(
        self,
        run_id: str,
        *,
        status: str = "completed",
        duration: int = 100,
        memory: int = 1000,
        sequences: int = 200,
        bases: int = 20000,
        load_bucket: str = "0",
        spec: dict | None = None,
    ) -> None:
        run_dir = self.run_root / run_id
        (run_dir / "workflows/upstream/config").mkdir(parents=True)
        (run_dir / "workflows/upstream/config/config.yaml").write_text(
            (self.workflow1 / "config/config.yaml").read_text(encoding="utf-8"),
            encoding="utf-8",
        )
        started = "2026-01-01T00:00:00+00:00"
        finished = f"2026-01-01T00:{duration // 60:02d}:{duration % 60:02d}+00:00"
        state = {
            "run_id": run_id,
            "run_dir": str(run_dir),
            "execution_spec": spec or self.workflow1_spec(),
        }
        telemetry = {
            "version": 3,
            "run_id": run_id,
            "status": status,
            "started_at": started,
            "finished_at": finished,
            "sequence_metrics": [
                {
                    "source": "raw_input",
                    "num_seqs": sequences,
                    "sum_len": bases,
                }
            ],
            "stages": [],
            "resource_usage": {
                "peak_process_tree_rss_mb": memory,
                "load_bucket": load_bucket,
            },
        }
        self.calibrator.record_run(state, telemetry, telemetry_mtime_ns=1)

    def test_input_stats_are_cached_without_storing_paths(self):
        with patch("resource_calibration.count_active_runners", return_value=0):
            first = self.calibrator.estimate(self.workflow1_spec())
            second = self.calibrator.estimate(self.workflow1_spec())

        self.assertEqual(first["matched_runs"], 0)
        self.assertEqual(second["peak_memory_mb"]["reserve"], 8 * 1024)
        self.assertEqual(
            self.counter.read_text(encoding="utf-8").splitlines(), ["x", "x"]
        )
        with sqlite3.connect(self.calibrator.database_path) as connection:
            rows = connection.execute(
                "SELECT cache_key, sequences, bases FROM input_stats"
            ).fetchall()
        self.assertEqual(len(rows), 2)
        self.assertNotIn(str(self.read1), json.dumps(rows))

    def test_quantiles_exclude_failures_and_apply_low_confidence_margin(self):
        self.record("run-1", duration=100, memory=1000)
        self.record("run-2", duration=200, memory=2000)
        self.record("failed", status="failed", duration=50, memory=500)

        with (
            patch.object(
                self.calibrator,
                "_cached_file_stats",
                return_value=("reads", 100, 10000),
            ),
            patch("resource_calibration.count_active_runners", return_value=0),
        ):
            estimate = self.calibrator.estimate(self.workflow1_spec())

        self.assertEqual(
            estimate["duration_seconds"], {"p50": 150, "p90": 200, "budget": 250}
        )
        self.assertEqual(estimate["peak_memory_mb"]["p90"], 2000)
        self.assertEqual(estimate["peak_memory_mb"]["reserve"], 8 * 1024)
        self.assertEqual(estimate["matched_runs"], 2)
        self.assertEqual(estimate["confidence"], "low")

    def test_medium_confidence_requires_five_runs_and_three_input_sizes(self):
        for index, scale in enumerate((80, 90, 100, 110, 120), start=1):
            self.record(
                f"run-{index}",
                duration=60 + index,
                memory=1000 + index,
                sequences=scale * 2,
                bases=scale * 200,
            )
        with (
            patch.object(
                self.calibrator,
                "_cached_file_stats",
                return_value=("reads", 100, 10000),
            ),
            patch("resource_calibration.count_active_runners", return_value=0),
        ):
            estimate = self.calibrator.estimate(self.workflow1_spec())

        self.assertEqual(estimate["matched_runs"], 5)
        self.assertEqual(estimate["distinct_inputs"], 5)
        self.assertEqual(estimate["confidence"], "medium")
        self.assertEqual(estimate["status"], "calibrated")

    def test_raw_classification_without_history_uses_minimum_admission(self):
        with (
            patch.object(
                self.calibrator,
                "_cached_file_stats",
                return_value=("reads", 100, 10000),
            ),
            patch("resource_calibration.count_active_runners", return_value=0),
        ):
            estimate = self.calibrator.estimate(self.classification_spec())

        self.assertIsNone(estimate["duration_seconds"]["p50"])
        self.assertEqual(estimate["peak_memory_mb"]["reserve"], 24 * 1024)
        self.assertIn("最低准入", estimate["warning"])
        self.assertIn("OOM", estimate["warning"])

    def test_monitor_returns_aggregate_without_time_series(self):
        monitor = ProcessResourceMonitor(lambda: os.getpid(), interval_seconds=0.01)
        monitor.start()
        time.sleep(0.03)
        result = monitor.stop()

        self.assertGreaterEqual(result["samples"], 2)
        self.assertGreater(result["peak_process_tree_rss_mb"], 0)
        self.assertNotIn("time_series", result)


if __name__ == "__main__":
    unittest.main()

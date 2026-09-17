from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import standard_report


class StandardReportTests(unittest.TestCase):
    def test_w1_to_w6_extractor_contract_and_bundled_helper_root(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            output = root / "report"
            phases = []
            for workflow_id in standard_report.WORKFLOWS:
                workflow = root / workflow_id
                results = workflow / "results"
                (workflow / "scripts").mkdir(parents=True)
                results.mkdir()
                (workflow / "scripts" / "extract_results.py").write_text(
                    "# fixture\n", encoding="utf-8"
                )
                phases.append(
                    {
                        "workflow_id": workflow_id,
                        "status": "completed",
                        "workflow_dir": str(workflow),
                        "results_dir": str(results),
                    }
                )

            environments = []

            def fake_run(command, **kwargs):
                environments.append(kwargs["env"])
                target = Path(command[command.index("--output-dir") + 1])
                prefix = command[command.index("--prefix") + 1]
                target.mkdir(parents=True, exist_ok=True)
                (target / f"{prefix}_extracted.json").write_text(
                    json.dumps({"tables": {"fixture": [{"sample_id": "S1"}]}}),
                    encoding="utf-8",
                )
                return subprocess.CompletedProcess(command, 0, "", "")

            with patch("standard_report.subprocess.run", side_effect=fake_run):
                payloads, coverage = standard_report._run_extractors(
                    {"phases": phases}, output, "/report-python"
                )

            self.assertEqual(set(payloads), {f"W{index}" for index in range(1, 7)})
            self.assertEqual([item["status"] for item in coverage], ["extracted"] * 6)
            expected_root = str(Path(standard_report.__file__).resolve().parent)
            self.assertTrue(environments)
            self.assertTrue(
                all(environment["XVIRBENCH_ROOT"] == expected_root for environment in environments)
            )

    def test_missing_workflows_are_skipped_not_failed(self):
        payloads, coverage = standard_report._run_extractors({}, Path("unused"), "python")

        self.assertEqual(payloads, {})
        self.assertEqual([item["status"] for item in coverage], ["missing_workflow"] * 6)

    def test_master_table_preserves_missing_values_and_marks_low_depth(self):
        payloads = {
            "W1": {
                "tables": {
                    "read_summary": [
                        {"sample_id": "S1", "merge_r1_reads": 4_999_999},
                        {"sample_id": "S2", "merge_r1_reads": 5_000_000},
                        {"sample_id": "S3", "q30": 92.0},
                    ]
                }
            }
        }
        state = {
            "metadata_snapshot": {
                "content": {
                    "samples": {
                        "S1": {"group": "A"},
                        "S2": {"group": "A"},
                        "S3": {"group": "B"},
                    }
                }
            }
        }
        with tempfile.TemporaryDirectory() as temporary:
            master, _ = standard_report._build_tables(
                payloads, state, Path(temporary)
            )

        indexed = master.set_index("sample_id")
        self.assertTrue(bool(indexed.loc["S1", "low_depth"]))
        self.assertFalse(bool(indexed.loc["S2", "low_depth"]))
        self.assertTrue(pd.isna(indexed.loc["S3", "w1__merge_r1_reads"]))

    def test_group_threshold_bh_and_effect_sizes(self):
        master = pd.DataFrame(
            {
                "sample_id": ["A1", "A2", "B1", "B2"],
                "group": ["A", "A", "B", "B"],
                "batch": ["x", "x", "x", "y"],
                "metric": [1.0, 2.0, 3.0, 4.0],
                "low_depth": [True, False, False, False],
            }
        )
        groups, notes = standard_report._usable_groups(master)
        self.assertEqual(groups, ["group"])
        self.assertTrue(any("不足两个 n≥2" in note for note in notes))
        self.assertEqual(standard_report._bh([0.01, 0.04, 0.03]), [0.03, 0.04, 0.04])

        with tempfile.TemporaryDirectory() as temporary:
            _, grouped, tests = standard_report._write_stats(
                master, groups, Path(temporary)
            )

        core = tests[tests["dataset"] == "core"].set_index("test")
        self.assertGreaterEqual(core.loc["kruskal_wallis", "effect"], 0)
        self.assertAlmostEqual(core.loc["mannwhitney_bh", "effect"], 1.0)
        self.assertEqual(set(grouped["dataset"]), {"core", "core_excl_lowdepth"})
        self.assertFalse(
            ((tests["dataset"] == "core_excl_lowdepth") & (tests["test"] == "mannwhitney_bh")).any()
        )

    def test_w6_plot_contract_covers_quality_strategy_and_results(self):
        w6_names = {
            name for name, workflow, _pattern in standard_report.FIGURES if workflow == "W6"
        }
        self.assertEqual(
            w6_names,
            {
                "fig16_w6_checkv_quality",
                "fig17_w6_annotation_strategy",
                "fig18_w6_annotation_outcomes",
            },
        )

    def test_public_table_redacts_absolute_paths(self):
        rendered = standard_report._table(
            pd.DataFrame([{"sample_id": "S1", "source": "/data/user/private.fastq"}])
        )

        self.assertNotIn("/data/user", rendered)
        self.assertIn("[路径已隐藏]", rendered)

    def test_empty_metrics_still_write_reproducible_r_script(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            figures = standard_report._run_plots(pd.DataFrame(), root, "/missing-rscript")

            self.assertEqual(figures, [])
            self.assertTrue((root / "r" / "standard_figures.R").is_file())


if __name__ == "__main__":
    unittest.main()

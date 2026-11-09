from __future__ import annotations

import json
import math
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

    def test_nan_sample_ids_are_rejected_and_curated_metrics_are_mapped(self):
        payloads = {
            "W1": {
                "tables": {
                    "run_qc": [
                        {"sample_id": "S1", "reads_before": 100, "q30_after_fastp": 0.91},
                        {"sample_id": float("nan"), "reads_before": 1},
                    ],
                    "samples": [{"sample_id": "S1", "merge_r1_reads": 4_999_999}],
                    "summary": [],
                }
            },
            "W2": {
                "tables": {
                    "contigs": [
                        {
                            "source_sample": None,
                            "length": 1234,
                            "checkv_quality": "High-quality",
                            "completeness": 92,
                        }
                    ],
                    "sample_cluster": [{"sample_id": float("nan"), "n_contigs": 1, "n_clusters": 1}],
                }
            },
        }
        with tempfile.TemporaryDirectory() as temporary:
            master, _ = standard_report._build_tables(payloads, {}, Path(temporary))

        self.assertEqual(master["sample_id"].tolist(), ["S1"])
        row = master.iloc[0]
        self.assertEqual(row["reads_before_sum"], 100)
        self.assertEqual(row["n_putative_viral_contigs"], 1)
        self.assertEqual(row["n_high_quality"], 1)
        self.assertEqual(row["n_clusters"], 1)
        self.assertTrue(bool(row["low_depth"]))
        self.assertNotIn("nan", master["sample_id"].str.lower().tolist())

    def test_w1_to_w6_curated_table_mapping(self):
        payloads = {
            "W3": {"tables": {"host_sample": [{"sample_id": "S1", "n_species_inferred": 2}]}},
            "W4": {"tables": {"sample_qc": [{"sample_id": "S1", "unclassified_fraction": 0.8}]}},
            "W5": {"tables": {"sample_summary": [{"sample_id": "S1", "max_rpm": 12.5}]}},
            "W6": {"tables": {"summary": [{"sample_id": "S1", "n_genes": 4}]}},
        }
        with tempfile.TemporaryDirectory() as temporary:
            master, _ = standard_report._build_tables(payloads, {}, Path(temporary))

        row = master.iloc[0]
        self.assertEqual(row["n_species_inferred"], 2)
        self.assertEqual(row["kraken_unclassified_frac"], 0.8)
        self.assertEqual(row["max_rpm"], 12.5)
        self.assertEqual(row["n_genes"], 4)

    def test_group_threshold_full_stats_and_effect_sizes(self):
        master = pd.DataFrame(
            {
                "sample_id": ["A1", "A2", "B1", "B2"],
                "group": ["A", "A", "B", "B"],
                "batch": ["x", "x", "x", "y"],
                "metric": [1.0, 2.0, 3.0, 4.0],
                "missing_metric": [1.0, math.nan, 3.0, math.nan],
                "low_depth": [True, False, False, False],
                "in_core_cohort": [True] * 4,
            }
        )
        groups, notes = standard_report._usable_groups(master)
        self.assertEqual(groups, ["group"])
        self.assertTrue(any("不足两个 n≥2" in note for note in notes))
        self.assertEqual(standard_report._bh([0.01, 0.04, 0.03]), [0.03, 0.04, 0.04])

        with tempfile.TemporaryDirectory() as temporary:
            overall, grouped, tests = standard_report._write_stats(
                master, groups, Path(temporary)
            )

        self.assertEqual(
            list(overall.columns),
            ["dataset", "metric", "n", "n_missing", "mean", "sd", "median", "q25", "q75", "min", "max"],
        )
        missing = overall[(overall["dataset"] == "core") & (overall["metric"] == "missing_metric")].iloc[0]
        self.assertEqual(missing["n_missing"], 2)
        core = tests[tests["dataset"] == "core"].set_index("test")
        self.assertGreaterEqual(core.loc["kruskal_wallis", "effect"], 0)
        self.assertAlmostEqual(core.loc["mannwhitney_bh", "effect"], 1.0)
        self.assertEqual(set(grouped["dataset"]), {"core", "core_excl_lowdepth"})

    def test_single_sample_never_enables_group_tests(self):
        master = pd.DataFrame(
            {
                "sample_id": ["S1"],
                "group": ["A"],
                "metric": [1.0],
                "low_depth": [False],
                "in_core_cohort": [True],
            }
        )
        groups, _ = standard_report._usable_groups(master)
        with tempfile.TemporaryDirectory() as temporary:
            overall, grouped, tests = standard_report._write_stats(master, groups, Path(temporary))
        self.assertFalse(overall.empty)
        self.assertTrue(grouped.empty)
        self.assertTrue(tests.empty)

    def test_number_format_and_public_table_redact_paths(self):
        rendered = standard_report._table(
            pd.DataFrame(
                [{"sample_id": "S1", "reads_before_sum": 1234567, "q30_after_fastp_median": 0.9, "source": "/data/user/private.fastq"}]
            )
        )

        self.assertNotIn("/data/user", rendered)
        self.assertIn("[路径已隐藏]", rendered)
        self.assertIn("1,234,567", rendered)
        self.assertIn("90.00%", rendered)

    def test_html_contract_has_reference_structure_without_reference_batch(self):
        payloads = {"W1": {"tables": {"run_qc": [{"sample_id": "S1"}]}}}
        coverage = pd.DataFrame(
            [{"workflow": f"W{i}", "status": "extracted" if i == 1 else "missing_workflow", "tables": 1 if i == 1 else 0} for i in range(1, 7)]
        )
        master = pd.DataFrame(
            [{"sample_id": "S1", "q30_after_fastp_median": 0.9, "low_depth": False, "in_core_cohort": True}]
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "r").mkdir()
            (root / "r" / "make_figures.R").write_text("# reproducible\n", encoding="utf-8")
            empty = pd.DataFrame()
            document = standard_report._build_html(
                {"run_id": "safe-run"}, coverage, master, (empty, empty, empty), [], ["仅总体"], payloads, root
            )

        for marker in (
            'class="kicker"',
            'class="tag"',
            '<nav class="muted">',
            'class="notice warn"',
            'id="w1"',
            'id="w6"',
            'id="tables"',
            'id="methods"',
            'id="r-appendix"',
            '<footer>',
        ):
            self.assertIn(marker, document)
        self.assertEqual(document.count('class="tag"'), 5)
        self.assertNotIn("pxy_V350399508", document)
        self.assertNotIn("/data/", document)

    def test_bundled_r_assets_use_point_fallback(self):
        theme = standard_report._asset_path("00_sci_theme.R")
        script = standard_report._asset_path("make_figures.R").read_text(encoding="utf-8")

        self.assertTrue(theme.is_file())
        self.assertIn("counts >= 2", script)
        self.assertIn("geom_point", script)
        self.assertIn("height = 0, seed = 1", script)
        self.assertIn("nrow(core) == 1", script)
        for stem in ("fig16_w6_checkv_quality", "fig17_w6_annotation_strategy", "fig18_w6_annotation_outcomes"):
            self.assertIn(stem, script)

    def test_publish_replaces_owned_outputs_and_preserves_priority_reports(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            output = root / "standard"
            staging = root / "staging"
            output.mkdir()
            staging.mkdir()
            (output / "scientific").mkdir()
            (output / "scientific" / "facts.json").write_text("keep", encoding="utf-8")
            (output / "report.html").write_text("old", encoding="utf-8")
            (staging / "report.html").write_text("new", encoding="utf-8")
            (staging / "tables").mkdir()
            (staging / "tables" / "coverage.tsv").write_text("new", encoding="utf-8")
            manifest = standard_report._manifest(staging)

            standard_report._publish(staging, output)

            self.assertEqual((output / "report.html").read_text(), "new")
            self.assertEqual((output / "scientific" / "facts.json").read_text(), "keep")
            paths = {item["path"] for item in manifest["files"]}
            self.assertIn("report.html", paths)
            self.assertNotIn("scientific/facts.json", paths)

    def test_empty_metrics_still_copy_reproducible_r_assets(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            figures = standard_report._run_plots(pd.DataFrame(), root, "/missing-rscript")

            self.assertEqual(figures, [])
            self.assertTrue((root / "r" / "00_sci_theme.R").is_file())
            self.assertTrue((root / "r" / "make_figures.R").is_file())


if __name__ == "__main__":
    unittest.main()

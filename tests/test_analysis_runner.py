from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
from pathlib import Path

from analysis_runner import (
    _instrument_workflow1,
    _parse_benchmarks,
    _parse_fastp,
    _parse_log,
    _parse_seqkit_table,
    _upstream_metrics,
    _workflow1_report_data,
    redact_text,
    run,
)
from workflow_adapters import WORKFLOW1_RULES


class AnalysisRunnerTests(unittest.TestCase):
    @staticmethod
    def _write_workflow_source(root: Path) -> Path:
        source = root / "workflow-source"
        (source / "config").mkdir(parents=True)
        (source / "data").mkdir()
        (source / "rules").mkdir()
        (source / "Snakefile").write_text(
            'configfile: "config/config.yaml"\ninclude: "rules/upstream.smk"\n',
            encoding="utf-8",
        )
        (source / "config" / "config.yaml").write_text(
            'sample_list: "data/samples.txt"\noutput_dir: "example"\n',
            encoding="utf-8",
        )
        (source / "data" / "samples.txt").write_text("", encoding="utf-8")
        (source / "rules" / "upstream.smk").write_text(
            """
rule fastp:
    output: "clean.fastq"
    shell:
        "{params.sk} stat -j 1 input.fastq"

rule bowtie2:
    output: "rrna.fastq"
    shell:
        "true"

rule merge:
    output: "merged.fastq"
    shell:
        "true"
""".lstrip(),
            encoding="utf-8",
        )
        return source

    @staticmethod
    def _write_state(run_dir: Path, *, entry_rule="fastp", exit_rule="fastp") -> Path:
        bioagent = run_dir / ".bioagent"
        bioagent.mkdir(parents=True)
        input_dir = run_dir.parent / "inputs"
        input_dir.mkdir(exist_ok=True)
        read1 = input_dir / f"{entry_rule}_r1.fq.gz"
        read2 = input_dir / f"{entry_rule}_r2.fq.gz"
        read1.write_text("reads\n", encoding="utf-8")
        read2.write_text("reads\n", encoding="utf-8")
        rules = ("fastp", "bowtie2", "merge")
        selected = rules[rules.index(entry_rule) : rules.index(exit_rule) + 1]
        input_type = {
            "fastp": "raw_paired_reads",
            "bowtie2": "fastp_paired_reads",
            "merge": "clean_paired_reads",
        }[entry_rule]
        state_path = bioagent / "state.json"
        state_path.write_text(
            json.dumps(
                {
                    "version": 2,
                    "run_id": "analysis-test",
                    "run_dir": str(run_dir),
                    "openwebui_chat_id": "chat-test",
                    "steps": [
                        {
                            "id": f"step_{index + 1}",
                            "label": rule,
                            "rules": [rule],
                            "status": "pending",
                        }
                        for index, rule in enumerate(selected)
                    ],
                    "execution_spec": {
                        "adapter": "workflow1",
                        "entry_rule": entry_rule,
                        "exit_rule": exit_rule,
                        "inputs": [
                            {
                                "id": "primary",
                                "type": input_type,
                                "data": {
                                    "runs": [
                                        {
                                            "run_id": "run",
                                            "sample_id": "sample",
                                            "read1": str(read1),
                                            "read2": str(read2),
                                        }
                                    ]
                                },
                            }
                        ],
                    },
                    "execution": {
                        "status": "running",
                        "ok": None,
                        "log": str(bioagent / "execution.log"),
                    },
                    "technical_report": {
                        "status": "collecting",
                        "available": False,
                        "agent_trace": str(bioagent / "agent-trace.jsonl"),
                    },
                }
            ),
            encoding="utf-8",
        )
        return state_path

    @staticmethod
    def _write_seqkit(root: Path) -> Path:
        command = root / "seqkit"
        command.write_text(
            "#!/bin/sh\n"
            "printf 'file\\tformat\\ttype\\tnum_seqs\\tsum_len\\tmin_len\\tQ1\\tQ2\\tQ3\\tmax_len\\tN50\\n'\n"
            "exit 0\n",
            encoding="utf-8",
        )
        command.chmod(0o755)
        return command

    @staticmethod
    def _runner_env(source: Path, snakemake: Path, seqkit: Path):
        return {
            "BIOAGENT_WORKFLOW1_SOURCE": str(source),
            "BIOAGENT_SNAKEMAKE": str(snakemake),
            "BIOAGENT_SEQKIT": str(seqkit),
            "BIOAGENT_SNAKEMAKE_CORES": "8",
        }

    def test_redacts_headers_and_sensitive_environment_values(self):
        command = (
            "Authorization: Bearer abc WEBUI_SECRET_KEY=hidden "
            "MINIMAX_API_KEY=also-hidden password=bad"
        )

        redacted = redact_text(command)

        self.assertNotIn("hidden", redacted)
        self.assertNotIn("also-hidden", redacted)
        self.assertNotIn("abc", redacted)
        self.assertNotIn("bad", redacted)
        self.assertGreaterEqual(redacted.count("[REDACTED]"), 4)

    def test_workflow1_instrumentation_adds_extended_stats_and_benchmarks(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            workflow = Path(temp_dir)
            rules = workflow / "rules" / "upstream.smk"
            rules.parent.mkdir()
            rules.write_text(
                """
rule fastp:
    output: "clean.fastq"
    shell:
        "{params.sk} stat -j 2 input.fastq"

rule bowtie2:
    output: "rrna.fastq"
    shell:
        "{params.sw} {params.pm} -x {params.dbs} |\\
        samtools fastq"

rule megahit:
    output: "contigs.fa"
    shell:
        "{params.sk} stats -j 1 -a contigs.fa"
""".lstrip(),
                encoding="utf-8",
            )

            _instrument_workflow1(workflow, workflow / ".bioagent" / "benchmarks")
            content = rules.read_text(encoding="utf-8")

        self.assertIn("stats -a -T -j 2", content)
        self.assertIn("stats -a -T -j 1", content)
        self.assertIn("fastp.{run_index}.jsonl", content)
        self.assertIn("megahit.{sample_index}.jsonl", content)
        self.assertIn("2> {params.wd}/bowtie2-summary.txt", content)

    def test_parses_seqkit_quartiles_and_benchmark_jobs(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            stats = root / "stats.tsv"
            stats.write_text(
                "file\tformat\ttype\tnum_seqs\tsum_len\tmin_len\tQ1\tQ2\tQ3\tmax_len\tN50\n"
                "reads.fastq\tFASTQ\tDNA\t10\t1000\t80\t90\t100\t110\t120\t105\n",
                encoding="utf-8",
            )
            benchmark_dir = root / "benchmarks"
            benchmark_dir.mkdir()
            (benchmark_dir / "fastp.sample.jsonl").write_text(
                json.dumps(
                    {
                        "jobid": 1,
                        "rule_name": "fastp",
                        "s": 2.5,
                        "cpu_time": 5.0,
                        "max_rss": 100,
                        "params": {"api_key": "hidden"},
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            records = _parse_seqkit_table(stats, "raw_input")
            jobs = _parse_benchmarks(benchmark_dir)

        self.assertEqual(records[0]["median"], 100)
        self.assertEqual((records[0]["q1"], records[0]["q3"]), (90, 110))
        self.assertEqual(jobs[0]["rule_name"], "fastp")
        self.assertEqual(jobs[0]["cpu_time"], 5.0)
        self.assertEqual(jobs[0]["params"]["api_key"], "[REDACTED]")

    def test_preserves_each_rrna_alignment_rate_without_guessing_an_average(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            log = Path(temp_dir) / "execution.log"
            log.write_text(
                "12.5% overall alignment rate\n31.75% overall alignment rate\n",
                encoding="utf-8",
            )

            parsed = _parse_log(log)

        self.assertEqual(parsed["rrna_alignment_percents"], [12.5, 31.75])

    def test_parses_fastp_and_aggregates_rrna_assembly_and_final_metrics(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = Path(temp_dir)
            report_path = run_dir / "results" / "01_fastp.json"
            report_path.parent.mkdir()
            report_path.write_text(
                json.dumps(
                    {
                        "summary": {
                            "before_filtering": {
                                "total_reads": 100,
                                "total_bases": 10_000,
                            },
                            "after_filtering": {
                                "total_reads": 90,
                                "total_bases": 9_000,
                            },
                        }
                    }
                ),
                encoding="utf-8",
            )
            reports = _parse_fastp(run_dir)
            metrics = _upstream_metrics(
                [
                    {
                        "source": "/run/results/filter/a/01_fastp_r1.fq.stat.txt",
                        "num_seqs": 100,
                        "sum_len": 10_000,
                    },
                    {
                        "source": "/run/results/filter/a/cleanreads_r1.fq.stat.txt",
                        "num_seqs": 80,
                        "sum_len": 8_000,
                    },
                    {
                        "source": "/run/results/assembly/a/a.contigs.stat.txt",
                        "num_seqs": 20,
                        "sum_len": 6_000,
                    },
                    {
                        "source": "final_filter",
                        "num_seqs": 10,
                        "sum_len": 5_000,
                    },
                ],
                [12.5, 31.75],
            )

        self.assertEqual(reports[0]["summary"]["after_filtering"]["total_reads"], 90)
        self.assertEqual(metrics["rrna"]["reads_retained_percent"], 80)
        self.assertEqual(metrics["assembly"]["sequences"], 20)
        self.assertEqual(
            metrics["final_filter"]["sequences_retained_percent"],
            50,
        )

    def test_success_and_failure_write_terminal_telemetry_atomically(self):
        for exit_code, expected_status in ((0, "completed"), (7, "failed")):
            with (
                self.subTest(exit_code=exit_code),
                tempfile.TemporaryDirectory() as temp_dir,
            ):
                run_dir = Path(temp_dir) / "run"
                bioagent = run_dir / ".bioagent"
                root = Path(temp_dir)
                state_path = self._write_state(
                    run_dir,
                    entry_rule="fastp",
                    exit_rule="merge",
                )
                source = self._write_workflow_source(root)
                command = root / "snakemake"
                command.write_text(
                    f"#!/bin/sh\nprintf 'partial output\\n'\nexit {exit_code}\n",
                    encoding="utf-8",
                )
                command.chmod(0o755)
                seqkit = self._write_seqkit(root)

                with patch.dict(
                    os.environ,
                    self._runner_env(source, command, seqkit),
                ):
                    result = run(
                        argparse.Namespace(
                            state=str(state_path),
                        )
                    )
                state = json.loads(state_path.read_text(encoding="utf-8"))
                telemetry = json.loads(
                    (bioagent / "telemetry.json").read_text(encoding="utf-8")
                )

                self.assertEqual(result, exit_code)
                self.assertEqual(state["execution"]["status"], expected_status)
                self.assertEqual(telemetry["status"], expected_status)
                self.assertIn("--benchmark-extended", telemetry["runner_command"])
                self.assertIn("--printshellcmds", telemetry["runner_command"])
                self.assertTrue(state["technical_report"]["available"])
                self.assertEqual(state["report"]["available"], exit_code == 0)
                self.assertEqual(state["report"]["schema_version"], 1)
                self.assertEqual(
                    telemetry["delivery_report"]["schema_version"],
                    1,
                )
                selected_stage_statuses = [
                    stage["status"]
                    for stage in telemetry["delivery_report"]["stages"]
                    if stage["rule"] in {"fastp", "bowtie2", "merge"}
                ]
                if exit_code == 0:
                    self.assertEqual(
                        selected_stage_statuses,
                        ["completed", "completed", "completed"],
                    )
                else:
                    self.assertEqual(selected_stage_statuses[0], "failed")
                self.assertIn("agent_trace", state["technical_report"])
                self.assertIn(
                    "partial output", (bioagent / "execution.log").read_text()
                )
                self.assertTrue((bioagent / "input-manifest.json").is_file())
                self.assertTrue((bioagent / "output-manifest.json").is_file())
                self.assertFalse((bioagent / "state.json.tmp").exists())

    def test_merge_only_run_materializes_clean_reads_without_upstream_steps(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            run_dir = root / "run"
            state_path = self._write_state(
                run_dir,
                entry_rule="merge",
                exit_rule="merge",
            )
            source = self._write_workflow_source(root)
            command = root / "snakemake"
            command.write_text(
                "#!/bin/sh\nprintf 'merge only\\n'\nexit 0\n",
                encoding="utf-8",
            )
            command.chmod(0o755)
            seqkit = self._write_seqkit(root)

            with patch.dict(
                os.environ,
                self._runner_env(source, command, seqkit),
            ):
                result = run(argparse.Namespace(state=str(state_path)))

            state = json.loads(state_path.read_text(encoding="utf-8"))
            clean_read = run_dir / "results/filter/run/cleanreads_r1.fq.gz"
            clean_read_is_symlink = clean_read.is_symlink()

        self.assertEqual(result, 0)
        self.assertTrue(clean_read_is_symlink)
        self.assertEqual([step["rules"] for step in state["steps"]], [["merge"]])
        self.assertEqual(state["steps"][0]["status"], "completed")

    def test_invalid_input_still_writes_failed_telemetry_and_report_state(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            run_dir = root / "run"
            state_path = self._write_state(run_dir)
            state = json.loads(state_path.read_text(encoding="utf-8"))
            state["execution_spec"]["inputs"][0]["data"]["runs"][0]["read1"] = str(
                root / "missing.fastq.gz"
            )
            state_path.write_text(json.dumps(state), encoding="utf-8")
            source = self._write_workflow_source(root)
            command = root / "snakemake"
            command.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            command.chmod(0o755)
            seqkit = self._write_seqkit(root)

            with patch.dict(
                os.environ,
                self._runner_env(source, command, seqkit),
            ):
                result = run(argparse.Namespace(state=str(state_path)))

            state = json.loads(state_path.read_text(encoding="utf-8"))
            telemetry = json.loads(
                (run_dir / ".bioagent/telemetry.json").read_text(encoding="utf-8")
            )

        self.assertEqual(result, 1)
        self.assertEqual(state["execution"]["status"], "failed")
        self.assertTrue(state["technical_report"]["available"])
        self.assertFalse(state["report"]["available"])
        self.assertIn("does not exist", telemetry["error"])

    def test_sigterm_preserves_cancelled_partial_telemetry(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = Path(temp_dir) / "run"
            root = Path(temp_dir)
            state_path = self._write_state(run_dir)
            source = self._write_workflow_source(root)
            command = root / "snakemake"
            command.write_text(
                "#!/bin/sh\n"
                'if [ "$1" = "--version" ]; then printf \'9.23.1\\n\'; exit 0; fi\n'
                "printf 'started\\n'\n"
                "exec sleep 30\n",
                encoding="utf-8",
            )
            command.chmod(0o755)
            seqkit = self._write_seqkit(root)
            env = os.environ.copy()
            env.update(self._runner_env(source, command, seqkit))
            process = subprocess.Popen(
                [
                    sys.executable,
                    str(Path(__file__).resolve().parents[1] / "analysis_runner.py"),
                    "--state",
                    str(state_path),
                ],
                env=env,
            )
            log_path = run_dir / ".bioagent" / "execution.log"
            deadline = time.monotonic() + 5
            while (
                not log_path.exists() or "started" not in log_path.read_text()
            ) and time.monotonic() < deadline:
                time.sleep(0.02)

            process.terminate()
            process.wait(timeout=10)
            state = json.loads(state_path.read_text(encoding="utf-8"))
            telemetry = json.loads(
                (run_dir / ".bioagent" / "telemetry.json").read_text(encoding="utf-8")
            )

        self.assertTrue(state["execution"]["cancelled"])
        self.assertEqual(state["execution"]["status"], "failed")
        self.assertEqual(telemetry["status"], "cancelled")
        self.assertTrue(state["technical_report"]["available"])
        self.assertFalse(state["report"]["available"])

    def test_delivery_report_aggregates_runs_and_preserves_rrna_alignment(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = Path(temp_dir)
            records = [
                {
                    "run_id": run_id,
                    "sample_id": "sample",
                    "read1": str(run_dir / "inputs" / f"{run_id}_r1.fq.gz"),
                    "read2": str(run_dir / "inputs" / f"{run_id}_r2.fq.gz"),
                }
                for run_id in ("lane1", "lane2")
            ]
            state = {
                "execution_spec": {
                    "adapter": "workflow1",
                    "entry_rule": "fastp",
                    "exit_rule": "bowtie2",
                    "inputs": [
                        {
                            "id": "primary",
                            "type": "raw_paired_reads",
                            "data": {"runs": records},
                        }
                    ],
                },
                "steps": [
                    {"rules": ["fastp"], "status": "completed"},
                    {"rules": ["bowtie2"], "status": "completed"},
                ],
            }
            sequence_metrics = []
            for lane_index, record in enumerate(records, start=1):
                for mate in (1, 2):
                    sequence_metrics.extend(
                        [
                            {
                                "file": record[f"read{mate}"],
                                "num_seqs": 100,
                                "sum_len": 15_000,
                                "q30_percent": 90 + lane_index,
                            },
                            {
                                "file": str(
                                    run_dir
                                    / "results"
                                    / "filter"
                                    / record["run_id"]
                                    / f"01_fastp_r{mate}.fq.gz"
                                ),
                                "num_seqs": 90,
                                "sum_len": 13_500,
                            },
                            {
                                "file": str(
                                    run_dir
                                    / "results"
                                    / "filter"
                                    / record["run_id"]
                                    / f"cleanreads_r{mate}.fq.gz"
                                ),
                                "num_seqs": 80,
                                "sum_len": 12_000,
                            },
                        ]
                    )
                summary = (
                    run_dir
                    / "results"
                    / "filter"
                    / record["run_id"]
                    / "bowtie2-summary.txt"
                )
                summary.parent.mkdir(parents=True, exist_ok=True)
                summary.write_text(
                    f"{lane_index}.00% overall alignment rate\n",
                    encoding="utf-8",
                )
            fastp_reports = [
                {
                    "path": str(
                        run_dir
                        / "results"
                        / "filter"
                        / record["run_id"]
                        / "01_fastp.json"
                    ),
                    "summary": {
                        "before_filtering": {
                            "total_reads": 200,
                            "total_bases": 30_000,
                            "q30_bases": 27_000,
                        },
                        "after_filtering": {
                            "total_reads": 180,
                            "total_bases": 27_000,
                            "q30_bases": 25_650,
                        },
                    },
                    "filtering_result": {"low_quality_reads": 10},
                    "duplication": {"rate": 0.1},
                }
                for record in records
            ]

            report = _workflow1_report_data(
                state,
                run_dir,
                sequence_metrics,
                fastp_reports,
            )

        sample = report["samples"][0]
        self.assertEqual(sample["input"]["metrics"]["reads"], 400)
        self.assertEqual(sample["fastp"]["before"]["reads"], 400)
        self.assertEqual(sample["fastp"]["after"]["reads"], 360)
        self.assertEqual(sample["fastp"]["reads_retained_percent"], 90)
        self.assertEqual(sample["fastp"]["before"]["q30_percent"], 90)
        self.assertEqual(sample["fastp"]["filtering"]["low_quality_reads"], 20)
        self.assertEqual(sample["rrna"]["after"]["reads"], 320)
        self.assertEqual(
            sample["rrna"]["alignments"],
            [
                {
                    "run_id": "lane1",
                    "alignment_percent": 1.0,
                    "input_reads": 180,
                },
                {
                    "run_id": "lane2",
                    "alignment_percent": 2.0,
                    "input_reads": 180,
                },
            ],
        )
        self.assertEqual(sample["rrna"]["alignment_percent"], 1.5)

    def test_delivery_report_marks_each_workflow_entry_stage(self):
        cases = {
            "fastp": ("raw_paired_reads", "runs", "raw", None),
            "bowtie2": ("fastp_paired_reads", "runs", "post_fastp", "fastp"),
            "merge": ("clean_paired_reads", "runs", "post_rrna", "bowtie2"),
            "megahit": (
                "merged_paired_reads",
                "samples",
                "merged_reads",
                "merge",
            ),
            "rename_filter": (
                "contigs_fasta",
                "samples",
                "assembly",
                "megahit",
            ),
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            for entry_rule, (
                input_type,
                collection,
                input_stage,
                provided_rule,
            ) in cases.items():
                fields = {"sample_id": "sample"}
                if collection == "runs":
                    fields["run_id"] = "run"
                if entry_rule == "rename_filter":
                    fields["path"] = str(root / "contigs.fa")
                else:
                    fields["read1"] = str(root / "r1.fq")
                    fields["read2"] = str(root / "r2.fq")
                selected = WORKFLOW1_RULES[WORKFLOW1_RULES.index(entry_rule) :]
                state = {
                    "execution_spec": {
                        "adapter": "workflow1",
                        "entry_rule": entry_rule,
                        "exit_rule": WORKFLOW1_RULES[-1],
                        "inputs": [
                            {
                                "id": "primary",
                                "type": input_type,
                                "data": {collection: [fields]},
                            }
                        ],
                    },
                    "steps": [
                        {"rules": [rule], "status": "completed"} for rule in selected
                    ],
                }

                report = _workflow1_report_data(state, root, [], [])
                statuses = {
                    stage["rule"]: stage["status"] for stage in report["stages"]
                }

                self.assertEqual(
                    report["samples"][0]["input"]["stage"],
                    input_stage,
                )
                if provided_rule:
                    self.assertEqual(statuses[provided_rule], "provided")


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

from analysis_runner import (
    _attach_report_evidence,
    _classification_report_data,
    _instrument_workflow1,
    _instrument_workflow2,
    _parse_benchmarks,
    _parse_fastp,
    _parse_log,
    _parse_seqkit_table,
    _pipeline_report_data,
    _stage_records,
    _upstream_metrics,
    _workflow1_report_data,
    _workflow6_input_is_empty,
    redact_text,
    run,
)
from analysis_runner import main as runner_main
from pipeline_spec import canonical_steps, normalize_execution_spec
from workflow_adapters import PreparedExecution, PreparedPhase


class AnalysisRunnerTests(unittest.TestCase):
    def test_workflow6_empty_input_detection(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "config").mkdir()
            fasta = root / "empty.fna"
            fasta.write_text("", encoding="utf-8")
            (root / "config/config.yaml").write_text(
                f'input_fasta: "{fasta}"\n', encoding="utf-8"
            )
            self.assertTrue(_workflow6_input_is_empty(root))
            fasta.write_text(">virus\nACGT\n", encoding="utf-8")
            self.assertFalse(_workflow6_input_is_empty(root))

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

    def test_main_terminalizes_unexpected_runner_error(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "state.json"
            state_path.write_text(
                json.dumps(
                    {
                        "version": 3,
                        "execution": {"status": "running", "ok": None},
                        "technical_report": {"status": "collecting"},
                        "report": {"status": "collecting"},
                    }
                ),
                encoding="utf-8",
            )
            with patch("analysis_runner.run", side_effect=RuntimeError("broken")):
                returncode = runner_main(["--state", str(state_path)])

            state = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(returncode, 1)
            self.assertEqual(state["execution"]["status"], "failed")
            self.assertFalse(state["execution"]["ok"])
            self.assertEqual(state["execution"]["error"], "broken")

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
        input_type = {
            "fastp": "raw_paired_reads",
            "bowtie2": "qc_paired_reads",
            "merge": "clean_run_paired_reads",
        }[entry_rule]
        deliverable = {
            "fastp": "read_qc",
            "bowtie2": "rrna_filter",
            "merge": "sample_merge",
        }[exit_rule]
        execution_spec = normalize_execution_spec(
            {
                "version": 3,
                "adapter": "analysis_pipeline",
                "inputs": [
                    {
                        "id": "primary",
                        "type": input_type,
                        "records": [
                            {
                                "run_id": "run",
                                "sample_id": "sample",
                                "files": {
                                    "read1": str(read1),
                                    "read2": str(read2),
                                },
                            }
                        ],
                    }
                ],
                "deliverables": [deliverable],
                "policy": {"mode": "standard"},
            }
        )
        state_path = bioagent / "state.json"
        state_path.write_text(
            json.dumps(
                {
                    "version": 3,
                    "run_id": "analysis-test",
                    "run_dir": str(run_dir),
                    "openwebui_chat_id": "chat-test",
                    "steps": [
                        {**step, "status": "pending"}
                        for step in canonical_steps(execution_spec)
                    ],
                    "execution_spec": execution_spec,
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

    @staticmethod
    def _contig_spec(path: Path, viral_type: str = "RNA+DNA") -> dict:
        return normalize_execution_spec(
            {
                "version": 3,
                "adapter": "analysis_pipeline",
                "inputs": [
                    {
                        "id": "primary",
                        "type": "contigs_fasta",
                        "records": [
                            {
                                "sample_id": "sample",
                                "files": {"fasta": str(path)},
                            }
                        ],
                    }
                ],
                "deliverables": ["virus_summary"],
                "options": {"viral_type": viral_type},
                "policy": {"mode": "standard"},
            }
        )

    @staticmethod
    def _write_adaptive_state(run_dir: Path, mode: str = "emergency") -> Path:
        bioagent = run_dir / ".bioagent"
        bioagent.mkdir(parents=True)
        read1 = run_dir / "input_r1.fq"
        read2 = run_dir / "input_r2.fq"
        read1.write_text("@read/1\nAC\n+\nII\n", encoding="utf-8")
        read2.write_text("@read/2\nTG\n+\nII\n", encoding="utf-8")
        execution_spec = normalize_execution_spec(
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
                                    "read1": str(read1),
                                    "read2": str(read2),
                                },
                            }
                        ],
                    }
                ],
                "deliverables": ["read_qc"],
                "policy": {"mode": mode},
            }
        )
        state_path = bioagent / "state.json"
        state_path.write_text(
            json.dumps(
                {
                    "version": 3,
                    "run_id": "adaptive-test",
                    "run_dir": str(run_dir),
                    "steps": [
                        {**step, "status": "pending"}
                        for step in canonical_steps(execution_spec)
                    ],
                    "execution_spec": execution_spec,
                    "resource_estimate": {
                        "cpu_cores": 8,
                        "peak_memory_mb": {"reserve": 8192},
                    },
                    "execution": {
                        "status": "planned",
                        "ok": None,
                        "log": str(bioagent / "execution.log"),
                    },
                }
            ),
            encoding="utf-8",
        )
        return state_path

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

    def test_emergency_low_confidence_finishes_with_screen_report(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = self._write_adaptive_state(Path(temp_dir) / "run")
            screening = {
                "sample_fraction": 0.05,
                "known_virus_high_confidence": False,
                "samples": [],
            }
            admitted = {"admission_ok": True, "reason_codes": []}
            with (
                patch(
                    "analysis_runner.ResourceCalibrator.history_summary",
                    return_value={},
                ),
                patch(
                    "analysis_runner.collect_resource_snapshot", return_value=admitted
                ),
                patch("analysis_runner.run_rapid_screen", return_value=screening),
            ):
                returncode = run(argparse.Namespace(state=str(state_path)))

            state = json.loads(state_path.read_text(encoding="utf-8"))
            telemetry = json.loads(
                (state_path.parent / "telemetry.json").read_text(encoding="utf-8")
            )
            routing = json.loads(
                (state_path.parent / "routing.json").read_text(encoding="utf-8")
            )

        self.assertEqual(returncode, 0)
        self.assertEqual(state["execution"]["status"], "completed")
        self.assertEqual(routing["decision"]["selected_route"], "A")
        self.assertEqual(routing["decision"]["report_kind"], "preliminary_screen")
        self.assertNotIn("deadline_met", routing)
        self.assertNotIn("deadline_seconds", routing)
        self.assertIsInstance(routing["actual_duration_seconds"], (int, float))
        self.assertEqual(telemetry["delivery_report"]["kind"], "analysis_pipeline")
        self.assertTrue(state["reports"]["preliminary"]["available"])
        self.assertFalse(state["reports"]["final"]["available"])

    def test_auto_high_and_low_confidence_publish_preliminary_then_run_full(self):
        for high_confidence, selected_route in ((True, "A"), (False, "B")):
            with (
                self.subTest(high_confidence=high_confidence),
                tempfile.TemporaryDirectory() as temp_dir,
            ):
                state_path = self._write_adaptive_state(
                    Path(temp_dir) / "run", mode="auto"
                )
                screening = {
                    "sample_fraction": 0.05,
                    "known_virus_high_confidence": high_confidence,
                    "samples": [],
                }
                admitted = {"admission_ok": True, "reason_codes": []}
                frozen_preliminary = None

                def finish_full(_args, current_state_path=state_path):
                    nonlocal frozen_preliminary
                    frozen_preliminary = (
                        current_state_path.parent / "preliminary-telemetry.json"
                    ).read_bytes()
                    state = json.loads(current_state_path.read_text(encoding="utf-8"))
                    state["execution"].update(
                        {"status": "completed", "ok": True, "returncode": 0}
                    )
                    state.setdefault("reports", {})["final"] = {
                        "status": "ready",
                        "available": True,
                        "schema_version": 4,
                        "report_kind": "final_analysis",
                        "telemetry": str(current_state_path.parent / "telemetry.json"),
                    }
                    current_state_path.write_text(json.dumps(state), encoding="utf-8")
                    (current_state_path.parent / "telemetry.json").write_text(
                        json.dumps(
                            {
                                "version": 3,
                                "delivery_report": {
                                    "schema_version": 4,
                                    "kind": "analysis_pipeline",
                                    "report_kind": "final_analysis",
                                },
                            }
                        ),
                        encoding="utf-8",
                    )
                    return 0

                with (
                    patch(
                        "analysis_runner.ResourceCalibrator.history_summary",
                        return_value={},
                    ),
                    patch(
                        "analysis_runner.collect_resource_snapshot",
                        side_effect=[admitted.copy(), admitted.copy()],
                    ),
                    patch("analysis_runner.run_rapid_screen", return_value=screening),
                    patch("analysis_runner.run", side_effect=finish_full) as full_run,
                ):
                    returncode = run(argparse.Namespace(state=str(state_path)))

                state = json.loads(state_path.read_text(encoding="utf-8"))
                preliminary_path = state_path.parent / "preliminary-telemetry.json"
                preliminary_bytes = preliminary_path.read_bytes()
                preliminary = json.loads(preliminary_bytes)
                self.assertEqual(returncode, 0)
                full_run.assert_called_once()
                self.assertEqual(
                    state["routing"]["decision"]["selected_route"], selected_route
                )
                self.assertEqual(
                    state["routing"]["completion_behavior"], "screen_then_full"
                )
                self.assertTrue(state["reports"]["preliminary"]["available"])
                self.assertTrue(state["reports"]["final"]["available"])
                self.assertEqual(preliminary_bytes, frozen_preliminary)
                self.assertEqual(
                    state["reports"]["preliminary"]["sha256"],
                    hashlib.sha256(preliminary_bytes).hexdigest(),
                )
                json_evidence_paths = {
                    item["source"]["path"]
                    for item in preliminary["delivery_report"]["evidence"]
                    if (item.get("source") or {}).get("type") == "json"
                }
                self.assertIn(
                    ".bioagent/preliminary-telemetry.json", json_evidence_paths
                )
                self.assertEqual(
                    json_evidence_paths,
                    {".bioagent/preliminary-telemetry.json"},
                )
                self.assertIn(
                    preliminary["delivery_report"]["report_kind"],
                    {"preliminary_alert", "preliminary_screen"},
                )
                self.assertNotIn("skipped", {step["status"] for step in state["steps"]})

    def test_auto_full_failure_keeps_preliminary_report(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = self._write_adaptive_state(Path(temp_dir) / "run", mode="auto")
            screening = {
                "sample_fraction": 0.05,
                "known_virus_high_confidence": True,
                "samples": [],
            }
            admitted = {"admission_ok": True, "reason_codes": []}

            def fail_full(_args):
                state = json.loads(state_path.read_text(encoding="utf-8"))
                state["execution"].update(
                    {"status": "failed", "ok": False, "returncode": 7}
                )
                state.setdefault("reports", {})["final"] = {
                    "status": "unavailable",
                    "available": False,
                    "schema_version": 4,
                    "telemetry": str(state_path.parent / "telemetry.json"),
                }
                state_path.write_text(json.dumps(state), encoding="utf-8")
                (state_path.parent / "telemetry.json").write_text(
                    json.dumps({"version": 3}), encoding="utf-8"
                )
                return 7

            with (
                patch(
                    "analysis_runner.ResourceCalibrator.history_summary",
                    return_value={},
                ),
                patch(
                    "analysis_runner.collect_resource_snapshot",
                    side_effect=[admitted.copy(), admitted.copy()],
                ),
                patch("analysis_runner.run_rapid_screen", return_value=screening),
                patch("analysis_runner.run", side_effect=fail_full),
            ):
                returncode = run(argparse.Namespace(state=str(state_path)))

            state = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(returncode, 7)
            self.assertTrue(state["reports"]["preliminary"]["available"])
            self.assertFalse(state["reports"]["final"]["available"])
            self.assertTrue(
                (state_path.parent / "preliminary-telemetry.json").is_file()
            )

    def test_route_b_resource_failure_is_blocked_without_queueing(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = self._write_adaptive_state(Path(temp_dir) / "run", mode="auto")
            screening = {
                "sample_fraction": 0.05,
                "known_virus_high_confidence": False,
                "samples": [],
            }
            admitted = {"admission_ok": True, "reason_codes": []}
            blocked = {"admission_ok": False, "reason_codes": ["MEMORY_INSUFFICIENT"]}
            with (
                patch(
                    "analysis_runner.ResourceCalibrator.history_summary",
                    return_value={},
                ),
                patch(
                    "analysis_runner.collect_resource_snapshot",
                    side_effect=[admitted, blocked],
                ),
                patch("analysis_runner.run_rapid_screen", return_value=screening),
            ):
                returncode = run(argparse.Namespace(state=str(state_path)))

            state = json.loads(state_path.read_text(encoding="utf-8"))
            telemetry = json.loads(
                (state_path.parent / "telemetry.json").read_text(encoding="utf-8")
            )
            routing = json.loads(
                (state_path.parent / "routing.json").read_text(encoding="utf-8")
            )

        self.assertEqual(returncode, 2)
        self.assertEqual(state["execution"]["status"], "blocked")
        self.assertEqual(routing["decision"]["selected_route"], "B")
        self.assertEqual(routing["decision"]["report_kind"], "preliminary_screen")
        self.assertIn("MEMORY_INSUFFICIENT", routing["decision"]["reason_codes"])
        self.assertEqual(
            routing["resources"]["full_analysis"]["requirement_source"], "minimum"
        )
        self.assertTrue(state["reports"]["preliminary"]["available"])
        self.assertFalse(state["reports"]["final"]["available"])
        self.assertEqual(telemetry["delivery_report"]["kind"], "analysis_pipeline")
        self.assertEqual(
            telemetry["delivery_report"]["report_kind"], "preliminary_screen"
        )

    def test_calibrated_requirement_still_blocks_when_reserve_is_unavailable(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = self._write_adaptive_state(Path(temp_dir) / "run", mode="auto")
            state = json.loads(state_path.read_text(encoding="utf-8"))
            state["resource_estimate"] = {
                "status": "calibrated",
                "cpu_cores": 16,
                "peak_memory_mb": {"reserve": 96 * 1024},
            }
            state_path.write_text(json.dumps(state), encoding="utf-8")
            screening = {
                "sample_fraction": 0.05,
                "known_virus_high_confidence": False,
                "samples": [],
            }
            admitted = {"admission_ok": True, "reason_codes": []}
            blocked = {
                "admission_ok": False,
                "reason_codes": ["MEMORY_INSUFFICIENT"],
            }
            with (
                patch(
                    "analysis_runner.ResourceCalibrator.history_summary",
                    return_value={},
                ),
                patch(
                    "analysis_runner.collect_resource_snapshot",
                    side_effect=[admitted, blocked],
                ) as snapshot,
                patch("analysis_runner.run_rapid_screen", return_value=screening),
            ):
                returncode = run(argparse.Namespace(state=str(state_path)))

            routing = json.loads(
                (state_path.parent / "routing.json").read_text(encoding="utf-8")
            )

        self.assertEqual(returncode, 2)
        self.assertEqual(snapshot.call_args_list[1].kwargs["required_threads"], 16)
        self.assertEqual(
            snapshot.call_args_list[1].kwargs["required_memory_mb"],
            96 * 1024,
        )
        self.assertEqual(
            routing["resources"]["full_analysis"]["requirement_source"],
            "calibrated",
        )

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

    def test_workflow2_instrumentation_adds_benchmarks_and_machine_readable_tables(
        self,
    ):
        with tempfile.TemporaryDirectory() as temp_dir:
            workflow = Path(temp_dir)
            rules = workflow / "rules"
            scripts = workflow / "scripts"
            rules.mkdir()
            scripts.mkdir()
            (rules / "upstream_rename.smk").write_text(
                'rule get_orfs:\n    shell:\n        "true"\n',
                encoding="utf-8",
            )
            (rules / "summarize.smk").write_text(
                """rule summarize_tables:
\toutput:
\t\tvertvirus_seqid = os.path.join(config['wdir'], 'summary/sequences/vert.seqids'),
\tparams:
\t\tscript = config['scripts']['table_summarize']
\tshell:
\t\t"true"
""",
                encoding="utf-8",
            )
            (scripts / "table_summarize.py").write_text(
                "fullinfo = None\n", encoding="utf-8"
            )
            (scripts / "iterativehmmsearch.py").write_text(
                """
import os

def iterative_hmmsearch(query, wdir, ncpu, iteration_num, evalue, motifcov):
        sys.stderr.write(curtime()+'[INFO] hmmsearch done, %s sequences hit (Time Elapsed: %.3fs)\\n' % (num_hitseq, _t3-_t2))
        sys.stderr.write(curtime()+"[INFO] %d sequences covered >%d%% RdRp motif region (Time Elapsed: %.3fs)\\n" % (num_filtered, int(100*motifcov), _t5-_t4))
""".lstrip(),
                encoding="utf-8",
            )

            _instrument_workflow2(workflow, workflow / ".bioagent" / "benchmarks")

            upstream = (rules / "upstream_rename.smk").read_text(encoding="utf-8")
            summary = (rules / "summarize.smk").read_text(encoding="utf-8")
            script = (scripts / "table_summarize.py").read_text(encoding="utf-8")
            iterative = (scripts / "iterativehmmsearch.py").read_text(encoding="utf-8")
            namespace = {}
            exec(
                compile(iterative, str(scripts / "iterativehmmsearch.py"), "exec"),
                namespace,
            )
            empty_output = workflow / "empty-search"
            namespace["finish_empty_search"](empty_output, 1, 5)
            empty_targets = [
                empty_output / f"Iteration_{iteration}" / "hmmsearch.domtblout"
                for iteration in range(2, 6)
            ]
            empty_targets_exist = all(path.is_file() for path in empty_targets)

        self.assertIn("get_orfs.{part}.jsonl", upstream)
        self.assertIn("summarize_tables.jsonl", summary)
        self.assertIn("virus_candidates_csv", summary)
        self.assertIn("taxonomy_tsv", summary)
        self.assertIn("virus_candidates.csv", script)
        self.assertIn("vertebrate_virus_representatives.csv", script)
        self.assertIn("nr_vote_vfamily", script)
        self.assertIn("Unclassified_viral_contig", script)
        self.assertIn("taxonomy.tsv", script)
        self.assertIn("def finish_empty_search", iterative)
        self.assertIn("if int(num_hitseq) == 0:", iterative)
        self.assertIn("if num_filtered == 0:", iterative)
        self.assertTrue(empty_targets_exist)

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
            (benchmark_dir / "merge.sample.jsonl").write_text(
                json.dumps(
                    {
                        "jobid": 2,
                        "rule_name": "merge",
                        "s": 0.01,
                        "cpu_time": "NA",
                        "max_rss": "NA",
                        "resources": {"tmp_mb": "NA"},
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            records = _parse_seqkit_table(stats, "raw_input")
            jobs = _parse_benchmarks(benchmark_dir)
            stages = _stage_records(
                {
                    "steps": [
                        {
                            "id": "merge",
                            "label": "样本合并",
                            "rules": ["merge"],
                            "status": "completed",
                        }
                    ]
                },
                jobs,
                [],
            )

        self.assertEqual(records[0]["median"], 100)
        self.assertEqual((records[0]["q1"], records[0]["q3"]), (90, 110))
        self.assertEqual(jobs[0]["rule_name"], "fastp")
        self.assertEqual(jobs[0]["cpu_time"], 5.0)
        self.assertEqual(jobs[0]["params"]["api_key"], "[REDACTED]")
        self.assertIsNone(jobs[1]["cpu_time"])
        self.assertIsNone(jobs[1]["max_rss"])
        self.assertIsNone(jobs[1]["resources"]["tmp_mb"])
        self.assertEqual(stages[0]["cpu_time_seconds"], 0)
        self.assertIsNone(stages[0]["max_job_rss_mb"])

    def test_preserves_each_rrna_alignment_rate_without_guessing_an_average(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            log = Path(temp_dir) / "execution.log"
            log.write_text(
                "12.5% overall alignment rate\n31.75% overall alignment rate\n",
                encoding="utf-8",
            )

            parsed = _parse_log(log)

        self.assertEqual(parsed["rrna_alignment_percents"], [12.5, 31.75])

    def test_log_jobs_keep_phase_inputs_outputs_and_classification_commands(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            log = Path(temp_dir) / "classification.log"
            log.write_text(
                "[Thu Jul 30 10:00:00 2026]\n"
                "rule blastx_nr:\n"
                "    input: candidates.fna, nr\n"
                "    output: blastx_nr.tsv\n"
                "    jobid: 7\n"
                "    /tools/blastx -query candidates.fna -db nr --out blastx_nr.tsv\n"
                "[Thu Jul 30 10:01:00 2026]\n"
                "Finished jobid: 7 (Rule: blastx_nr)\n",
                encoding="utf-8",
            )

            parsed = _parse_log(log, "classification")

        self.assertEqual(parsed["jobs"][0]["phase"], "classification")
        self.assertEqual(parsed["jobs"][0]["inputs"], ["candidates.fna", "nr"])
        self.assertEqual(parsed["jobs"][0]["outputs"], ["blastx_nr.tsv"])
        self.assertEqual(
            parsed["jobs"][0]["commands"],
            ["/tools/blastx -query candidates.fna -db nr --out blastx_nr.tsv"],
        )
        self.assertEqual(parsed["jobs"][0]["status"], "completed")
        self.assertRegex(
            parsed["jobs"][0]["started_at"], r"2026-07-30T10:00:00[+-]\d\d:\d\d"
        )

    def test_stage_times_are_chronological_across_midnight(self):
        jobs = [
            {
                "jobid": 1,
                "rule_name": "blastx_nr",
                "s": 43_931.5,
                "cpu_time": 87_840.0,
                "max_rss": 1024,
            }
        ]
        log_jobs = [
            {
                "job_id": "1",
                "rule": "blastx_nr",
                "started_at": "Thu Aug 13 16:45:01 2026",
                "finished_at": "Fri Aug 14 04:57:01 2026",
                "status": "completed",
            }
        ]
        stages = _stage_records(
            {
                "steps": [
                    {
                        "id": "classification",
                        "label": "分类证据分析",
                        "rules": ["blastx_nr"],
                        "status": "running",
                    }
                ]
            },
            jobs,
            log_jobs,
        )

        stage = stages[0]
        self.assertRegex(stage["started_at"], r"2026-08-13T16:45:01[+-]\d\d:\d\d")
        self.assertRegex(stage["finished_at"], r"2026-08-14T04:57:01[+-]\d\d:\d\d")
        self.assertEqual(stage["wall_seconds"], 43_920)
        self.assertEqual(stage["job_wall_seconds"], 43_931.5)
        self.assertEqual(stage["effective_cores"], 2.0)

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
                state_payload = json.loads(state_path.read_text(encoding="utf-8"))
                history_path = root / "resource-history.sqlite3"
                state_payload["resource_history_db"] = str(history_path)
                state_path.write_text(
                    json.dumps(state_payload, ensure_ascii=False), encoding="utf-8"
                )
                source = self._write_workflow_source(root)
                command = root / "snakemake"
                command.write_text(
                    (
                        "#!/bin/sh\n"
                        f'case ":$PATH:" in *":{root}:"*) ;; *) exit 23 ;; esac\n'
                        "printf 'partial output\\n'\n"
                        f"exit {exit_code}\n"
                    ),
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
                self.assertIn("--benchmark-extended", telemetry["runner_commands"][0])
                self.assertIn("--printshellcmds", telemetry["runner_commands"][0])
                self.assertGreaterEqual(telemetry["resource_usage"]["samples"], 2)
                self.assertIn(
                    telemetry["resource_usage"]["load_bucket"], {"0", "1", "2+"}
                )
                self.assertEqual(telemetry["resource_profile"]["version"], 1)
                self.assertEqual(
                    telemetry["resource_profile"]["rules"],
                    ["fastp", "bowtie2", "merge"],
                )
                self.assertTrue(state["technical_report"]["available"])
                self.assertEqual(state["reports"]["final"]["available"], exit_code == 0)
                self.assertEqual(state["reports"]["final"]["schema_version"], 4)
                self.assertEqual(
                    telemetry["delivery_report"]["schema_version"],
                    4,
                )
                selected_stage_statuses = [
                    stage["status"]
                    for stage in telemetry["delivery_report"]["sections"]["upstream"][
                        "stages"
                    ]
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
                with sqlite3.connect(history_path) as connection:
                    observation = connection.execute(
                        "SELECT terminal_status FROM run_observations"
                    ).fetchone()
                self.assertEqual(observation, (expected_status,))
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
            clean_read = (
                run_dir
                / "results"
                / "upstream"
                / "filter"
                / "run"
                / "cleanreads_r1.fq.gz"
            )
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
            state["execution_spec"]["inputs"][0]["records"][0]["files"]["read1"] = str(
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
        self.assertFalse(state["reports"]["final"]["available"])
        self.assertIn("does not exist", telemetry["error"])

    def test_multiphase_execution_stops_after_first_failure(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            run_dir = root / "run"
            bioagent = run_dir / ".bioagent"
            bioagent.mkdir(parents=True)
            input_path = root / "contigs.fa"
            input_path.write_text(">contig\nACGT\n", encoding="utf-8")
            state_path = bioagent / "state.json"
            state_path.write_text(
                json.dumps(
                    {
                        "version": 3,
                        "run_id": "analysis-test",
                        "run_dir": str(run_dir),
                        "openwebui_chat_id": "chat-test",
                        "execution_spec": self._contig_spec(input_path),
                        "steps": [],
                        "execution": {
                            "status": "running",
                            "ok": None,
                            "log": str(bioagent / "execution.log"),
                        },
                    }
                ),
                encoding="utf-8",
            )
            workflows = []
            for phase_id in ("upstream", "classification"):
                workflow = run_dir / "workflows" / phase_id
                (workflow / "rules").mkdir(parents=True)
                (workflow / "data").mkdir()
                (workflow / "rules" / "upstream.smk").write_text(
                    'rule fastp:\n    shell:\n        "true"\n',
                    encoding="utf-8",
                )
                workflows.append(workflow)
            marker = root / "second-phase-ran"
            first_command = root / "phase-one"
            first_command.write_text(
                "#!/bin/sh\nprintf 'first failed\\n'\nexit 7\n",
                encoding="utf-8",
            )
            first_command.chmod(0o755)
            second_command = root / "phase-two"
            second_command.write_text(
                f"#!/bin/sh\ntouch {marker}\nexit 0\n",
                encoding="utf-8",
            )
            second_command.chmod(0o755)
            seqkit = self._write_seqkit(root)
            phases = (
                PreparedPhase(
                    id="upstream",
                    workflow_id="workflow1",
                    workflow_dir=workflows[0],
                    results_dir=run_dir / "results" / "upstream",
                    command=[str(first_command)],
                    selected_rules=("fastp",),
                ),
                PreparedPhase(
                    id="classification",
                    workflow_id="workflow1",
                    workflow_dir=workflows[1],
                    results_dir=run_dir / "results" / "classification",
                    command=[str(second_command)],
                    selected_rules=("fastp",),
                ),
            )
            prepared = PreparedExecution(
                workflow_id="analysis_pipeline",
                phases=phases,
                seqkit=str(seqkit),
                selected_rules=("fastp",),
            )

            with patch("analysis_runner.prepare_execution", return_value=prepared):
                result = run(argparse.Namespace(state=str(state_path)))
            telemetry = json.loads(
                (bioagent / "telemetry.json").read_text(encoding="utf-8")
            )
            marker_exists = marker.exists()

        self.assertEqual(result, 7)
        self.assertFalse(marker_exists)
        self.assertEqual(
            [(phase["id"], phase["status"]) for phase in telemetry["phases"]],
            [("upstream", "failed")],
        )

    def test_multiphase_execution_starts_second_only_after_first_completes(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            run_dir = root / "run"
            bioagent = run_dir / ".bioagent"
            bioagent.mkdir(parents=True)
            state_path = bioagent / "state.json"
            state_path.write_text(
                json.dumps(
                    {
                        "version": 3,
                        "run_id": "analysis-test",
                        "run_dir": str(run_dir),
                        "openwebui_chat_id": "chat-test",
                        "execution_spec": self._contig_spec(root / "contigs.fa", "RNA"),
                        "steps": [],
                        "execution": {
                            "status": "running",
                            "ok": None,
                            "log": str(bioagent / "execution.log"),
                        },
                    }
                ),
                encoding="utf-8",
            )
            (root / "contigs.fa").write_text(">contig\nACGT\n", encoding="utf-8")
            workflows = []
            for phase_id in ("upstream", "classification"):
                workflow = run_dir / "workflows" / phase_id
                (workflow / "rules").mkdir(parents=True)
                (workflow / "data").mkdir()
                (workflow / "rules" / "upstream.smk").write_text(
                    'rule fastp:\n    shell:\n        "true"\n',
                    encoding="utf-8",
                )
                workflows.append(workflow)
            upstream_done = root / "upstream-done"
            classification_started = root / "classification-started"
            first_command = root / "phase-one"
            first_command.write_text(
                f"#!/bin/sh\ntouch {upstream_done}\nexit 0\n",
                encoding="utf-8",
            )
            first_command.chmod(0o755)
            second_command = root / "phase-two"
            second_command.write_text(
                (
                    "#!/bin/sh\n"
                    f"test -f {upstream_done} || exit 99\n"
                    f"touch {classification_started}\n"
                    "exit 9\n"
                ),
                encoding="utf-8",
            )
            second_command.chmod(0o755)
            phases = (
                PreparedPhase(
                    id="upstream",
                    workflow_id="workflow1",
                    workflow_dir=workflows[0],
                    results_dir=run_dir / "results" / "upstream",
                    command=[str(first_command)],
                    selected_rules=("fastp",),
                ),
                PreparedPhase(
                    id="classification",
                    workflow_id="workflow1",
                    workflow_dir=workflows[1],
                    results_dir=run_dir / "results" / "classification",
                    command=[str(second_command)],
                    selected_rules=("fastp",),
                ),
            )
            prepared = PreparedExecution(
                workflow_id="analysis_pipeline",
                phases=phases,
                seqkit=str(self._write_seqkit(root)),
                selected_rules=("fastp",),
            )

            with patch("analysis_runner.prepare_execution", return_value=prepared):
                result = run(argparse.Namespace(state=str(state_path)))
            telemetry = json.loads(
                (bioagent / "telemetry.json").read_text(encoding="utf-8")
            )
            classification_started_exists = classification_started.exists()

        self.assertEqual(result, 9)
        self.assertTrue(classification_started_exists)
        self.assertEqual(
            [(phase["id"], phase["status"]) for phase in telemetry["phases"]],
            [("upstream", "completed"), ("classification", "failed")],
        )

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
            deadline = time.monotonic() + 30
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
        self.assertEqual(state["phases"][0]["status"], "cancelled")
        self.assertEqual(telemetry["phases"][0]["status"], "cancelled")
        self.assertTrue(state["technical_report"]["available"])
        self.assertFalse(state["reports"]["final"]["available"])

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
            execution_spec = normalize_execution_spec(
                {
                    "version": 3,
                    "adapter": "analysis_pipeline",
                    "inputs": [
                        {
                            "id": "primary",
                            "type": "raw_paired_reads",
                            "records": [
                                {
                                    "run_id": record["run_id"],
                                    "sample_id": record["sample_id"],
                                    "files": {
                                        "read1": record["read1"],
                                        "read2": record["read2"],
                                    },
                                }
                                for record in records
                            ],
                        }
                    ],
                    "deliverables": ["rrna_filter"],
                    "policy": {"mode": "standard"},
                }
            )
            state = {
                "execution_spec": execution_spec,
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
                                    / "upstream"
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
                                    / "upstream"
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
                    / "upstream"
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
                        / "upstream"
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
            "fastp": ("raw_paired_reads", "raw", None),
            "bowtie2": ("qc_paired_reads", "post_fastp", "fastp"),
            "merge": ("clean_run_paired_reads", "post_rrna", "bowtie2"),
            "megahit": ("clean_sample_paired_reads", "merged_reads", "merge"),
            "rename_filter": ("assembled_contigs", "assembly", "megahit"),
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            for entry_rule, (
                input_type,
                input_stage,
                provided_rule,
            ) in cases.items():
                fields = {"sample_id": "sample"}
                if input_type in {
                    "raw_paired_reads",
                    "qc_paired_reads",
                    "clean_run_paired_reads",
                }:
                    fields["run_id"] = "run"
                if entry_rule == "rename_filter":
                    fields["files"] = {"fasta": str(root / "contigs.fa")}
                else:
                    fields["files"] = {
                        "read1": str(root / "r1.fq"),
                        "read2": str(root / "r2.fq"),
                    }
                execution_spec = normalize_execution_spec(
                    {
                        "version": 3,
                        "adapter": "analysis_pipeline",
                        "inputs": [
                            {
                                "id": "primary",
                                "type": input_type,
                                "records": [fields],
                            }
                        ],
                        "deliverables": ["contig_filter"],
                        "policy": {"mode": "standard"},
                    }
                )
                state = {
                    "execution_spec": execution_spec,
                    "steps": [
                        {**step, "status": "completed"}
                        for step in canonical_steps(execution_spec)
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

    def test_classification_report_parses_counts_evidence_and_sample(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = Path(temp_dir)
            tables = run_dir / "results" / "virus-identification" / "summary" / "tables"
            tables.mkdir(parents=True)
            header = (
                "qseqid,qlen,viral_type,checkv_quality,completeness,"
                "contamination,clusid,nr_svorder,nr_svfamily,nr_svgenus,"
                "nr_besthit,nr_pident,approval_rate\n"
            )
            row = (
                "sample-a-contig-1,1234,RNA,High-quality,98.5,0.2,c1,"
                "OrderX,FamilyX,GenusX,HitX,91.2,0.8\n"
            )
            for name, content in {
                "virus_candidates.csv": header + row + row,
                "virus_representatives.csv": header + row,
                "vertebrate_virus_candidates.csv": header + row,
                "vertebrate_virus_representatives.csv": header + row,
            }.items():
                (tables / name).write_text(content, encoding="utf-8")
            state = {
                "execution_spec": normalize_execution_spec(
                    {
                        "version": 3,
                        "adapter": "analysis_pipeline",
                        "inputs": [
                            {
                                "id": "primary",
                                "type": "contigs_fasta",
                                "records": [
                                    {
                                        "sample_id": "sample-a",
                                        "files": {"fasta": "/input/contigs.fa"},
                                    }
                                ],
                            }
                        ],
                        "deliverables": ["virus_summary"],
                        "policy": {"mode": "standard"},
                    }
                )
            }

            report = _classification_report_data(state, run_dir)

        self.assertEqual(report["candidate_count"], 2)
        self.assertEqual(report["representative_count"], 1)
        self.assertEqual(report["vertebrate_candidate_count"], 1)
        self.assertEqual(report["representatives"][0]["sample_id"], "sample-a")
        self.assertEqual(report["representatives"][0]["nr_svfamily"], "FamilyX")
        self.assertEqual(report["representatives"][0]["completeness"], 98.5)
        self.assertTrue(
            report["artifacts"]["完整证据表（XLSX）"].endswith("fullinfo.xlsx")
        )
        self.assertTrue(
            report["artifacts"]["病毒候选序列（FASTA）"].endswith(
                "putative_viralcontigs.fna"
            )
        )

    def test_report_evidence_links_conclusions_to_raw_classification_table(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = Path(temp_dir)
            tables = run_dir / "results/virus-identification/summary/tables"
            tables.mkdir(parents=True)
            candidates = tables / "virus_candidates.csv"
            candidates.write_text("qseqid\ncontig-1\n", encoding="utf-8")
            delivery = {
                "schema_version": 4,
                "kind": "analysis_pipeline",
                "sections": {
                    "virus_identification": {
                        "candidate_count": 1,
                        "representative_count": 0,
                        "vertebrate_candidate_count": 0,
                        "vertebrate_representative_count": 0,
                        "representatives": [],
                        "tables": {
                            "candidates": str(candidates),
                            "representatives": str(
                                tables / "virus_representatives.csv"
                            ),
                            "vertebrate_candidates": str(
                                tables / "vertebrate_virus_candidates.csv"
                            ),
                            "vertebrate_representatives": str(
                                tables / "vertebrate_virus_representatives.csv"
                            ),
                        },
                    }
                },
                "stages": [],
                "manifest": {},
            }

            result = _attach_report_evidence(delivery, {}, run_dir)

        evidence = next(
            item
            for item in result["evidence"]
            if item["report_pointer"]
            == "/sections/virus_identification/candidate_count"
        )
        self.assertEqual(evidence["source"]["type"], "delimited_count")
        self.assertEqual(
            evidence["source"]["path"],
            "results/virus-identification/summary/tables/virus_candidates.csv",
        )
        self.assertEqual(result["conclusions"][0]["evidence_ids"], [evidence["id"]])

    def test_pipeline_report_extracts_branch_specific_metrics(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = Path(temp_dir)
            annotation_input = run_dir / "input-virus.fna"
            annotation_input.write_text(">v1\nAC\n>v2\nTG\n", encoding="utf-8")

            host_report = (
                run_dir / "results/host-identification/HostIdentification_result.xlsx"
            )
            host_report.parent.mkdir(parents=True)
            with zipfile.ZipFile(host_report, "w") as archive:
                archive.writestr(
                    "xl/worksheets/sheet1.xml",
                    "<worksheet><sheetData><row/><row/><row/></sheetData></worksheet>",
                )

            microbial = run_dir / "results/microbial-profile/results/summary"
            microbial.mkdir(parents=True)
            for level in ("species", "genus", "family"):
                (microbial / f"combined_{level}.txt").write_text(
                    "taxonomy_id\tname\tsample-a_frac\n"
                    "1\tTaxon A\t0.25\n2\tTaxon B\t0.75\n",
                    encoding="utf-8",
                )

            quant = run_dir / "results/virus-quantification/results/summary/rpm.csv"
            quant.parent.mkdir(parents=True)
            quant.write_text(
                "sample_id,reference_id,RPM\n"
                "sample-a,v1,120.5\nsample-a,v2,8\nsample-b,v1,64\n",
                encoding="utf-8",
            )

            annotation = run_dir / "results/virus-annotation/results"
            (annotation / "summary").mkdir(parents=True)
            (annotation / "checkv").mkdir()
            (annotation / "function/dramv").mkdir(parents=True)
            (annotation / "summary/strategy.tsv").write_text(
                "sequence\tstrategy\nv1\tprovirus\nv2\tstandard\n",
                encoding="utf-8",
            )
            (annotation / "checkv/filtered_viral_gt50.fna").write_text(
                ">v1\nAC\n", encoding="utf-8"
            )
            (annotation / "function/dramv/annotations.tsv").write_text(
                "sequence\tfunction\nv1\tcapsid\n", encoding="utf-8"
            )
            (annotation / "summary/consistency_report.tsv").write_text(
                "sequence\twarning\nv2\ttaxonomy conflict\n", encoding="utf-8"
            )

            state = {
                "execution_spec": {
                    "deliverables": [
                        "host_summary",
                        "microbe_summary",
                        "virus_rpm",
                        "annotation_delivery",
                    ],
                    "stages": [
                        {"id": "host_summary"},
                        {"id": "microbe_summary"},
                        {"id": "virus_rpm"},
                        {"id": "annotation_delivery"},
                    ],
                    "inputs": [
                        {
                            "id": "reads",
                            "type": "clean_sample_paired_reads",
                            "records": [
                                {
                                    "sample_id": "sample-a",
                                    "files": {
                                        "read1": "/input/a.1.fq",
                                        "read2": "/input/a.2.fq",
                                    },
                                },
                                {
                                    "sample_id": "sample-b",
                                    "files": {
                                        "read1": "/input/b.1.fq",
                                        "read2": "/input/b.2.fq",
                                    },
                                },
                            ],
                        },
                        {
                            "id": "virus",
                            "type": "virus_reference_fasta",
                            "records": [{"files": {"fasta": str(annotation_input)}}],
                        },
                    ],
                }
            }

            report = _pipeline_report_data(
                state,
                run_dir,
                {
                    "stages": [{"id": "host_summary", "status": "completed"}],
                    "manifest": {"files": []},
                },
            )

        self.assertEqual(report["kind"], "analysis_pipeline")
        host = report["sections"]["host_identification"]
        self.assertEqual(host["sample_count"], 2)
        self.assertEqual(host["identified_result_count"], 2)
        microbial_report = report["sections"]["microbial_profile"]
        self.assertEqual(microbial_report["species"]["taxa_count"], 2)
        self.assertEqual(
            microbial_report["species"]["top_taxa"]["sample-a"]["name"],
            "Taxon B",
        )
        quant_report = report["sections"]["virus_quantification"]
        self.assertEqual(quant_report["sample_count"], 2)
        self.assertEqual(quant_report["reference_count"], 2)
        self.assertEqual(quant_report["max_rpm"], 120.5)
        annotation_report = report["sections"]["virus_annotation"]
        self.assertEqual(annotation_report["input_sequence_count"], 2)
        self.assertEqual(annotation_report["filtered_sequence_count"], 1)
        self.assertEqual(
            annotation_report["strategy_distribution"],
            {"provirus": 1, "standard": 1},
        )
        self.assertEqual(annotation_report["functional_annotation_count"], 1)
        self.assertEqual(annotation_report["consistency_warning_count"], 1)


if __name__ == "__main__":
    unittest.main()

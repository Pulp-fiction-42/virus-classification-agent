from __future__ import annotations

import copy
import json
import tempfile
import unittest
from collections import Counter
from pathlib import Path
from unittest.mock import patch

from jsonschema import Draft202012Validator

from pipeline_spec import ExecutionSpecError, compile_execution_spec
from priority_reports import (
    applicable_claims,
    build_fact_packet,
    build_reasoning_plan,
    build_skill_comparison,
    generate_priority_reports,
    load_literature,
    reasoning_request,
    render_scientific_html,
    report_artifact,
)
from tool_library import (
    ToolLibraryError,
    build_tool_contract,
    build_tool_results,
    observe_tool_versions,
    public_tool_catalog,
    resolve_tool,
)
from workflow_skills import load_workflow_skill


def _raw_input() -> dict:
    return {
        "id": "primary",
        "type": "raw_paired_reads",
        "records": [
            {
                "record_id": "sample-a",
                "sample_id": "sample-a",
                "run_id": "run-a",
                "files": {
                    "read1": "/data/sample-a_1.fastq.gz",
                    "read2": "/data/sample-a_2.fastq.gz",
                },
            }
        ],
    }


def _compile_read_qc(length_required: int | None = None) -> dict:
    values = {} if length_required is None else {"length_required": length_required}
    return compile_execution_spec(
        {
            "version": 8,
            "adapter": "analysis_pipeline",
            "inputs": [_raw_input()],
            "deliverables": ["read_qc"],
            "options": {
                "viral_type": "RNA+DNA",
                "capability_parameters": (
                    [
                        {
                            "capability_id": "workflow1.read_qc.fastp",
                            "manifest_version": 1,
                            "preset_id": "workflow_default",
                            "values": values,
                        }
                    ]
                    if values
                    else []
                ),
            },
            "workflow_skill": {
                "skill_id": "bioagent-w1-read-qc",
                "skill_version": "1.0.0",
                "preset_id": "workflow_default",
            },
            "policy": {"mode": "standard"},
        }
    )


def _report_fixture(root: Path) -> tuple[Path, dict, dict]:
    run_dir = root / "analysis-priority-test"
    state_path = run_dir / ".bioagent" / "state.json"
    telemetry_path = state_path.parent / "telemetry.json"
    state_path.parent.mkdir(parents=True)
    telemetry = {
        "delivery_report": {
            "sections": {
                "virus_identification": {
                    "sample_ids": ["sample-a"],
                    "representatives": [
                        {
                            "qseqid": "sample-a-contig-1",
                            "nr_svfamily": "Orthomyxoviridae",
                            "nr_sscinames": "Influenza A virus (A/duck/test/H6N2)",
                            "nr_besthit": "A/duck/test/H6N2",
                            "checkv_quality": "High-quality",
                        }
                    ],
                }
            },
            "evidence": [
                {
                    "report_pointer": "/sections/upstream/samples/0/fastp/reads_retained_percent",
                    "value": 92.5,
                    "unit": "percent",
                },
                {
                    "report_pointer": "/sections/virus_identification/candidate_count",
                    "value": 4,
                    "unit": "sequences",
                },
                {
                    "report_pointer": "/sections/virus_identification/representatives/0/completeness",
                    "value": 98.2,
                    "unit": "percent",
                },
            ],
        }
    }
    state = {
        "run_id": run_dir.name,
        "run_dir": str(run_dir),
        "execution": {
            "status": "completed",
            "ok": True,
            "finished_at": "2026-09-30T12:00:00+08:00",
        },
        "execution_spec": {
            "stages": [{"id": "read_qc"}, {"id": "virus_summary"}],
            "workflow_skill": _compile_read_qc()["workflow_skill"],
        },
        "technical_report": {"telemetry": str(telemetry_path)},
        "tool_results": [
            {
                "schema_version": 1,
                "tool_id": "fastp",
                "requested_version": "1.3.2",
                "observed_version": "1.3.2",
                "invocation_adapter": "workflow1.read_qc.fastp",
                "parameters": {"length_required": 50},
                "inputs": [{"artifact_type": "raw_paired_reads", "sha256": "a" * 64}],
                "outputs": [{"artifact_type": "qc_paired_reads", "sha256": "b" * 64}],
                "metrics": [],
                "status": "completed",
            }
        ],
    }
    state_path.write_text(json.dumps(state), encoding="utf-8")
    telemetry_path.write_text(json.dumps(telemetry), encoding="utf-8")
    return state_path, state, telemetry


class ToolAndSkillTests(unittest.TestCase):
    def test_tool_catalog_is_path_free_and_rejects_unknown_or_wrong_version(self):
        catalog = public_tool_catalog()
        self.assertEqual(catalog[0]["id"], "fastp")
        self.assertNotIn("executable", catalog[0])
        with self.assertRaises(ToolLibraryError) as missing:
            resolve_tool("not-registered", "1.0")
        self.assertEqual(missing.exception.code, "TOOL_NOT_FOUND")
        with self.assertRaises(ToolLibraryError) as mismatch:
            resolve_tool("fastp", "0.0.0")
        self.assertEqual(mismatch.exception.code, "TOOL_VERSION_MISMATCH")

    def test_worker_version_probe_returns_structured_observation(self):
        completed = type(
            "Completed", (), {"returncode": 0, "stdout": "fastp 1.3.2\n", "stderr": ""}
        )()
        with patch("tool_library.subprocess.run", return_value=completed) as run:
            observed = observe_tool_versions(build_tool_contract(include_fastp=True))
        self.assertEqual(observed["fastp"]["observed_version"], "1.3.2")
        self.assertEqual(run.call_args.args[0][-1], "--version")

    def test_completed_tool_result_contains_frozen_artifact_identity_and_metrics(self):
        spec = _compile_read_qc()
        state = {
            "execution_spec": spec,
            "execution": {"ok": True},
            "dataset": {"id": "dataset-a", "revision": 3},
            "tool_observations": {
                "fastp": {
                    "requested_version": "1.3.2",
                    "observed_version": "1.3.2",
                }
            },
            "recovery_contract": {
                "dataset_sha256": "a" * 64,
                "phase_checkpoints": {
                    "upstream": {
                        "sha256": "b" * 64,
                        "bytes": 123,
                        "file_count": 2,
                    }
                },
            },
        }
        result = build_tool_results(
            state,
            {
                "fastp": [
                    {
                        "path": "/run/results/filter/run-a/01_fastp.json",
                        "summary": {"before_filtering": {"total_reads": 20}},
                        "filtering_result": {"passed_filter_reads": 18},
                    }
                ]
            },
        )[0]
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["inputs"][0]["record_ids"], ["run-a"])
        self.assertEqual(result["inputs"][0]["sha256"], "a" * 64)
        self.assertEqual(result["outputs"][0]["sha256"], "b" * 64)
        self.assertEqual(
            result["metrics"][0]["filtering_result"]["passed_filter_reads"], 18
        )

    def test_skill_default_and_controlled_override_compile_immutably(self):
        skill = load_workflow_skill()
        self.assertEqual(skill["id"], "bioagent-w1-read-qc")
        baseline = _compile_read_qc()
        modified = _compile_read_qc(80)
        self.assertEqual(
            baseline["workflow_skill"]["effective_parameters"]["length_required"], 50
        )
        self.assertEqual(modified["workflow_skill"]["overrides"], {"length_required": 80})
        self.assertNotEqual(
            baseline["workflow_skill"]["compiled_sha256"],
            modified["workflow_skill"]["compiled_sha256"],
        )
        with self.assertRaises(ExecutionSpecError) as raw_command:
            compile_execution_spec(
                {
                    "version": 8,
                    "adapter": "analysis_pipeline",
                    "inputs": [_raw_input()],
                    "deliverables": ["read_qc"],
                    "options": {
                        "viral_type": "RNA+DNA",
                        "capability_parameters": [
                            {
                                "capability_id": "workflow1.read_qc.fastp",
                                "manifest_version": 1,
                                "preset_id": "workflow_default",
                                "values": {"raw_cli": "--output /tmp/escape"},
                            }
                        ],
                    },
                    "policy": {"mode": "standard"},
                }
            )
        self.assertEqual(raw_command.exception.code, "PARAMETER_UNKNOWN")

    def test_compile_rejects_an_unknown_tool_contract(self):
        with self.assertRaises(ExecutionSpecError) as unknown:
            compile_execution_spec(
                {
                    "version": 8,
                    "adapter": "analysis_pipeline",
                    "inputs": [_raw_input()],
                    "deliverables": ["read_qc"],
                    "options": {"viral_type": "RNA+DNA"},
                    "tool_contract": {
                        "schema_version": 1,
                        "registry_sha256": "forged",
                        "entries": [{"id": "unknown", "version": "1.0"}],
                    },
                    "policy": {"mode": "standard"},
                }
            )
        self.assertEqual(unknown.exception.code, "TOOL_NOT_FOUND")
        with self.assertRaises(ExecutionSpecError) as mismatch:
            compile_execution_spec(
                {
                    "version": 8,
                    "adapter": "analysis_pipeline",
                    "inputs": [_raw_input()],
                    "deliverables": ["read_qc"],
                    "options": {"viral_type": "RNA+DNA"},
                    "tool_contract": {
                        "schema_version": 1,
                        "registry_sha256": "forged",
                        "entries": [{"id": "fastp", "version": "1.3.1"}],
                    },
                    "policy": {"mode": "standard"},
                }
            )
        self.assertEqual(mismatch.exception.code, "TOOL_VERSION_MISMATCH")

    def test_compile_rejects_an_unavailable_skill_version(self):
        with self.assertRaises(ExecutionSpecError) as unavailable:
            compile_execution_spec(
                {
                    "version": 8,
                    "adapter": "analysis_pipeline",
                    "inputs": [_raw_input()],
                    "deliverables": ["read_qc"],
                    "options": {"viral_type": "RNA+DNA"},
                    "workflow_skill": {
                        "skill_id": "bioagent-w1-read-qc",
                        "skill_version": "0.9.0",
                        "preset_id": "workflow_default",
                    },
                    "policy": {"mode": "standard"},
                }
            )
        self.assertEqual(unavailable.exception.code, "SKILL_VERSION_UNAVAILABLE")


class PriorityReportTests(unittest.TestCase):
    def test_literature_release_has_three_entries_per_required_category(self):
        literature = load_literature()
        counts = Counter(entry["category"] for entry in literature["entries"])
        self.assertEqual(counts, {"method": 3, "taxon": 3, "threshold": 3})
        for entry in literature["entries"]:
            self.assertTrue(entry["source_version"])
            self.assertTrue(entry["locator"])
            self.assertTrue(entry["summary"])
            self.assertEqual(len(entry["content_sha256"]), 64)

    def test_reasoning_context_contains_ids_only_and_plan_rejects_unknown_claims(self):
        with tempfile.TemporaryDirectory() as temporary:
            _, state, telemetry = _report_fixture(Path(temporary))
            packet = build_fact_packet(state, telemetry)
        literature = load_literature()
        context, schema, _ = reasoning_request(packet, literature)
        serialized = json.dumps(context, ensure_ascii=False)
        self.assertNotIn("92.5", serialized)
        self.assertNotIn("Influenza", serialized)
        self.assertNotIn("fastp performs", serialized)
        self.assertEqual(set(context), {"fact_ids", "source_ids", "claim_options"})
        expected_claims = [item["id"] for item in applicable_claims(packet, literature)]
        validator = Draft202012Validator(schema)
        self.assertTrue(validator.is_valid({"claim_ids": expected_claims}))
        self.assertFalse(
            validator.is_valid({"claim_ids": expected_claims, "number": 92.5})
        )
        self.assertFalse(validator.is_valid({"claim_ids": ["C-UNKNOWN"]}))
        self.assertEqual(
            packet["taxonomy"][0]["best_hit"],
            "Influenza A virus (A/duck/test/H6N2)",
        )
        with self.assertRaises(ValueError):
            build_reasoning_plan(
                packet,
                literature,
                ["C-UNKNOWN"],
                provider="test",
                model="test",
                backend_version="test",
            )

    def test_missing_literature_degrades_to_insufficient(self):
        with tempfile.TemporaryDirectory() as temporary:
            _, state, telemetry = _report_fixture(Path(temporary))
            packet = build_fact_packet(state, telemetry)
        literature = load_literature()
        literature["entries"] = [
            entry
            for entry in literature["entries"]
            if entry["id"] != "L-METHOD-FASTP"
        ]
        claims = applicable_claims(packet, literature)
        self.assertEqual(claims[0]["kind"], "insufficient")
        reasoning = build_reasoning_plan(
            packet,
            literature,
            [claim["id"] for claim in claims],
            provider="test",
            model="test",
            backend_version="test",
        )
        html = render_scientific_html(state, packet, literature, reasoning)
        self.assertIn("依据不足", html)
        self.assertNotIn("L-METHOD-FASTP ·", html)

    def test_uncovered_taxon_does_not_borrow_influenza_sources(self):
        with tempfile.TemporaryDirectory() as temporary:
            _, state, telemetry = _report_fixture(Path(temporary))
            representative = telemetry["delivery_report"]["sections"][
                "virus_identification"
            ]["representatives"][0]
            representative.update(
                {
                    "nr_svfamily": "Rhabdoviridae",
                    "nr_sscinames": "Wuhan tick virus 1",
                    "nr_besthit": True,
                }
            )
            packet = build_fact_packet(state, telemetry)
        taxonomy_claim = next(
            claim
            for claim in applicable_claims(packet, load_literature())
            if claim["id"] == "C-TAXON"
        )
        self.assertEqual(taxonomy_claim["kind"], "insufficient")
        self.assertEqual(taxonomy_claim["source_ids"], [])

    def test_report_rerun_is_stable_and_all_evidence_artifacts_are_verified(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            state_path, state, telemetry = _report_fixture(root)
            packet = build_fact_packet(state, telemetry)
            literature = load_literature()
            claims = applicable_claims(packet, literature)
            reasoning = build_reasoning_plan(
                packet,
                literature,
                [claim["id"] for claim in reversed(claims)],
                provider="minimax",
                model="MiniMax-M3",
                backend_version="test",
            )
            fake_weasyprint = root / "weasyprint"
            fake_weasyprint.write_text(
                "#!/bin/sh\ncp \"$1\" \"$2\"\n", encoding="utf-8"
            )
            fake_weasyprint.chmod(0o755)
            reports = generate_priority_reports(
                state_path,
                reasoning_plan=reasoning,
                weasyprint=str(fake_weasyprint),
            )
            run_dir = state_path.parent.parent
            facts_before = (run_dir / "reports/standard/scientific/facts.json").read_bytes()
            html_before = (run_dir / "reports/standard/scientific/report.html").read_bytes()
            reports_again = generate_priority_reports(
                state_path,
                reasoning_plan=None,
                weasyprint=str(fake_weasyprint),
            )
            self.assertEqual(
                facts_before,
                (run_dir / "reports/standard/scientific/facts.json").read_bytes(),
            )
            self.assertEqual(
                html_before,
                (run_dir / "reports/standard/scientific/report.html").read_bytes(),
            )
            self.assertEqual(
                reports["scientific"]["cache_key"],
                reports_again["scientific"]["cache_key"],
            )
            self.assertIn("不能据此确认 H/N 亚型", html_before.decode("utf-8"))
            self.assertNotIn("True 只是数据库最佳命中", html_before.decode("utf-8"))
            current_state = json.loads(state_path.read_text(encoding="utf-8"))
            for format_name in (
                "html",
                "pdf",
                "facts_json",
                "literature_json",
                "reasoning_json",
            ):
                _, content = report_artifact(
                    current_state,
                    run_dir,
                    kind="scientific",
                    format_name=format_name,
                )
                self.assertTrue(content)

    def test_report_rejects_telemetry_outside_the_run_directory(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            state_path, state, _ = _report_fixture(root)
            external = root / "outside.json"
            external.write_text("{}", encoding="utf-8")
            state["technical_report"]["telemetry"] = str(external)
            state_path.write_text(json.dumps(state), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "outside the run directory"):
                generate_priority_reports(
                    state_path,
                    reasoning_plan=None,
                    weasyprint="unused",
                )

    def test_skill_comparison_allows_only_length_change(self):
        baseline = {
            "run_id": "baseline",
            "execution_spec": {"workflow_skill": _compile_read_qc()["workflow_skill"]},
            "tool_results": [{"status": "completed"}],
        }
        modified = {
            "run_id": "modified",
            "execution_spec": {"workflow_skill": _compile_read_qc(80)["workflow_skill"]},
            "tool_results": [{"status": "completed"}],
        }
        result = build_skill_comparison(baseline, modified)
        self.assertEqual(
            result["changes"],
            [{"parameter": "length_required", "before": 50, "after": 80}],
        )
        invalid = copy.deepcopy(modified)
        invalid["execution_spec"]["workflow_skill"]["effective_parameters"][
            "average_qual"
        ] = 30
        with self.assertRaises(ValueError):
            build_skill_comparison(baseline, invalid)

from __future__ import annotations

import unittest
from pathlib import Path

from deterministic_planner import GOAL_ORDER, DeterministicPlanner, PlannerError


def artifact(
    artifact_type: str, roles: tuple[str, ...], *, identity: bool = True
) -> dict:
    record = {
        "record_id": f"rec-{artifact_type}",
        **({"sample_id": "sample", "run_id": "run"} if identity else {}),
        "files": {
            role: {
                "path": f"/data/{artifact_type}-{role}",
                "fingerprint": {"device": 1, "inode": 1, "size": 1, "mtime_ns": 1},
            }
            for role in roles
        },
    }
    return {"id": artifact_type, "type": artifact_type, "records": [record]}


RAW = artifact("raw_paired_reads", ("read1", "read2"))
CONTIGS = artifact("contigs_fasta", ("fasta",))
REFERENCE = artifact("virus_reference_fasta", ("fasta",), identity=False)
TAXONOMY = artifact("virus_taxonomy", ("taxonomy",), identity=False)


class FakeCalibrator:
    def estimate(self, _spec):
        return {"version": 1, "status": "insufficient"}


class FakeManager:
    resource_calibrator = FakeCalibrator()
    analysis_run_root = Path("/data/runs")

    def __init__(self):
        self.launched = None

    async def launch_deterministic_plan(self, plan, chat_id):
        self.launched = (plan, chat_id)
        return {"type": "analysis_run", "status": "running", "run": plan["run"]}


def dataset(*artifacts: dict, status: str = "ready") -> dict:
    return {
        "id": "ds-test",
        "revision": 3,
        "status": status,
        "artifacts": list(artifacts),
        "checks": {},
        "issues": [],
    }


class DeterministicPlannerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.manager = FakeManager()
        self.planner = DeterministicPlanner(self.manager)

    def compile(self, value, goals, **overrides):
        return self.planner.compile(
            dataset=value,
            goals=goals,
            options=overrides.get("options", {"viral_type": "RNA+DNA"}),
            routing=overrides.get("routing", {"mode": "standard"}),
        )

    def test_public_goals_map_to_fixed_deliverables(self):
        inputs = {
            "read_qc": dataset(RAW),
            "rrna_filter": dataset(RAW),
            "sample_merge": dataset(RAW),
            "assembly": dataset(RAW),
            "contig_filter": dataset(RAW),
            "virus_summary": dataset(CONTIGS),
            "host_summary": dataset(RAW, CONTIGS),
            "microbe_summary": dataset(RAW),
            "virus_rpm": dataset(RAW, REFERENCE),
            "annotation_delivery": dataset(REFERENCE, TAXONOMY),
        }
        for goal in GOAL_ORDER:
            with self.subTest(goal=goal):
                plan = self.compile(inputs[goal], [goal])
                self.assertEqual(plan["execution_spec"]["deliverables"], [goal])

    def test_workflow1_terminal_goals_compile_minimal_prefixes(self):
        expected = {
            "read_qc": ["read_qc"],
            "rrna_filter": ["read_qc", "rrna_filter"],
            "sample_merge": ["read_qc", "rrna_filter", "sample_merge"],
            "assembly": ["read_qc", "rrna_filter", "sample_merge", "assembly"],
            "contig_filter": [
                "read_qc",
                "rrna_filter",
                "sample_merge",
                "assembly",
                "contig_filter",
            ],
        }
        for goal, stages in expected.items():
            with self.subTest(goal=goal):
                plan = self.compile(dataset(RAW), [goal])
                self.assertEqual(
                    [stage["id"] for stage in plan["execution_spec"]["stages"]],
                    stages,
                )

    def test_workflow1_contract_freezes_each_selected_stage_manifest(self):
        plan = self.compile(dataset(RAW), ["contig_filter"])

        self.assertEqual(plan["planner_version"], 4)
        self.assertEqual(plan["execution_spec"]["version"], 4)
        self.assertEqual(
            [
                item["capability_id"]
                for item in plan["execution_spec"]["capability_contract"][
                    "manifests"
                ]
            ],
            [
                "workflow1.read_qc.fastp",
                "workflow1.rrna_filter.bowtie2",
                "workflow1.sample_merge.seqkit",
                "workflow1.assembly.megahit",
                "workflow1.contig_filter.seqkit",
            ],
        )

    def test_excluded_stage_conflict_is_structured(self):
        with self.assertRaises(PlannerError) as raised:
            self.compile(
                dataset(RAW),
                ["contig_filter"],
                options={
                    "viral_type": "RNA+DNA",
                    "excluded_stages": ["assembly"],
                },
            )
        self.assertEqual(raised.exception.code, "PATCH_DEPENDENCY_CONFLICT")

        plan = self.compile(
            dataset(CONTIGS),
            ["virus_summary"],
            options={
                "viral_type": "RNA+DNA",
                "excluded_stages": ["assembly"],
            },
        )
        self.assertNotIn("assembly", [stage["id"] for stage in plan["steps"]])

    def test_fastp_parameters_are_normalized_and_must_apply(self):
        selection = {
            "capability_id": "workflow1.read_qc.fastp",
            "manifest_version": 1,
            "preset_id": "workflow_default",
            "values": {"length_required": 50, "detect_adapter_for_pe": True},
        }
        plan = self.compile(
            dataset(RAW),
            ["read_qc"],
            options={
                "viral_type": "RNA+DNA",
                "capability_parameters": [selection],
            },
        )
        self.assertEqual(
            plan["options"]["capability_parameters"][0]["values"],
            {"detect_adapter_for_pe": True, "length_required": 50},
        )
        self.assertEqual(
            plan["execution_spec"]["capability_contract"],
            {
                "adapter_version": 1,
                "manifests": [
                    {
                        "capability_id": "workflow1.read_qc.fastp",
                        "manifest_version": 1,
                        "preset_id": "workflow_default",
                        "effective_values": {
                            "detect_adapter_for_pe": True,
                            "length_required": 50,
                        },
                    }
                ],
            },
        )
        with self.assertRaises(PlannerError) as inapplicable:
            self.compile(
                dataset(CONTIGS),
                ["virus_summary"],
                options={
                    "viral_type": "RNA+DNA",
                    "capability_parameters": [selection],
                },
            )
        self.assertEqual(inapplicable.exception.code, "PARAMETER_NOT_APPLICABLE")
        invalid = {**selection, "values": {"length_required": 0}}
        with self.assertRaises(PlannerError) as out_of_range:
            self.compile(
                dataset(RAW),
                ["read_qc"],
                options={
                    "viral_type": "RNA+DNA",
                    "capability_parameters": [invalid],
                },
            )
        self.assertEqual(out_of_range.exception.code, "PARAMETER_OUT_OF_RANGE")
        unknown = {**selection, "values": {"raw_cli": "--output /tmp/x"}}
        with self.assertRaises(PlannerError) as unknown_parameter:
            self.compile(
                dataset(RAW),
                ["read_qc"],
                options={
                    "viral_type": "RNA+DNA",
                    "capability_parameters": [unknown],
                },
            )
        self.assertEqual(unknown_parameter.exception.code, "PARAMETER_UNKNOWN")

    def test_multiple_goals_share_upstream_and_ignore_unused_artifacts(self):
        plan = self.compile(dataset(RAW, CONTIGS), ["virus_summary", "microbe_summary"])
        step_ids = [step["id"] for step in plan["steps"]]
        self.assertEqual(len(step_ids), len(set(step_ids)))
        self.assertEqual(
            {item["type"] for item in plan["execution_spec"]["inputs"]},
            {"raw_paired_reads", "contigs_fasta"},
        )

    def test_direct_contigs_drop_unneeded_raw_reads(self):
        plan = self.compile(dataset(RAW, CONTIGS), ["virus_summary"])
        self.assertEqual(
            [item["type"] for item in plan["execution_spec"]["inputs"]],
            ["contigs_fasta"],
        )

    def test_viral_type_and_routing_are_authoritative(self):
        for viral_type in ("RNA", "DNA", "RNA+DNA"):
            with self.subTest(viral_type=viral_type):
                plan = self.compile(
                    dataset(RAW),
                    ["contig_filter"],
                    options={"viral_type": viral_type},
                )
                self.assertEqual(
                    plan["execution_spec"]["options"]["viral_type"], viral_type
                )
        emergency = self.compile(
            dataset(RAW),
            ["contig_filter"],
            routing={"mode": "emergency"},
        )
        self.assertTrue(emergency["routing"]["adaptive"])
        self.assertEqual(emergency["routing"]["available_routes"], ["A"])
        self.assertEqual(
            [step["id"] for step in emergency["steps"][:2]],
            ["adaptive_screen", "adaptive_route"],
        )
        standard = self.compile(
            dataset(RAW), ["contig_filter"], routing={"mode": "standard"}
        )
        self.assertNotIn("adaptive_screen", [step["id"] for step in standard["steps"]])
        fallback = self.compile(
            dataset(CONTIGS),
            ["virus_summary"],
            routing={"mode": "emergency"},
        )
        self.assertEqual(fallback["routing"]["effective_mode"], "standard")

    def test_checking_dataset_has_provisional_plan(self):
        plan = self.compile(dataset(RAW, status="checking"), ["contig_filter"])
        self.assertFalse(plan["ready_to_execute"])
        self.assertTrue(plan["steps"])

    def test_same_input_has_identical_spec_and_steps(self):
        first = self.compile(dataset(RAW), ["contig_filter"])
        second = self.compile(dataset(RAW), ["contig_filter"])
        self.assertEqual(first["execution_spec"], second["execution_spec"])
        self.assertEqual(first["steps"], second["steps"])

    def test_rejects_unknown_goal_and_mode(self):
        with self.assertRaises(PlannerError):
            self.compile(dataset(RAW), ["unknown"])
        with self.assertRaises(PlannerError):
            self.compile(dataset(RAW), ["contig_filter"], routing={"mode": "deep"})
        with self.assertRaises(PlannerError):
            self.compile(
                dataset(RAW),
                ["contig_filter"],
                routing={"mode": "emergency", "deadline_seconds": 900},
            )

    def test_disabled_goal_is_rejected(self):
        disabled = DeterministicPlanner(self.manager, enabled_goals=[])
        with self.assertRaisesRegex(PlannerError, "not enabled"):
            disabled.compile(
                dataset=dataset(RAW),
                goals=["contig_filter"],
                options={"viral_type": "RNA+DNA"},
                routing={"mode": "standard"},
            )

    async def test_start_recompiles_and_passes_server_owned_run_paths(self):
        plan = self.compile(dataset(RAW), ["contig_filter"])
        result = await self.planner.start(
            persisted_plan=plan,
            dataset=dataset(RAW),
            chat_id="00000000-0000-0000-0000-000000000001",
            plan_id="pl_test",
            plan_revision=2,
            run_id="analysis-test-run",
            queue_context={"project_id": "project-test", "project_priority": "high"},
        )
        launched, chat_id = self.manager.launched
        self.assertEqual(result["status"], "running")
        self.assertEqual(chat_id, "00000000-0000-0000-0000-000000000001")
        self.assertEqual(launched["run"]["run_dir"], "/data/runs/analysis-test-run")
        self.assertEqual(launched["plan_ref"], {"id": "pl_test", "revision": 2})
        self.assertEqual(launched["queue_context"]["project_priority"], "high")

    async def test_start_rejects_tampered_execution_spec(self):
        plan = self.compile(dataset(RAW), ["contig_filter"])
        plan["execution_spec"]["deliverables"] = ["microbe_summary"]
        with self.assertRaises(PlannerError):
            await self.planner.start(
                persisted_plan=plan,
                dataset=dataset(RAW),
                chat_id="00000000-0000-0000-0000-000000000001",
                plan_id="pl_test",
                plan_revision=2,
                run_id="analysis-test-run",
                queue_context={"project_id": "project-test", "project_priority": "normal"},
            )
        self.assertIsNone(self.manager.launched)

    async def test_start_rejects_caller_controlled_run_path(self):
        plan = self.compile(dataset(RAW), ["contig_filter"])
        with self.assertRaises(PlannerError):
            await self.planner.start(
                persisted_plan=plan,
                dataset=dataset(RAW),
                chat_id="00000000-0000-0000-0000-000000000001",
                plan_id="pl_test",
                plan_revision=2,
                run_id="../../outside",
                queue_context={"project_id": "project-test", "project_priority": "normal"},
            )
        self.assertIsNone(self.manager.launched)


if __name__ == "__main__":
    unittest.main()

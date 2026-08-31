from __future__ import annotations

import unittest
from pathlib import Path

from deterministic_planner import DeterministicPlanner, GOAL_ORDER, PlannerError


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

    def test_six_goals_map_to_fixed_deliverables(self):
        inputs = {
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

    async def test_start_recompiles_and_passes_server_owned_run_paths(self):
        plan = self.compile(dataset(RAW), ["contig_filter"])
        result = await self.planner.start(
            persisted_plan=plan,
            dataset=dataset(RAW),
            chat_id="00000000-0000-0000-0000-000000000001",
            plan_id="pl_test",
            plan_revision=2,
            run_id="analysis-test-run",
        )
        launched, chat_id = self.manager.launched
        self.assertEqual(result["status"], "running")
        self.assertEqual(chat_id, "00000000-0000-0000-0000-000000000001")
        self.assertEqual(launched["run"]["run_dir"], "/data/runs/analysis-test-run")
        self.assertEqual(launched["plan_ref"], {"id": "pl_test", "revision": 2})

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
            )
        self.assertIsNone(self.manager.launched)


if __name__ == "__main__":
    unittest.main()

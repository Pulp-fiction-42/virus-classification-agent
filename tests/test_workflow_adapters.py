import subprocess
import tempfile
import unittest
from pathlib import Path

from pipeline_spec import ExecutionSpecError, canonical_steps, normalize_execution_spec
from workflow_adapters import (
    AdapterSettings,
    _prepare_workflow1_inputs,
    _rewrite_workflow1_config,
    prepare_execution,
)
from workflow_catalog import ARTIFACTS, STAGES


def artifact_input(artifact_type: str, root: Path, *, sample_id: str = "sample1"):
    artifact = ARTIFACTS[artifact_type]
    files = {}
    for role in artifact.required_files:
        path = root / f"{artifact_type}-{role}"
        if artifact.kind(role) == "directory":
            path.mkdir(parents=True, exist_ok=True)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("x\n", encoding="utf-8")
        files[role] = str(path)
    record = {"files": files}
    if "sample_id" in artifact.identity_fields:
        record["sample_id"] = sample_id
    if "run_id" in artifact.identity_fields:
        record["run_id"] = f"{sample_id}-run1"
    return {"id": artifact_type, "type": artifact_type, "records": [record]}


def pipeline(inputs, deliverables, *, mode="standard", viral_type="RNA+DNA"):
    return {
        "version": 2,
        "adapter": "analysis_pipeline",
        "inputs": inputs,
        "deliverables": deliverables,
        "options": {"viral_type": viral_type},
        "policy": {"mode": mode},
    }


class PipelineSpecTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def test_every_public_stage_accepts_a_complete_external_bundle(self):
        for stage in STAGES:
            with self.subTest(stage=stage.id):
                inputs = [
                    artifact_input(alternatives[0], self.root / stage.id)
                    for alternatives in stage.requirements
                ]
                normalized = normalize_execution_spec(pipeline(inputs, [stage.id]))
                self.assertEqual(
                    [item["id"] for item in normalized["stages"]], [stage.id]
                )
                self.assertTrue(normalized["stages"][0]["is_deliverable"])

    def test_raw_reads_compile_shared_multi_branch_closure(self):
        reference = artifact_input("virus_reference_fasta", self.root / "reference")
        raw = artifact_input("raw_paired_reads", self.root / "reads")
        normalized = normalize_execution_spec(
            pipeline(
                [raw, reference],
                ["virus_summary", "host_summary", "microbe_summary", "virus_rpm"],
            )
        )
        ids = [stage["id"] for stage in normalized["stages"]]
        self.assertEqual(ids[:3], ["read_qc", "rrna_filter", "sample_merge"])
        self.assertIn("virus_summary", ids)
        self.assertIn("host_summary", ids)
        self.assertIn("microbe_summary", ids)
        self.assertIn("virus_rpm", ids)
        self.assertEqual(ids.count("sample_merge"), 1)

    def test_direct_contigs_compile_only_requested_virus_segment(self):
        normalized = normalize_execution_spec(
            pipeline(
                [artifact_input("contigs_fasta", self.root / "contigs")],
                ["virus_candidate_extract"],
                viral_type="RNA",
            )
        )
        self.assertEqual(
            [stage["id"] for stage in normalized["stages"]],
            [
                "virus_sequence_prep",
                "virus_candidate_search",
                "virus_candidate_extract",
            ],
        )
        self.assertEqual(normalized["stages"][1]["rules"], ["rna_virusidentification"])

    def test_annotation_requires_taxonomy_as_an_independent_input(self):
        with self.assertRaisesRegex(ExecutionSpecError, "virus_taxonomy"):
            normalize_execution_spec(
                pipeline(
                    [artifact_input("virus_reference_fasta", self.root / "virus")],
                    ["annotation_delivery"],
                )
            )

    def test_raw_reads_annotation_uses_generated_summary_taxonomy(self):
        normalized = normalize_execution_spec(
            pipeline(
                [artifact_input("raw_paired_reads", self.root / "reads-annotation")],
                ["annotation_delivery"],
            )
        )
        self.assertEqual(
            [stage["workflow_id"] for stage in normalized["stages"]],
            ["workflow1"] * 5 + ["workflow2"] * 5 + ["workflow6"] * 5,
        )
        quality = next(
            stage for stage in normalized["stages"] if stage["id"] == "virus_quality"
        )
        self.assertEqual(
            quality["bindings"],
            {"input_0": "virus_summary_bundle", "input_1": "virus_summary_bundle"},
        )

    def test_schema_rejects_relative_paths_duplicate_ids_and_sample_mismatch(self):
        bad_path = artifact_input("contigs_fasta", self.root / "bad")
        bad_path["records"][0]["files"]["fasta"] = "relative.fa"
        with self.assertRaisesRegex(ExecutionSpecError, "absolute path"):
            normalize_execution_spec(pipeline([bad_path], ["virus_sequence_prep"]))

        one = artifact_input("clean_sample_paired_reads", self.root / "one")
        two = artifact_input("virus_reference_fasta", self.root / "two")
        two["id"] = one["id"]
        with self.assertRaisesRegex(ExecutionSpecError, "ids must be unique"):
            normalize_execution_spec(pipeline([one, two], ["virus_read_mapping"]))

        contigs = artifact_input(
            "contigs_fasta", self.root / "contigs2", sample_id="other"
        )
        reads = artifact_input("clean_sample_paired_reads", self.root / "reads2")
        with self.assertRaisesRegex(ExecutionSpecError, "sample sets must match"):
            normalize_execution_spec(pipeline([reads, contigs], ["host_integrate"]))

    def test_rejects_old_adapters_unknown_deliverables_and_unused_inputs(self):
        reads = artifact_input("raw_paired_reads", self.root / "raw")
        for adapter in (
            "workflow1",
            "virus_classification",
            "adaptive_virus_analysis",
        ):
            with self.subTest(adapter=adapter), self.assertRaises(ExecutionSpecError):
                normalize_execution_spec(
                    {**pipeline([reads], ["read_qc"]), "adapter": adapter}
                )
        with self.assertRaisesRegex(ExecutionSpecError, "unsupported deliverable"):
            normalize_execution_spec(pipeline([reads], ["not_a_stage"]))
        taxonomy = artifact_input("virus_taxonomy", self.root / "unused")
        with self.assertRaisesRegex(ExecutionSpecError, "unused execution inputs"):
            normalize_execution_spec(pipeline([reads, taxonomy], ["read_qc"]))

    def test_routing_modes_are_normalized_by_input_applicability(self):
        reads = artifact_input("raw_paired_reads", self.root / "routing-reads")
        contigs = artifact_input("contigs_fasta", self.root / "routing-contigs")
        standard = normalize_execution_spec(
            pipeline([reads], ["read_qc"], mode="standard")
        )
        self.assertFalse(standard["policy"]["adaptive"])
        self.assertNotIn(
            "adaptive_screen", [step["id"] for step in canonical_steps(standard)]
        )

        auto = normalize_execution_spec(pipeline([reads], ["read_qc"], mode="auto"))
        self.assertTrue(auto["policy"]["adaptive"])
        self.assertEqual(auto["policy"]["short_circuit_scope"], "pipeline")
        self.assertEqual(canonical_steps(auto)[0]["id"], "adaptive_screen")

        emergency = normalize_execution_spec(
            pipeline([reads], ["read_qc"], mode="emergency")
        )
        self.assertEqual(emergency["policy"]["effective_mode"], "emergency")
        self.assertNotIn("deadline_seconds", emergency["policy"])

        fallback = normalize_execution_spec(
            pipeline([contigs], ["virus_summary"], mode="auto")
        )
        self.assertEqual(fallback["policy"]["effective_mode"], "standard")
        self.assertEqual(
            fallback["policy"]["applicability_reason"],
            "paired_reads_unavailable",
        )


class PipelinePreparationTests(unittest.TestCase):
    def test_technical_replicates_keep_run_identity_and_share_sample(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            records = []
            for run in ("run-a", "run-b"):
                reads = {}
                for role in ("read1", "read2"):
                    path = root / f"{run}-{role}"
                    path.write_text("synthetic\n")
                    reads[role] = str(path)
                records.append({"run_id": run, "sample_id": "sample-a", **reads})
            runs, samples = _prepare_workflow1_inputs(
                {"entry_rule": "fastp", "records": records},
                root / "workflow",
                root / "results",
            )
            self.assertEqual(runs, ["run-a", "run-b"])
            self.assertEqual(samples, ["sample-a"])
            rows = (root / "workflow/data/samples.txt").read_text().splitlines()
            self.assertEqual(
                [row.split()[:2] for row in rows],
                [["run-a", "sample-a"], ["run-b", "sample-a"]],
            )
            self.assertEqual(
                [row.split()[2:] for row in rows],
                [
                    [str(Path(r["read1"]).resolve()), str(Path(r["read2"]).resolve())]
                    for r in records
                ],
            )

    def test_workflow1_run_local_config_uses_configured_seqkit(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            workflow_dir = root / "workflow"
            config_dir = workflow_dir / "config"
            config_dir.mkdir(parents=True)
            (config_dir / "config.yaml").write_text(
                'sample_list: "data/samples.txt"\n'
                'output_dir: "example"\n'
                "software:\n"
                '  seqkit: "/slow/seqkit"\n'
                '  fastp: "/tools/fastp"\n',
                encoding="utf-8",
            )

            rewritten = _rewrite_workflow1_config(
                workflow_dir,
                root / "results",
                seqkit="/home/user/.pixi/bin/seqkit",
            ).read_text(encoding="utf-8")

            self.assertIn('  seqkit: "/home/user/.pixi/bin/seqkit"', rewritten)
            self.assertNotIn("/slow/seqkit", rewritten)

    def test_prepares_one_run_local_phase_per_selected_source_workflow(self):
        settings = AdapterSettings.from_env()
        sources = [
            settings.workflow1_source,
            settings.workflow2_source,
            settings.workflow3_source,
            settings.workflow4_source,
            settings.workflow5_source,
            settings.workflow6_source,
        ]
        if not all(path.is_dir() for path in sources):
            self.skipTest("source workflows are unavailable")
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            raw = artifact_input("raw_paired_reads", root / "reads")
            reference = artifact_input("virus_reference_fasta", root / "reference")
            taxonomy = artifact_input("virus_taxonomy", root / "taxonomy")
            raw_spec = pipeline(
                [raw, reference, taxonomy],
                [
                    "virus_summary",
                    "host_summary",
                    "microbe_summary",
                    "virus_rpm",
                    "annotation_delivery",
                ],
            )
            spec = normalize_execution_spec(raw_spec)
            run_dir = root / "run"
            prepared = prepare_execution(
                {
                    "run_dir": str(run_dir),
                    "execution_spec": spec,
                    "steps": canonical_steps(spec),
                },
                settings,
            )
            self.assertEqual(
                [phase.workflow_id for phase in prepared.phases],
                [
                    "workflow1",
                    "workflow2",
                    "workflow3",
                    "workflow4",
                    "workflow5",
                    "workflow6",
                ],
            )
            self.assertEqual(len({phase.workflow_id for phase in prepared.phases}), 6)
            self.assertTrue(all(phase.command for phase in prepared.phases))

    def test_each_source_workflow_accepts_compiled_targets_in_dry_run(self):
        settings = AdapterSettings.from_env()
        if not Path(settings.snakemake).is_file():
            self.skipTest("Snakemake is unavailable")
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            reads = artifact_input("clean_sample_paired_reads", root / "reads")
            raw = artifact_input("raw_paired_reads", root / "raw")
            contigs = artifact_input("contigs_fasta", root / "contigs")
            reference = artifact_input("virus_reference_fasta", root / "reference")
            taxonomy = artifact_input("virus_taxonomy", root / "taxonomy")
            scenarios = [
                ("workflow1", [raw], ["contig_filter"]),
                ("workflow2", [contigs], ["virus_summary"]),
                ("workflow3", [reads, contigs], ["host_summary"]),
                ("workflow4", [reads], ["microbe_summary"]),
                ("workflow5", [reads, reference], ["virus_rpm"]),
                ("workflow6", [reference, taxonomy], ["annotation_delivery"]),
            ]
            for workflow_id, inputs, deliverables in scenarios:
                with self.subTest(workflow=workflow_id):
                    spec = normalize_execution_spec(pipeline(inputs, deliverables))
                    run_dir = root / f"run-{workflow_id}"
                    prepared = prepare_execution(
                        {
                            "run_dir": str(run_dir),
                            "execution_spec": spec,
                            "steps": canonical_steps(spec),
                        },
                        settings,
                    )
                    self.assertEqual(len(prepared.phases), 1)
                    result = subprocess.run(
                        [
                            *prepared.phases[0].command,
                            "--dry-run",
                            "--quiet",
                            "all",
                        ],
                        text=True,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                        timeout=60,
                        check=False,
                    )
                    self.assertEqual(result.returncode, 0, result.stdout[-4000:])


if __name__ == "__main__":
    unittest.main()

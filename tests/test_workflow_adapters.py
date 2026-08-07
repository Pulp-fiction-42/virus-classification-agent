from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from workflow_adapters import (
    AdapterSettings,
    ExecutionSpecError,
    WORKFLOW1_RULES,
    normalize_execution_spec,
    normalize_plan_execution_spec,
    prepare_execution,
    selected_rules,
    validate_plan_rules,
)


class WorkflowAdapterTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.workflow1_source = self.root / "workflow1-source"
        (self.workflow1_source / "config").mkdir(parents=True)
        (self.workflow1_source / "data").mkdir()
        (self.workflow1_source / "rules").mkdir()
        (self.workflow1_source / "Snakefile").write_text(
            'configfile: "config/config.yaml"\ninclude: "rules/upstream.smk"\n',
            encoding="utf-8",
        )
        (self.workflow1_source / "config" / "config.yaml").write_text(
            'sample_list: "data/samples.txt"\noutput_dir: "example"\n',
            encoding="utf-8",
        )
        (self.workflow1_source / "data" / "samples.txt").write_text(
            "", encoding="utf-8"
        )
        (self.workflow1_source / "rules" / "upstream.smk").write_text(
            "", encoding="utf-8"
        )

        self.workflow2_source = self.root / "workflow2-source"
        (self.workflow2_source / "config").mkdir(parents=True)
        (self.workflow2_source / "data").mkdir()
        (self.workflow2_source / "rules").mkdir()
        (self.workflow2_source / "scripts").mkdir()
        (self.workflow2_source / "Snakefile").write_text(
            'configfile: "config/config.yaml"\n', encoding="utf-8"
        )
        (self.workflow2_source / "config" / "config.yaml").write_text(
            "\n".join(
                [
                    'input_list: "data/contigs_list.txt"',
                    'wdir: "example"',
                    'SEARCH_VIRALTYPE: "RNA"',
                    'RNA_SEARCHMODE: "fast"',
                    "ITERATION_TIME: 5",
                    "RENAME_CONTIGS: true",
                    "softwares:",
                    '  iterhmmsearch: "/source/iterativehmmsearch.py"',
                    "scripts:",
                    '  coverage_summarize: "/source/parse_iterhmmresult.py"',
                    '  cluster_summarize: "/source/cluster_summarize.py"',
                    '  blast_summarize: "/source/parse_blastresult.py"',
                    '  table_summarize: "/source/table_summarize.py"',
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        for name in (
            "iterativehmmsearch.py",
            "parse_iterhmmresult.py",
            "cluster_summarize.py",
            "parse_blastresult.py",
            "table_summarize.py",
        ):
            (self.workflow2_source / "scripts" / name).write_text("", encoding="utf-8")

        self.settings = AdapterSettings(
            workflow1_source=self.workflow1_source,
            workflow2_source=self.workflow2_source,
            snakemake="/tools/snakemake",
            seqkit="/tools/seqkit",
            cores=8,
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def _file(self, name: str) -> str:
        path = self.root / "inputs" / name
        path.parent.mkdir(exist_ok=True)
        path.write_text("data\n", encoding="utf-8")
        return str(path)

    def _workflow1_spec(self, entry: str, exit_rule: str | None = None):
        if entry == "rename_filter":
            input_type = "contigs_fasta"
            collection = "samples"
            records = [{"sample_id": "sample", "path": self._file("contigs.fa")}]
        elif entry == "megahit":
            input_type = "merged_paired_reads"
            collection = "samples"
            records = [
                {
                    "sample_id": "sample",
                    "read1": self._file("merged_r1.fq"),
                    "read2": self._file("merged_r2.fq"),
                }
            ]
        else:
            input_type = {
                "fastp": "raw_paired_reads",
                "bowtie2": "fastp_paired_reads",
                "merge": "clean_paired_reads",
            }[entry]
            collection = "runs"
            records = [
                {
                    "run_id": "run",
                    "sample_id": "sample",
                    "read1": self._file(f"{entry}_r1.fq.gz"),
                    "read2": self._file(f"{entry}_r2.fq.gz"),
                }
            ]
        return {
            "adapter": "workflow1",
            "entry_rule": entry,
            "exit_rule": exit_rule or entry,
            "inputs": [
                {
                    "id": "primary",
                    "type": input_type,
                    "data": {collection: records},
                }
            ],
        }

    def _classification_spec(self, input_type: str, viral_type: str | None = None):
        if input_type == "raw_paired_reads":
            data = {
                "runs": [
                    {
                        "run_id": "run",
                        "sample_id": "sample",
                        "read1": self._file("raw_r1.fq.gz"),
                        "read2": self._file("raw_r2.fq.gz"),
                    }
                ]
            }
        else:
            data = {
                "samples": [
                    {
                        "sample_id": "sample",
                        "path": self._file("contigs.fa"),
                    }
                ]
            }
        spec = {
            "adapter": "virus_classification",
            "inputs": [{"id": "primary", "type": input_type, "data": data}],
        }
        if viral_type:
            spec["viral_type"] = viral_type
        return spec

    @staticmethod
    def _steps(spec: dict):
        normalized = normalize_execution_spec(spec)
        return [
            {
                "id": f"step_{index + 1}",
                "label": rule,
                "rules": [rule],
                "status": "pending",
            }
            for index, rule in enumerate(selected_rules(normalized))
        ]

    def _prepare_workflow1(self, entry: str, exit_rule: str | None = None):
        exit_rule = exit_rule or entry
        spec = self._workflow1_spec(entry, exit_rule)
        run_dir = self.root / f"run-{entry}-{exit_rule}"
        state = {
            "run_dir": str(run_dir),
            "execution_spec": spec,
            "steps": self._steps(spec),
        }
        return run_dir, prepare_execution(state, self.settings)

    def test_supports_each_workflow1_entry_type(self):
        expected_links = {
            "fastp": None,
            "bowtie2": "results/upstream/filter/run/01_fastp_r1.fq.gz",
            "merge": "results/upstream/filter/run/cleanreads_r1.fq.gz",
            "megahit": "results/upstream/merge/sample/cleanreads_r1.fq",
            "rename_filter": ("results/upstream/assembly/sample/sample.contigs.fa"),
        }
        for entry, relative_link in expected_links.items():
            with self.subTest(entry=entry):
                run_dir, prepared = self._prepare_workflow1(entry)
                phase = prepared.phases[0]

                self.assertEqual(phase.selected_rules, (entry,))
                self.assertIn("--benchmark-extended", phase.command)
                self.assertIn("--printshellcmds", phase.command)
                self.assertIn("--runtime-source-cache-path", phase.command)
                if relative_link:
                    self.assertTrue((run_dir / relative_link).is_symlink())

    def test_allows_a_contiguous_middle_segment_and_builds_exit_targets(self):
        run_dir, prepared = self._prepare_workflow1("merge", "megahit")
        phase = prepared.phases[0]

        self.assertEqual(prepared.selected_rules, ("merge", "megahit"))
        self.assertIn(
            str(
                run_dir
                / "results"
                / "upstream"
                / "assembly"
                / "sample"
                / "sample.contigs.fa"
            ),
            phase.command,
        )

    def test_rejects_reverse_segments_unknown_inputs_and_rule_mismatches(self):
        reverse = self._workflow1_spec("merge")
        reverse["exit_rule"] = "bowtie2"
        with self.assertRaises(ExecutionSpecError):
            normalize_execution_spec(reverse)

        wrong_type = self._workflow1_spec("merge")
        wrong_type["inputs"][0]["type"] = "raw_paired_reads"
        with self.assertRaises(ExecutionSpecError):
            normalize_execution_spec(wrong_type)

        spec = normalize_execution_spec(self._workflow1_spec("bowtie2", "merge"))
        with self.assertRaises(ExecutionSpecError):
            validate_plan_rules(spec, [{"label": "跳过依赖", "rules": ["merge"]}])

    def test_infers_missing_workflow1_boundaries_from_contiguous_plan_rules(self):
        raw = self._workflow1_spec("merge", "megahit")
        raw.pop("entry_rule")
        raw.pop("exit_rule")
        raw["viral_type"] = "RNA+DNA"
        steps = [
            {"label": "样本合并", "rules": ["merge"]},
            {"label": "组装", "rules": ["megahit"]},
        ]

        normalized = normalize_plan_execution_spec(raw, steps)

        self.assertEqual(normalized["entry_rule"], "merge")
        self.assertEqual(normalized["exit_rule"], "megahit")
        self.assertNotIn("viral_type", normalized)

    def test_infers_one_missing_boundary_but_rejects_explicit_conflicts(self):
        raw = self._workflow1_spec("merge", "megahit")
        raw.pop("exit_rule")
        steps = [
            {"label": "样本合并", "rules": ["merge"]},
            {"label": "组装", "rules": ["megahit"]},
        ]
        self.assertEqual(
            normalize_plan_execution_spec(raw, steps)["exit_rule"], "megahit"
        )

        raw["entry_rule"] = "bowtie2"
        with self.assertRaisesRegex(ExecutionSpecError, "entry_rule conflicts"):
            normalize_plan_execution_spec(raw, steps)

    def test_does_not_infer_workflow1_boundaries_from_non_contiguous_rules(self):
        raw = self._workflow1_spec("fastp", "rename_filter")
        raw.pop("entry_rule")
        raw.pop("exit_rule")

        with self.assertRaisesRegex(ExecutionSpecError, "contiguous rule segment"):
            normalize_plan_execution_spec(
                raw,
                [
                    {"label": "质控", "rules": ["fastp"]},
                    {"label": "组装", "rules": ["megahit"]},
                ],
            )

    def test_rejects_relative_input_paths(self):
        spec = self._workflow1_spec("fastp")
        spec["inputs"][0]["data"]["runs"][0]["read1"] = "reads.fastq.gz"

        with self.assertRaises(ExecutionSpecError):
            normalize_execution_spec(spec)

    def test_classification_defaults_scope_and_supports_each_input(self):
        raw = normalize_execution_spec(self._classification_spec("raw_paired_reads"))
        contigs = normalize_execution_spec(
            self._classification_spec("contigs_fasta", "RNA")
        )

        self.assertEqual(raw["viral_type"], "RNA+DNA")
        self.assertIn("fastp", selected_rules(raw))
        self.assertNotIn("rename_contigs", selected_rules(raw))
        self.assertIn("rna_virusidentification", selected_rules(raw))
        self.assertIn("dna_virusidentification", selected_rules(raw))
        self.assertNotIn("fastp", selected_rules(contigs))
        self.assertIn("rename_contigs", selected_rules(contigs))
        self.assertIn("rna_virusidentification", selected_rules(contigs))
        self.assertNotIn("dna_virusidentification", selected_rules(contigs))

    def test_prepares_two_phases_for_reads_and_one_for_contigs(self):
        raw_spec = self._classification_spec("raw_paired_reads")
        raw_dir = self.root / "classification-reads"
        raw = prepare_execution(
            {
                "run_dir": str(raw_dir),
                "execution_spec": raw_spec,
                "steps": self._steps(raw_spec),
            },
            self.settings,
        )

        self.assertEqual(
            [phase.id for phase in raw.phases],
            ["upstream", "classification"],
        )
        self.assertEqual(
            (raw.phases[1].workflow_dir / "data" / "contigs_list.txt")
            .read_text(encoding="utf-8")
            .strip(),
            str(
                raw_dir
                / "results"
                / "upstream"
                / "assembly"
                / "sample"
                / "sample.contigs.1kb.renamed.fa"
            ),
        )
        config = (raw.phases[1].workflow_dir / "config" / "config.yaml").read_text(
            encoding="utf-8"
        )
        self.assertIn('SEARCH_VIRALTYPE: "RNA+DNA"', config)
        self.assertIn("RENAME_CONTIGS: false", config)
        self.assertNotIn("rename_contigs", raw.phases[1].selected_rules)

        contig_spec = self._classification_spec("contigs_fasta", "DNA")
        contig_dir = self.root / "classification-contigs"
        contigs = prepare_execution(
            {
                "run_dir": str(contig_dir),
                "execution_spec": contig_spec,
                "steps": self._steps(contig_spec),
            },
            self.settings,
        )
        self.assertEqual([phase.id for phase in contigs.phases], ["classification"])
        self.assertIn("rename_contigs", contigs.phases[0].selected_rules)
        self.assertIn(
            f"sample {self.root / 'inputs' / 'contigs.fa'}",
            (contigs.phases[0].workflow_dir / "data" / "contigs_list.txt").read_text(
                encoding="utf-8"
            ),
        )

    def test_classification_rejects_bad_scope_mixed_inputs_and_rule_mismatch(self):
        mixed = self._classification_spec("raw_paired_reads")
        mixed["inputs"].append(mixed["inputs"][0])
        with self.assertRaises(ExecutionSpecError):
            normalize_execution_spec(mixed)

        unknown_scope = self._classification_spec("contigs_fasta")
        unknown_scope["viral_type"] = "all"
        with self.assertRaises(ExecutionSpecError):
            normalize_execution_spec(unknown_scope)

        duplicate_sample = self._classification_spec("contigs_fasta")
        duplicate_sample["inputs"][0]["data"]["samples"].append(
            dict(duplicate_sample["inputs"][0]["data"]["samples"][0])
        )
        with self.assertRaises(ExecutionSpecError):
            normalize_execution_spec(duplicate_sample)

        relative_path = self._classification_spec("contigs_fasta")
        relative_path["inputs"][0]["data"]["samples"][0]["path"] = "contigs.fa"
        with self.assertRaises(ExecutionSpecError):
            normalize_execution_spec(relative_path)

        spec = normalize_execution_spec(self._classification_spec("contigs_fasta"))
        with self.assertRaises(ExecutionSpecError):
            validate_plan_rules(
                spec,
                [{"label": "结果汇总", "rules": ["summarize_tables"]}],
            )

    def test_workflow1_rule_constant_stays_in_execution_order(self):
        self.assertEqual(
            WORKFLOW1_RULES,
            ("fastp", "bowtie2", "merge", "megahit", "rename_filter"),
        )


if __name__ == "__main__":
    unittest.main()

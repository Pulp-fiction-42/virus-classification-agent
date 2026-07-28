from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from workflow_adapters import (
    AdapterSettings,
    ExecutionSpecError,
    WORKFLOW1_RULES,
    normalize_execution_spec,
    prepare_execution,
    validate_plan_rules,
)


class WorkflowAdapterTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.source = self.root / "source"
        (self.source / "config").mkdir(parents=True)
        (self.source / "data").mkdir()
        (self.source / "rules").mkdir()
        (self.source / "Snakefile").write_text(
            'configfile: "config/config.yaml"\ninclude: "rules/upstream.smk"\n',
            encoding="utf-8",
        )
        (self.source / "config" / "config.yaml").write_text(
            'sample_list: "data/samples.txt"\noutput_dir: "example"\n',
            encoding="utf-8",
        )
        (self.source / "data" / "samples.txt").write_text("", encoding="utf-8")
        (self.source / "rules" / "upstream.smk").write_text("", encoding="utf-8")
        self.settings = AdapterSettings(
            workflow1_source=self.source,
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

    def _spec(self, entry: str, exit_rule: str | None = None):
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

    @staticmethod
    def _steps(entry: str, exit_rule: str):
        start = WORKFLOW1_RULES.index(entry)
        end = WORKFLOW1_RULES.index(exit_rule)
        return [
            {
                "id": f"step_{index + 1}",
                "label": rule,
                "rules": [rule],
                "status": "pending",
            }
            for index, rule in enumerate(WORKFLOW1_RULES[start : end + 1])
        ]

    def _prepare(self, entry: str, exit_rule: str | None = None):
        exit_rule = exit_rule or entry
        run_dir = self.root / f"run-{entry}-{exit_rule}"
        (run_dir / ".bioagent").mkdir(parents=True)
        state = {
            "run_dir": str(run_dir),
            "execution_spec": self._spec(entry, exit_rule),
            "steps": self._steps(entry, exit_rule),
        }
        return run_dir, prepare_execution(state, self.settings)

    def test_supports_each_workflow1_entry_type(self):
        expected_links = {
            "fastp": None,
            "bowtie2": "results/filter/run/01_fastp_r1.fq.gz",
            "merge": "results/filter/run/cleanreads_r1.fq.gz",
            "megahit": "results/merge/sample/cleanreads_r1.fq",
            "rename_filter": "results/assembly/sample/sample.contigs.fa",
        }
        for entry, relative_link in expected_links.items():
            with self.subTest(entry=entry):
                run_dir, prepared = self._prepare(entry)

                self.assertEqual(prepared.selected_rules, (entry,))
                self.assertIn("--benchmark-extended", prepared.command)
                self.assertIn("--printshellcmds", prepared.command)
                self.assertIn("--runtime-source-cache-path", prepared.command)
                if relative_link:
                    self.assertTrue((run_dir / relative_link).is_symlink())

    def test_allows_a_contiguous_middle_segment_and_builds_exit_targets(self):
        run_dir, prepared = self._prepare("merge", "megahit")

        self.assertEqual(prepared.selected_rules, ("merge", "megahit"))
        self.assertIn(
            str(run_dir / "results/assembly/sample/sample.contigs.fa"),
            prepared.command,
        )
        self.assertNotIn(
            str(run_dir / "results/filter/run/01_fastp_r1.fq.gz"),
            prepared.command,
        )

    def test_rejects_reverse_segments_unknown_inputs_and_rule_mismatches(self):
        reverse = self._spec("merge")
        reverse["exit_rule"] = "bowtie2"
        with self.assertRaises(ExecutionSpecError):
            normalize_execution_spec(reverse)

        wrong_type = self._spec("merge")
        wrong_type["inputs"][0]["type"] = "raw_paired_reads"
        with self.assertRaises(ExecutionSpecError):
            normalize_execution_spec(wrong_type)

        spec = normalize_execution_spec(self._spec("bowtie2", "merge"))
        with self.assertRaises(ExecutionSpecError):
            validate_plan_rules(
                spec,
                [{"label": "跳过依赖", "rules": ["merge"]}],
            )

    def test_rejects_relative_input_paths(self):
        spec = self._spec("fastp")
        spec["inputs"][0]["data"]["runs"][0]["read1"] = "reads.fastq.gz"

        with self.assertRaises(ExecutionSpecError):
            normalize_execution_spec(spec)


if __name__ == "__main__":
    unittest.main()

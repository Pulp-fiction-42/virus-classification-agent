import copy
import json
import os
import shutil
import sys
import tempfile
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import yaml

from intent_router import resolve_intent
from pipeline_runner import run_pipeline, validate_execution_plan
from pipeline_spec import ExecutionSpecError
from workflow_adapters import (
    AdapterSettings,
    _patch_workflow2_empty_hmm_results,
    _rewrite_workflow1_config,
    _workflow2_targets,
    _workflow_environment,
)


class ExecutionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / "source"
        (self.source / "config").mkdir(parents=True)
        (self.source / "Snakefile").write_text("# fixture\n")
        self.original_config = {
            "sample_list": "data/samples.txt", "output_dir": "example",
            "params": {"fastp": "--dedup --correction --length_required 50 --average_qual 20",
                       "megahit": "--memory 20000000000 --min-contig-len 300"},
        }
        (self.source / "config/config.yaml").write_text(yaml.safe_dump(self.original_config))
        paths = [self.root / "demo_R1.fq", self.root / "demo_R2.fq"]
        for path in paths:
            path.touch()
        planning = resolve_intent("只做质控，fastp最短长度改成80", {"paths": [str(p) for p in paths]})
        self.plan = {
            "ready_to_execute": True, "planning": planning, "steps": planning["steps"],
            "execution_spec": planning["execution_spec"], "parameters": planning["parameters"],
            "plan_revision": planning["plan_revision"],
        }
        self.run = self.root / "run"
        (self.run / ".bioagent").mkdir(parents=True)
        self.state_path = self.run / ".bioagent/state.json"
        self.state = {
            "run_dir": str(self.run), "execution_spec": self.plan["execution_spec"],
            "confirmed_plan": self.plan, "steps": [{**step, "status": "pending"} for step in self.plan["steps"]],
            "execution": {"ok": None, "status": "running"},
        }
        self.state_path.write_text(json.dumps(self.state))
        self.settings = AdapterSettings(self.source, self.source, "snakemake", "seqkit", 2)

    def test_plan_scope_rules_and_parameters_are_revalidated(self):
        for mutate in (
            lambda p: p["steps"][0].update(rules=["megahit"]),
            lambda p: p["planning"].update(authorized_stages=[]),
            lambda p: p.update(plan_revision=100),
            lambda p: p["execution_spec"].update(parameters={"fastp.length_required": 2}),
        ):
            plan = copy.deepcopy(self.plan)
            mutate(plan)
            with self.assertRaises(ExecutionSpecError):
                validate_execution_plan(plan)

    def test_real_run_uses_only_dryrun_admitted_rules_and_records_targets(self):
        calls = []

        def command(argv, **kwargs):
            calls.append(argv)
            if "--dry-run" in argv:
                kwargs["stdout"].write("rule fastp:\n")
            else:
                for item in argv:
                    if str(self.run / "results") in item:
                        path = Path(item)
                        path.parent.mkdir(parents=True, exist_ok=True)
                        path.touch()
            return SimpleNamespace(returncode=0)

        state = run_pipeline(self.state_path, settings=self.settings, run_command=command)
        self.assertTrue(state["execution"]["ok"])
        self.assertEqual(len(calls), 2)
        self.assertEqual(state["execution"]["phases"][0]["scheduled_rules"], ["fastp"])
        self.assertTrue(all(step["status"] == "completed" for step in state["steps"]))
        self.assertEqual(state["provenance_capture"]["skill_tracking"], "not_invoked_by_design")
        self.assertEqual({item["name"] for item in state["provenance_capture"]["libraries"]}, {"Python", "PyYAML"})
        copied = yaml.safe_load((self.run / "workflows/upstream/config/config.yaml").read_text())
        self.assertIn("--length_required 80", copied["params"]["fastp"])
        self.assertIn("--average_qual 20", copied["params"]["fastp"])
        self.assertEqual(yaml.safe_load((self.source / "config/config.yaml").read_text()), self.original_config)
        run_pipeline(self.state_path, settings=self.settings, run_command=command)
        self.assertEqual(len(calls), 2, "report follow-up must not recompute")

    def test_dryrun_extra_stage_never_starts_real_computation(self):
        calls = []

        def command(argv, **kwargs):
            calls.append(argv)
            kwargs["stdout"].write("rule fastp:\nrule megahit:\n")
            return SimpleNamespace(returncode=0)

        state = run_pipeline(self.state_path, settings=self.settings, run_command=command)
        self.assertFalse(state["execution"]["ok"])
        self.assertEqual(len(calls), 1)
        self.assertIn("megahit", state["execution"]["error"])

    def test_success_exit_without_outputs_is_a_failure(self):
        def command(argv, **kwargs):
            kwargs["stdout"].write("rule fastp:\n")
            return SimpleNamespace(returncode=0)
        state = run_pipeline(self.state_path, settings=self.settings, run_command=command)
        self.assertFalse(state["execution"]["ok"])
        self.assertIn("缺少目标产物", state["execution"]["error"])

    def test_missing_inputs_never_launch_subprocess(self):
        first = self.plan["execution_spec"]["inputs"][0]["records"][0]["files"]["read1"]
        Path(first).unlink()
        def command(*args, **kwargs):
            self.fail("subprocess must not start")
        state = run_pipeline(self.state_path, settings=self.settings, run_command=command)
        self.assertFalse(state["execution"]["ok"])

    def test_boolean_and_contig_overrides_touch_only_run_copy(self):
        (self.source / "rules").mkdir()
        rule = self.source / "rules/upstream.smk"
        rule.write_text("{params.seqkit} seq -j 1 -m 1000 {input} > {output}\n")
        path = _rewrite_workflow1_config(self.source, self.run / "results", parameters={
            "fastp.dedup": False, "contig_filter.min_length": 1500,
        })
        config = yaml.safe_load(path.read_text())
        self.assertNotIn("--dedup", config["params"]["fastp"])
        self.assertIn("--correction", config["params"]["fastp"])
        self.assertEqual(config["bioagent_contig_min_length"], 1500)
        self.assertIn("{config[bioagent_contig_min_length]}", rule.read_text())


class WorkflowOutputTests(unittest.TestCase):
    def test_helper_binaries_resolve_from_workflow_configuration(self):
        with tempfile.TemporaryDirectory() as directory:
            config_path = Path(directory) / "config.yaml"
            config_path.write_text(yaml.safe_dump({"softwares": {"python": sys.executable}}))
            environment = _workflow_environment(config_path)
            self.assertEqual(
                Path(shutil.which(Path(sys.executable).name, path=environment["PATH"])).resolve(),
                Path(sys.executable).resolve(),
            )
            self.assertTrue(environment["PATH"].endswith(os.environ.get("PATH", "")))

    def test_workflow2_summary_targets_match_workflow_outputs(self):
        results = Path("virus-identification")
        self.assertEqual(
            _workflow2_targets("virus_summary", results, "RNA+DNA"),
            [
                results / "summary/sequences/rep_vert_viruses.fna",
                results / "summary/sequences/vert_viruses.fna",
            ],
        )

    def test_workflow2_empty_hmm_results_parse_as_zero_candidates(self):
        with tempfile.TemporaryDirectory() as directory:
            workflow_dir = Path(directory)
            scripts = workflow_dir / "scripts"
            scripts.mkdir()
            (scripts / "iterativehmmsearch.py").write_text(
                "import os, sys\n"
                "def curtime(): return ''\n"
                "def run(wdir, iteration_num):\n"
                "    for i in range(1, iteration_num + 1):\n"
                "        num_hitseq = '0'\n"
                "        _t2 = _t3 = 0\n"
                "        sys.stderr.write(curtime()+'[INFO] hmmsearch done, %s sequences hit "
                "(Time Elapsed: %.3fs)\\n' % (num_hitseq, _t3-_t2))\n\n"
                "        #Step02. Confirm existence of RdRp Motif\n",
                encoding="utf-8",
            )
            parser_path = scripts / "parse_iterhmmresult.py"
            parser_path.write_text(
                "import pandas as pd\n"
                "from pathlib import Path\n"
                "def filt_motifcov(itersearch_path, wdir=''):\n"
                "    hmmsearch_cols_clean = 'target_name target_accession tlen query_name query_accession qlen seq_evalue seq_score seq_bias domain_# domain_of domain_c-evalue domain_i-evalue domain_score domain_bias hmm_start hmm_end aln_start aln_end env_start env_end acc desc'.split()\n"
                "    iterpaths = list(Path(wdir, itersearch_path).glob('Iteration_*/hmmsearch.domtblout'))\n"
                "    fulliter = pd.concat(list(map(lambda x: pd.read_table(x, comment='#', delim_whitespace=True, names=hmmsearch_cols_clean), iterpaths)))\n"
                "    fulliter['iteration'] = fulliter['query_name'].apply(lambda x: x[-1]).astype(int) + 1\n",
                encoding="utf-8",
            )

            _patch_workflow2_empty_hmm_results(workflow_dir)

            iterative_path = scripts / "iterativehmmsearch.py"
            iterative_namespace = {}
            exec(
                compile(iterative_path.read_text(encoding="utf-8"), str(iterative_path), "exec"),
                iterative_namespace,
            )
            iterative_namespace["run"](str(workflow_dir / "output_dna"), 3)
            compile(parser_path.read_text(encoding="utf-8"), str(parser_path), "exec")
            namespace = {}
            class EmptyFrame:
                def __init__(self, columns):
                    self.columns = columns

                def __len__(self):
                    return 0

            class EmptyDataError(Exception):
                pass

            def read_table(*args, **kwargs):
                raise EmptyDataError

            pandas_stub = types.SimpleNamespace(
                errors=types.SimpleNamespace(EmptyDataError=EmptyDataError),
                read_table=read_table,
                DataFrame=EmptyFrame,
            )
            with patch.dict("sys.modules", {"pandas": pandas_stub}):
                exec(
                    compile(parser_path.read_text(encoding="utf-8"), str(parser_path), "exec"),
                    namespace,
                )
            result, best_orfs = namespace["filt_motifcov"](
                "output_dna", str(workflow_dir)
            )

            self.assertEqual(len(result), 0)
            self.assertEqual(best_orfs, [])
            self.assertEqual(
                list(result.columns),
                [
                    "qorfid",
                    "besthit_hmm",
                    "Profile_start",
                    "Profile_end",
                    "Profile_cov",
                    "qseqid",
                    "valid_Profile_cov",
                ],
            )


if __name__ == "__main__":
    unittest.main()

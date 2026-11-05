import copy
import tempfile
import unittest
from pathlib import Path

from intent_router import resolve_intent
from parameter_rules import ParameterError, parse_parameter_patch, validate_patch
from pipeline_spec import ExecutionSpecError, compile_execution_spec


class PlannerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.raw = {"paths": [str(self.root / "demo_R1.fastq.gz"), str(self.root / "demo_R2.fastq.gz")]}
        for path in self.raw["paths"]:
            Path(path).touch()
        self.contigs = self.root / "demo.fa"
        self.contigs.write_text(">test\nACGT\n")

    def resolve(self, text, previous=None):
        return resolve_intent(text, previous or self.raw)

    def test_bare_groups_require_scope_clarification(self):
        for text in ("只做W1", "只做W2", "只做 W1 和 W2", "W6"):
            with self.subTest(text=text):
                result = self.resolve(text)
                self.assertFalse(result["ready_to_execute"])
                self.assertTrue(result["ambiguous_workflows"])
                self.assertNotIn("execution_spec", result)

    def test_clarification_full_flow_applies_only_to_pending_group(self):
        pending = self.resolve("只做W2")
        resolved = self.resolve("执行完整流程", pending)
        self.assertEqual(resolved["goals"], ["virus_summary"])
        self.assertFalse(resolved["ready_to_execute"])
        self.assertTrue(resolved["missing_dependencies"])

    def test_partial_clarification_does_not_drop_other_pending_group(self):
        pending = self.resolve("只做W1和W2")
        partial = self.resolve("W1做到组装", pending)
        self.assertFalse(partial["ready_to_execute"])
        self.assertEqual(partial["ambiguous_workflows"], ["workflow2"])
        final = self.resolve("W2只提取病毒候选", partial)
        self.assertEqual(final["goals"], ["assembly", "virus_candidate_extract"])
        self.assertTrue(final["ready_to_execute"])
        self.assertNotIn("contig_filter", [stage["id"] for stage in final["stages"]])

    def test_qc_is_only_one_stage(self):
        result = self.resolve("只做质控")
        self.assertTrue(result["ready_to_execute"])
        self.assertEqual([stage["id"] for stage in result["stages"]], ["read_qc"])

    def test_stop_at_assembly_does_not_run_filter(self):
        result = self.resolve("W1做到组装为止")
        self.assertTrue(result["ready_to_execute"])
        self.assertEqual(result["goals"], ["assembly"])
        self.assertEqual([stage["id"] for stage in result["stages"]],
                         ["read_qc", "rrna_filter", "sample_merge", "assembly"])

    def test_only_assembly_from_reads_needs_consent(self):
        result = self.resolve("只做组装")
        self.assertFalse(result["ready_to_execute"])
        self.assertEqual(result["missing_dependencies"], ["read_qc", "rrna_filter", "sample_merge"])

    def test_w2_from_raw_reads_never_implicitly_authorizes_w1(self):
        result = self.resolve("W2只提取病毒候选，后面的证据分析先不做")
        self.assertEqual(result["goals"], ["virus_candidate_extract"])
        self.assertFalse(result["ready_to_execute"])
        self.assertNotIn("execution_spec", result)
        self.assertEqual(set(result["missing_dependencies"]),
                         {"read_qc", "rrna_filter", "sample_merge", "assembly", "contig_filter"})
        self.assertNotIn("read_qc", result["authorized_stages"])
        self.assertNotIn("virus_evidence", result["authorized_stages"])

    def test_dependency_consent_preserves_w2_target(self):
        pending = self.resolve("W2只提取病毒候选，后面的证据分析先不做")
        ready = self.resolve("同意补跑W1", pending)
        self.assertTrue(ready["ready_to_execute"])
        self.assertEqual(ready["goals"], pending["goals"])
        self.assertEqual(ready["confirmed_dependencies"], pending["missing_dependencies"])
        self.assertNotIn("virus_evidence", [stage["id"] for stage in ready["stages"]])

    def test_consent_for_single_dependency_is_not_consent_for_all(self):
        pending = self.resolve("W2只提取病毒候选")
        partial = self.resolve("同意补跑组装", pending)
        self.assertFalse(partial["ready_to_execute"])
        self.assertEqual(partial["confirmed_dependencies"], ["assembly"])
        self.assertIn("read_qc", partial["missing_dependencies"])

    def test_execute_confirmation_does_not_approve_missing_dependencies(self):
        pending = self.resolve("W2只提取病毒候选")
        attempted = self.resolve("确认启动分析", pending)
        self.assertFalse(attempted["ready_to_execute"])

    def test_declining_dependencies_keeps_target_blocked(self):
        pending = self.resolve("W2只提取病毒候选")
        declined = self.resolve("不补跑W1", pending)
        self.assertFalse(declined["ready_to_execute"])
        self.assertEqual(declined["goals"], pending["goals"])
        self.assertNotIn("execution_spec", declined)

    def test_providing_contigs_satisfies_dependency_without_w1(self):
        pending = self.resolve("W2只提取病毒候选")
        ready = self.resolve(f"我提供已有contigs：{self.contigs}", pending)
        self.assertTrue(ready["ready_to_execute"])
        self.assertEqual({stage["workflow_id"] for stage in ready["stages"]}, {"workflow2"})

    def test_contigs_allow_w2_to_stop_before_evidence(self):
        result = self.resolve(f"W2只提取病毒候选，后面的证据分析先不做。{self.contigs}")
        self.assertTrue(result["ready_to_execute"])
        self.assertEqual([stage["id"] for stage in result["stages"]],
                         ["virus_sequence_prep", "virus_candidate_search", "virus_candidate_extract"])

    def test_full_w1_and_partial_w2_do_not_expand_w2(self):
        for text in ("完整执行W1，W2只提取病毒候选", "完整W1和W2只提取病毒候选"):
            with self.subTest(text=text):
                result = self.resolve(text)
                self.assertTrue(result["ready_to_execute"])
                self.assertEqual(result["goals"], ["contig_filter", "virus_candidate_extract"])

    def test_parameter_only_edit_preserves_every_scope_field_and_inputs(self):
        previous = self.resolve("完整执行W1，W2只提取病毒候选，后面的证据分析先不做")
        previous["parameters"]["fastp.average_qual"] = 20
        original = copy.deepcopy(previous)
        changed = self.resolve("fastp最短长度改成八十，其他保持不变", previous)
        self.assertTrue(changed["ready_to_execute"])
        for key in ("goals", "inputs", "workflows", "excluded_stages", "stages"):
            self.assertEqual(changed[key], previous[key], key)
        self.assertEqual(changed["parameters"], {"fastp.average_qual": 20, "fastp.length_required": 80})
        self.assertEqual(changed["scope_patch"], {})
        self.assertEqual(previous, original)
        self.assertEqual(changed["base_plan_revision"], previous["plan_revision"])
        self.assertEqual(changed["json_patch"][0],
                         {"op": "test", "path": "/plan_revision", "value": previous["plan_revision"]})

    def test_invalid_patch_is_atomic_including_mixed_scope_change(self):
        previous = self.resolve("完整执行W1")
        result = self.resolve("只做质控，fastp.length_required改成-10", previous)
        self.assertFalse(result["accepted"])
        self.assertFalse(result["ready_to_execute"])
        self.assertEqual(result["goals"], previous["goals"])
        self.assertEqual(result["parameters"], previous["parameters"])
        self.assertEqual(result["json_patch"], [])

    def test_ambiguous_length_needs_clarification(self):
        result = self.resolve("最短长度改成80", self.resolve("完整执行W1"))
        self.assertFalse(result["accepted"])
        self.assertEqual(result["patch"], {})

    def test_parameters_for_unselected_stage_do_not_add_stage(self):
        previous = self.resolve(f"W2只提取病毒候选。{self.contigs}")
        result = self.resolve("fastp最短长度改成80", previous)
        self.assertFalse(result["ready_to_execute"])
        self.assertFalse(result["accepted"])
        self.assertEqual(result["parameters"], previous["parameters"])

    def test_cancel_w3_keeps_other_goals(self):
        previous = self.resolve("完整执行W1、W2、W3")
        result = self.resolve("取消W3", previous)
        self.assertTrue(result["ready_to_execute"])
        self.assertEqual(result["goals"], ["contig_filter", "virus_summary"])
        self.assertNotIn("workflow3", {stage["workflow_id"] for stage in result["stages"]})

    def test_excluded_dependency_is_not_silently_skipped(self):
        result = self.resolve("W1做到组装，但不要去rRNA")
        self.assertFalse(result["ready_to_execute"])
        self.assertIn("rrna_filter", result["missing_dependencies"])
        with self.assertRaises(ExecutionSpecError):
            compile_execution_spec({
                "version": 8, "adapter": "analysis_pipeline", "inputs": result["inputs"],
                "deliverables": ["assembly"], "options": {"excluded_stages": ["rrna_filter"]},
            })

    def test_actual_missing_file_blocks_execution(self):
        Path(self.raw["paths"][0]).unlink()
        result = self.resolve("只做质控")
        self.assertFalse(result["ready_to_execute"])
        self.assertNotIn("execution_spec", result)

    def test_parameter_aliases_and_chinese_numbers(self):
        self.assertEqual(parse_parameter_patch("fastp的最短长度改成八十", {"read_qc"}), {"fastp.length_required": 80})
        self.assertEqual(parse_parameter_patch("megahit最短contig长度改成一千", {"assembly"}), {"megahit.min_contig_len": 1000})
        for patch in ({"arbitrary.flag": 1}, {"fastp.length_required": True}, {"fastp.length_required": -1}):
            with self.subTest(patch=patch), self.assertRaises(ParameterError):
                validate_patch(patch)


if __name__ == "__main__":
    unittest.main()

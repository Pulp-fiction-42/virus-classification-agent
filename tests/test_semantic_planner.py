"""Model transport faults and proposal admission, without a live provider."""
import copy
import json
import socket
import tempfile
import time
import unittest
import uuid
import urllib.error
from pathlib import Path
from unittest.mock import patch

from plan_proposals import ProposalFormatError, admit_proposal, blocked_plan, parse_proposal
from semantic_planner import PlannerFailure, QwenPlanner
from runtime import EXECUTE_MODE, PLAN_MODE, OutputItemAdded, SessionManager, TextDelta


def proposal(revision=0, *, mode="keep", goals=None, parameters=None, tasks=None, **kwargs):
    value = {
        "version": 1, "base_plan_revision": revision,
        "tasks": tasks or (["intent"] if mode != "keep" else ["param_suggest"]),
        "scope_change": {"mode": mode}, "parameter_patch": parameters or {},
        "questions": [], "message": "",
    }
    if goals:
        value["scope_change"]["goals"] = goals
    value.update(kwargs)
    return value


def response(content, finish="stop"):
    return {"choices": [{"message": {"content": content}, "finish_reason": finish}], "usage": {"total_tokens": 10}}


class ProposalTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        paths = [str(root / "demo_R1.fastq.gz"), str(root / "demo_R2.fastq.gz")]
        for path in paths:
            Path(path).touch()
        self.raw = {"paths": paths}
        self.contigs = root / "demo.fa"
        self.contigs.write_text(">fixture\nACGT\n")
        self.qc = admit_proposal(proposal(mode="replace", goals=["read_qc"]), self.raw, "只做质控")
        self.assertTrue(self.qc["ready_to_execute"])

    def candidate(self):
        p = proposal(mode="replace", goals=["virus_candidate_extract"])
        p["scope_change"].update(prerequisite_groups=["workflow2"], excluded_stages=["virus_evidence", "virus_summary"])
        return p

    def test_multi_intent_atomic_patch_and_explanation(self):
        p = proposal(mode="replace", goals=["read_qc"], tasks=["intent", "param_suggest", "explain"],
                      parameters={"fastp.length_required": 80, "fastp.dedup": False, "fastp.average_qual": 25}, message="提高长度阈值会过滤更多短读段。")
        state = admit_proposal(p, self.raw, "只做质控；长度八十、平均质量25、不要去重；解释影响")
        self.assertTrue(state["ready_to_execute"])
        self.assertEqual([x["id"] for x in state["stages"]], ["read_qc"])
        self.assertEqual(state["parameters"], p["parameter_patch"])
        self.assertIn("提高", state["model_message"])

    def test_parameter_only_preserves_scope_inputs_other_parameters(self):
        old = copy.deepcopy(self.qc)
        old["parameters"] = {"fastp.length_required": 50, "fastp.average_qual": 20}
        state = admit_proposal(proposal(1, parameters={"fastp.length_required": 90}), old, "长度改九十")
        self.assertEqual(state["goals"], old["goals"])
        self.assertEqual(state["inputs"], old["inputs"])
        self.assertEqual(state["scope_patch"], {})
        self.assertEqual(state["parameters"], {"fastp.length_required": 90, "fastp.average_qual": 20})
        self.assertEqual(state["json_patch"][0]["op"], "test")

    def test_raw_to_w2_requires_cross_group_consent(self):
        state = admit_proposal(self.candidate(), self.raw, "W2提取候选")
        self.assertFalse(state["ready_to_execute"])
        self.assertIn("assembly", state["missing_dependencies"])
        self.assertNotIn("assembly", state["authorized_stages"])
        self.assertEqual(state["goals"], ["virus_candidate_extract"])
        self.assertNotIn("execution_spec", state)

    def test_dependency_consent_retains_target(self):
        old = admit_proposal(self.candidate(), self.raw, "W2提取候选")
        p = proposal(1, tasks=["dependency_response"], dependency_response={"allow": old["missing_dependencies"], "deny": []})
        state = admit_proposal(p, old, "允许补跑所列前置")
        self.assertTrue(state["ready_to_execute"])
        self.assertEqual(state["goals"], old["goals"])
        self.assertNotIn("virus_evidence", [x["id"] for x in state["stages"]])

    def test_partial_consent_and_denial_stay_blocked(self):
        old = admit_proposal(self.candidate(), self.raw, "W2提取候选")
        for decision in ({"allow": ["read_qc"], "deny": []}, {"allow": [], "deny": old["missing_dependencies"]}):
            with self.subTest(decision=decision):
                p = proposal(1, tasks=["dependency_response"], dependency_response=decision)
                state = admit_proposal(p, old, "部分同意或拒绝")
                self.assertFalse(state["ready_to_execute"])
                self.assertEqual(state["goals"], old["goals"])

    def test_ungrounded_dependency_consent_rejected(self):
        p = proposal(1, tasks=["dependency_response"], dependency_response={"allow": ["assembly"], "deny": []})
        state = admit_proposal(p, self.qc, "同意")
        self.assertEqual(state["failure_code"], "invalid_dependency_consent")

    def test_providing_contigs_satisfies_w2_without_w1(self):
        old = admit_proposal(self.candidate(), self.raw, "W2提取候选")
        p = proposal(1, tasks=["provide_inputs"], input_updates=[{"id": "contigs", "type": "contigs_fasta", "records": [{"sample_id": "demo", "files": {"fasta": str(self.contigs)}}]}])
        state = admit_proposal(p, old, "用已有contigs " + str(self.contigs))
        self.assertTrue(state["ready_to_execute"], state["questions"])
        self.assertFalse(any(x["id"] == "assembly" for x in state["stages"]))

    def test_unmentioned_input_path_rejected(self):
        p = proposal(1, tasks=["provide_inputs"], input_updates=[{"id": "contigs", "type": "contigs_fasta", "records": [{"sample_id": "demo", "files": {"fasta": str(self.contigs)}}]}])
        state = admit_proposal(p, self.qc, "用另一个文件")
        self.assertEqual(state["failure_code"], "ungrounded_input")

    def test_fasta_sample_identity_can_be_derived_from_filename(self):
        p = self.candidate()
        p['tasks'].append('provide_inputs')
        p['input_updates'] = [{'id':'contigs','type':'contigs_fasta','records':[{'files':{'fasta':str(self.contigs)}}]}]
        state = admit_proposal(p, self.raw, 'contigs ' + str(self.contigs))
        self.assertTrue(state['ready_to_execute'], state['questions'])
        self.assertEqual(state['inputs'][-1]['records'][0]['sample_id'], 'demo')

    def test_same_input_type_reuses_existing_id(self):
        p = proposal(1, tasks=['provide_inputs'], input_updates=[{**copy.deepcopy(self.qc['inputs'][0]),'id':'another_reads_id'}])
        state = admit_proposal(p, self.qc, '保持这些原始文件')
        self.assertTrue(state['ready_to_execute'], state['questions'])
        self.assertEqual(len(state['inputs']), 1)
        self.assertEqual(state['inputs'][0]['id'], self.qc['inputs'][0]['id'])

    def test_invalid_parameters_reject_entire_scope_change(self):
        for invalid in ({"fastp.length_required": -10}, {"fastp.dedup": "false"}, {"fastp.threads": 6}, {"fastp.average_qual": 41}):
            with self.subTest(invalid=invalid):
                p = proposal(1, mode="replace", goals=["assembly"], tasks=["intent", "param_suggest"], parameters=invalid)
                state = admit_proposal(p, self.qc, "改范围和参数")
                self.assertFalse(state["accepted"])
                self.assertEqual(state["goals"], self.qc["goals"])
                self.assertEqual(state["plan_revision"], 1)
                self.assertEqual(state["parameter_patch"], {})

    def test_parameter_outside_scope_rejected(self):
        state = admit_proposal(proposal(1, parameters={"megahit.min_contig_len": 1000}), self.qc, "组装长度1000")
        self.assertFalse(state["accepted"])
        self.assertEqual(state["parameters"], self.qc["parameters"])

    def test_ambiguous_multi_intent_keeps_original_pending_request(self):
        p = proposal(1, tasks=["param_suggest"], parameters={"fastp.dedup": False}, questions=["长度指哪一个参数？"])
        state = admit_proposal(p, self.qc, "最短长度80，关闭去重")
        failed = blocked_plan(state, "是reads长度", ["模型超时"], code="model_timeout")
        self.assertEqual(failed["pending_request"], "最短长度80，关闭去重")
        self.assertEqual(failed["pending_proposal"]["parameter_patch"], {"fastp.dedup": False})
        self.assertEqual(failed["pending_followups"][-1]["user_text"], "是reads长度")
        self.assertEqual(failed["parameters"], self.qc["parameters"])

    def test_stale_revision_rejected(self):
        state = admit_proposal(proposal(0, parameters={"fastp.length_required": 80}), self.qc, "长度80")
        self.assertEqual(state["failure_code"], "stale_revision")

    def test_unknown_stage_never_reaches_execution_compiler(self):
        state = admit_proposal(proposal(1, mode="replace", goals=["invented_step"]), self.qc, "自定义步骤")
        self.assertFalse(state["accepted"])
        self.assertNotIn("execution_spec", state)

    def test_contradictory_scope_rejected(self):
        p = proposal(1, mode="replace", goals=["read_qc"])
        p["scope_change"]["excluded_stages"] = ["read_qc"]
        self.assertEqual(admit_proposal(p, self.qc, "冲突")["failure_code"], "conflicting_intents")

    def test_extend_then_remove_dependency_does_not_drop_target(self):
        p = proposal(1, mode="extend", goals=["assembly"])
        p["scope_change"]["prerequisite_groups"] = ["workflow1"]
        old = admit_proposal(p, self.qc, "W1继续到组装")
        self.assertTrue(old["ready_to_execute"])
        p = proposal(2, mode="remove", goals=["rrna_filter"])
        state = admit_proposal(p, old, "不要去rRNA")
        self.assertFalse(state["ready_to_execute"])
        self.assertIn("assembly", state["goals"])
        self.assertIn("rrna_filter", state["missing_dependencies"])

    def test_bare_workflow_requires_clarification(self):
        p = proposal(1, mode="replace")
        p["scope_change"]["ambiguous_workflows"] = ["workflow2"]
        state = admit_proposal(p, self.qc, "只做W2")
        self.assertFalse(state["ready_to_execute"])
        self.assertFalse(state["accepted"])

    def test_edit_and_execute_deferred(self):
        p = proposal(1, tasks=["param_suggest", "execute"], parameters={"fastp.length_required": 80})
        state = admit_proposal(p, self.qc, "长度80并执行")
        self.assertTrue(state["ready_to_execute"])
        self.assertTrue(state["execution_deferred"])

    def test_readonly_with_execute_keeps_revision(self):
        p = proposal(1, tasks=["execute", "explain", "report"])
        state = admit_proposal(p, self.qc, "开始，解释并展示报告")
        self.assertEqual(state["action"], "execute")
        self.assertEqual(state["tasks"], p["tasks"])

    def test_duplicate_keys_extra_fields_and_nonfinite_rejected(self):
        for content in ('{"version":1,"version":1}', '{"version":NaN}', json.dumps({**proposal(), "ready_to_execute": True})):
            with self.subTest(content=content), self.assertRaises(ProposalFormatError):
                parse_proposal(content)

    def test_markdown_json_fence_allowed(self):
        self.assertEqual(parse_proposal("```json\n" + json.dumps(proposal()) + "\n```")["version"], 1)


class TransportTests(unittest.IsolatedAsyncioTestCase):
    def test_http_timeout_and_http_error_classification(self):
        planner = QwenPlanner(token="private-test-token")
        for error, code in ((urllib.error.URLError(socket.timeout()), "model_timeout"),
                            (urllib.error.HTTPError("http://example", 503, "private body", {}, None), "model_http_503")):
            with self.subTest(code=code), patch("urllib.request.urlopen", side_effect=error):
                with self.assertRaises(PlannerFailure) as caught:
                    planner._http({}, 1)
                self.assertEqual(str(caught.exception), code)

    async def test_one_format_repair(self):
        calls = []
        def transport(payload, timeout):
            calls.append((copy.deepcopy(payload), timeout))
            return response("not json" if len(calls) == 1 else json.dumps(proposal(tasks=["conversation"])))
        p, trace = await QwenPlanner(transport=transport, review_enabled=False).propose("你好", {}, [])
        self.assertEqual(p["tasks"], ["conversation"])
        self.assertEqual(trace["attempts"], 2)
        self.assertTrue(trace["format_repaired"])
        self.assertLessEqual(calls[1][1], calls[0][1])
        self.assertNotIn("tools", calls[0][0])
        self.assertEqual(calls[0][0]["response_format"], {"type":"json_object"})

    async def test_two_malformed_responses_fail_closed(self):
        with self.assertRaises(PlannerFailure) as caught:
            await QwenPlanner(transport=lambda *a: response("not json")).propose("质控", {}, [])
        self.assertEqual(caught.exception.code, "invalid_model_proposal")
        self.assertEqual(caught.exception.attempts, 2)

    async def test_truncated_response_is_repaired_once(self):
        calls = []
        def transport(*args):
            calls.append(1)
            return response(json.dumps(proposal(tasks=["conversation"])), "length" if len(calls) == 1 else "stop")
        _, trace = await QwenPlanner(transport=transport, review_enabled=False).propose("你好", {}, [])
        self.assertEqual(trace["attempts"], 2)

    async def test_transport_errors_do_not_retry_or_expose_credentials(self):
        def transport(*args):
            raise PlannerFailure("model_http_503")
        with self.assertRaises(PlannerFailure) as caught:
            await QwenPlanner(transport=transport, token="secret-test-token").propose("质控", {}, [])
        self.assertEqual(str(caught.exception), "model_http_503")
        self.assertEqual(caught.exception.attempts, 1)

    async def test_total_timeout(self):
        def transport(*args):
            time.sleep(0.04)
            return response(json.dumps(proposal(tasks=["conversation"])))
        with self.assertRaises(PlannerFailure) as caught:
            await QwenPlanner(timeout=0.01, transport=transport).propose("质控", {}, [])
        self.assertEqual(caught.exception.code, "model_timeout")

    async def test_illegal_business_value_does_not_trigger_format_repair(self):
        calls = []
        def transport(*args):
            calls.append(1)
            return response(json.dumps(proposal(parameters={"fastp.length_required": -10})))
        p, _ = await QwenPlanner(transport=transport, review_enabled=False).propose("长度负十", {}, [])
        self.assertEqual(p["parameter_patch"]["fastp.length_required"], -10)
        self.assertEqual(len(calls), 1)

    async def test_independent_review_restores_missing_auxiliary_intent(self):
        calls = []
        def transport(payload, timeout):
            calls.append(payload)
            return response(json.dumps(proposal(parameters={'fastp.length_required':80}))) if len(calls)==1 else response(json.dumps({'ok':True,'questions':[],'requested_tasks':['execute','explain'],'message':'过滤较短读段。'}))
        p, trace = await QwenPlanner(transport=transport).propose('长度80并执行，解释影响', {}, [])
        self.assertEqual(p['tasks'], ['param_suggest','execute','explain'])
        self.assertEqual(p['message'], '过滤较短读段。')
        self.assertEqual(trace['model_calls'], 2)
        self.assertTrue(trace['semantic_review']['ok'])

    async def test_independent_review_blocks_semantically_ambiguous_proposal(self):
        calls = []
        def transport(payload, timeout):
            calls.append(payload)
            return response(json.dumps(proposal(parameters={'fastp.length_required':80}))) if len(calls)==1 else response(json.dumps({'ok':False,'questions':['指reads还是contigs长度？'],'requested_tasks':[]}))
        p, trace = await QwenPlanner(transport=transport).propose('最短长度80', {}, [])
        self.assertEqual(p['questions'], ['指reads还是contigs长度？'])
        self.assertFalse(trace['semantic_review']['ok'])

    async def test_review_failure_does_not_accept_primary_proposal(self):
        calls = []
        def transport(payload, timeout):
            calls.append(payload)
            return response(json.dumps(proposal(parameters={'fastp.length_required':80}))) if len(calls)==1 else response('{"ok":true,"questions":["inconsistent"],"requested_tasks":[]}')
        with self.assertRaises(PlannerFailure) as caught:
            await QwenPlanner(transport=transport).propose('长度80', {}, [])
        self.assertEqual(caught.exception.code,'invalid_semantic_review')

    async def test_review_can_repeat_known_tasks_without_changing_domain_payload(self):
        calls = []
        def transport(payload, timeout):
            calls.append(payload)
            return response(json.dumps(proposal(parameters={'fastp.length_required':80}))) if len(calls)==1 else response(json.dumps({'ok':True,'questions':[],'requested_tasks':['param_suggest','execute']}))
        p, _ = await QwenPlanner(transport=transport).propose('长度80并执行', {}, [])
        self.assertEqual(p['tasks'], ['param_suggest','execute'])
        self.assertEqual(p['parameter_patch'], {'fastp.length_required':80})

    async def test_missing_dependency_questions_remain_backend_owned(self):
        p = proposal(mode='replace',goals=['virus_candidate_extract'])
        p['scope_change']['prerequisite_groups']=['workflow2']
        calls = []
        def transport(payload, timeout):
            calls.append(payload)
            return response(json.dumps(p))
        raw = {'paths':[str(Path(tempfile.gettempdir()) / name) for name in ('demo_R1.fastq.gz','demo_R2.fastq.gz')]}
        _, trace = await QwenPlanner(transport=transport).propose('W2提取候选', raw, [])
        self.assertTrue(trace['review_deferred_for_dependencies'])
        self.assertEqual(len(calls),1)


class RuntimeModelTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.manager = SessionManager(root, root / "config", analysis_run_root=root / "runs")
        paths = [str(root / "demo_R1.fastq.gz"), str(root / "demo_R2.fastq.gz")]
        for path in paths:
            Path(path).touch()
        state = admit_proposal(proposal(mode="replace", goals=["read_qc"]), {"paths": paths}, "只做质控")
        self.old = self.manager._build_analysis_plan({"steps": []}, state)
        self.chat = str(uuid.uuid4())
        self.manager._save_analysis_plan(self.chat, self.old)
        self.addAsyncCleanup(self.manager.close_all)

    async def turn(self, text):
        return [x async for x in self.manager.stream_turn([{"role": "user", "content": text}], self.chat)]

    def use_planner(self, value=None, error=None):
        manager, chat = self.manager, self.chat
        class Stub:
            async def propose(self, *args):
                # The disk guard must already have revoked the old run.
                assert manager._load_analysis_plan(chat)["ready_to_execute"] is False
                if error:
                    raise error
                return value, {"source": "qwen", "attempts": 1, "model": "fake"}
        self.manager.semantic_planner = Stub()

    async def test_failed_model_revokes_old_plan_and_cannot_execute(self):
        self.use_planner(error=PlannerFailure("model_timeout", attempts=1))
        await self.turn("长度改80，同时只做质控")
        saved = self.manager._load_analysis_plan(self.chat)
        self.assertFalse(saved["ready_to_execute"])
        self.assertEqual(saved["planning"]["goals"], ["read_qc"])
        await self.manager.set_mode(self.chat, EXECUTE_MODE)
        with patch("runtime.run_pipeline") as runner:
            events = await self.turn("确认启动分析")
            runner.assert_not_called()
        self.assertIn("先生成可执行", "".join(e.text for e in events if isinstance(e, TextDelta)))

    async def test_edit_in_execute_mode_creates_draft_without_running(self):
        self.use_planner(proposal(1, parameters={"fastp.length_required": 80}, tasks=["param_suggest", "execute", "explain"], message="短read会被过滤"))
        await self.manager.set_mode(self.chat, EXECUTE_MODE)
        with patch("runtime.run_pipeline") as runner:
            events = await self.turn("长度80并执行，解释影响")
            runner.assert_not_called()
        saved = self.manager._load_analysis_plan(self.chat)
        self.assertEqual(saved["parameters"]["fastp.length_required"], 80)
        self.assertEqual(self.manager._sessions[self.chat].mode, PLAN_MODE)
        text = "".join(e.text for e in events if isinstance(e, TextDelta))
        self.assertIn("短read", text)
        self.assertIn("最新方案", text)
        self.assertEqual(saved["planning"]["planner_trace"]["source"], "qwen")

    async def test_readonly_preserves_saved_plan(self):
        self.use_planner(proposal(1, tasks=["explain"], message="这是质控步骤"))
        await self.turn("解释当前方案")
        saved = self.manager._load_analysis_plan(self.chat)
        for key in self.old:
            self.assertEqual(saved[key], self.old[key])

    async def test_report_and_edit_both_acknowledged_without_rerun(self):
        self.use_planner(proposal(1, parameters={"fastp.length_required": 80}, tasks=["param_suggest", "report"]))
        with patch("runtime.run_pipeline") as runner:
            events = await self.turn("长度80，并给我旧报告")
            runner.assert_not_called()
        self.assertEqual(self.manager._load_analysis_plan(self.chat)["parameters"]["fastp.length_required"], 80)
        self.assertIn("结果报告", "".join(e.text for e in events if isinstance(e, TextDelta)))
        self.assertTrue(any(isinstance(e, OutputItemAdded) for e in events))

    async def test_no_rule_parser_is_called_for_normal_model_turn(self):
        self.use_planner(proposal(1, parameters={"fastp.dedup": False}))
        with patch("runtime.resolve_intent", side_effect=AssertionError("legacy parser must not run")):
            await self.turn("重复读段这次留下")
        self.assertFalse(self.manager._load_analysis_plan(self.chat)["parameters"]["fastp.dedup"])

    async def test_completed_run_is_available_after_a_parameter_edit(self):
        state_path = Path(self.old["run"]["state_path"])
        state_path.parent.mkdir(parents=True)
        state_path.write_text(json.dumps({"execution":{"status":"completed", "ok":True}}))
        self.use_planner(proposal(1, parameters={"fastp.length_required":80}))
        await self.turn("长度80")
        saved = self.manager._load_analysis_plan(self.chat)
        self.assertEqual(saved["last_completed_plan"]["run"], self.old["run"])
        with patch.object(self.manager, "_save_diagnostic_report", return_value={"type":"diagnostic_report"}) as report, patch("runtime.collect_evidence", return_value={}), patch("runtime.render_fragment", return_value="<div>report</div>"), patch("runtime.summary_text", return_value="old result"), patch("runtime.run_pipeline") as runner:
            events = self.manager._existing_report_events(saved)
            report.assert_called_once()
            self.assertEqual(report.call_args.args[0]["run"], self.old["run"])
            runner.assert_not_called()
        self.assertEqual(events[0].text, "old result")


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import json
import unittest
from pathlib import Path

import config
from conversation_contract import (
    ConversationContractError,
    ConversationReply,
    KnowledgeInsufficientError,
    RetrievalRequest,
    parse_conversation_action,
    proposal_output,
    validate_conversation_action,
    validate_conversation_reply,
)
from taxonagent_adapter import TaxonAgentRetrievalAdapter, taxonagent_citation


class ConversationContractTests(unittest.IsolatedAsyncioTestCase):
    def test_dashboard_reply_is_message_only(self):
        reply = validate_conversation_reply({"message": "说明"})
        self.assertEqual(reply, ConversationReply(message="说明"))
        for value in ({}, {"message": "说明", "action": "respond"}):
            with self.subTest(value=value), self.assertRaises(
                ConversationContractError
            ):
                validate_conversation_reply(value)

    def test_prompt_supports_controlled_qc_and_requires_explicit_values(self):
        self.assertIn('goals=["read_qc"]', config.SYSTEM_PROMPT)
        self.assertIn("exact value", config.SYSTEM_PROMPT)
        self.assertIn("dependency conflict", config.SYSTEM_PROMPT)

    def test_accepts_all_actions(self):
        respond = parse_conversation_action(
            '```conversation_action\n{"action":"respond","message":"回答","citations":[]}\n```'
        )
        clarify = parse_conversation_action(
            '```conversation_action\n{"action":"clarify","message":"请确认样本。","citations":[]}\n```'
        )
        patch = parse_conversation_action(
            "```conversation_action\n"
            '{"action":"propose_patch","message":"建议调整。",'
            '"proposed_patch":{"goals":["virus_summary"],"viral_type":"RNA"},"citations":[]}\n```'
        )
        self.assertEqual(respond.action, "respond")
        self.assertEqual(clarify.action, "clarify")
        self.assertEqual(proposal_output(patch)["patch"]["viral_type"], "RNA")

        qc = validate_conversation_action(
            {
                "action": "propose_patch",
                "message": "只做质控。",
                "proposed_patch": {"goals": ["read_qc"]},
                "citations": [],
            }
        )
        fastp = validate_conversation_action(
            {
                "action": "propose_patch",
                "message": "把最短保留长度改为 50 bp。",
                "proposed_patch": {
                    "capability_parameters": [
                        {
                            "capability_id": "workflow1.read_qc.fastp",
                            "manifest_version": 1,
                            "preset_id": "workflow_default",
                            "values": {"length_required": 50},
                        }
                    ]
                },
                "citations": [],
            }
        )
        self.assertEqual(qc.proposed_patch.goals, ["read_qc"])
        self.assertEqual(
            fastp.proposed_patch.capability_parameters[0].values,
            {"length_required": 50},
        )

    def test_rejects_forbidden_patch_fields_and_invalid_envelopes(self):
        values = [
            "plain text",
            '```conversation_action\n{"action":"unknown","message":"x"}\n```',
            (
                "```conversation_action\n"
                '{"action":"propose_patch","message":"x","proposed_patch":{"mode":"emergency"}}\n```'
            ),
            (
                "```conversation_action\n"
                '{"action":"propose_patch","message":"x","proposed_patch":{"goals":["unknown"]}}\n```'
            ),
            (
                "```conversation_action\n"
                '{"action":"propose_patch","message":"x","proposed_patch":{"capability_parameters":['
                '{"capability_id":"workflow1.read_qc.fastp","manifest_version":1,'
                '"values":{"command":"rm"}}]}}\n```'
            ),
        ]
        for value in values:
            with (
                self.subTest(value=value),
                self.assertRaises(ConversationContractError),
            ):
                parse_conversation_action(value)

    def test_structured_output_accepts_three_actions_and_rejects_all_control_fields(
        self,
    ):
        for action in (
            {"action": "respond", "message": "回答", "citations": []},
            {"action": "clarify", "message": "请补充目标。", "citations": []},
            {
                "action": "propose_patch",
                "message": "建议调整。",
                "proposed_patch": {"goals": ["contig_filter"], "viral_type": "DNA"},
                "citations": [],
            },
        ):
            with self.subTest(action=action["action"]):
                self.assertEqual(
                    validate_conversation_action(action).action, action["action"]
                )

        for field in (
            "patch",
            "mode",
            "dataset",
            "path",
            "sample",
            "run",
            "steps",
            "resources",
            "execution",
        ):
            value = {
                "action": "propose_patch",
                "message": "越权修改",
                "proposed_patch": {"viral_type": "RNA"},
                "citations": [],
                field: {},
            }
            with (
                self.subTest(field=field),
                self.assertRaises(ConversationContractError) as raised,
            ):
                validate_conversation_action(value)
            self.assertEqual(raised.exception.code, "PATCH_FIELD_FORBIDDEN")

        uncited = validate_conversation_action(
            {
                "action": "respond",
                "message": "一般知识回答",
                "citations": ["unverified-source"],
            }
        )
        self.assertEqual(uncited.citations, [])

        clarify = validate_conversation_action(
            {
                "action": "clarify",
                "message": "请补充目标。",
                "proposed_patch": {},
                "citations": [],
            }
        )
        self.assertIsNone(clarify.proposed_patch)
        with self.assertRaises(ConversationContractError):
            validate_conversation_action(
                {
                    "action": "propose_patch",
                    "message": "空修改",
                    "proposed_patch": {},
                    "citations": [],
                }
            )

    def test_taxonagent_citation_requires_version_and_locator_for_quality(self):
        incomplete = taxonagent_citation({"text": "result"}, retrieved_at="2026-01-01")
        verified = taxonagent_citation(
            {"text": "result", "source_version": "v1", "locator": "family/1"},
            retrieved_at="2026-01-01",
        )
        self.assertEqual(incomplete.quality, "incomplete")
        self.assertEqual(verified.quality, "verified")
        self.assertEqual(KnowledgeInsufficientError.code, "KNOWLEDGE_INSUFFICIENT")

    def test_provider_neutral_golden_cases(self):
        cases = json.loads(
            (
                Path(__file__).parents[1] / "benchmarks" / "conversation_actions.json"
            ).read_text(encoding="utf-8")
        )
        for case in cases:
            with self.subTest(case=case["id"]):
                if "provider_error" in case:
                    self.assertFalse(case["valid"])
                    continue
                if case["valid"]:
                    action = parse_conversation_action(case["output"])
                    self.assertEqual(action.action, case["action"])
                else:
                    with self.assertRaises(ConversationContractError):
                        parse_conversation_action(case["output"])

    async def test_taxonagent_retrieval_boundary_is_fail_closed(self):
        async def fetch(_query: str, _limit: int):
            return [{"text": "ICTV result"}]

        adapter = TaxonAgentRetrievalAdapter(fetch, clock=lambda: "2026-01-01")
        result = await adapter.retrieve(
            RetrievalRequest(query="virus", corpora=("ictv",))
        )
        unsupported = await adapter.retrieve(
            RetrievalRequest(query="workflow", corpora=("workflow_capabilities",))
        )
        self.assertEqual(result.passages[0].quality, "incomplete")
        self.assertEqual(unsupported.error_code, "KNOWLEDGE_INSUFFICIENT")


if __name__ == "__main__":
    unittest.main()

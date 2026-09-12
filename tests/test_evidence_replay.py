"""Synthetic engineering fixtures; these are not biological validation cases."""

from __future__ import annotations

import copy
import json
import socket
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from conversation_contract import RetrievalRequest
from evidence_contract import ReleaseSet
from evidence_replay import evaluate_rules, explain, screening_candidate, validate_draft
from knowledge import KnowledgeStore, digest, terms
from provider import MiniMaxProvider, ProviderResult

ASSETS = Path(__file__).resolve().parents[1] / "assets/m0"


def synthetic_bundle(root):
    sample = {
        "sample_id": "synthetic-sample",
        "sampled_read_pairs": 100,
        "post_filter_read_pairs": 80,
        "viral_reads": 40,
        "top_species": {"taxid": "999", "name": "Synthetic virus", "reads": 39},
        "top_species_sample_fraction": 0.39,
        "top_species_viral_fraction": 0.975,
        "post_filter_ratio": 0.8,
    }
    base = "results/screening/synthetic-sample"
    contents = {
        ".bioagent/state.json": json.dumps(
            {"run_id": "analysis-synthetic", "execution": {"status": "completed"}}
        ),
        ".bioagent/telemetry.json": json.dumps(
            {
                "delivery_report": {
                    "schema_version": 4,
                    "kind": "analysis_pipeline",
                    "report_kind": "preliminary_alert",
                    "screening": {"samples": [sample], "database": "/reference/test"},
                }
            }
        ),
        ".bioagent/screening.log": f"$ kraken2 --db /reference/test --paired --report /runs/analysis-synthetic/{base}/kraken.report /runs/analysis-synthetic/{base}/clean_r1.fastq.gz /runs/analysis-synthetic/{base}/clean_r2.fastq.gz\n",
        f"{base}/fastp.json": json.dumps(
            {
                "summary": {
                    "fastp_version": "test",
                    "before_filtering": {"total_reads": 200},
                }
            }
        ),
        f"{base}/kraken.report": "37.5\t30\t30\tU\t0\tunclassified\n62.5\t50\t10\tR\t1\troot\n50\t40\t1\tD\t10239\t  Viruses\n48.75\t39\t39\tS\t999\t    Synthetic virus\n",
    }
    for name, text in contents.items():
        p = root / name
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text)
    return {
        "run_id": "analysis-synthetic",
        "sample_id": "synthetic-sample",
        "taxid": "999",
        "source_kind": "synthetic_engineering_fixture",
        "files": {name: digest(text.encode()) for name, text in contents.items()},
    }


class DraftProvider:
    def __init__(self):
        self.requests = []

    async def generate(self, request):
        self.requests.append(request)
        context = request.context
        return ProviderResult(
            provider="fake",
            model="fake",
            output={
                "candidate_id": context["candidate_id"],
                "claims": [
                    {
                        "text": "候选分支计数为 {{E-CANDIDATE}}，仅用于初筛解释。",
                        "evidence_ids": ["E-CANDIDATE"],
                        "citation_ids": [context["passages"][0]["citation_id"]],
                    }
                ],
                "finding_ids": [item["rule_id"] for item in context["findings"]],
            },
        )


class EvidenceReplayTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.manifest = synthetic_bundle(self.root)
        self.candidate = screening_candidate(self.root, self.manifest)
        self.store = KnowledgeStore(
            ASSETS / "knowledge.json", digest((ASSETS / "knowledge.json").read_bytes())
        )
        self.addCleanup(self.store.close)
        self.rules = json.loads((ASSETS / "rules.json").read_text())
        self.releases = ReleaseSet.model_validate_json(
            (ASSETS / "releases.json").read_text()
        )

    def test_raw_counts_and_distinct_denominators_are_recomputed(self):
        evidence = {o.evidence_id: o for o in self.candidate.observations}
        self.assertEqual(evidence["E-SAMPLED"].value, 100)
        self.assertEqual(evidence["E-CANDIDATE"].unit, "read_pairs")
        self.assertEqual(evidence["E-SAMPLE-FRACTION"].value, 0.39)
        self.assertEqual(evidence["E-FILTERED-FRACTION"].value, 39 / 80)
        self.assertEqual(evidence["E-VIRAL-FRACTION"].denominator, "E-VIRAL")
        self.assertEqual(len({o.source_group for o in evidence.values()}), 1)

    def test_hash_identity_traversal_and_symlinks_are_rejected(self):
        for key, value in [
            ("sample_id", "wrong"),
            ("taxid", "wrong"),
            ("run_id", "wrong"),
        ]:
            changed = {**self.manifest, key: value}
            with self.assertRaises(ValueError):
                screening_candidate(self.root, changed)
        path = self.root / "results/screening/synthetic-sample/kraken.report"
        original = path.read_text()
        path.write_text(original.replace("\t39\t39\t", "\t38\t38\t"))
        with self.assertRaisesRegex(ValueError, "hash mismatch"):
            screening_candidate(self.root, self.manifest)
        path.unlink()
        outside = self.root / "other.report"
        outside.write_text(original)
        path.symlink_to(outside)
        with self.assertRaisesRegex(ValueError, "symlink"):
            screening_candidate(self.root, self.manifest)
        with self.assertRaises(ValueError):
            screening_candidate(
                self.root, {**self.manifest, "files": {"../escape": "x"}}
            )

    def test_rehashed_metadata_with_wrong_denominator_still_fails_raw_verification(
        self,
    ):
        relative = ".bioagent/telemetry.json"
        path = self.root / relative
        data = json.loads(path.read_text())
        data["delivery_report"]["screening"]["samples"][0][
            "top_species_sample_fraction"
        ] = 39 / 80
        path.write_text(json.dumps(data))
        self.manifest["files"][relative] = digest(path.read_bytes())
        with self.assertRaisesRegex(ValueError, "sample denominator"):
            screening_candidate(self.root, self.manifest)

    async def test_pending_sources_are_excluded_by_default_and_query_is_offline(self):
        with patch.object(
            socket, "socket", side_effect=AssertionError("network forbidden")
        ):
            approved = await self.store.retrieve(
                RetrievalRequest(query="Kraken 配对", corpora=("tools_databases",))
            )
            development = await self.store.retrieve(
                RetrievalRequest(
                    query="Kraken 配对",
                    corpora=("tools_databases",),
                    include_unreviewed=True,
                )
            )
        self.assertEqual(approved.error_code, "KNOWLEDGE_INSUFFICIENT")
        self.assertTrue(development.passages)
        self.assertTrue(
            all(
                p.review_status == "pending" and p.quality != "verified"
                for p in development.passages
            )
        )
        self.assertIn("配对", terms("Kraken2 配对分类"))
        for passage in development.passages:
            self.store.verify(passage)
        altered = development.passages[0].model_copy(update={"locator": "invented"})
        with self.assertRaises(ValueError):
            self.store.verify(altered)

    async def test_observation_replay_generates_only_pending_draft(self):
        provider = DraftProvider()
        assessment, retrieval, trace = await explain(
            self.candidate,
            store=self.store,
            rules=self.rules,
            release_set=self.releases,
            provider=provider,
            include_unreviewed=True,
        )
        self.assertEqual(assessment.status, "draft")
        self.assertEqual(assessment.review_status, "pending")
        self.assertEqual(trace["model_calls"], 1)
        self.assertNotIn("source_path", str(provider.requests[0].context))
        self.assertTrue(retrieval.passages)
        self.assertIn("REFERENCE_UNKNOWN", assessment.draft.finding_ids)

    async def test_empty_knowledge_and_cloud_disabled_preserve_numeric_evidence(self):
        for include_unreviewed, expected in [
            (False, "KNOWLEDGE_INSUFFICIENT"),
            (True, "CLOUD_DISABLED"),
        ]:
            assessment, _, trace = await explain(
                self.candidate,
                store=self.store,
                rules=self.rules,
                release_set=self.releases,
                provider=MiniMaxProvider(),
                include_unreviewed=include_unreviewed,
            )
            self.assertEqual(assessment.status, "not_generated")
            self.assertEqual(assessment.error_code, expected)
            self.assertEqual(trace["model_calls"], 0)
            self.assertEqual(self.candidate.observations[0].value, 100)

    async def test_numeric_invention_citation_forgery_and_omitted_findings_rejected(
        self,
    ):
        assessment, retrieval, _ = await explain(
            self.candidate,
            store=self.store,
            rules=self.rules,
            release_set=self.releases,
            provider=DraftProvider(),
            include_unreviewed=True,
        )
        original = assessment.draft.model_dump()
        mutations = [
            lambda d: d.update(candidate_id="other"),
            lambda d: d.update(finding_ids=[]),
            lambda d: d["claims"][0].update(citation_ids=["invented"]),
            lambda d: d["claims"][0].update(text="计数为 99999"),
            lambda d: d["claims"][0].update(text="计数 {{E-UNKNOWN}}"),
            lambda d: d["claims"][0].update(evidence_ids=[]),
        ]
        for mutate in mutations:
            data = copy.deepcopy(original)
            mutate(data)
            with self.assertRaises(ValueError):
                validate_draft(
                    data, self.candidate, retrieval, assessment.findings, self.store
                )

    def test_explicitly_derived_conflict_missing_and_zero_cases(self):
        # Deliberately modified observations; never counted as biological cases.
        derived = self.candidate.model_copy(deep=True)
        derived.observations[-1].value = 1.2
        findings = evaluate_rules(derived, self.rules, knowledge_available=True)
        self.assertIn("COUNT_CONFLICT", {f.rule_id for f in findings})
        derived.observations[-1].value = None
        findings = evaluate_rules(derived, self.rules, knowledge_available=True)
        self.assertIn("EVIDENCE_MISSING", {f.rule_id for f in findings})
        derived.observations[-1].value = 0
        findings = evaluate_rules(derived, self.rules, knowledge_available=True)
        self.assertNotIn("EVIDENCE_MISSING", {f.rule_id for f in findings})
        self.assertNotIn("COUNT_CONFLICT", {f.rule_id for f in findings})

    def test_asset_hash_and_source_locator_tampering_are_rejected(self):
        with self.assertRaises(ValueError):
            KnowledgeStore(ASSETS / "knowledge.json", "wrong-version")
        data = json.loads((ASSETS / "knowledge.json").read_text())
        data["sources"][0]["content"] = "modified"
        path = self.root / "knowledge.json"
        path.write_text(json.dumps(data))
        with self.assertRaisesRegex(ValueError, "source hash"):
            KnowledgeStore(path, digest(path.read_bytes()))

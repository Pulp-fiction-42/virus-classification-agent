"""M1 development boundaries. Synthetic fixtures are not expert truth labels."""

import copy
import json
import socket
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from jsonschema import Draft202012Validator, ValidationError
from test_evidence_replay import ASSETS, synthetic_bundle

from conversation_contract import RetrievalRequest
from evidence_replay import screening_candidate
from knowledge import KnowledgeStore, digest
from m1_assets import check_assets, observe
from m1_evaluation import (
    evaluate,
    fixture_output,
    load_cases,
    negative_outputs,
    prepare,
    validate_output,
)
from provider import MiniMaxProvider, ProviderResult

CASES = ASSETS.parents[1] / "benchmarks/m1_development.json"
PROTOCOL = CASES.with_name("evaluation_protocol_v1.json")


class M1Tests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        raw_suite = json.loads(CASES.read_text())
        self.suite = load_cases(CASES, raw_suite["bundle_sha256"], ASSETS)
        self.manifest = synthetic_bundle(self.root)
        self.candidate = screening_candidate(self.root, self.manifest)
        self.store = KnowledgeStore(
            ASSETS / "knowledge.json", digest((ASSETS / "knowledge.json").read_bytes())
        )
        self.addCleanup(self.store.close)
        self.rules = json.loads((ASSETS / "rules.json").read_text())

    def test_frozen_suite_rejects_wrong_bundle_and_duplicate_ids(self):
        self.assertEqual(
            len(load_cases(CASES, self.suite["bundle_sha256"], ASSETS)["cases"]), 16
        )
        with self.assertRaisesRegex(ValueError, "bundle"):
            load_cases(CASES, "wrong", ASSETS)
        self.suite["cases"][0]["id"] = self.suite["cases"][1]["id"]
        changed = self.root / "cases.json"
        changed.write_text(json.dumps(self.suite))
        with self.assertRaisesRegex(ValueError, "duplicate"):
            load_cases(changed, self.suite["bundle_sha256"], ASSETS)

    def test_protocol_rejects_suite_digest_drift(self):
        protocol = json.loads(PROTOCOL.read_text())
        protocol["suite_sha256"] = "0" * 64
        changed = self.root / "wrong-digest.json"
        changed.write_text(json.dumps(protocol))
        with self.assertRaisesRegex(ValueError, "SHA-256"):
            load_cases(
                CASES,
                self.suite["bundle_sha256"],
                ASSETS,
                changed,
            )

    def test_protocol_rejects_partition_group_metric_and_gate_drift(self):
        variants = []
        missing_case = json.loads(PROTOCOL.read_text())
        missing_case["splits"]["development"]["case_ids"].pop()
        variants.append((missing_case, "partition"))

        missing_metric = json.loads(PROTOCOL.read_text())
        missing_metric["metrics"].pop()
        variants.append((missing_metric, "metric set"))

        open_gate = json.loads(PROTOCOL.read_text())
        open_gate["release_gate"] = {"status": "ready", "blockers": []}
        variants.append((open_gate, "remain blocked"))

        for index, (protocol, error) in enumerate(variants):
            with self.subTest(error=error):
                changed = self.root / f"invalid-protocol-{index}.json"
                changed.write_text(json.dumps(protocol))
                with self.assertRaisesRegex(ValueError, error):
                    load_cases(
                        CASES,
                        self.suite["bundle_sha256"],
                        ASSETS,
                        changed,
                    )

        protocol = json.loads(PROTOCOL.read_text())
        moved = "intent_clarification-4"
        protocol["splits"]["development"]["case_ids"].remove(moved)
        protocol["splits"]["final_test"]["case_ids"] = [moved]
        changed = self.root / "cross-split.json"
        changed.write_text(json.dumps(protocol))
        with self.assertRaisesRegex(ValueError, "crosses split"):
            load_cases(
                CASES,
                self.suite["bundle_sha256"],
                ASSETS,
                changed,
            )

    async def test_all_offline_cases_pass_and_injected_failures_are_rejected(self):
        before = self.candidate.model_dump()
        with patch.object(
            socket, "socket", side_effect=AssertionError("network forbidden")
        ):
            report = await evaluate(self.suite, self.candidate, self.store, self.rules)
            for case, row in zip(self.suite["cases"], report["records"], strict=True):
                self.assertTrue(row["engineering_pass"], case["id"])
                request, candidate, retrieval, findings = await prepare(
                    case, self.candidate, self.store, self.rules
                )
                with self.assertRaises((ValueError, ValidationError)):
                    validate_output(
                        case,
                        negative_outputs(case, row["result"]["output"]),
                        request,
                        candidate,
                        retrieval,
                        findings,
                        self.store,
                    )
                self.assertNotIn("source_path", str(request.context))
                self.assertNotIn("/reference/", str(request.context))
        self.assertEqual(before, self.candidate.model_dump())
        self.assertFalse(report["business_writes"])
        self.assertIn(
            "COUNT_CONFLICT", [r["rule_id"] for r in report["records"][-1]["findings"]]
        )
        self.assertEqual(report["semantic_accuracy"], "not_reviewed")
        protocol = report["evaluation_protocol"]
        self.assertEqual(protocol["split_counts"]["development"], 16)
        self.assertEqual(protocol["split_counts"]["final_test"], 0)
        self.assertEqual(
            protocol["automated"]["schema_pass_rate"],
            {"numerator": 16, "denominator": 16, "rate": 1.0},
        )
        self.assertEqual(protocol["release_gate"]["status"], "blocked")

    async def test_provider_called_once_per_case_and_failures_remain_results(self):
        class FailingProvider:
            calls = 0

            async def generate(self, request):
                self.calls += 1
                if self.calls % 2:
                    return ProviderResult(
                        provider="fake", model="fake", error_code="MODEL_TIMEOUT"
                    )
                return ProviderResult(
                    provider="fake", model="fake", output={"invented": True}
                )

        provider = FailingProvider()
        report = await evaluate(
            self.suite, self.candidate, self.store, self.rules, provider
        )
        self.assertEqual(provider.calls, 16)
        self.assertFalse(any(r["engineering_pass"] for r in report["records"]))
        self.assertEqual(report["records"][0]["result"]["error_code"], "MODEL_TIMEOUT")
        self.assertEqual(report["records"][1]["validation_error"], "SCHEMA_INVALID")
        protocol = report["evaluation_protocol"]["automated"]
        self.assertEqual(
            protocol["provider_error_rate"],
            {"numerator": 8, "denominator": 16, "rate": 0.5},
        )
        self.assertEqual(
            protocol["total_latency_ms"],
            {
                "status": "measured",
                "count": 16,
                "p50": 0,
                "p95": 0,
                "max": 0,
                "first_response_latency": "unavailable",
            },
        )

    async def test_cloud_disabled_does_not_load_sdk(self):
        request, *_ = await prepare(
            self.suite["cases"][0], self.candidate, self.store, self.rules
        )
        provider = MiniMaxProvider()
        with patch.object(
            provider, "_generate", side_effect=AssertionError("SDK forbidden")
        ):
            self.assertEqual(
                (await provider.generate(request)).error_code, "CLOUD_DISABLED"
            )

    async def test_action_citations_and_wrong_legal_patch_are_rejected(self):
        case = self.suite["cases"][4]
        request, candidate, retrieval, findings = await prepare(
            case, self.candidate, self.store, self.rules
        )
        good = fixture_output(case, candidate, retrieval, findings)
        for bad in [
            {**good, "citations": ["fake"]},
            {**good, "proposed_patch": {"viral_type": "DNA"}},
        ]:
            with self.assertRaises(ValueError):
                validate_output(
                    case, bad, request, candidate, retrieval, findings, self.store
                )

    async def test_chinese_english_taxon_filters_and_empty_unavailable_are_distinct(
        self,
    ):
        for query in ("配对", "fastp", "Kraken"):
            result = await self.store.retrieve(
                RetrievalRequest(
                    query=query,
                    corpora=("tools_databases", "artifact_glossary"),
                    include_unreviewed=True,
                )
            )
            self.assertTrue(result.passages, query)
        empty = await self.store.retrieve(
            RetrievalRequest(
                query="zzzz_unmatched_m1_query",
                corpora=("tools_databases",),
                include_unreviewed=True,
            )
        )
        self.assertEqual(empty.error_code, "KNOWLEDGE_INSUFFICIENT")
        self.store.db.execute("DROP TABLE search")
        failed = await self.store.retrieve(
            RetrievalRequest(
                query="Kraken", corpora=("tools_databases",), include_unreviewed=True
            )
        )
        self.assertEqual(failed.error_code, "RETRIEVAL_UNAVAILABLE")

    async def test_conflicting_sources_coexist_with_taxon_and_snapshot_boundaries(self):
        package = json.loads((ASSETS / "knowledge.json").read_text())
        base = copy.deepcopy(package["passages"][0])
        for index, content in enumerate(("工程冲突例：支持", "工程冲突例：反对")):
            sid, pid = f"TEST-S{index}", f"TEST-P{index}"
            package["sources"].append(
                {
                    "id": sid,
                    "origin": "synthetic",
                    "version": "test",
                    "locator": "test",
                    "license": "test",
                    "content": content,
                    "sha256": digest(content.encode()),
                }
            )
            package["passages"].append(
                {
                    **base,
                    "id": pid,
                    "source_id": sid,
                    "lines": [1, 1],
                    "terms": "工程冲突例",
                    "taxids": ["999"],
                }
            )
        path = self.root / "knowledge.json"
        path.write_text(json.dumps(package))
        store = KnowledgeStore(path, digest(path.read_bytes()))
        self.addCleanup(store.close)
        query = {
            "query": "工程冲突例",
            "corpora": (base["corpus"],),
            "include_unreviewed": True,
        }
        matched = await store.retrieve(RetrievalRequest(**query, taxid="999"))
        self.assertTrue(
            {"TEST-P0", "TEST-P1"} <= {p.citation_id for p in matched.passages}
        )
        wrong = await store.retrieve(RetrievalRequest(**query, taxid="other"))
        self.assertFalse(
            {"TEST-P0", "TEST-P1"} & {p.citation_id for p in wrong.passages}
        )
        with self.assertRaises(ValueError):
            store.verify(
                matched.passages[0].model_copy(update={"snapshot_id": "other"})
            )
        pending = await store.retrieve(
            RetrievalRequest(query="工程冲突例", corpora=(base["corpus"],), taxid="999")
        )
        self.assertFalse(pending.passages)

    def inventory(self):
        source = self.root / "workflow/config/config.yaml"
        source.parent.mkdir(parents=True)
        source.write_text("software: {}\n")
        (source.parent.parent / "Snakefile").write_text("rule all:\n    input: []\n")
        reference = self.root / "reference"
        reference.mkdir()
        (reference / "hash.k2d").write_bytes(b"not-a-real-index")
        return {
            "workflow1": {
                "config_path": str(source),
                "config_sha256": digest(source.read_bytes()),
                "dependencies": [
                    {
                        "name": "missing",
                        "kind": "software",
                        "path": str(self.root / "missing"),
                    },
                    {"name": "kraken", "kind": "reference", "path": str(reference)},
                ],
                "resources": {},
            }
        }

    def test_assets_keep_unknown_identity_and_detect_changes_without_hashing_index(
        self,
    ):
        inventory = self.inventory()
        current = observe(inventory, str(self.root / "missing"))
        observed_ref = current["workflows"]["workflow1"]["dependencies"][1]
        self.assertNotIn("sha256", observed_ref["index_entries"][0])
        bundle = {"manifest": self.manifest, "inventory": inventory}
        report = check_assets(bundle, current, ASSETS)
        self.assertEqual(report["status"], "not_release_ready")
        codes = {i["code"] for i in report["issues"]}
        self.assertTrue(
            {"DEPENDENCY_UNVERIFIED", "REFERENCE_RELEASE_UNKNOWN", "REVIEW_PENDING"}
            <= codes
        )
        Path(inventory["workflow1"]["config_path"]).write_text("changed")
        changed = check_assets(
            bundle, observe(inventory, str(self.root / "missing")), ASSETS
        )
        self.assertIn(
            "CONFIG_CHANGED_SINCE_BASELINE", {i["code"] for i in changed["issues"]}
        )
        self.assertFalse(report["production_admission_changed"])
        for capability in report["capability_baseline"]["capabilities"]:
            with self.assertRaises(ValidationError):
                Draft202012Validator(capability["parameter_schema"]).validate(
                    {"viral_type": "bad", "shell": "forbidden"}
                )

    def test_inventory_bounds_small_hashes_and_directory_entries(self):
        inventory = self.inventory()
        ref = self.root / "reference"
        for i in range(70):
            (ref / f"entry-{i}").touch()
        snakefile = self.root / "workflow/Snakefile"
        with snakefile.open("wb") as stream:
            stream.truncate(3 * 1024 * 1024)
        current = observe(inventory, str(self.root / "missing"))
        self.assertNotIn("sha256", current["workflows"]["workflow1"]["source_files"][1])
        dep = current["workflows"]["workflow1"]["dependencies"][1]
        self.assertEqual(len(dep["index_entries"]), 64)
        self.assertTrue(dep["entries_truncated"])

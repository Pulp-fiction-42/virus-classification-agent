from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from conversation_contract import RetrievalRequest
from knowledge import (
    ENGINEERING_CORPORA,
    ENGINEERING_RELEASES,
    MAX_ONLINE_PASSAGE_CHARS,
    MAX_ONLINE_PASSAGES,
    engineering_knowledge_intent,
    load_engineering_store,
    online_citation,
)


class EngineeringKnowledgeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.store = load_engineering_store()

    def tearDown(self):
        self.store.close()

    async def test_release_supports_bounded_chinese_and_exact_id_queries(self):
        chinese = await self.store.retrieve(
            RetrievalRequest(
                query="fastp 有哪些可控参数",
                corpora=ENGINEERING_CORPORA,
                limit=MAX_ONLINE_PASSAGES,
            )
        )
        exact = await self.store.retrieve(
            RetrievalRequest(
                query="workflow1.read_qc.fastp length_required",
                corpora=ENGINEERING_CORPORA,
                limit=MAX_ONLINE_PASSAGES,
            )
        )

        self.assertFalse(chinese.insufficient)
        self.assertLessEqual(len(chinese.passages), 4)
        self.assertIn("ENG-K08", {item.citation_id for item in exact.passages})
        self.assertTrue(
            all(item.review_status == "approved" for item in chinese.passages)
        )

        workflow2 = await self.store.retrieve(
            RetrievalRequest(
                query="Workflow 2 候选证据使用哪些数据库",
                corpora=ENGINEERING_CORPORA,
                limit=MAX_ONLINE_PASSAGES,
            )
        )
        self.assertIn("ENG-K11", {item.citation_id for item in workflow2.passages})
        self.assertTrue(all(item.locator.startswith("$") for item in chinese.passages))
        self.assertTrue(
            all(
                len(online_citation(item)["excerpt"]) <= MAX_ONLINE_PASSAGE_CHARS
                for item in chinese.passages
            )
        )

    async def test_domain_release_stays_out_of_online_store(self):
        self.assertNotIn("ictv", ENGINEERING_CORPORA)
        self.assertTrue(
            all(citation.corpus != "ictv" for citation in self.store.citations.values())
        )
        self.assertFalse(engineering_knowledge_intent("ICTV 病毒分类标准"))
        self.assertTrue(engineering_knowledge_intent("fastp 的输入输出是什么"))
        self.assertFalse(engineering_knowledge_intent("解释当前方案"))

    async def test_cross_snapshot_citation_is_rejected(self):
        result = await self.store.retrieve(
            RetrievalRequest(
                query="read_qc",
                corpora=ENGINEERING_CORPORA,
                limit=1,
            )
        )
        forged = result.passages[0].model_copy(update={"snapshot_id": "other@hash"})
        with self.assertRaises(ValueError):
            self.store.verify(forged)

    def test_release_hash_and_catalog_identity_fail_closed(self):
        registry = json.loads(ENGINEERING_RELEASES.read_text(encoding="utf-8"))
        active = next(
            release
            for release in registry["releases"]
            if release["release_id"] == registry["active_release"]
        )
        package = ENGINEERING_RELEASES.parent / active["file"]
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            copied = root / package.name
            copied.write_bytes(package.read_bytes() + b"\n")
            active["sha256"] = hashlib.sha256(copied.read_bytes()).hexdigest()
            active["catalog_sha256"] = "0" * 64
            target = root / "engineering-releases.json"
            target.write_text(json.dumps(registry), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "catalog identity mismatch"):
                load_engineering_store(target)

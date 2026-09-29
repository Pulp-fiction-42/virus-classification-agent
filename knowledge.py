"""Small immutable source packages with SQLite relations and offline FTS5."""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from conversation_contract import (
    Citation,
    RetrievalAdapter,
    RetrievalRequest,
    RetrievalResult,
)
from workflow_catalog import capability_catalog

ENGINEERING_ASSETS = Path(__file__).parent / "assets" / "m1"
ENGINEERING_RELEASES = ENGINEERING_ASSETS / "engineering-releases.json"
ENGINEERING_CORPORA = (
    "workflow_capabilities",
    "artifact_glossary",
    "tools_databases",
    "faq_policy",
)
MAX_ONLINE_PASSAGES = 4
MAX_ONLINE_QUERY_CHARS = 500
MAX_ONLINE_PASSAGE_CHARS = 512


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def terms(text: str) -> str:
    english = re.findall(r"[a-z0-9_]+", text.lower())
    chinese = re.findall(r"[\u4e00-\u9fff]+", text)
    tokens = english + [
        part[i : i + 2] for part in chinese for i in range(max(1, len(part) - 1))
    ]
    return " ".join(dict.fromkeys(tokens))


class KnowledgeStore(RetrievalAdapter):
    def __init__(self, package: Path, expected_sha256: str):
        raw = package.read_bytes()
        if digest(raw) != expected_sha256:
            raise ValueError("knowledge release hash mismatch")
        self.package = json.loads(raw)
        self.release_id = self.package["release_id"]
        self.release_scope = self.package.get("release_scope")
        self.snapshot_id = f"{self.package['release_id']}@{expected_sha256}"
        self.db = sqlite3.connect(":memory:")
        self.db.executescript("""
            PRAGMA foreign_keys=ON;
            CREATE TABLE source(id TEXT PRIMARY KEY, payload TEXT NOT NULL);
            CREATE TABLE entity(id TEXT PRIMARY KEY, payload TEXT NOT NULL);
            CREATE TABLE passage(id TEXT PRIMARY KEY, source_id TEXT REFERENCES source(id), payload TEXT NOT NULL);
            CREATE TABLE relation(subject TEXT REFERENCES entity(id), predicate TEXT, object TEXT REFERENCES entity(id), passage_id TEXT REFERENCES passage(id), payload TEXT);
            CREATE VIRTUAL TABLE search USING fts5(id UNINDEXED, tokens);
        """)
        self.citations: dict[str, Citation] = {}
        sources = {s["id"]: s for s in self.package["sources"]}
        if len(sources) != len(self.package["sources"]):
            raise ValueError("duplicate source ID")
        for source in sources.values():
            if digest(source["content"].encode()) != source["sha256"]:
                raise ValueError("source hash mismatch")
            self.db.execute(
                "INSERT INTO source VALUES (?, ?)", (source["id"], json.dumps(source))
            )
        for entity in self.package["entities"]:
            self.db.execute(
                "INSERT INTO entity VALUES (?, ?)", (entity["id"], json.dumps(entity))
            )
        for passage in self.package["passages"]:
            source = sources[passage["source_id"]]
            lines = source["content"].splitlines()
            start, end = passage["lines"]
            if not 1 <= start <= end <= len(lines):
                raise ValueError("invalid source locator")
            content = "\n".join(lines[start - 1 : end])
            locator = passage.get("locator")
            if locator is not None and (
                not isinstance(locator, str) or not locator.startswith("$")
            ):
                raise ValueError("invalid JSON locator")
            approved = passage["review_status"] == "approved"
            if approved and not passage.get("review_record"):
                raise ValueError("approved passage requires review record")
            citation = Citation(
                citation_id=passage["id"],
                corpus=passage["corpus"],
                source=source["origin"],
                source_version=source["version"],
                locator=locator or f"lines:{start}-{end}; origin:{source['locator']}",
                content=content,
                retrieved_at=datetime.now(timezone.utc).isoformat(),
                quality="verified" if approved else "incomplete",
                snapshot_id=self.snapshot_id,
                source_sha256=source["sha256"],
                review_status=passage["review_status"],
                review_record=passage.get("review_record"),
            )
            self.db.execute(
                "INSERT INTO passage VALUES (?, ?, ?)",
                (passage["id"], source["id"], json.dumps(passage)),
            )
            self.db.execute(
                "INSERT INTO search VALUES (?, ?)",
                (passage["id"], terms(content + " " + passage["terms"])),
            )
            self.citations[citation.citation_id] = citation
        for relation in self.package["relations"]:
            if relation.get("review_status") == "approved" and not relation.get(
                "review_record"
            ):
                raise ValueError("approved relation requires review record")
            self.db.execute(
                "INSERT INTO relation VALUES (?, ?, ?, ?, ?)",
                (
                    relation["subject"],
                    relation["predicate"],
                    relation["object"],
                    relation["passage_id"],
                    json.dumps(relation),
                ),
            )

    async def retrieve(self, request: RetrievalRequest) -> RetrievalResult:
        result = RetrievalResult(
            query=request.query,
            corpora=list(request.corpora),
            snapshot_id=self.snapshot_id,
        )
        if not 1 <= request.limit <= 50:
            raise ValueError("retrieval limit must be 1..50")
        query = " OR ".join(f'"{token}"' for token in terms(request.query).split())
        if query:
            try:
                rows = self.db.execute(
                    "SELECT search.id, bm25(search), passage.payload FROM search JOIN passage ON passage.id=search.id WHERE search MATCH ? ORDER BY bm25(search), search.id",
                    (query,),
                )
                for passage_id, score, raw in rows:
                    passage = json.loads(raw)
                    citation = self.citations[passage_id]
                    if citation.corpus not in request.corpora:
                        continue
                    if (
                        citation.review_status != "approved"
                        and not request.include_unreviewed
                    ):
                        continue
                    if passage["taxids"] and request.taxid not in passage["taxids"]:
                        continue
                    result.passages.append(
                        citation.model_copy(update={"score": -score})
                    )
                    if len(result.passages) >= request.limit:
                        break
            except sqlite3.Error:
                result.error_code = "RETRIEVAL_UNAVAILABLE"
                return result
        if not result.passages:
            result.insufficient = True
            result.error_code = "KNOWLEDGE_INSUFFICIENT"
        return result

    def verify(self, citation: Citation) -> None:
        registered = self.citations.get(citation.citation_id)
        ignored = {"score", "retrieved_at"}
        if registered is None or registered.model_dump(
            exclude=ignored
        ) != citation.model_dump(exclude=ignored):
            raise ValueError("citation does not match frozen source")

    def close(self) -> None:
        self.db.close()


def canonical_json_digest(value: Any) -> str:
    return digest(
        json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode()
    )


def engineering_catalog_digest() -> str:
    """Keep the v1 engineering corpus bound to its unchanged catalog projection."""

    catalog = capability_catalog()
    return canonical_json_digest(
        {
            "schema_version": 1,
            "deliverables": catalog["deliverables"],
            "capabilities": catalog["capabilities"],
        }
    )


def load_engineering_store(
    releases_path: Path = ENGINEERING_RELEASES,
) -> KnowledgeStore:
    registry = json.loads(releases_path.read_text(encoding="utf-8"))
    if registry.get("schema_version") != 1:
        raise ValueError("unsupported engineering knowledge release registry")
    active = registry.get("active_release")
    matches = [
        value
        for value in registry.get("releases", [])
        if value.get("release_id") == active
    ]
    if len(matches) != 1:
        raise ValueError("engineering knowledge active release is unavailable")
    release = matches[0]
    if release.get("scope") != "engineering_contract":
        raise ValueError("engineering knowledge release scope is invalid")
    relative = Path(str(release.get("file") or ""))
    if (
        relative.is_absolute()
        or ".." in relative.parts
        or relative.name != str(relative)
    ):
        raise ValueError("engineering knowledge release path is invalid")
    package_path = releases_path.parent / relative
    expected_catalog = engineering_catalog_digest()
    if release.get("catalog_sha256") != expected_catalog:
        raise ValueError("engineering knowledge catalog identity mismatch")
    store = KnowledgeStore(package_path, str(release.get("sha256") or ""))
    if (
        store.release_id != active
        or store.release_scope != "engineering_contract"
        or store.package.get("catalog_sha256") != expected_catalog
    ):
        store.close()
        raise ValueError("engineering knowledge package identity mismatch")
    return store


def engineering_knowledge_intent(query: str) -> bool:
    normalized = query.lower()
    exact_terms = {
        "fastp",
        "read_qc",
        "rrna_filter",
        "sample_merge",
        "assembly",
        "contig_filter",
        "virus_summary",
        "host_summary",
        "microbe_summary",
        "virus_rpm",
        "annotation_delivery",
        "raw_paired_reads",
        "qc_paired_reads",
        "clean_run_paired_reads",
        "clean_sample_paired_reads",
        "assembled_contigs",
        "filtered_contigs",
        "length_required",
        "qualified_quality_phred",
        "unqualified_percent_limit",
        "n_base_limit",
        "average_qual",
        "detect_adapter_for_pe",
    }
    if any(term in normalized for term in exact_terms):
        return True
    return any(
        phrase in query
        for phrase in (
            "仅序列质控",
            "去 rRNA",
            "样本合并",
            "序列组装",
            "参数白名单",
            "能力目录",
            "工作流能力",
            "输入 artifact",
            "输出 artifact",
        )
    )


def online_citation(citation) -> dict[str, Any]:
    return {
        "citation_id": citation.citation_id,
        "corpus": citation.corpus,
        "source": citation.source,
        "source_version": citation.source_version,
        "locator": citation.locator,
        "excerpt": citation.content[:MAX_ONLINE_PASSAGE_CHARS],
        "quality": citation.quality,
        "review_status": citation.review_status,
    }

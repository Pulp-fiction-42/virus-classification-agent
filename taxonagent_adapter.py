"""Typed boundary for the legacy TaxonAgent source."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from conversation_contract import Citation, RetrievalAdapter, RetrievalRequest, RetrievalResult


def taxonagent_citation(value: dict[str, Any], *, retrieved_at: str) -> Citation:
    source_version = value.get("source_version")
    locator = value.get("locator")
    quality = "verified" if source_version and locator else "incomplete"
    return Citation(
        citation_id=str(value.get("citation_id") or value.get("id") or "taxonagent-unlocated"),
        corpus="ictv",
        source=str(value.get("source") or "TaxonAgent / ICTV"),
        source_version=str(source_version) if source_version else None,
        locator=str(locator) if locator else None,
        content=str(value.get("content") or value.get("text") or ""),
        retrieved_at=retrieved_at,
        score=float(value["score"]) if isinstance(value.get("score"), (int, float)) else None,
        quality=quality,
    )


class TaxonAgentRetrievalAdapter(RetrievalAdapter):
    """Boundary around a future direct TaxonAgent client; it does not own a corpus or index."""

    def __init__(
        self,
        fetch: Callable[[str, int], Awaitable[list[dict[str, Any]]]],
        *,
        clock: Callable[[], str],
    ) -> None:
        self.fetch = fetch
        self.clock = clock

    async def retrieve(self, request: RetrievalRequest) -> RetrievalResult:
        if "ictv" not in request.corpora:
            return RetrievalResult(
                query=request.query,
                insufficient=True,
                error_code="KNOWLEDGE_INSUFFICIENT",
            )
        try:
            values = await self.fetch(request.query, request.limit)
        except Exception:
            return RetrievalResult(
                query=request.query,
                insufficient=True,
                error_code="RETRIEVAL_UNAVAILABLE",
            )
        passages = [taxonagent_citation(value, retrieved_at=self.clock()) for value in values]
        return RetrievalResult(
            query=request.query,
            passages=passages,
            insufficient=not passages,
            error_code=None if passages else "KNOWLEDGE_INSUFFICIENT",
        )

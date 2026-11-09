"""Read-only TaxonAgent MCP retrieval; never grants tools to the model."""

from __future__ import annotations

import asyncio
import hashlib
import time
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

import httpx
from conversation_contract import (
    Citation,
    RetrievalAdapter,
    RetrievalRequest,
    RetrievalResult,
)
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

TAXONAGENT_TOOL = "taxonagent_query"
MAX_TAXONAGENT_CHARS = 16000


class TaxonAgentClient:
    def __init__(self, url: str, *, timeout: float = 300):
        self.url = url.strip()
        self.timeout = timeout
        self._health: tuple[float, str] = (0, "unavailable")
        self._health_lock = asyncio.Lock()

    @asynccontextmanager
    async def _session(self):
        async with (
            httpx.AsyncClient(
                timeout=httpx.Timeout(10, read=self.timeout), trust_env=False
            ) as client,
            streamable_http_client(self.url, http_client=client) as streams,
            ClientSession(streams[0], streams[1]) as session,
        ):
            await session.initialize()
            yield session

    async def status(self) -> dict:
        if not self.url:
            return {"status": "disabled", "provider": "taxonagent"}
        async with self._health_lock:
            if time.monotonic() - self._health[0] >= 30:
                status = "unavailable"
                try:
                    async with asyncio.timeout(2), self._session() as session:
                        tools = await session.list_tools()
                        if any(tool.name == TAXONAGENT_TOOL for tool in tools.tools):
                            status = "ready"
                except Exception:  # noqa: BLE001 - health reports transport availability only
                    status = "unavailable"
                self._health = (time.monotonic(), status)
        return {"status": self._health[1], "provider": "taxonagent"}

    async def fetch(self, query: str, limit: int) -> list[dict[str, Any]]:
        if not self.url:
            raise OSError("TaxonAgent is disabled")
        try:
            async with asyncio.timeout(self.timeout), self._session() as session:
                result = await session.call_tool(TAXONAGENT_TOOL, {"question": query})
                if result.isError:
                    raise ValueError("TaxonAgent tool failed")
                content = "\n".join(
                    part.text for part in result.content if part.type == "text"
                ).strip()
                if not content or content.startswith("(no answer"):
                    raise ValueError("TaxonAgent returned no answer")
        except Exception:
            self._health = (time.monotonic(), "unavailable")
            raise
        self._health = (time.monotonic(), "ready")
        digest = hashlib.sha256((query + "\n" + content).encode()).hexdigest()
        return [
            {
                "citation_id": "taxonagent-" + digest[:24],
                "source": "TaxonAgent / ICTV",
                "content": content[:MAX_TAXONAGENT_CHARS],
            }
        ][:limit]


def taxonagent_source(citation: Citation) -> dict:
    return {
        "citation_id": citation.citation_id,
        "corpus": "ictv",
        "source": citation.source,
        "source_version": citation.source_version,
        "locator": citation.locator,
        "excerpt": citation.content[:512],
        "retrieved_at": citation.retrieved_at,
        "quality": citation.quality,
        "review_status": "pending",
    }


def taxonagent_citation(value: dict[str, Any], *, retrieved_at: str) -> Citation:
    source_version = value.get("source_version")
    locator = value.get("locator")
    quality = "verified" if source_version and locator else "incomplete"
    return Citation(
        citation_id=str(
            value.get("citation_id") or value.get("id") or "taxonagent-unlocated"
        ),
        corpus="ictv",
        source=str(value.get("source") or "TaxonAgent / ICTV"),
        source_version=str(source_version) if source_version else None,
        locator=str(locator) if locator else None,
        content=str(value.get("content") or value.get("text") or ""),
        retrieved_at=retrieved_at,
        score=float(value["score"])
        if isinstance(value.get("score"), (int, float))
        else None,
        quality=quality,
    )


class TaxonAgentRetrievalAdapter(RetrievalAdapter):
    """Typed retrieval without pretending the external answer is a reviewed corpus."""

    def __init__(
        self,
        fetch: Callable[[str, int], Awaitable[list[dict[str, Any]]]],
        *,
        clock: Callable[[], str] = lambda: datetime.now(UTC).isoformat(),
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
        except Exception:  # noqa: BLE001 - expose no transport details to the model
            return RetrievalResult(
                query=request.query,
                insufficient=True,
                error_code="RETRIEVAL_UNAVAILABLE",
            )
        passages = [
            taxonagent_citation(value, retrieved_at=self.clock()) for value in values
        ]
        return RetrievalResult(
            query=request.query,
            passages=passages,
            insufficient=not passages,
            error_code=None if passages else "KNOWLEDGE_INSUFFICIENT",
        )

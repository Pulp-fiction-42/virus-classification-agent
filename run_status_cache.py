"""Bounded HTTP waits around one shared, uncancelled status-summary RPC."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from typing import Any

from remote_client import RemoteExecutionError


class RunStatusCache:
    def __init__(
        self,
        *,
        ttl: float = 5,
        failure_ttl: float = 1,
        missing_ttl: float = 2,
        wait_timeout: float = 5,
    ) -> None:
        self.ttl = ttl
        self.failure_ttl = failure_ttl
        self.missing_ttl = missing_ttl
        self.wait_timeout = wait_timeout
        self.entries: dict[str, tuple[dict[str, Any], float]] = {}
        self.inflight: asyncio.Task[None] | None = None

    def observe(self, observation: dict[str, Any]) -> bool:
        run_id = observation.get("run_id")
        if not isinstance(run_id, str) or observation.get("found") is not True:
            return False
        if observation.get("stale") or observation.get("connection_lost"):
            return False
        if (
            type(observation.get("current_attempt")) is not int
            or type(observation.get("state_mtime_ns")) is not int
        ):
            return False
        previous = self.entries.get(run_id)
        if (
            previous
            and previous[0].get("found")
            and (
                observation.get("current_attempt", 0),
                observation.get("state_mtime_ns", 0),
                observation.get("observed_at", 0),
            )
            < (
                previous[0].get("current_attempt", 0),
                previous[0].get("state_mtime_ns", 0),
                previous[0].get("observed_at", 0),
            )
        ):
            return False
        self.entries[run_id] = (dict(observation), time.monotonic() + self.ttl)
        return True

    def _unavailable(self, run_id: str, error: str) -> dict[str, Any]:
        previous = self.entries.get(run_id)
        return {
            **(previous[0] if previous else {"run_id": run_id, "found": False}),
            "stale": True,
            "connection_lost": True,
            "error": error,
        }

    async def _refresh(
        self, run_ids: list[str], fetch: Callable[..., Awaitable[dict[str, Any]]]
    ) -> None:
        previous = {run_id: self.entries.get(run_id) for run_id in run_ids}
        try:
            response = await fetch("status_summary", run_ids=run_ids)
            if not isinstance(response.get("items"), list):
                raise TypeError("invalid status summary")
            rows = {
                row["run_id"]: row
                for row in response.get("items", [])
                if isinstance(row, dict) and isinstance(row.get("run_id"), str)
            }
        except (RemoteExecutionError, OSError, TypeError, ValueError):
            rows = {}
        now = time.monotonic()
        for run_id in run_ids:
            row = rows.get(run_id)
            if row and row.get("found") is True and self.observe(row):
                continue
            if self.entries.get(run_id) is not previous[run_id]:
                # A newer start/resume or snapshot receipt arrived during this RPC.
                continue
            elif row and row.get("error") == "RUN_NOT_FOUND":
                self.entries[run_id] = (row, now + self.missing_ttl)
            else:
                self.entries[run_id] = (
                    self._unavailable(
                        run_id,
                        str((row or {}).get("error") or "RUN_STATUS_UNAVAILABLE"),
                    ),
                    now + self.failure_ttl,
                )

    async def summary(
        self, run_ids: list[str], fetch: Callable[..., Awaitable[dict[str, Any]]]
    ) -> dict[str, Any]:
        deadline = time.monotonic() + self.wait_timeout
        while True:
            now = time.monotonic()
            needed = [
                run_id
                for run_id in run_ids
                if self.entries.get(run_id, ({}, 0))[1] <= now
            ]
            if not needed:
                break
            if self.inflight is None or self.inflight.done():
                self.inflight = asyncio.create_task(self._refresh(needed, fetch))
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                await asyncio.wait_for(asyncio.shield(self.inflight), remaining)
            except TimeoutError:
                break
        now = time.monotonic()
        return {
            "items": [
                dict(self.entries[run_id][0])
                if run_id in self.entries and self.entries[run_id][1] > now
                else self._unavailable(run_id, "RUN_STATUS_PENDING")
                for run_id in run_ids
            ]
        }

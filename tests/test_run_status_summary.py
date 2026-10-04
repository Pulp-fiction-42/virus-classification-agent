from __future__ import annotations

import asyncio
import json
import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from pydantic import ValidationError

import adapter
import remote_worker
from remote_client import RemoteConnectionError
from run_observation import read_run_observation
from run_status_cache import RunStatusCache


def observation(run_id="analysis-one", *, attempt=1, stamp=100, status="running"):
    return {
        "run_id": run_id,
        "found": True,
        "openwebui_chat_id": "chat-one",
        "plan_ref": {"id": "plan-one", "revision": 1},
        "execution": {"status": status, "ok": status == "completed"},
        "current_attempt": attempt,
        "state_mtime_ns": stamp,
        "observed_at": 123,
        "run": {"run_id": run_id},
        "stale": False,
        "connection_lost": False,
    }


class WorkerStatusSummaryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.settings = {"run_root": str(self.root)}

    def tearDown(self):
        self.temp.cleanup()

    def state(self, run_id="analysis-one", status="completed", attempt=1):
        path = self.root / run_id / ".bioagent" / "state.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    "run_id": run_id,
                    "openwebui_chat_id": "chat-one",
                    "plan_ref": {"id": "plan-one", "revision": 1},
                    "execution": {
                        "status": status,
                        "ok": status == "completed",
                        "log": str(path.parent / "attempt.log"),
                    },
                    "current_attempt": attempt,
                    "private_state": "not included in the summary",
                }
            )
        )
        return path

    def summary(self, run_ids):
        return remote_worker.handle(
            {
                "operation": "status_summary",
                "settings": self.settings,
                "worker_version": "test",
                "run_ids": run_ids,
            }
        )

    def test_batch_reads_only_selected_states_and_isolates_missing_and_corrupt(self):
        path = self.state()
        corrupt = self.state("analysis-corrupt")
        corrupt.write_text("broken JSON")
        with (
            patch("remote_worker._scheduler_session_exists") as probe,
            patch(
                "remote_worker._read_optional",
                side_effect=AssertionError("large artifact read"),
            ),
            patch.object(Path, "glob", side_effect=AssertionError("run scan")),
        ):
            result = self.summary(
                ["analysis-one", "analysis-missing", "analysis-corrupt"]
            )
        probe.assert_not_called()
        found, missing, broken = result["items"]
        self.assertEqual(found["state_mtime_ns"], path.stat().st_mtime_ns)
        self.assertEqual(found["current_attempt"], 1)
        self.assertEqual(found["openwebui_chat_id"], "chat-one")
        self.assertNotIn("private_state", found)
        self.assertEqual(missing["error"], "RUN_NOT_FOUND")
        self.assertEqual(broken["error"], "RUN_STATE_INVALID")

    def test_active_batch_checks_scheduler_once_and_ensures_at_most_once(self):
        self.state(status="running")
        self.state("analysis-two", status="waiting_resources")
        with (
            patch(
                "remote_worker._scheduler_session_exists", return_value=False
            ) as probe,
            patch("remote_worker._ensure_scheduler") as ensure,
        ):
            self.summary(["analysis-one", "analysis-two"])
        probe.assert_called_once()
        ensure.assert_called_once_with(self.settings, "test")
        with (
            patch(
                "remote_worker._scheduler_session_exists", return_value=True
            ) as probe,
            patch("remote_worker._ensure_scheduler") as ensure,
        ):
            self.summary(["analysis-one", "analysis-two"])
        probe.assert_called_once()
        ensure.assert_not_called()

    def test_bounds_ids_identity_and_symlinks_are_rejected(self):
        for run_ids in ([], ["analysis-one"] * 51, ["../escape"], [1]):
            with (
                self.subTest(run_ids=run_ids),
                self.assertRaises(remote_worker.WorkerError),
            ):
                self.summary(run_ids)
        path = self.state()
        wrong = json.loads(path.read_text())
        wrong["run_id"] = "analysis-other"
        path.write_text(json.dumps(wrong))
        self.assertEqual(
            self.summary(["analysis-one"])["items"][0]["error"],
            "RUN_STATE_IDENTITY_MISMATCH",
        )
        linked = self.root / "analysis-link"
        linked.symlink_to(path.parent.parent, target_is_directory=True)
        self.assertEqual(
            self.summary(["analysis-link"])["items"][0]["error"],
            "RUN_STATE_PATH_INVALID",
        )

    def test_atomic_replacement_preserves_stamp_of_bytes_read(self):
        path = self.state()
        old_stamp = path.stat().st_mtime_ns
        original_fstat = os.fstat
        replaced = False

        def replace_after_open(fd):
            nonlocal replaced
            value = original_fstat(fd)
            if not replaced:
                replaced = True
                replacement = path.with_suffix(".next")
                replacement.write_text(
                    json.dumps(
                        {
                            "run_id": "analysis-one",
                            "execution": {"status": "running"},
                            "current_attempt": 2,
                        }
                    )
                )
                os.utime(replacement, ns=(old_stamp + 100, old_stamp + 100))
                replacement.replace(path)
            return value

        with patch("run_observation.os.fstat", side_effect=replace_after_open):
            state, summary = read_run_observation(path, run_id="analysis-one")
        self.assertEqual(summary["state_mtime_ns"], old_stamp)
        self.assertEqual(summary["execution"], {"status": "completed", "ok": True})
        self.assertEqual(state["current_attempt"], 1)
        self.assertEqual(json.loads(path.read_text())["current_attempt"], 2)

    def test_snapshot_and_receipt_use_same_observation(self):
        path = self.state()
        with patch("remote_worker._scheduler_session_exists", return_value=False):
            snapshot = remote_worker.snapshot(
                {
                    "settings": self.settings,
                    "run_id": "analysis-one",
                    "worker_version": "test",
                }
            )
        receipt = remote_worker._run_item("analysis-one", self.settings, "queued")
        summary = self.summary(["analysis-one"])["items"][0]
        for item in (snapshot["observation"], receipt["observation"], summary):
            self.assertEqual(item["state_mtime_ns"], path.stat().st_mtime_ns)
            self.assertEqual(item["execution"]["status"], "completed")
        self.assertEqual(receipt["status"], "completed")


class RunStatusCacheTests(unittest.IsolatedAsyncioTestCase):
    async def test_equal_state_version_preserves_newer_observation_time(self):
        cache = RunStatusCache()
        cache.observe({**observation(), "observed_at": 200})
        cache.observe({**observation(), "observed_at": 100})
        fetch = AsyncMock()
        result = await cache.summary(["analysis-one"], fetch)
        self.assertEqual(result["items"][0]["observed_at"], 200)
        fetch.assert_not_awaited()

    async def test_missing_item_does_not_discard_successful_batch_members(self):
        cache = RunStatusCache()
        fetch = AsyncMock(
            return_value={
                "items": [
                    observation(),
                    {
                        "run_id": "analysis-missing",
                        "found": False,
                        "error": "RUN_NOT_FOUND",
                    },
                ]
            }
        )
        found, missing = (
            await cache.summary(["analysis-one", "analysis-missing"], fetch)
        )["items"]
        self.assertTrue(found["found"])
        self.assertFalse(found["stale"])
        self.assertEqual(found["state_mtime_ns"], 100)
        self.assertFalse(missing["found"])
        self.assertEqual(missing["error"], "RUN_NOT_FOUND")

    async def test_cached_batch_uses_one_rpc_and_same_wire_routes(self):
        cache = RunStatusCache()
        fetch = AsyncMock(return_value={"items": [observation()]})
        with (
            patch.object(adapter, "run_status_cache", cache),
            patch.object(adapter, "_remote_required") as remote,
        ):
            remote.return_value.invoke = fetch
            request = adapter.RunStatusSummaryRequest(
                run_ids=["analysis-one", "analysis-one"]
            )
            first = await adapter.remote_analysis_status_summary(request)
            second = await adapter.remote_analysis_status_summary(request)
        self.assertEqual(first, second)
        fetch.assert_awaited_once_with("status_summary", run_ids=["analysis-one"])
        paths = {route.path for route in adapter.app.routes}
        self.assertTrue(
            {"/analysis/runs/status-summary", "/v1/analysis/runs/status-summary"}
            <= paths
        )
        with self.assertRaises(ValidationError):
            adapter.RunStatusSummaryRequest(run_ids=["analysis-one"] * 51)

    async def test_timeout_and_request_cancellation_leave_single_rpc_running(self):
        cache = RunStatusCache(wait_timeout=0.02)
        release = asyncio.Event()
        calls = []

        async def fetch(operation, *, run_ids):
            calls.append((operation, run_ids))
            await release.wait()
            return {"items": [observation(run_id) for run_id in run_ids]}

        cancelled = asyncio.create_task(cache.summary(["analysis-one"], fetch))
        await asyncio.sleep(0)
        cancelled.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await cancelled
        results = await asyncio.gather(
            *(cache.summary(["analysis-one"], fetch) for _ in range(5))
        )
        self.assertEqual(len(calls), 1)
        self.assertTrue(all(value["items"][0]["stale"] for value in results))
        self.assertFalse(cache.inflight.done())
        release.set()
        await cache.inflight
        self.assertFalse(
            (await cache.summary(["analysis-one"], fetch))["items"][0]["stale"]
        )
        self.assertEqual(len(calls), 1)

    async def test_disjoint_requests_share_one_rpc_then_fetch_remaining_ids(self):
        cache = RunStatusCache()
        active = peak = 0

        async def fetch(operation, *, run_ids):
            nonlocal active, peak
            active += 1
            peak = max(active, peak)
            await asyncio.sleep(0.001)
            active -= 1
            return {"items": [observation(run_id) for run_id in run_ids]}

        first, second = await asyncio.gather(
            cache.summary(["analysis-one"], fetch),
            cache.summary(["analysis-two"], fetch),
        )
        self.assertEqual(peak, 1)
        self.assertEqual(first["items"][0]["run_id"], "analysis-one")
        self.assertEqual(second["items"][0]["run_id"], "analysis-two")

    async def test_failure_keeps_last_status_and_failure_and_missing_are_cached(self):
        cache = RunStatusCache()
        prior = observation(status="completed")
        cache.entries["analysis-one"] = (prior, 0)
        failed = AsyncMock(side_effect=RemoteConnectionError("offline"))
        first = await cache.summary(["analysis-one"], failed)
        second = await cache.summary(["analysis-one"], failed)
        self.assertEqual(first, second)
        self.assertEqual(first["items"][0]["execution"]["status"], "completed")
        self.assertTrue(first["items"][0]["stale"])
        failed.assert_awaited_once()
        missing = AsyncMock(
            return_value={
                "items": [
                    {
                        "run_id": "analysis-missing",
                        "found": False,
                        "error": "RUN_NOT_FOUND",
                    }
                ]
            }
        )
        await cache.summary(["analysis-missing"], missing)
        await cache.summary(["analysis-missing"], missing)
        missing.assert_awaited_once()

    async def test_slow_old_summary_or_missing_cannot_overwrite_resume_receipt(self):
        for row in (
            observation(stamp=100),
            {"run_id": "analysis-one", "found": False, "error": "RUN_NOT_FOUND"},
        ):
            with self.subTest(row=row):
                cache = RunStatusCache()
                started, release = asyncio.Event(), asyncio.Event()

                async def fetch(
                    operation, *, run_ids, started=started, release=release, row=row
                ):
                    started.set()
                    await release.wait()
                    return {"items": [row]}

                task = asyncio.create_task(cache.summary(["analysis-one"], fetch))
                await started.wait()
                cache.observe(observation(attempt=2, stamp=200))
                release.set()
                result = await task
                self.assertEqual(result["items"][0]["current_attempt"], 2)
                self.assertFalse(result["items"][0]["stale"])

    async def test_expired_newer_version_and_malformed_rows_do_not_spin(self):
        for row in (observation(stamp=99), {"run_id": "analysis-one", "found": True}):
            cache = RunStatusCache()
            cache.entries["analysis-one"] = (observation(), time.monotonic() - 1)
            fetch = AsyncMock(return_value={"items": [row]})
            result = await cache.summary(["analysis-one"], fetch)
            self.assertEqual(result["items"][0]["state_mtime_ns"], 100)
            self.assertTrue(result["items"][0]["stale"])
            fetch.assert_awaited_once()

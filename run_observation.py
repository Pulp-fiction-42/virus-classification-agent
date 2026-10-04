"""Small, consistently versioned observations of a persisted analysis state."""

from __future__ import annotations

import json
import os
import stat
import time
from pathlib import Path
from typing import Any


class ObservationError(ValueError):
    pass


def read_run_observation(
    state_path: Path, *, run_id: str | None = None
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Read JSON and its nanosecond version from the same open file description."""
    try:
        with os.fdopen(
            os.open(state_path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW), "rb"
        ) as stream:
            before = os.fstat(stream.fileno())
            if not stat.S_ISREG(before.st_mode):
                raise ObservationError("RUN_STATE_INVALID")
            raw = stream.read(8 * 1024 * 1024 + 1)
            after = os.fstat(stream.fileno())
    except FileNotFoundError as exc:
        raise ObservationError("RUN_NOT_FOUND") from exc
    except OSError as exc:
        raise ObservationError("RUN_STATE_UNAVAILABLE") from exc
    if (
        len(raw) > 8 * 1024 * 1024
        or before.st_mtime_ns != after.st_mtime_ns
        or before.st_size != after.st_size
    ):
        raise ObservationError("RUN_STATE_UNAVAILABLE")
    try:
        state = json.loads(raw)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ObservationError("RUN_STATE_INVALID") from exc
    if not isinstance(state, dict):
        raise ObservationError("RUN_STATE_INVALID")
    actual_run_id = state.get("run_id")
    if not isinstance(actual_run_id, str) or not actual_run_id:
        raise ObservationError("RUN_STATE_INVALID")
    if run_id is not None and actual_run_id != run_id:
        raise ObservationError("RUN_STATE_IDENTITY_MISMATCH")
    execution = state.get("execution")
    if not isinstance(execution, dict) or not isinstance(execution.get("status"), str):
        raise ObservationError("RUN_STATE_INVALID")
    run_dir = state_path.parent.parent
    attempt = state.get("current_attempt", 0)
    if type(attempt) is not int or attempt < 0:
        raise ObservationError("RUN_STATE_INVALID")
    observation = {
        "found": True,
        "run_id": actual_run_id,
        "openwebui_chat_id": state.get("openwebui_chat_id"),
        "plan_ref": state.get("plan_ref"),
        "execution": {
            key: execution.get(key)
            for key in ("status", "ok", "error")
            if key != "error" or key in execution
        },
        "current_attempt": attempt,
        "state_mtime_ns": after.st_mtime_ns,
        "observed_at": int(time.time()),
        "run": {
            "run_id": actual_run_id,
            "run_dir": str(run_dir),
            "state_path": str(state_path),
            "log_path": str(
                execution.get("log") or state_path.parent / "execution.log"
            ),
        },
        "stale": False,
        "connection_lost": False,
    }
    return state, observation

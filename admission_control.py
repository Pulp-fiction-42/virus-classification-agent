"""Read the shared, operator-controlled analysis admission state."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

MAX_STATE_BYTES = 8192
FIELDS = {
    "schema_version",
    "admission",
    "reason",
    "updated_at",
    "updated_by",
    "state_sha256",
}
SHA256 = re.compile(r"^[0-9a-f]{64}$")


class AdmissionUnavailable(RuntimeError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _text(value: Any, maximum: int, *, allow_empty: bool = False) -> bool:
    return (
        isinstance(value, str)
        and (allow_empty or bool(value.strip()))
        and len(value.strip()) <= maximum
        and not any(character in value for character in ("\0", "\n", "\r"))
    )


def _validate(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != FIELDS:
        raise ValueError("fields")
    admission = value.get("admission")
    if value.get("schema_version") != 1 or admission not in {"accepting", "draining"}:
        raise ValueError("schema")
    if not _text(value.get("reason"), 500, allow_empty=admission == "accepting"):
        raise ValueError("reason")
    if not _text(value.get("updated_by"), 100) or not _text(value.get("updated_at"), 64):
        raise ValueError("metadata")
    timestamp = datetime.fromisoformat(value["updated_at"])
    if timestamp.tzinfo is None:
        raise ValueError("timezone")
    digest = value.get("state_sha256")
    unsigned = {key: value[key] for key in FIELDS if key != "state_sha256"}
    if (
        not isinstance(digest, str)
        or not SHA256.fullmatch(digest)
        or hashlib.sha256(_canonical(unsigned)).hexdigest() != digest
    ):
        raise ValueError("digest")
    return value


def admission_status(state_file: str) -> dict[str, Any]:
    if not state_file:
        return {"status": "accepting", "reason": "", "source": "disabled"}
    path = Path(state_file)
    if not path.exists():
        return {"status": "accepting", "reason": "", "source": "default"}
    try:
        if not path.is_file() or path.is_symlink():
            raise ValueError("file")
        raw = path.read_bytes()
        if not raw or len(raw) > MAX_STATE_BYTES:
            raise ValueError("size")
        value = _validate(json.loads(raw))
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError):
        return {
            "status": "invalid",
            "reason": "准入状态文件无效",
            "source": "file",
        }
    return {
        "status": value["admission"],
        "reason": value["reason"],
        "updated_at": value["updated_at"],
        "source": "file",
    }


def require_admission(state_file: str) -> None:
    status = admission_status(state_file)
    if status["status"] == "draining":
        raise AdmissionUnavailable(
            "ADMISSION_DRAINING", "系统维护中，暂不接收新的分析任务"
        )
    if status["status"] != "accepting":
        raise AdmissionUnavailable(
            "ADMISSION_STATE_INVALID", "分析准入状态无效，请联系管理员"
        )

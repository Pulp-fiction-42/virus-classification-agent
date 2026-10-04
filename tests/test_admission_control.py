from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from admission_control import AdmissionUnavailable, admission_status, require_admission


def _state(path: Path, admission: str = "draining") -> None:
    unsigned = {
        "schema_version": 1,
        "admission": admission,
        "reason": "部署维护" if admission == "draining" else "维护完成",
        "updated_at": "2026-10-04T18:00:00+08:00",
        "updated_by": "operator",
    }
    raw = json.dumps(
        unsigned,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    path.write_text(
        json.dumps(
            {**unsigned, "state_sha256": hashlib.sha256(raw).hexdigest()},
            ensure_ascii=False,
        )
    )


def test_admission_defaults_to_accepting_and_draining_blocks(tmp_path: Path) -> None:
    path = tmp_path / "admission.json"
    assert admission_status("")["status"] == "accepting"
    assert admission_status(str(path))["status"] == "accepting"

    _state(path)
    assert admission_status(str(path))["status"] == "draining"
    with pytest.raises(AdmissionUnavailable) as raised:
        require_admission(str(path))
    assert raised.value.code == "ADMISSION_DRAINING"


def test_invalid_state_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "admission.json"
    _state(path)
    value = json.loads(path.read_text())
    value["reason"] = "tampered"
    path.write_text(json.dumps(value))

    assert admission_status(str(path))["status"] == "invalid"
    with pytest.raises(AdmissionUnavailable) as raised:
        require_admission(str(path))
    assert raised.value.code == "ADMISSION_STATE_INVALID"

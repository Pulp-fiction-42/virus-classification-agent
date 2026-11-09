from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from asset_release_binding import (
    AssetReleaseBindingError,
    load_active_release_binding,
    normalize_release_binding,
    release_binding_status,
)


def _digest(value: dict) -> str:
    return hashlib.sha256(
        json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode()
    ).hexdigest()


def _store(root: Path) -> tuple[Path, dict]:
    store = root / "store"
    package = store / "packages/knowledge/engineering/1"
    package.mkdir(parents=True)
    payload = package / "payload.json"
    payload.write_text('{"fixture":true}\n')
    manifest = {
        "schema_version": 1,
        "asset": {
            "asset_type": "knowledge",
            "asset_id": "engineering",
            "version": "1",
            "source": "test",
            "license": "test",
        },
        "requires": [],
        "files": [
            {
                "path": "payload.json",
                "size": payload.stat().st_size,
                "sha256": hashlib.sha256(payload.read_bytes()).hexdigest(),
            }
        ],
        "validation": {"status": "passed", "record": "test"},
    }
    manifest_path = package / "package-manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    packages = [
        {
            "asset_type": "knowledge",
            "asset_id": "engineering",
            "version": "1",
            "manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
        }
    ]
    release_set = {
        "schema_version": 1,
        "release_set_id": "engineering-v1",
        "created_at": "2026-10-04T20:00:00+08:00",
        "packages": packages,
        "validation": {"status": "passed", "record": "test"},
    }
    release_root = store / "release-sets"
    release_root.mkdir()
    (release_root / "engineering-v1.json").write_text(
        json.dumps(release_set, indent=2) + "\n"
    )
    body = {
        "schema_version": 2,
        "active_release_set": "engineering-v1",
        "previous_release_set": None,
        "release_set_sha256": _digest(release_set),
        "packages": packages,
        "activated_at": "2026-10-04T20:01:00+08:00",
        "activated_by": "test operator",
    }
    (store / "active-release-set.json").write_text(
        json.dumps({**body, "sha256": _digest(body)}, indent=2) + "\n"
    )
    return store, release_set


def test_active_binding_checks_small_control_files(tmp_path: Path) -> None:
    store, _ = _store(tmp_path)

    binding = load_active_release_binding(store)

    assert normalize_release_binding(binding) == binding
    assert binding["release_set_id"] == "engineering-v1"
    assert binding["packages"][0]["asset_id"] == "engineering"
    assert release_binding_status(store)["status"] == "ready"


def test_active_binding_rejects_state_release_and_manifest_drift(
    tmp_path: Path,
) -> None:
    store, release_set = _store(tmp_path / "state")
    state_path = store / "active-release-set.json"
    state = json.loads(state_path.read_text())
    state["active_release_set"] = "other"
    state_path.write_text(json.dumps(state))
    with pytest.raises(AssetReleaseBindingError, match="state identity"):
        load_active_release_binding(store)

    store, release_set = _store(tmp_path / "release")
    release_set["validation"]["record"] = "changed"
    (store / "release-sets/engineering-v1.json").write_text(json.dumps(release_set))
    with pytest.raises(AssetReleaseBindingError, match="release set identity"):
        load_active_release_binding(store)

    store, _ = _store(tmp_path / "manifest")
    (store / "packages/knowledge/engineering/1/package-manifest.json").write_text(
        "{}\n"
    )
    with pytest.raises(AssetReleaseBindingError, match="manifest identity"):
        load_active_release_binding(store)


def test_unconfigured_and_missing_store_are_explicit(tmp_path: Path) -> None:
    assert release_binding_status(None) == {"status": "not_configured"}
    status = release_binding_status(tmp_path / "missing")
    assert status["status"] == "unavailable"

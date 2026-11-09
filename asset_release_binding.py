"""Read the small active ReleaseSet identity without hashing large payloads."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1
STATE_SCHEMA_VERSION = 2
STATE_NAME = "active-release-set.json"
MAX_JSON_BYTES = 2 * 1024 * 1024
SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")
ASSET_TYPES = {"knowledge", "rules", "reference", "capability", "model"}


class AssetReleaseBindingError(ValueError):
    pass


def _json_digest(value: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode()
    ).hexdigest()


def _file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: Path) -> Any:
    if not path.is_file() or path.is_symlink():
        raise AssetReleaseBindingError(f"{path.name} must be a regular file")
    try:
        raw = path.read_bytes()
        if not raw or len(raw) > MAX_JSON_BYTES:
            raise AssetReleaseBindingError(f"{path.name} size is invalid")
        return json.loads(raw)
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise AssetReleaseBindingError(f"{path.name} is invalid JSON") from error


def _safe_child(root: Path, *parts: str) -> Path:
    current = root
    for part in parts:
        current /= part
        if current.is_symlink():
            raise AssetReleaseBindingError("asset release path contains a symlink")
    return current


def _identifier(value: Any, name: str) -> str:
    if not isinstance(value, str) or not SAFE_ID.fullmatch(value):
        raise AssetReleaseBindingError(f"{name} is invalid")
    return value


def _text(value: Any, name: str, maximum: int) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > maximum
        or any(character in value for character in ("\0", "\n", "\r"))
    ):
        raise AssetReleaseBindingError(f"{name} is invalid")
    return value


def _sha256(value: Any, name: str) -> str:
    if not isinstance(value, str) or not SHA256.fullmatch(value):
        raise AssetReleaseBindingError(f"{name} is invalid")
    return value


def _packages(value: Any) -> list[dict[str, str]]:
    if not isinstance(value, list) or not value:
        raise AssetReleaseBindingError("release packages must be a non-empty list")
    normalized = []
    keys = []
    logical = []
    for index, raw in enumerate(value):
        if not isinstance(raw, dict) or set(raw) != {
            "asset_type",
            "asset_id",
            "version",
            "manifest_sha256",
        }:
            raise AssetReleaseBindingError(f"release packages[{index}] is invalid")
        asset_type = raw["asset_type"]
        if asset_type not in ASSET_TYPES:
            raise AssetReleaseBindingError(
                f"release packages[{index}].asset_type is invalid"
            )
        item = {
            "asset_type": asset_type,
            "asset_id": _identifier(
                raw["asset_id"], f"release packages[{index}].asset_id"
            ),
            "version": _identifier(
                raw["version"], f"release packages[{index}].version"
            ),
            "manifest_sha256": _sha256(
                raw["manifest_sha256"],
                f"release packages[{index}].manifest_sha256",
            ),
        }
        normalized.append(item)
        keys.append((item["asset_type"], item["asset_id"], item["version"]))
        logical.append((item["asset_type"], item["asset_id"]))
    if (
        normalized != value
        or keys != sorted(keys)
        or len(keys) != len(set(keys))
        or len(logical) != len(set(logical))
    ):
        raise AssetReleaseBindingError(
            "release packages must be sorted and logically unique"
        )
    return normalized


def normalize_release_binding(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != {
        "schema_version",
        "release_set_id",
        "release_set_sha256",
        "packages",
        "sha256",
    }:
        raise AssetReleaseBindingError("asset release binding fields are invalid")
    if value["schema_version"] != SCHEMA_VERSION:
        raise AssetReleaseBindingError("asset release binding schema is unsupported")
    body = {
        "schema_version": SCHEMA_VERSION,
        "release_set_id": _identifier(value["release_set_id"], "release_set_id"),
        "release_set_sha256": _sha256(
            value["release_set_sha256"], "release_set_sha256"
        ),
        "packages": _packages(value["packages"]),
    }
    digest = _sha256(value["sha256"], "sha256")
    if _json_digest(body) != digest:
        raise AssetReleaseBindingError("asset release binding identity mismatch")
    return {**body, "sha256": digest}


def load_active_release_binding(store: Path) -> dict[str, Any]:
    if not store.is_absolute() or store == Path("/"):
        raise AssetReleaseBindingError("asset release store must be a non-root path")
    if not store.is_dir() or store.is_symlink():
        raise AssetReleaseBindingError("asset release store is unavailable")
    state = _read_json(store / STATE_NAME)
    if not isinstance(state, dict) or set(state) != {
        "schema_version",
        "active_release_set",
        "previous_release_set",
        "release_set_sha256",
        "packages",
        "activated_at",
        "activated_by",
        "sha256",
    }:
        raise AssetReleaseBindingError("active release state fields are invalid")
    if state["schema_version"] != STATE_SCHEMA_VERSION:
        raise AssetReleaseBindingError("active release state schema is unsupported")
    active_id = _identifier(state["active_release_set"], "active_release_set")
    previous = state["previous_release_set"]
    if previous is not None:
        _identifier(previous, "previous_release_set")
    timestamp = _text(state["activated_at"], "active release timestamp", 64)
    try:
        parsed_timestamp = datetime.fromisoformat(timestamp)
    except ValueError as error:
        raise AssetReleaseBindingError("active release timestamp is invalid") from error
    if parsed_timestamp.tzinfo is None:
        raise AssetReleaseBindingError(
            "active release timestamp must include a timezone"
        )
    _text(state["activated_by"], "activated_by", 128)
    state_digest = _sha256(state["sha256"], "active state sha256")
    if (
        _json_digest({key: value for key, value in state.items() if key != "sha256"})
        != state_digest
    ):
        raise AssetReleaseBindingError("active release state identity mismatch")

    packages = _packages(state["packages"])
    release_set_sha256 = _sha256(state["release_set_sha256"], "release_set_sha256")
    release_set = _read_json(_safe_child(store, "release-sets", f"{active_id}.json"))
    if (
        not isinstance(release_set, dict)
        or set(release_set)
        != {"schema_version", "release_set_id", "created_at", "packages", "validation"}
        or release_set.get("schema_version") != SCHEMA_VERSION
        or release_set.get("release_set_id") != active_id
        or release_set.get("packages") != packages
        or not isinstance(release_set.get("validation"), dict)
        or set(release_set["validation"]) != {"status", "record"}
        or release_set["validation"].get("status") != "passed"
        or _json_digest(release_set) != release_set_sha256
    ):
        raise AssetReleaseBindingError("active release set identity mismatch")
    for package in packages:
        manifest_path = _safe_child(
            store,
            "packages",
            package["asset_type"],
            package["asset_id"],
            package["version"],
            "package-manifest.json",
        )
        if (
            not manifest_path.is_file()
            or _file_digest(manifest_path) != package["manifest_sha256"]
        ):
            raise AssetReleaseBindingError(
                f"asset package manifest identity mismatch: {package['asset_id']}"
            )
        manifest = _read_json(manifest_path)
        asset = manifest.get("asset") if isinstance(manifest, dict) else None
        validation = manifest.get("validation") if isinstance(manifest, dict) else None
        if (
            not isinstance(asset, dict)
            or any(
                asset.get(key) != package[key]
                for key in ("asset_type", "asset_id", "version")
            )
            or not isinstance(asset.get("license"), str)
            or asset["license"].strip().casefold().startswith("unknown")
            or not isinstance(validation, dict)
            or validation.get("status") != "passed"
        ):
            raise AssetReleaseBindingError(
                f"asset package manifest is not approved: {package['asset_id']}"
            )
    body = {
        "schema_version": SCHEMA_VERSION,
        "release_set_id": active_id,
        "release_set_sha256": release_set_sha256,
        "packages": packages,
    }
    return normalize_release_binding({**body, "sha256": _json_digest(body)})


def release_binding_status(store: Path | None) -> dict[str, Any]:
    if store is None:
        return {"status": "not_configured"}
    try:
        binding = load_active_release_binding(store)
    except (AssetReleaseBindingError, OSError) as error:
        return {"status": "unavailable", "error": str(error)}
    return {
        "status": "ready",
        "release_set_id": binding["release_set_id"],
        "release_set_sha256": binding["release_set_sha256"],
        "package_count": len(binding["packages"]),
    }

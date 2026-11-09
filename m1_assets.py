"""Development inventory checks, independent of production execution admission."""

from __future__ import annotations

import hashlib
import itertools
import json
import os
from datetime import datetime, timezone
from pathlib import Path


def observe(inventory: dict, seqkit: str) -> dict:
    """Read only registered paths. No commands, recursive walks or reference hashing."""
    budget = 16 * 1024 * 1024

    def inspect(path: str, *, hash_small: bool = False) -> dict:
        nonlocal budget
        p = Path(path)
        result = {"path": path}
        try:
            before = p.stat()
            result.update(
                status="present",
                canonical_path=str(p.resolve()),
                size=before.st_size,
                mtime_ns=before.st_mtime_ns,
                device=before.st_dev,
                inode=before.st_ino,
                executable=p.is_file() and os.access(p, os.X_OK),
                kind="directory" if p.is_dir() else "file",
            )
            if hash_small and p.is_file():
                if before.st_size > min(2 * 1024 * 1024, budget):
                    result["hash_status"] = "size_budget_exceeded"
                else:
                    with p.open("rb") as stream:
                        raw = stream.read(min(2 * 1024 * 1024, budget) + 1)
                    budget -= len(raw)
                    after = p.stat()
                    if len(raw) != before.st_size or (
                        before.st_ino,
                        before.st_size,
                        before.st_mtime_ns,
                    ) != (after.st_ino, after.st_size, after.st_mtime_ns):
                        result["status"] = "changed_during_read"
                    else:
                        result["sha256"] = hashlib.sha256(raw).hexdigest()
        except FileNotFoundError:
            result["status"] = "missing"
        except OSError:
            result["status"] = "unavailable"
        return result

    workflows = {}
    for workflow_id, previous in inventory.items():
        config = Path(previous["config_path"])
        sources = [inspect(str(config), hash_small=True)]
        sources.append(
            inspect(str(config.parent.parent / "Snakefile"), hash_small=True)
        )
        dependencies = []
        for dependency in previous["dependencies"]:
            item = {"name": dependency["name"], "kind": dependency["kind"]}
            item["observation"] = inspect(
                dependency["path"], hash_small=dependency["kind"] == "software"
            )
            path = Path(dependency["path"])
            if dependency["kind"] == "reference":
                try:
                    entries = (
                        path.iterdir()
                        if path.is_dir()
                        else path.parent.glob(path.name + ".*")
                    )
                    paths = list(itertools.islice(entries, 65))
                    item["index_entries"] = [inspect(str(p)) for p in paths[:64]]
                    item["entries_truncated"] = len(paths) > 64
                    if item["observation"]["status"] == "missing" and paths:
                        item["observation"]["status"] = "prefix_present"
                except OSError:
                    item["listing_status"] = "unavailable"
            dependencies.append(item)
        workflows[workflow_id] = {"source_files": sources, "dependencies": dependencies}
    return {
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "scope": "current_observation_not_historical_identity_or_release",
        "source_coverage": "config and Snakefile only; included rules/scripts not inventoried",
        "reference_hashing": "not_performed",
        "workflows": workflows,
        "run_local_seqkit": inspect(seqkit, hash_small=True),
        "hashed_bytes": 16 * 1024 * 1024 - budget,
    }


def check_assets(bundle: dict, current: dict, assets: Path) -> dict:
    from evidence_contract import ReleaseSet
    from evidence_replay import safe_file
    from jsonschema import Draft202012Validator
    from knowledge import KnowledgeStore, digest
    from m0_baseline import capability_baseline
    from workflow_catalog import STAGE_BY_ID

    issues = []
    release = ReleaseSet.model_validate_json((assets / "releases.json").read_text())
    for item in release.releases:
        for name, expected in item.files.items():
            if digest(safe_file(assets, name).read_bytes()) != expected:
                issues.append({"asset": item.asset_id, "code": "HASH_MISMATCH"})
        if item.review_status != "approved":
            issues.append({"asset": item.asset_id, "code": "REVIEW_PENDING"})
        if not item.license or "unknown" in item.license:
            issues.append({"asset": item.asset_id, "code": "LICENSE_UNKNOWN"})
    knowledge = next(r for r in release.releases if r.asset_type == "knowledge")
    store = KnowledgeStore(assets / "knowledge.json", knowledge.files["knowledge.json"])
    store.close()
    baseline = capability_baseline(bundle["inventory"])
    for capability in baseline["capabilities"]:
        if capability["capability_id"] not in STAGE_BY_ID:
            raise ValueError("unregistered capability")
        Draft202012Validator.check_schema(capability["parameter_schema"])
        Draft202012Validator(capability["parameter_schema"]).validate(
            {"viral_type": "RNA"}
        )
    for workflow_id, previous in bundle["inventory"].items():
        observed = current["workflows"][workflow_id]
        for source in observed["source_files"]:
            if source["status"] != "present" or "sha256" not in source:
                issues.append(
                    {
                        "asset": workflow_id,
                        "code": "SOURCE_UNVERIFIED",
                        "path": source["path"],
                    }
                )
            if source["path"] == previous["config_path"] and source.get(
                "sha256"
            ) != previous.get("config_sha256"):
                issues.append(
                    {"asset": workflow_id, "code": "CONFIG_CHANGED_SINCE_BASELINE"}
                )
        for dep in observed["dependencies"]:
            if dep["observation"]["status"] not in {"present", "prefix_present"}:
                issues.append(
                    {
                        "asset": workflow_id,
                        "dependency": dep["name"],
                        "code": "DEPENDENCY_UNVERIFIED",
                    }
                )
            if dep["kind"] == "reference":
                issues.append(
                    {
                        "asset": workflow_id,
                        "dependency": dep["name"],
                        "code": "REFERENCE_RELEASE_UNKNOWN",
                    }
                )
        issues.append(
            {"asset": workflow_id, "code": "FULL_SOURCE_AND_REAL_EXECUTION_UNVERIFIED"}
        )
    if current["run_local_seqkit"]["status"] != "present" or not current[
        "run_local_seqkit"
    ].get("executable"):
        issues.append({"asset": "run-local-seqkit", "code": "DEPENDENCY_UNVERIFIED"})
    root = Path(__file__).parent
    code = {
        name: digest((root / name).read_bytes())
        for name in (
            "workflow_catalog.py",
            "workflow_adapters.py",
            "pipeline_spec.py",
            "analysis_runner.py",
            "adaptive_screening.py",
            "remote_worker.py",
            "evidence_replay.py",
            "m0_baseline.py",
        )
    }
    return {
        "schema_version": 1,
        "status": "not_release_ready",
        "message": "不具备发布条件",
        "production_admission_changed": False,
        "historical_run_identity": bundle["manifest"],
        "baseline_inventory": bundle["inventory"],
        "current_observation": current,
        "capability_baseline": baseline,
        "local_code_sha256": code,
        "identity_note": "catalog hash is not the complete plugin version; current dependencies do not prove historical versions",
        "issues": issues,
    }


if __name__ == "__main__":
    import sys

    request = json.load(sys.stdin)
    print(json.dumps(observe(request["inventory"], request["seqkit"])))

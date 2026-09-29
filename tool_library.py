"""Versioned local tool registry and structured execution results."""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
from dataclasses import dataclass
from typing import Any


class ToolLibraryError(ValueError):
    """A requested local tool cannot be resolved safely."""

    def __init__(self, message: str, *, code: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class ToolManifest:
    id: str
    name: str
    version: str
    executable: str
    version_arguments: tuple[str, ...]
    version_pattern: str
    invocation_adapter: str
    input_artifacts: tuple[str, ...]
    output_artifacts: tuple[str, ...]
    parameters: tuple[str, ...]


FASTP = ToolManifest(
    id="fastp",
    name="fastp",
    version="1.3.2",
    executable="/data/user/guojindan/softwares/xvirmin/bin/fastp",
    version_arguments=("--version",),
    version_pattern=r"\bfastp\s+(?P<version>[0-9]+(?:\.[0-9]+)+)\b",
    invocation_adapter="workflow1.read_qc.fastp",
    input_artifacts=("raw_paired_reads",),
    output_artifacts=("qc_paired_reads", "fastp_json", "fastp_html"),
    parameters=(
        "length_required",
        "qualified_quality_phred",
        "unqualified_percent_limit",
        "n_base_limit",
        "average_qual",
        "detect_adapter_for_pe",
    ),
)

TOOL_MANIFESTS = {FASTP.id: FASTP}


def _digest(value: Any) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _public_manifest(manifest: ToolManifest) -> dict[str, Any]:
    return {
        "id": manifest.id,
        "name": manifest.name,
        "version": manifest.version,
        "invocation_adapter": manifest.invocation_adapter,
        "input_artifacts": list(manifest.input_artifacts),
        "output_artifacts": list(manifest.output_artifacts),
        "parameters": list(manifest.parameters),
    }


def public_tool_catalog() -> list[dict[str, Any]]:
    """Return the path-free tool catalog exposed to clients."""

    return [_public_manifest(manifest) for manifest in TOOL_MANIFESTS.values()]


def resolve_tool(tool_id: str, version: str) -> ToolManifest:
    manifest = TOOL_MANIFESTS.get(tool_id)
    if manifest is None:
        raise ToolLibraryError(
            f"local tool is not registered: {tool_id}", code="TOOL_NOT_FOUND"
        )
    if version != manifest.version:
        raise ToolLibraryError(
            f"local tool version is unavailable: {tool_id}@{version}",
            code="TOOL_VERSION_MISMATCH",
        )
    return manifest


def build_tool_contract(*, include_fastp: bool) -> dict[str, Any]:
    entries = [_public_manifest(FASTP)] if include_fastp else []
    return {
        "schema_version": 1,
        "registry_sha256": _digest(public_tool_catalog()),
        "entries": entries,
    }


def validate_tool_contract(contract: Any, *, include_fastp: bool) -> dict[str, Any]:
    expected = build_tool_contract(include_fastp=include_fastp)
    if isinstance(contract, dict) and isinstance(contract.get("entries"), list):
        for entry in contract["entries"]:
            if not isinstance(entry, dict):
                raise ToolLibraryError(
                    "local tool contract is invalid", code="TOOL_VERSION_MISMATCH"
                )
            resolve_tool(str(entry.get("id") or ""), str(entry.get("version") or ""))
    if contract != expected:
        raise ToolLibraryError(
            "local tool contract is unavailable", code="TOOL_VERSION_MISMATCH"
        )
    for entry in expected["entries"]:
        resolve_tool(str(entry["id"]), str(entry["version"]))
    return expected


def observe_tool_versions(contract: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Verify installed versions before a remote run is created."""

    observations: dict[str, dict[str, Any]] = {}
    for entry in contract.get("entries") or []:
        manifest = resolve_tool(str(entry.get("id")), str(entry.get("version")))
        try:
            completed = subprocess.run(
                [manifest.executable, *manifest.version_arguments],
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            raise ToolLibraryError(
                f"local tool is unavailable: {manifest.id}", code="TOOL_NOT_FOUND"
            ) from exc
        output = "\n".join(
            value for value in (completed.stdout, completed.stderr) if value
        )
        match = re.search(manifest.version_pattern, output)
        observed = match.group("version") if match else None
        if completed.returncode or observed != manifest.version:
            raise ToolLibraryError(
                f"local tool version mismatch: {manifest.id} expected "
                f"{manifest.version}, observed {observed or 'unknown'}",
                code="TOOL_VERSION_MISMATCH",
            )
        observations[manifest.id] = {
            "tool_id": manifest.id,
            "requested_version": manifest.version,
            "observed_version": observed,
            "invocation_adapter": manifest.invocation_adapter,
            "status": "ready",
        }
    return observations


def build_tool_results(
    state: dict[str, Any], telemetry: dict[str, Any]
) -> list[dict[str, Any]]:
    """Build path-free fastp results from a completed W1 phase."""

    spec = state.get("execution_spec") or {}
    stages = {stage.get("id") for stage in spec.get("stages") or []}
    if "read_qc" not in stages:
        return []
    observation = (state.get("tool_observations") or {}).get("fastp") or {}
    invocation = spec.get("workflow_skill") or {}
    reports = []
    for report in telemetry.get("fastp") or []:
        if not isinstance(report, dict):
            continue
        path = str(report.get("path") or "")
        reports.append(
            {
                "run_id": path.rsplit("/", 2)[-2] if "/" in path else None,
                "summary": report.get("summary") or {},
                "filtering_result": report.get("filtering_result") or {},
                "duplication": report.get("duplication") or {},
            }
        )
    recovery = state.get("recovery_contract") or {}
    checkpoint = (recovery.get("phase_checkpoints") or {}).get("upstream") or {}
    dataset = state.get("dataset") or {}
    source_inputs = [
        item
        for item in spec.get("inputs") or []
        if item.get("type") == "raw_paired_reads"
    ]
    completed = (state.get("execution") or {}).get("ok") is True
    return [
        {
            "schema_version": 1,
            "tool_id": "fastp",
            "requested_version": observation.get("requested_version", FASTP.version),
            "observed_version": observation.get("observed_version"),
            "invocation_adapter": FASTP.invocation_adapter,
            "parameters": invocation.get("effective_parameters") or {},
            "inputs": [
                {
                    "artifact_id": (source_inputs[0].get("id") if source_inputs else None),
                    "artifact_type": "raw_paired_reads",
                    "dataset_id": dataset.get("id"),
                    "dataset_revision": dataset.get("revision"),
                    "record_ids": [
                        record.get("record_id") or record.get("run_id")
                        for record in (
                            source_inputs[0].get("records") if source_inputs else []
                        )
                    ],
                    "sha256": recovery.get("dataset_sha256"),
                }
            ],
            "outputs": [
                {
                    "artifact_id": "w1-read-qc-output",
                    "artifact_type": "qc_paired_reads",
                    "sha256": checkpoint.get("sha256"),
                    "bytes": checkpoint.get("bytes"),
                    "file_count": checkpoint.get("file_count"),
                }
            ],
            "metrics": reports,
            "status": "completed" if completed else "failed",
            "error_code": None if completed else "TOOL_EXECUTION_FAILED",
        }
    ]

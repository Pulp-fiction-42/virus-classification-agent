"""Read-only capability export from the existing catalog and observed inventory."""

from dataclasses import asdict
from pathlib import Path

from deterministic_planner import GOAL_ORDER, VIRAL_TYPES
from evidence_contract import CapabilityManifest
from knowledge import digest
from workflow_adapters import AdapterSettings
from workflow_catalog import ARTIFACTS, STAGES


def capability_baseline(inventory: dict) -> dict:
    manifests = []
    for stage in STAGES:
        workflow = inventory.get(stage.workflow_id, {})
        dependencies = workflow.get("dependencies", [])
        manifests.append(
            CapabilityManifest(
                capability_id=stage.id,
                version=digest(
                    Path(__file__).with_name("workflow_catalog.py").read_bytes()
                ),
                input_artifacts=[list(group) for group in stage.requirements],
                output_artifact=stage.produces,
                parameter_schema={
                    "type": "object",
                    "properties": {"viral_type": {"enum": sorted(VIRAL_TYPES)}},
                    "additionalProperties": False,
                },
                adapter=f"workflow_adapters:{stage.workflow_id}",
                parser="analysis_runner; candidate replay: evidence_replay.screening_candidate",
                resources={
                    "scope": "workflow_configuration",
                    "configured": workflow.get("resources", {}),
                },
                software_dependencies=[
                    item["name"] for item in dependencies if item["kind"] == "software"
                ],
                reference_dependencies=[
                    item["name"] for item in dependencies if item["kind"] == "reference"
                ],
            ).model_dump()
        )
    return {
        "schema_version": 1,
        "goals": {goal: {"target_stage": goal} for goal in GOAL_ORDER},
        "capabilities": manifests,
        "artifacts": {key: asdict(value) for key, value in ARTIFACTS.items()},
        "dependencies_scope": "workflow-level inventory; not a new execution registry",
        "inventory": inventory,
        "run_local_override": {
            "workflow1.software.seqkit": str(AdapterSettings.from_env().seqkit)
        },
        "limitations": [
            "存在与可执行性检查不代表运行时版本或完整性验证",
            "BOLD 未发现已注册依赖，未进行全盘搜索",
            "当前库快照不能补证历史运行使用的版本",
        ],
    }

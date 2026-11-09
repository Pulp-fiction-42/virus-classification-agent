"""Bounded development evaluation for controlled analysis requirement changes."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from conversation_context import build_conversation_context
from conversation_contract import ConversationAction
from provider import ProviderRequest, safe_context

CASES = Path(__file__).parent / "benchmarks" / "controlled_requirements.json"


def load_cases(path: Path = CASES) -> list[dict[str, Any]]:
    suite = json.loads(path.read_text(encoding="utf-8"))
    if suite.get("schema_version") != 1 or suite.get("split") != "development":
        raise ValueError("unsupported controlled-requirements evaluation suite")
    cases = suite.get("cases")
    if not isinstance(cases, list) or len(cases) != 5:
        raise ValueError("controlled-requirements suite must contain five cases")
    if len({case.get("id") for case in cases}) != len(cases):
        raise ValueError("duplicate controlled-requirements case")
    return cases


def request_for(case: dict[str, Any]) -> ProviderRequest:
    case_context = case.get("context") if isinstance(case.get("context"), dict) else {}
    context = build_conversation_context(
        dataset=case_context.get("dataset"),
        plan=case_context.get("plan"),
    )
    context["user_request"] = case["prompt"]
    return ProviderRequest(
        task_kind="controlled_patch",
        output_schema=ConversationAction.model_json_schema(),
        context=safe_context(context),
        instructions=(
            "使用简体中文，仅输出符合 schema 的对象。只允许修改 goals、viral_type、"
            "excluded_stages 或 capability_parameters，不得输出路径、命令或原始 CLI。"
            '只做质控使用 goals=["read_qc"]；明确的 fastp 数值使用 capability catalog。'
            "用户只说长度更长时必须 clarify 并追问具体 bp，不得猜测。"
            "raw reads 的下游目标依赖 assembly，而用户同时排除 assembly 时必须 clarify，"
            "不得生成提案。完整且可编译的修改才 propose_patch。citations 必须为空。"
        ),
        timeout_seconds=180,
        max_context_chars=24000,
        max_output_tokens=2048,
    )


def validate_case(case: dict[str, Any], raw: dict[str, Any]) -> ConversationAction:
    action = ConversationAction.model_validate(raw)
    if action.action != case["expected_action"]:
        raise ValueError("unexpected action")
    actual_patch = (
        action.proposed_patch.model_dump(exclude_none=True)
        if action.proposed_patch is not None
        else None
    )
    if actual_patch != case.get("expected_patch"):
        raise ValueError("unexpected controlled patch")
    return action


async def evaluate(
    provider=None, *, case_ids: set[str] | None = None
) -> list[dict[str, Any]]:
    records = []
    for case in load_cases():
        if case_ids is not None and case["id"] not in case_ids:
            continue
        request = request_for(case)
        if provider is None:
            raw = {
                "action": case["expected_action"],
                "message": "受控需求评测夹具。",
                "citations": [],
                **(
                    {"proposed_patch": case["expected_patch"]}
                    if case.get("expected_patch") is not None
                    else {}
                ),
            }
            provider_error = None
            latency_ms = 0
        else:
            result = await provider.generate(request)
            raw = result.output
            provider_error = result.error_code
            latency_ms = result.latency_ms
        passed = False
        if provider_error is None and isinstance(raw, dict):
            try:
                validate_case(case, raw)
                passed = True
            except ValueError:
                pass
        records.append(
            {
                "case_id": case["id"],
                "passed": passed,
                "action": raw.get("action") if isinstance(raw, dict) else None,
                "error_code": provider_error,
                "latency_ms": latency_ms,
            }
        )
    return records

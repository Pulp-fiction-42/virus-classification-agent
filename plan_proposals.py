"""Strict, atomic admission of model-authored planning proposals.

No natural-language scope or parameter parser is called here. The model owns
meaning; the backend owns valid identifiers, state, dependencies and execution.
"""
from __future__ import annotations

import copy
import json
from typing import Any

from intent_router import (
    FILES_RE, WORKFLOW_GOALS, WORKFLOW_LABELS, _json_patch, _ordered,
    extract_inputs, finalize_plan, workflow_for,
)
from parameter_rules import ParameterError, validate_patch
from workflow_catalog import ARTIFACTS, STAGE_BY_ID


TASKS = {"intent", "param_suggest", "provide_inputs", "dependency_response", "execute", "report", "explain", "conversation"}
FIELDS = {"version", "base_plan_revision", "tasks", "scope_change", "parameter_patch", "input_updates", "dependency_response", "questions", "message", "viral_type"}
SCOPE_FIELDS = {"mode", "goals", "full_workflows", "excluded_stages", "excluded_workflows", "prerequisite_groups", "ambiguous_workflows"}
STATE_FIELDS = (
    "goals", "workflows", "full_workflows", "excluded_workflows", "excluded_stages",
    "strict_stages", "prerequisite_groups", "paths", "inputs", "parameters", "viral_type",
    "authorized_stages", "confirmed_dependencies", "missing_dependencies", "ambiguous_workflows",
    "pending_scope", "steps", "stages",
)


class ProposalFormatError(ValueError):
    """A structural error may be repaired once, without changing user meaning."""


def _strings(value: Any, name: str, *, limit: int = 40) -> list[str]:
    if not isinstance(value, list) or len(value) > limit or any(not isinstance(v, str) or not v.strip() for v in value):
        raise ProposalFormatError(f"{name} must be a list of nonempty strings (max {limit})")
    return list(dict.fromkeys(value))


def check_proposal(raw: Any) -> dict:
    """Check transport shape only. Illegal parameter requests are not repaired."""
    if not isinstance(raw, dict) or set(raw) - FIELDS:
        raise ProposalFormatError("proposal must be an object with only documented fields")
    required = {"version", "base_plan_revision", "tasks", "scope_change", "parameter_patch", "questions"}
    if required - set(raw):
        raise ProposalFormatError("missing fields: " + ", ".join(sorted(required - set(raw))))
    value = copy.deepcopy(raw)
    if type(value["version"]) is not int or value["version"] != 1:
        raise ProposalFormatError("version must be 1")
    if type(value["base_plan_revision"]) is not int or value["base_plan_revision"] < 0:
        raise ProposalFormatError("base_plan_revision must be a nonnegative integer")
    tasks = _strings(value["tasks"], "tasks", limit=8)
    if not tasks or set(tasks) - TASKS:
        raise ProposalFormatError("tasks contains an unsupported task")
    value["tasks"] = tasks
    scope = value["scope_change"]
    if not isinstance(scope, dict) or set(scope) - SCOPE_FIELDS or scope.get("mode") not in {"keep", "replace", "extend", "remove"}:
        raise ProposalFormatError("scope_change has invalid fields or mode")
    for key in SCOPE_FIELDS - {"mode"}:
        scope[key] = _strings(scope.get(key, []), "scope_change." + key)
    if scope["mode"] == "keep" and any(scope[k] for k in SCOPE_FIELDS - {"mode"}):
        raise ProposalFormatError("keep must not contain scope changes")
    if scope["mode"] != "keep" and "intent" not in tasks:
        raise ProposalFormatError("scope changes require task intent")
    if not isinstance(value["parameter_patch"], dict) or len(value["parameter_patch"]) > 30:
        raise ProposalFormatError("parameter_patch must be an object")
    if value["parameter_patch"] and "param_suggest" not in tasks:
        raise ProposalFormatError("parameter changes require task param_suggest")
    value["questions"] = _strings(value["questions"], "questions", limit=12)
    updates = value.setdefault("input_updates", [])
    if not isinstance(updates, list) or len(updates) > 20 or any(not isinstance(v, dict) for v in updates):
        raise ProposalFormatError("input_updates must be a list of typed input artifacts")
    if updates and "provide_inputs" not in tasks:
        raise ProposalFormatError("input updates require task provide_inputs")
    decision = value.setdefault("dependency_response", {"allow": [], "deny": []})
    if not isinstance(decision, dict) or set(decision) - {"allow", "deny"}:
        raise ProposalFormatError("dependency_response must contain allow and deny lists")
    for key in ("allow", "deny"):
        decision[key] = _strings(decision.get(key, []), "dependency_response." + key)
    if (decision["allow"] or decision["deny"]) and "dependency_response" not in tasks:
        raise ProposalFormatError("dependency decisions require task dependency_response")
    if not isinstance(value.setdefault("message", ""), str) or len(value["message"]) > 6000:
        raise ProposalFormatError("message must be a short string")
    if "viral_type" in value and value["viral_type"] not in {"RNA", "DNA", "RNA+DNA"}:
        raise ProposalFormatError("viral_type must be RNA, DNA or RNA+DNA")
    if "viral_type" in value and "intent" not in tasks:
        raise ProposalFormatError("viral_type changes require task intent")
    readonly = {"conversation", "explain", "report"}
    if set(tasks) <= readonly and (scope["mode"] != "keep" or value["parameter_patch"] or updates or any(decision.values()) or "viral_type" in value):
        raise ProposalFormatError("read-only tasks cannot mutate a plan")
    return value


def parse_proposal(content: str) -> dict:
    if not isinstance(content, str) or len(content) > 32000:
        raise ProposalFormatError("model response is empty or too long")
    rendered = content.strip()
    if rendered.startswith("```"):
        lines = rendered.splitlines()
        if lines[0] not in {"```json", "```analysis_proposal", "```"} or lines[-1] != "```":
            raise ProposalFormatError("return one JSON object only")
        rendered = "\n".join(lines[1:-1])

    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ProposalFormatError("duplicate JSON field: " + key)
            result[key] = value
        return result

    try:
        raw = json.loads(rendered, object_pairs_hook=unique, parse_constant=lambda _: (_ for _ in ()).throw(ProposalFormatError("nonfinite number")))
    except (ValueError, TypeError) as exc:
        raise ProposalFormatError("invalid planning JSON: " + str(exc)[:180]) from exc
    return check_proposal(raw)


def blocked_plan(previous: dict, user_text: str, questions: list[str], *, code: str, proposal: dict | None = None) -> dict:
    """Preserve baseline values but revoke execution until this turn is resolved."""
    revision = previous.get("plan_revision", 0)
    result = {key: copy.deepcopy(previous[key]) for key in STATE_FIELDS if key in previous}
    result.update(
        version=3, source="qwen", action="clarify", handled=True, accepted=False,
        ready_to_execute=False, previous_unchanged=True, plan_revision=revision,
        base_plan_revision=revision, patch={}, parameter_patch={}, scope_patch={}, json_patch=[],
        questions=questions, failure_code=code,
        pending_request=previous.get("pending_request", user_text),
    )
    if previous.get("pending_proposal"):
        result["pending_proposal"] = copy.deepcopy(previous["pending_proposal"])
    followups = copy.deepcopy(previous.get("pending_followups", []))
    if previous.get("pending_request") and user_text != previous["pending_request"]:
        followups.append({"user_text": user_text, "proposal": proposal})
    if followups:
        result["pending_followups"] = followups[-6:]
    if proposal is not None:
        result.setdefault("pending_proposal", copy.deepcopy(proposal))
        result["tasks"] = proposal.get("tasks", [])
    return result


def _known(values: list[str], catalog: dict, label: str) -> None:
    unknown = set(values) - set(catalog)
    if unknown:
        raise ValueError(f"不支持的{label}：" + "、".join(sorted(unknown)))


def admit_proposal(raw: dict, previous: dict | None, user_text: str, *, check_files: bool = True) -> dict:
    p = check_proposal(raw)
    previous = copy.deepcopy(previous or {})
    revision = previous.get("plan_revision", 0)
    reject = lambda reason, code="validation_error": blocked_plan(previous, user_text, [reason], code=code, proposal=p)
    if p["base_plan_revision"] != revision:
        return reject("方案版本已变化，请基于最新方案重新提交修改。", "stale_revision")
    if p["questions"]:
        return blocked_plan(previous, user_text, p["questions"], code="needs_clarification", proposal=p)
    tasks = set(p["tasks"])
    readonly = {"conversation", "explain", "report"}
    if tasks <= readonly:
        return {"handled": False, "action": "report" if "report" in tasks else "explain", "message": p["message"], "tasks": p["tasks"], "source": "qwen"}
    if "execute" in tasks and tasks <= readonly | {"execute"}:
        return {"handled": True, "action": "execute", "tasks": p["tasks"], "message": p["message"], "ready_to_execute": previous.get("ready_to_execute") is True, "questions": previous.get("questions", [])}
    scope = p["scope_change"]
    mode = scope["mode"]
    try:
        for key in ("goals", "excluded_stages"):
            _known(scope[key], STAGE_BY_ID, "步骤")
        for key in ("full_workflows", "excluded_workflows", "prerequisite_groups", "ambiguous_workflows"):
            _known(scope[key], WORKFLOW_GOALS, "流程分组")
        patch = validate_patch(p["parameter_patch"])
        if "param_suggest" in tasks and not patch:
            return reject("尚未确定要修改的参数和值，请明确后再提交。", "needs_clarification")
        if mode == "replace" and not (scope["goals"] or scope["full_workflows"] or scope["ambiguous_workflows"]):
            return reject("请指定新的分析步骤或终点，不能用空范围替换当前方案。")
        if mode == "remove" and (scope["full_workflows"] or scope["prerequisite_groups"] or scope["ambiguous_workflows"]):
            return reject("取消步骤的请求不能同时授权新的流程。")
        goal_groups = {workflow_for(sid) for sid in scope["goals"]} | set(scope["full_workflows"])
        if set(scope["prerequisite_groups"]) - goal_groups:
            return reject("前置步骤授权不能超出本次明确选择目标的流程分组。")
        if (set(scope["goals"]) & set(scope["excluded_stages"])) and mode != "remove":
            return reject("同一步骤同时要求执行和排除，请明确要保留哪项要求。", "conflicting_intents")
        if goal_groups & set(scope["excluded_workflows"]) and mode != "remove":
            return reject("同一流程同时要求执行和取消，请明确分析范围。", "conflicting_intents")
        allow, deny = set(p["dependency_response"]["allow"]), set(p["dependency_response"]["deny"])
        pending = set(previous.get("missing_dependencies", []))
        if allow & deny or (allow | deny) - pending:
            return reject("依赖确认必须对应上一轮已列出的缺失步骤，且不能同时同意和拒绝。", "invalid_dependency_consent")
        if "dependency_response" in tasks and not (allow or deny):
            return reject("请明确同意或拒绝哪些已列出的前置步骤。", "needs_clarification")
    except (ValueError, ParameterError) as exc:
        return reject(str(exc))

    result = {key: copy.deepcopy(previous[key]) for key in STATE_FIELDS if key in previous}
    goals = set(previous.get("goals", []))
    excluded = set(previous.get("excluded_stages", []))
    excluded_wf = set(previous.get("excluded_workflows", []))
    full = set(previous.get("full_workflows", []))
    groups = set(previous.get("prerequisite_groups", []))
    if "prerequisite_groups" not in previous and not previous.get("strict_stages", True):
        groups.update(workflow_for(sid) for sid in goals)
    authorized = set(previous.get("authorized_stages", []))
    confirmed = set(previous.get("confirmed_dependencies", []))
    if mode == "replace":
        goals, excluded, excluded_wf, full, groups, authorized, confirmed = set(), set(), set(), set(), set(), set(), set()
    if mode in {"replace", "extend"}:
        goals.update(scope["goals"])
        goals.update(WORKFLOW_GOALS[wf] for wf in scope["full_workflows"])
        excluded.difference_update(scope["goals"])
        excluded_wf.difference_update(goal_groups)
        full.update(scope["full_workflows"])
        groups.update(scope["prerequisite_groups"])
    if mode == "remove":
        goals.difference_update(scope["goals"])
        excluded.update(scope["goals"])
    if mode != "keep":
        excluded.update(scope["excluded_stages"])
        excluded_wf.update(scope["excluded_workflows"])
        excluded.update(sid for sid in STAGE_BY_ID if workflow_for(sid) in excluded_wf)
        goals.difference_update(excluded)
        groups.difference_update(excluded_wf)
        full.difference_update(excluded_wf)
        authorized.difference_update(excluded)
        confirmed.difference_update(excluded)
    confirmed = (confirmed | allow) - deny
    authorized.difference_update(deny)

    inputs = copy.deepcopy(previous.get("inputs", []))
    if not inputs and previous.get("paths"):
        inputs, problems = extract_inputs("", previous["paths"])
        if problems:
            return reject("；".join(problems))
    grounded = set(FILES_RE.findall(user_text)) | set(previous.get("paths", []))
    grounded.update(path for item in inputs for rec in item.get("records", []) for path in rec.get("files", {}).values())
    for item in p["input_updates"]:
        if set(item) != {"id", "type", "records"} or item.get("type") not in ARTIFACTS or not isinstance(item.get("id"), str):
            return reject("输入必须是目录中支持的、具有明确类型和样本记录的产物。")
        records = item.get("records")
        if not isinstance(records, list) or not records or any(not isinstance(rec, dict) or not isinstance(rec.get("files"), dict) for rec in records):
            return reject("输入文件的样本与双端对应关系不完整，请补充。")
        for rec in records:
            for path in rec["files"].values():
                if not isinstance(path, str) or path not in grounded:
                    return reject("模型提出了本轮需求或当前文件中不存在的输入路径，请重新选择文件。", "ungrounded_input")
            # Filename-derived sample/run IDs are mechanical metadata, not
            # semantic interpretation of processing status or scope.
            identities = ARTIFACTS[item["type"]].identity_fields
            if any(key not in rec for key in identities):
                derived, _problems = extract_inputs("", list(rec["files"].values()))
                matching = [r for bundle in derived for r in bundle["records"] if r["files"] == rec["files"]]
                if len(matching) == 1:
                    for key in identities:
                        if key in matching[0]:
                            rec.setdefault(key, matching[0][key])
        # Additional artifact types augment availability; replacing the same ID
        # explicitly updates that artifact without dropping unrelated inputs.
        old = next((i for i in inputs if i["type"] == item["type"]), None)
        if old is not None:
            item["id"] = old["id"]
            inputs[inputs.index(old)] = copy.deepcopy(item)
        else:
            if any(i["id"] == item["id"] for i in inputs):
                return reject("输入标识已被另一种产物占用，请明确输入文件类型。")
            inputs.append(copy.deepcopy(item))
    if mode != "keep" and scope["ambiguous_workflows"]:
        questions = [WORKFLOW_LABELS[wf] + "包含多个步骤，请说明具体步骤、终点或是否完整执行。" for wf in scope["ambiguous_workflows"]]
        blocked = blocked_plan(previous, user_text, questions, code="needs_clarification", proposal=p)
        blocked.update(inputs=inputs, paths=list(dict.fromkeys(path for item in inputs for rec in item["records"] for path in rec["files"].values())))
        return blocked

    parameters = copy.deepcopy(previous.get("parameters", {}))
    parameters.update(patch)
    paths = list(dict.fromkeys(path for item in inputs for rec in item["records"] for path in rec["files"].values()))
    result.update(
        version=3, source="qwen", tasks=p["tasks"], action="param_suggest" if patch else "plan",
        handled=True, accepted=True, ready_to_execute=False, questions=[], plan_revision=revision + 1,
        base_plan_revision=revision, goals=_ordered(goals), excluded_stages=_ordered(excluded),
        excluded_workflows=sorted(excluded_wf), full_workflows=sorted(full),
        workflows=sorted({workflow_for(sid) for sid in goals}), strict_stages=not bool(groups),
        prerequisite_groups=sorted(groups), authorized_stages=_ordered(authorized),
        confirmed_dependencies=_ordered(confirmed), inputs=inputs, paths=paths,
        parameters=parameters, viral_type=p.get("viral_type", previous.get("viral_type", "RNA+DNA")),
        patch=patch, parameter_patch=patch, json_patch=_json_patch(previous.get("parameters", {}), patch, revision),
        scope_patch={} if mode == "keep" else {"goals":_ordered(goals), "excluded_stages":_ordered(excluded), "excluded_workflows":sorted(excluded_wf)},
        ambiguous_workflows=[], model_proposal=p,
    )
    # Output from an earlier compilation must never survive a new request.
    for key in ("stages", "steps", "pending_scope"):
        result.pop(key, None)
    result = finalize_plan(result, previous, check_files=check_files, dependency_no=bool(deny))
    if result.get("accepted") is False:
        return blocked_plan(previous, user_text, result["questions"], code="validation_error", proposal=p)
    if not result.get("ready_to_execute"):
        result.update(pending_request=user_text, pending_proposal=p)
    if "execute" in tasks:
        result["execution_deferred"] = True
    if "explain" in tasks and p["message"]:
        result["model_message"] = p["message"]
    return result

"""Conservative conversational intent resolution and dependency admission.

The shared XvirMind catalog describes biology; this module decides what the
user actually requested. Compilation never grants permission to add scope.
"""

from __future__ import annotations

import copy
import re
import unicodedata
from pathlib import Path, PurePosixPath, PureWindowsPath

from parameter_rules import ParameterError, RULES, apply_patch, parse_parameter_patch
from pipeline_spec import ExecutionSpecError, canonical_steps, compile_execution_spec
from workflow_catalog import STAGES, STAGE_BY_ID, STAGE_ORDER


WORKFLOW_GOALS = {
    "workflow1": "contig_filter", "workflow2": "virus_summary",
    "workflow3": "host_summary", "workflow4": "microbe_summary",
    "workflow5": "virus_rpm", "workflow6": "annotation_delivery",
}
WORKFLOW_LABELS = {
    "workflow1": "W1 上游预处理",
    "workflow2": "W2 病毒候选发现与证据分析",
    "workflow3": "W3 宿主线索分析",
    "workflow4": "W4 微生物分类与丰度",
    "workflow5": "W5 病毒定量",
    "workflow6": "W6 病毒注释",
}
ALIASES = {
    "read_qc": r"fastp|质控|质量控制|质量检查|清洗reads|数据质量",
    "rrna_filter": r"去(?:除)?\s*rRNA|rRNA\s*(?:去除|过滤)",
    "sample_merge": r"样本合并|合并样本|合并\s*runs?",
    "assembly": r"megahit|组装|拼接",
    "contig_filter": r"contigs?\s*(?:过滤|重命名)|过滤\s*contigs?",
    "virus_candidate_extract": r"病毒候选提取|提取病毒候选|病毒候选发现|寻找病毒候选|候选病毒|病毒候选(?!搜索)",
    "virus_sequence_prep": r"病毒识别序列准备|病毒序列准备|ORF预测|预测ORF",
    "virus_candidate_search": r"病毒候选搜索|搜索病毒标记|标记蛋白搜索",
    "virus_evidence": r"病毒(?:分类)?证据(?:分析|判断)?|证据分析|分类证据",
    "virus_summary": r"病毒鉴定|病毒识别|病毒检测",
    "host_summary": r"宿主(?:识别|判断|分析|线索|来源)|宿主线索来源判断",
    "microbe_summary": r"微生物谱汇总|微生物谱|菌群分析|物种组成",
    "host_mapping": r"宿主标记(?:序列)?比对",
    "host_coverage": r"宿主标记覆盖度",
    "host_blast": r"宿主标记\s*BLAST",
    "host_integrate": r"宿主证据整合",
    "microbe_classify": r"kraken(?:2)?|微生物快速分类",
    "microbe_abundance": r"bracken|微生物丰度估计",
    "virus_read_mapping": r"病毒序列比对|reads?\s*比对回\s*contigs?",
    "virus_coverage": r"病毒覆盖度筛选",
    "virus_rpm": r"病毒(?:丰度)?定量|病毒丰度|RPM计算",
    "virus_quality": r"checkv|完整度(?:检查|评估)|污染度(?:检查|评估)",
    "annotation_strategy": r"注释策略分流",
    "structure_annotation": r"(?:病毒)?结构注释",
    "functional_annotation": r"(?:病毒)?功能注释",
    "annotation_delivery": r"病毒注释结果汇总|注释结果汇总",
}
WORKFLOW_ALIASES = {
    "workflow1": r"上游预处理|上游处理",
    "workflow2": r"W2\s*病毒候选发现",
    "workflow6": r"病毒注释(?!结果|策略)",
}
NEGATIVE = r"不要|不做|不跑|不执行|不进行|别做|暂不|跳过|排除|取消"
FILES_RE = re.compile(
    r"((?:(?<![A-Za-z0-9_])[A-Za-z]:[\\/]|\\\\|/)[^\s`\"'<>，。；、]+?\.(?:fastq|fq|fasta|fna|fa|tsv)(?:\.gz)?)(?=$|[\s`\"'<>，。；、)）])",
    re.I,
)
CONFIRM_RE = re.compile(r"^(?:确认(?:启动|执行|开始)?(?:分析)?|开始(?:执行|分析|运行)?|执行(?:当前|这个|该)?方案|按(?:当前|这个|该)方案执行|开始吧|运行吧)[，,。!！\s]*(?:请按当前方案执行[。.]?)?$", re.I)
FULL_WORKFLOW_RE = re.compile(r"完整|全部|全量|全流程|所有步骤|从头到尾|默认流程", re.I)


def workflow_for(stage: str) -> str:
    return STAGE_BY_ID[stage].workflow_id


def latest_text(messages: list[dict]) -> str:
    for message in reversed(messages):
        if message.get("role") != "user":
            continue
        value = message.get("content", "")
        if isinstance(value, str):
            return value.strip()
        if isinstance(value, list):
            return "\n".join(x.get("text", "") for x in value if isinstance(x, dict) and x.get("type") == "text").strip()
    return ""


def _matches(clause: str) -> tuple[set[str], set[str]]:
    workflows, stages = set(), set()
    for match in re.finditer(r"\bW\s*([1-6])\s*(?:-|~|—|至|到)\s*(?:W\s*)?([1-6])", clause, re.I):
        a, b = map(int, match.groups())
        workflows.update("workflow" + str(i) for i in range(min(a, b), max(a, b) + 1))
    # Chinese text often has no word boundary after the digit: ``W2病毒``.
    workflows.update(
        "workflow" + x
        for x in re.findall(
            r"(?:W|workflow)\s*([1-6])(?=$|[^0-9A-Za-z_])", clause, re.I
        )
    )
    for wf, pattern in WORKFLOW_ALIASES.items():
        if re.search(pattern, clause, re.I):
            workflows.add(wf)
    for stage, pattern in ALIASES.items():
        if re.search(pattern, clause, re.I):
            stages.add(stage)
    for stage in STAGES:
        if stage.id in clause or stage.label in clause:
            stages.add(stage.id)
    if "virus_sequence_prep" in stages:
        stages.discard("virus_summary")
    # A workflow name may be followed by a more precise stop point.
    if "workflow2" in workflows and "virus_candidate_extract" in stages:
        stages.discard("virus_summary")
    return workflows, stages


def extract_scope(text: str) -> dict:
    text = unicodedata.normalize("NFKC", text)
    if re.search(r"(?:W|workflow)\s*(?:0|[7-9]|\d{2,})(?=$|[^0-9A-Za-z_])", text, re.I):
        raise ValueError("当前只支持 W1–W6，请指定其中的流程。")
    explicit_negative_stages: set[str] = set()
    # Capture suffix negations before splitting prefix negations, e.g.
    # ``证据分析先不做`` and ``组装暂时不做``.
    for stage, alias in ALIASES.items():
        pattern = rf"(?:{alias})\s*(?:先|暂时)?\s*(?:{NEGATIVE})"
        if re.search(pattern, text, re.I):
            explicit_negative_stages.add(stage)
            text = re.sub(pattern, " ", text, flags=re.I)
    positive_wf, negative_wf, positive_stages = set(), set(), set()
    negative_stages = set(explicit_negative_stages)
    full_workflows: set[str] = set()
    clauses = re.split(r"[，,。;；\n]|但是|但|然后|(?=" + NEGATIVE + r")", text)
    for clause in clauses:
        workflows, stages = _matches(clause)
        negative = bool(re.search(NEGATIVE, clause))
        if negative:
            negative_wf.update(workflows)
            negative_stages.update(stages)
        else:
            positive_wf.update(workflows)
            positive_stages.update(stages)
            if FULL_WORKFLOW_RE.search(clause):
                full_workflows.update(workflows)
    full = bool(FULL_WORKFLOW_RE.search(text) or re.search(r"从\s*W1\s*到\s*W6", text, re.I))
    if full and not positive_wf:
        full_workflows.update(WORKFLOW_GOALS)
    if re.search(r"从\s*W1\s*到\s*W6", text, re.I):
        full_workflows.update(WORKFLOW_GOALS)
    if re.search(r"只(?:做|提取)|仅(?:做|提取)|做到|到.+为止", text):
        # ``完整W1和W2只提取病毒候选`` contains a local stop point for
        # W2. A nearby word "完整" must not widen that partial group.
        full_workflows.difference_update(workflow_for(stage) for stage in positive_stages)
    ambiguous_workflows: set[str] = set()
    if full and not positive_wf:
        positive_wf.update(WORKFLOW_GOALS)
    goals = set(positive_stages)
    for workflow in positive_wf:
        specific = [s for s in positive_stages if workflow_for(s) == workflow]
        # W1–W6 are groups.  A bare group name is ambiguous unless the user
        # explicitly asks for its complete default flow.
        if workflow not in full_workflows and not specific:
            ambiguous_workflows.add(workflow)
        elif workflow in full_workflows:
            goals.difference_update(specific)
            goals.add(WORKFLOW_GOALS[workflow])
        elif specific:
            # A named step wins over the containing workflow label.  The
            # compiler will add only technical prerequisites that are
            # explicitly permitted by the user or already supplied as files.
            goals.update(specific)
        else:
            goals.difference_update(specific)
            goals.add(WORKFLOW_GOALS[workflow])
    negative_stages.update(s.id for s in STAGES if s.workflow_id in negative_wf)
    goals.difference_update(negative_stages)
    return {
        "goals": sorted(goals), "workflows": sorted(positive_wf),
        "excluded_workflows": sorted(negative_wf), "excluded_stages": sorted(negative_stages),
        "strict_stages": bool(re.search(r"只(?:做|跑|执行|进行|要)|仅(?:做|跑|执行|进行)|only", text, re.I))
        and not bool(re.search(r"做到|到.+为止|病毒候选(?:发现|提取)|提取病毒候选", text)),
        "ambiguous_workflows": sorted(ambiguous_workflows),
        "full_workflows": sorted(full_workflows),
        "scope_mentioned": bool(positive_wf or positive_stages or negative_wf or negative_stages),
    }


def extract_inputs(text: str, paths: list[str]) -> tuple[list[dict], list[str]]:
    """Pair by read basename, never by incidental list position."""
    groups: dict[str, dict] = {}
    fasta = []
    problems = []
    for raw in dict.fromkeys(paths):
        path = PureWindowsPath(raw) if re.match(r"^[A-Za-z]:[\\/]", raw) else PurePosixPath(raw)
        if any(ch.isspace() for ch in raw):
            problems.append("当前样本表不支持带空格的输入路径，请先提供不带空格的路径。")
            continue
        if re.search(r"\.(?:fastq|fq)(?:\.gz)?$", raw, re.I):
            stem = re.sub(r"\.(?:fastq|fq)(?:\.gz)?$", "", path.name, flags=re.I)
            match = re.match(r"^(.*?)(?:[_.-]R?)([12])(?:[_.-]001)?$", stem, re.I)
            if not match:
                problems.append(f"无法判断 {path.name} 是 R1 还是 R2，请使用明确的双端文件名。")
                continue
            sample = match.group(1)
            parent_parts = list(path.parent.parts)
            if '.conversation-uploads' in parent_parts:
                marker = parent_parts.index('.conversation-uploads')
                # Upload IDs isolate same-named files on disk. Pair within the
                # original folder and owning conversation, ignoring that one ID.
                if len(parent_parts) >= marker + 4 and re.fullmatch(r'[0-9a-f-]{36}', parent_parts[marker + 3]):
                    del parent_parts[marker + 3]
            key = str(type(path)(*parent_parts) / sample)
            record = groups.setdefault(key, {"run_id": sample, "sample_id": sample, "files": {}})
            role = "read" + match.group(2)
            if role in record["files"] and record["files"][role] != raw:
                problems.append(f"样本 {sample} 的 {role} 有多个文件，请明确 run 对应关系。")
            record["files"][role] = raw
        elif re.search(r"\.(?:fasta|fna|fa)(?:\.gz)?$", raw, re.I):
            sample = re.sub(r"\.(?:fasta|fna|fa)(?:\.gz)?$", "", path.name, flags=re.I)
            sample = re.sub(r"[^A-Za-z0-9_.-]", "_", sample)
            fasta.append({"sample_id": sample, "files": {"fasta": raw}})
    inputs = []
    if groups:
        records = list(groups.values())
        for record in records:
            if set(record["files"]) != {"read1", "read2"}:
                problems.append(f"样本 {record['sample_id']} 缺少配对的 R1 或 R2 文件。")
        kind = "raw_paired_reads"
        if re.search(r"质控后|已(?:完成|做过?)\s*(?:fastp|质控)|qc_paired_reads", text, re.I):
            kind = "qc_paired_reads"
        if re.search(r"去(?:除)?\s*rRNA\s*后|clean_run_paired_reads", text, re.I):
            kind = "clean_run_paired_reads"
        if re.search(r"合并后|clean_sample_paired_reads|已合并的清洁", text, re.I):
            kind = "clean_sample_paired_reads"
            for record in records:
                record.pop("run_id", None)
        inputs.append({"id": "reads", "type": kind, "records": records})
    if fasta:
        if re.search(r"病毒参考|病毒序列|virus_reference_fasta", text, re.I) and re.search(r"W[56]|注释|定量", text, re.I):
            if len(fasta) != 1:
                problems.append("病毒参考序列需要一个明确的 FASTA 文件。")
            inputs.append({"id": "viral_reference", "type": "virus_reference_fasta", "records": [{"files": fasta[0]["files"]}]})
        else:
            kind = "filtered_contigs" if re.search(r"已过滤.{0,8}contig|filtered_contigs", text, re.I) else "contigs_fasta"
            inputs.append({"id": "contigs", "type": kind, "records": fasta})
    taxonomy = [p for p in paths if p.lower().endswith('.tsv')]
    if taxonomy and re.search(r"taxonomy|分类表|分类结果", text, re.I):
        if len(taxonomy) != 1:
            problems.append("请指定唯一的病毒 taxonomy 分类表。")
        inputs.append({"id": "taxonomy", "type": "virus_taxonomy", "records": [{"files": {"taxonomy": taxonomy[0]}}]})
    return inputs, problems


PARAMETER_REQUEST_RE = re.compile(
    r"改成|改为|设为|设置为|修改|调整|(?:关闭|开启|启用|禁用).{0,8}去重|"
    r"(?:fastp|megahit|contig_filter)\.[a-z_]+\s*=", re.I
)


def _scope_text(text: str) -> str:
    """A parameter mention is not a request to replace analysis scope."""
    clauses = re.split(r"([，,。;；\n])", FILES_RE.sub(" ", text))
    kept = []
    for clause in clauses:
        if PARAMETER_REQUEST_RE.search(clause) and not re.search(
            r"(?:只|仅)(?:做|跑|执行|提取)|做到|执行|运行|增加|加上|取消\s*W|不做\s*W", clause, re.I
        ):
            continue
        kept.append(clause)
    return "".join(kept)


def _ordered(stage_ids: set[str] | list[str]) -> list[str]:
    return [stage.id for stage in STAGES if stage.id in stage_ids]


def _json_patch(current: dict, patch: dict, revision: int) -> list[dict]:
    if not patch:
        return []
    operations = [{"op": "test", "path": "/plan_revision", "value": revision}]
    for key, value in patch.items():
        path = "/parameters/" + key.replace("~", "~0").replace("/", "~1")
        if key in current:
            operations.append({"op": "test", "path": path, "value": current[key]})
        operations.append({"op": "replace" if key in current else "add", "path": path, "value": value})
    return operations


def resolve_intent(text: str, previous: dict | None = None, *, check_files: bool = True) -> dict:
    """Propose a versioned draft; missing dependencies never authorize themselves.

    ``previous`` is the backend's last planning state, including pending questions.
    Only a ready result contains an execution_spec.  Refusing dependencies retains
    the requested goal so that later supplied intermediate files can satisfy it.
    """
    previous = copy.deepcopy(previous or {})
    text = unicodedata.normalize("NFKC", text).strip()
    revision = int(previous.get("plan_revision") or 0)
    baseline = {
        key: copy.deepcopy(previous[key]) for key in (
            "goals", "workflows", "full_workflows", "excluded_workflows", "excluded_stages",
            "strict_stages", "paths", "inputs", "parameters", "viral_type", "authorized_stages",
            "confirmed_dependencies", "ambiguous_workflows", "missing_dependencies", "pending_scope"
        ) if key in previous
    }
    result = {
        **baseline, "version": 2, "action": "plan", "handled": True,
        "accepted": True, "ready_to_execute": False, "questions": [],
        "base_plan_revision": revision, "plan_revision": revision + 1,
        "patch": {}, "parameter_patch": {}, "json_patch": [], "scope_patch": {},
    }
    if CONFIRM_RE.fullmatch(text):
        return {
            "action": "execute", "handled": True,
            "ready_to_execute": previous.get("ready_to_execute") is True,
            "base_plan_revision": revision,
            "questions": [] if previous.get("ready_to_execute") else [
                "当前方案还有未解决的问题，暂时不能执行。请先确认所需步骤或提供缺少的中间产物。"
            ],
        }
    if re.search(r"(?:重新生成|生成|下载|查看|修复).{0,8}(?:报告|结果总结)|报告(?:呢|在哪里)", text):
        return {"action": "report", "handled": False}
    if re.search(r"是什么|什么意思|怎么(?:做|实现|处理|检查)|如何|为什么|原理|介绍|讲解|支持.{0,8}吗", text) and not re.search(
        r"帮我|请.{0,3}(?:执行|运行)|只做|只跑|仅做|我要(?:运行|做)|改成|改为|设为|修改|调整", text
    ):
        return {"action": "explain", "handled": False}
    parameter_request = bool(PARAMETER_REQUEST_RE.search(text))
    try:
        scope = extract_scope(_scope_text(text))
    except ValueError as exc:
        result.update(action="clarify", accepted=False, previous_unchanged=True, questions=[str(exc)])
        return result
    new_paths = list(dict.fromkeys(FILES_RE.findall(text)))
    pending_dependencies = set(previous.get("missing_dependencies", []))
    dependency_yes = bool(pending_dependencies) and bool(re.fullmatch(
        r"(?:好的?|可以|同意|允许|确认)(?:先)?(?:补做|补跑|增加|添加|运行|执行)?"
        r"(?:这些|全部|所需|必要的?)?(?:前置|依赖)?(?:步骤|流程)?[。.!！\s]*", text
    ) or re.search(r"(?:同意|允许|可以|确认).{0,8}(?:补做|补跑|增加|添加|先跑|先做|前置|依赖)", text))
    dependency_no = bool(pending_dependencies) and bool(re.search(
        r"不(?:同意|允许|需要|要|补做|补跑)|拒绝|先不补", text
    ))
    # A bare group in a dependency confirmation approves only the displayed
    # missing steps of that group; it does not replace the user's W2 target.
    if dependency_yes or dependency_no:
        consent_workflows, consent_stages = _matches(text)
        consent = pending_dependencies
        if consent_workflows:
            consent = {sid for sid in consent if workflow_for(sid) in consent_workflows}
        elif consent_stages:
            consent &= consent_stages
        result["confirmed_dependencies"] = _ordered(
            set(previous.get("confirmed_dependencies", [])) | (consent if dependency_yes else set())
        )
        scope = extract_scope("")
    else:
        consent = set()
    if previous.get("ambiguous_workflows") and FULL_WORKFLOW_RE.search(text) and not _matches(text)[0]:
        groups = previous["ambiguous_workflows"]
        scope = extract_scope("执行完整 " + "、".join(item.replace("workflow", "W") for item in groups))
    if previous.get("ambiguous_workflows") and not re.search(r"改成|改为|重新选择|只做|仅做", text):
        resolved_groups = {workflow_for(goal) for goal in scope["goals"]}
        still_ambiguous = set(previous["ambiguous_workflows"]) - resolved_groups - set(scope["excluded_workflows"])
        scope["ambiguous_workflows"] = sorted(still_ambiguous | set(scope["ambiguous_workflows"]))
        pending = previous.get("pending_scope", {})
        scope["goals"] = _ordered(set(scope["goals"]) | set(pending.get("goals", [])))
        scope["workflows"] = sorted(set(scope["workflows"]) | set(pending.get("workflows", [])))
        scope["full_workflows"] = sorted(set(scope["full_workflows"]) | set(pending.get("full_workflows", [])))
    handled = bool(scope["scope_mentioned"] or new_paths or parameter_request or dependency_yes or dependency_no
                   or re.search(r"分析一下|处理一下|做个分析|跑一下", text))
    if not handled:
        return {"action": "conversation", "handled": False}

    # Parse new paths even if the scope still needs clarification, so the next
    # user reply can resolve it without repeating file selections.
    inputs = copy.deepcopy(previous.get("inputs", []))
    problems = []
    if new_paths:
        parsed, problems = extract_inputs(text, new_paths)
        # Providing contigs after reads is an additional available artifact;
        # it must not silently discard the reads or other selected inputs.
        replacements = {item["id"]: item for item in parsed}
        inputs = [replacements.pop(item["id"], item) for item in inputs]
        inputs.extend(replacements.values())
    elif not inputs and previous.get("paths"):
        inputs, problems = extract_inputs(text, previous["paths"])
    paths = list(dict.fromkeys(path for item in inputs for rec in item.get("records", [])
                               for path in rec.get("files", {}).values()))
    result.update(inputs=inputs, paths=paths)
    if scope["ambiguous_workflows"]:
        result.update(
            action="clarify", ambiguous_workflows=scope["ambiguous_workflows"],
            requested_workflows=scope["workflows"],
            pending_scope={key: scope[key] for key in ("goals", "workflows", "full_workflows")},
            questions=problems + [
                "、".join(WORKFLOW_LABELS[item] for item in scope["ambiguous_workflows"])
                + "包含多个步骤，请说明要做到哪一步，或明确执行该组完整流程。"
            ],
        )
        return result
    result["ambiguous_workflows"] = []
    result.pop("pending_scope", None)
    positive_scope = bool(scope["goals"] or scope["full_workflows"])
    additive = bool(re.search(r"增加|加上|追加|再做|还要", _scope_text(text)))
    if positive_scope:
        goals = set(scope["goals"])
        if additive:
            goals.update(previous.get("goals", []))
        excluded = (set(previous.get("excluded_stages", [])) if additive else set())
        excluded.update(scope["excluded_stages"])
        # An explicit positive selection may re-enable an earlier exclusion.
        excluded.difference_update(scope["goals"])
        workflows = set(scope["workflows"])
        full_workflows = set(scope["full_workflows"])
        if additive:
            workflows.update(previous.get("workflows", []))
            full_workflows.update(previous.get("full_workflows", []))
        strict = scope["strict_stages"]
        authorized = set(previous.get("authorized_stages", [])) if additive else set()
        confirmed = set(result.get("confirmed_dependencies", [])) if additive else set()
    else:
        goals = set(previous.get("goals", []))
        excluded = set(previous.get("excluded_stages", [])) | set(scope["excluded_stages"])
        workflows = set(previous.get("workflows", [])) - set(scope["excluded_workflows"])
        full_workflows = set(previous.get("full_workflows", [])) - set(scope["excluded_workflows"])
        strict = previous.get("strict_stages", False)
        authorized = set(previous.get("authorized_stages", []))
        confirmed = set(result.get("confirmed_dependencies", []))
    goals.difference_update(excluded)
    authorized.difference_update(excluded)
    confirmed.difference_update(excluded)
    # Cancelling a dependency of a retained target must be reported; never
    # automatically change the target to the last surviving upstream stage.
    excluded_workflows = (set(previous.get("excluded_workflows", [])) if not positive_scope or additive else set())
    excluded_workflows.update(scope["excluded_workflows"])
    result.update(
        goals=_ordered(goals), workflows=sorted(workflows), full_workflows=sorted(full_workflows),
        strict_stages=strict, excluded_stages=_ordered(excluded),
        excluded_workflows=sorted(excluded_workflows), confirmed_dependencies=_ordered(confirmed),
    )
    if scope["scope_mentioned"] and not dependency_yes and not dependency_no:
        result["scope_patch"] = {key: result[key] for key in ("goals", "excluded_stages", "excluded_workflows")}
    stages = goals | authorized | {stage.id for stage in STAGES if stage.workflow_id in workflows}
    try:
        patch = parse_parameter_patch(text, stages)
        if parameter_request and not patch:
            raise ParameterError("没有识别到可修改的参数，请说明参数名称和具体值。")
        parameters = apply_patch(previous.get("parameters", {}), patch)
    except ParameterError as exc:
        # Reject the entire proposed change, including any accompanying scope
        # edit. The previous version remains the baseline for the next turn.
        return {
            **baseline, "version": 2, "action": "param_suggest", "handled": True,
            "accepted": False, "ready_to_execute": False, "patch": {}, "parameter_patch": {},
            "json_patch": [], "scope_patch": {}, "questions": [str(exc)],
            "previous_unchanged": True, "base_plan_revision": revision, "plan_revision": revision,
        }
    result.update(
        parameters=parameters, patch=patch, parameter_patch=patch,
        json_patch=_json_patch(previous.get("parameters", {}), patch, revision),
        action="param_suggest" if parameter_request else "plan",
    )
    viral_type = previous.get("viral_type", "RNA+DNA")
    if re.search(r"(?:只|仅).{0,8}RNA", text, re.I):
        viral_type = "RNA"
    elif re.search(r"(?:只|仅).{0,8}DNA", text, re.I):
        viral_type = "DNA"
    result["viral_type"] = viral_type
    result.update(authorized_stages=_ordered(authorized), confirmed_dependencies=_ordered(confirmed))
    return finalize_plan(result, previous, check_files=check_files, dependency_no=dependency_no)


def finalize_plan(result: dict, previous: dict, *, check_files: bool = True, dependency_no: bool = False) -> dict:
    """Validate a structured draft without interpreting natural language.

    Both the historical router and the model proposal validator use this gate.
    Compilation finds technical dependencies; it never authorizes them.
    """
    goals = set(result.get("goals", []))
    full_workflows = set(result.get("full_workflows", []))
    strict = result.get("strict_stages", True)
    excluded = set(result.get("excluded_stages", []))
    authorized = set(result.get("authorized_stages", []))
    confirmed = set(result.get("confirmed_dependencies", []))
    patch = result.get("parameter_patch", {})
    parameters = result.get("parameters", {})
    inputs = result.get("inputs", [])
    viral_type = result.get("viral_type", "RNA+DNA")
    problems = list(result.get("questions", []))
    if not goals:
        problems.append("请指定要执行的具体步骤或终点，例如“只做质控”“W1做到组装”或“完整执行W2”。")
    if not inputs:
        problems.append("请提供输入文件：原始双端 reads 需要 R1/R2；W2 病毒候选发现需要 contigs FASTA。")
    if problems:
        result["questions"] = problems
        return result
    try:
        spec = compile_execution_spec({
            "version": 8, "adapter": "analysis_pipeline", "inputs": inputs,
            "deliverables": result["goals"], "options": {"viral_type": viral_type},
            "policy": {"mode": "standard"},
        })
    except ExecutionSpecError as exc:
        result["questions"] = ["缺少前置输入，不能跳过必要步骤执行：" + str(exc)]
        return result
    selected = {stage["id"] for stage in spec["stages"]}
    permitted_groups = set(result.get("prerequisite_groups", [])) | full_workflows
    if "prerequisite_groups" not in result and not strict:
        permitted_groups.update(workflow_for(goal) for goal in goals)
    authorized.update(goals | confirmed)
    endpoints = {
        group: max(STAGE_ORDER[goal] for goal in goals if workflow_for(goal) == group)
        for group in {workflow_for(goal) for goal in goals}
    }
    authorized.update(
        sid for sid in selected if workflow_for(sid) in permitted_groups
        and (workflow_for(sid) in full_workflows or STAGE_ORDER[sid] <= endpoints[workflow_for(sid)])
    )
    blocked = selected & excluded
    missing = selected - authorized | blocked
    result.update(authorized_stages=_ordered(authorized - excluded), missing_dependencies=_ordered(missing))
    # Only authorized stages are shown as selected. Proposed dependencies are
    # separate data and cannot be mistaken for an executable final plan.
    result["proposed_dependencies"] = [stage for stage in spec["stages"] if stage["id"] in missing]
    result["stages"] = [stage for stage in spec["stages"] if stage["id"] not in missing]
    result["steps"] = [step for step in canonical_steps(spec) if step["id"] not in missing]
    if missing:
        labels = "、".join(STAGE_BY_ID[sid].label for sid in _ordered(missing))
        prefix = "W2 病毒候选发现需要 contigs。" if any(workflow_for(goal) == "workflow2" for goal in goals) and any(
            workflow_for(sid) == "workflow1" for sid in missing
        ) else ""
        result["questions"] = [
            prefix + "当前输入还需要前置步骤：" + labels
            + "。是否允许补做这些步骤？也可以提供相应的已有中间产物；确认前不能执行。"
        ]
        if blocked:
            result["questions"][0] += "其中有被明确排除的步骤，请明确修改排除设置或提供可替代的输入。"
        if dependency_no:
            result["questions"] = ["已保留不补跑前置步骤的要求。请提供所需中间产物；当前方案不能执行。"]
        return result
    newly_invalid = [key for key in patch if RULES[key].stage not in selected]
    if newly_invalid:
        result.update(accepted=False, previous_unchanged=True, questions=[
            "参数对应的步骤不在当前方案中：" + "、".join(newly_invalid)
        ])
        result["parameters"] = previous.get("parameters", {})
        result["patch"] = result["parameter_patch"] = {}
        result["json_patch"] = []
        return result
    if check_files:
        for item in spec["inputs"]:
            for record in item["records"]:
                for path in record.get("files", {}).values():
                    if not Path(path).exists():
                        result["questions"].append(f"输入文件不存在或当前账号不可访问：{path}")
        if result["questions"]:
            return result
    spec["parameters"] = copy.deepcopy(parameters)
    result.update(execution_spec=spec, stages=spec["stages"], steps=canonical_steps(spec), ready_to_execute=True)
    return result

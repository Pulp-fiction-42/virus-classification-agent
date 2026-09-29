"""Deterministic leadership-priority reports built from frozen run evidence."""

from __future__ import annotations

import hashlib
import html
import json
import os
import re
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1
PROMPT_VERSION = "scientific-reasoning-v1"
LITERATURE_PATH = (
    Path(__file__).resolve().parent / "assets" / "scientific-report-literature-v1.json"
)
REPORT_KINDS = {"scientific", "tool_demo", "skill_demo"}
REPORT_FORMATS = {
    "scientific": {
        "html": "report.html",
        "pdf": "report.pdf",
        "json": "facts.json",
        "facts_json": "facts.json",
        "literature_json": "literature.json",
        "reasoning_json": "reasoning-plan.json",
    },
    "tool_demo": {"html": "report.html", "json": "result.json"},
    "skill_demo": {"html": "report.html", "json": "comparison.json"},
}


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def load_literature() -> dict[str, Any]:
    raw = json.loads(LITERATURE_PATH.read_text(encoding="utf-8"))
    entries = []
    for entry in raw.get("entries") or []:
        content = dict(entry)
        content["content_sha256"] = _digest(content)
        entries.append(content)
    release = {**raw, "entries": entries}
    release["release_sha256"] = _digest(raw)
    return release


def _format_value(value: float) -> str:
    if isinstance(value, int) or float(value).is_integer():
        return f"{int(value):,}"
    magnitude = abs(float(value))
    decimals = 4 if 0 < magnitude < 1 else 2
    return f"{float(value):,.{decimals}f}"


def build_fact_packet(
    state: dict[str, Any], telemetry: dict[str, Any]
) -> dict[str, Any]:
    delivery = telemetry.get("delivery_report") or {}
    evidence = delivery.get("evidence") or []
    selected = []
    for item in evidence:
        if not isinstance(item, dict):
            continue
        pointer = str(item.get("report_pointer") or "")
        if not pointer.startswith(("/sections/upstream", "/sections/virus_identification")):
            continue
        value = item.get("value")
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            continue
        selected.append((pointer, value, str(item.get("unit") or "")))
    selected.sort(key=lambda item: item[0])
    facts = [
        {
            "id": f"F-{index:04d}",
            "pointer": pointer,
            "label": pointer.rsplit("/", 1)[-1].replace("_", " "),
            "value": value,
            "formatted": _format_value(value),
            "unit": unit,
        }
        for index, (pointer, value, unit) in enumerate(selected, start=1)
    ]
    representatives = (
        ((delivery.get("sections") or {}).get("virus_identification") or {}).get(
            "representatives"
        )
        or []
    )
    taxonomy = []
    for index, item in enumerate(representatives, start=1):
        if not isinstance(item, dict):
            continue
        organism = item.get("nr_sscinames") or item.get("vhdb_svname")
        taxonomy.append(
            {
                "id": f"T-{index:04d}",
                "qseqid": item.get("qseqid"),
                "family": item.get("nr_svfamily") or item.get("vhdb_svfamily"),
                "genus": item.get("nr_svgenus"),
                "organism": organism,
                "best_hit": organism if item.get("nr_besthit") else None,
                "quality": item.get("checkv_quality") or item.get("miuvig_quality"),
            }
        )
    payload = {
        "schema_version": SCHEMA_VERSION,
        "run_id": state.get("run_id"),
        "sample_ids": (
            ((delivery.get("sections") or {}).get("virus_identification") or {}).get(
                "sample_ids"
            )
            or [
                record.get("sample_id")
                for item in (state.get("execution_spec") or {}).get("inputs") or []
                for record in item.get("records") or []
                if record.get("sample_id")
            ]
        ),
        "facts": facts,
        "taxonomy": taxonomy,
        "source_report_sha256": _digest(delivery),
    }
    return {**payload, "facts_sha256": _digest(payload)}


def _fact_by_suffix(packet: dict[str, Any], suffix: str) -> dict[str, Any] | None:
    return next(
        (
            fact
            for fact in packet.get("facts") or []
            if str(fact.get("pointer") or "").endswith(suffix)
        ),
        None,
    )


def applicable_claims(
    packet: dict[str, Any], literature: dict[str, Any]
) -> list[dict[str, Any]]:
    sources = {entry["id"]: entry for entry in literature.get("entries") or []}
    claims: list[dict[str, Any]] = []

    def add(
        claim_id: str,
        kind: str,
        fact_ids: list[str],
        source_ids: list[str],
        *,
        context: dict[str, Any] | None = None,
    ) -> None:
        missing = [source_id for source_id in source_ids if source_id not in sources]
        claims.append(
            {
                "id": claim_id,
                "kind": "insufficient" if missing else kind,
                "fact_ids": fact_ids,
                "source_ids": [] if missing else source_ids,
                "missing_source_ids": missing,
                "context": context or {},
            }
        )

    retained = _fact_by_suffix(packet, "/fastp/reads_retained_percent")
    if retained:
        add("C-QC", "qc", [retained["id"]], ["L-METHOD-FASTP"])
    candidates = _fact_by_suffix(packet, "/candidate_count")
    if candidates:
        add(
            "C-CANDIDATE",
            "candidate",
            [candidates["id"]],
            ["L-METHOD-RDRP", "L-THRESHOLD-PROFILE-COV", "L-THRESHOLD-NR-VOTE"],
        )
    completeness = _fact_by_suffix(packet, "/completeness")
    if completeness:
        add(
            "C-QUALITY",
            "quality",
            [completeness["id"]],
            ["L-METHOD-CHECKV", "L-THRESHOLD-MIUVIG"],
        )
    taxonomy_values = packet.get("taxonomy") or []
    taxonomy = next(
        (
            item
            for item in taxonomy_values
            if str(item.get("family") or "").casefold() == "orthomyxoviridae"
            and "influenza a virus" in str(item.get("organism") or "").casefold()
        ),
        next(iter(taxonomy_values), None),
    )
    if taxonomy and taxonomy.get("family"):
        family_matches = (
            str(taxonomy.get("family") or "").casefold() == "orthomyxoviridae"
        )
        organism_matches = "influenza a virus" in str(
            taxonomy.get("organism") or ""
        ).casefold()
        sources_for_taxon = (
            ["L-TAXON-MSL41", "L-TAXON-ORTHOMYXOVIRIDAE", "L-TAXON-NCBI-11320"]
            if family_matches and organism_matches
            else ["L-TAXON-RESULT-NOT-COVERED"]
        )
        add(
            "C-TAXON",
            "taxon",
            [],
            sources_for_taxon,
            context=taxonomy,
        )
    if taxonomy and taxonomy.get("best_hit"):
        best_hit = str(taxonomy["best_hit"])
        if re.search(r"\bH\d+N\d+\b", best_hit, flags=re.IGNORECASE):
            sources_for_subtype = (
                ["L-TAXON-NCBI-11320"]
                if "influenza a virus" in best_hit.casefold()
                else ["L-TAXON-SUBTYPE-NOT-COVERED"]
            )
            add(
                "C-SUBTYPE-LIMIT",
                "subtype_limit",
                [],
                sources_for_subtype,
                context=taxonomy,
            )
    return claims


def reasoning_request(
    packet: dict[str, Any], literature: dict[str, Any]
) -> tuple[dict[str, Any], dict[str, Any], str]:
    claims = applicable_claims(packet, literature)
    claim_ids = [claim["id"] for claim in claims]
    context = {
        "fact_ids": [fact["id"] for fact in packet.get("facts") or []],
        "source_ids": [entry["id"] for entry in literature.get("entries") or []],
        "claim_options": [
            {
                "id": claim["id"],
                "fact_ids": claim["fact_ids"],
                "source_ids": claim["source_ids"],
            }
            for claim in claims
        ],
    }
    schema = {
        "type": "object",
        "additionalProperties": False,
        "required": ["claim_ids"],
        "properties": {
            "claim_ids": {
                "type": "array",
                "items": {"type": "string", "enum": claim_ids},
                "minItems": len(claim_ids),
                "maxItems": len(claim_ids),
                "uniqueItems": True,
            }
        },
    }
    instructions = (
        "你只负责安排给定结论代号的解释顺序。输出必须恰好包含所有 claim_ids，"
        "不得输出句子、数字、来源、路径或额外字段；不得改变任何证据关系。"
    )
    return context, schema, instructions


def build_reasoning_plan(
    packet: dict[str, Any],
    literature: dict[str, Any],
    claim_ids: list[str],
    *,
    provider: str,
    model: str,
    backend_version: str,
) -> dict[str, Any]:
    claims = applicable_claims(packet, literature)
    by_id = {claim["id"]: claim for claim in claims}
    expected = set(by_id)
    if len(claim_ids) != len(expected) or set(claim_ids) != expected:
        raise ValueError("reasoning plan did not preserve the complete claim set")
    cache_payload = {
        "facts_sha256": packet["facts_sha256"],
        "literature_sha256": literature["release_sha256"],
        "model": model,
        "prompt_version": PROMPT_VERSION,
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "prompt_version": PROMPT_VERSION,
        "cache_key": _digest(cache_payload),
        "facts_sha256": packet["facts_sha256"],
        "literature_sha256": literature["release_sha256"],
        "provider": provider,
        "model": model,
        "backend_version": backend_version,
        "claims": [by_id[claim_id] for claim_id in claim_ids],
    }


def _fact_map(packet: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {fact["id"]: fact for fact in packet.get("facts") or []}


def _citations(claim: dict[str, Any], sources: dict[str, dict[str, Any]]) -> str:
    return " ".join(
        f'<a href="#{html.escape(source_id)}">[{html.escape(source_id)} · '
        f'{html.escape(str(sources[source_id]["locator"]))}]</a>'
        for source_id in claim.get("source_ids") or []
        if source_id in sources
    )


def _claim_text(
    claim: dict[str, Any], facts: dict[str, dict[str, Any]]
) -> str:
    if claim.get("kind") == "insufficient":
        missing = "、".join(claim.get("missing_source_ids") or [])
        return f"本地文献库缺少或无法匹配 {missing or '所需依据'}，因此该项依据不足。"
    selected = [facts[fact_id] for fact_id in claim.get("fact_ids") or [] if fact_id in facts]
    first = selected[0] if selected else None
    context = claim.get("context") or {}
    if claim.get("kind") == "qc" and first:
        return f"fastp 处理后的 reads 保留率为 {first['formatted']}%。"
    if claim.get("kind") == "candidate" and first:
        count = int(first["value"])
        return f"固定流程检出 {count} 条病毒候选；候选形成依赖既定的 profile coverage 与 NR bit-score 规则。"
    if claim.get("kind") == "quality" and first:
        value = float(first["value"])
        quality = "未达到 90% 的高质量完整度参考线" if value < 90 else "达到或超过 90% 的高质量完整度参考线"
        return f"CheckV completeness 为 {first['formatted']}%，{quality}；该结果仍需结合序列片段和污染信息解释。"
    if claim.get("kind") == "taxon":
        family = context.get("family") or "未定科"
        organism = context.get("organism") or context.get("best_hit") or "未定类群"
        return f"流程证据支持将代表序列关联到 {family}，最佳物种层级标注为 {organism}。"
    if claim.get("kind") == "subtype_limit":
        best_hit = context.get("best_hit") or "最佳命中"
        return f"{best_hit} 只是数据库最佳命中；本报告没有独立亚型判定证据，因此不能据此确认 H/N 亚型。"
    return "该结论依据不足。"


def render_scientific_html(
    state: dict[str, Any],
    packet: dict[str, Any],
    literature: dict[str, Any],
    reasoning: dict[str, Any],
) -> str:
    facts = _fact_map(packet)
    sources = {entry["id"]: entry for entry in literature.get("entries") or []}
    fact_rows = "".join(
        "<tr>"
        f"<td>{html.escape(fact['id'])}</td>"
        f"<td>{html.escape(fact['label'])}</td>"
        f"<td>{html.escape(fact['formatted'])}</td>"
        f"<td>{html.escape(str(fact.get('unit') or '—'))}</td>"
        f"<td><code>{html.escape(fact['pointer'])}</code></td>"
        "</tr>"
        for fact in packet.get("facts") or []
    )
    reasoning_items = "".join(
        f"<li>{html.escape(_claim_text(claim, facts))} {_citations(claim, sources)}</li>"
        for claim in reasoning.get("claims") or []
    )
    reference_items = "".join(
        f'<li id="{html.escape(entry["id"])}"><strong>{html.escape(entry["id"])}</strong> '
        f'{html.escape(entry["title"])}；来源：{html.escape(entry["source"])}；'
        f'版本：{html.escape(entry["source_version"])}；'
        f'定位：{html.escape(entry["locator"])}；SHA-256：<code>{entry["content_sha256"]}</code></li>'
        for entry in literature.get("entries") or []
    )
    sample_ids = "、".join(str(value) for value in packet.get("sample_ids") or []) or "未记录"
    generated = str((state.get("execution") or {}).get("finished_at") or _now())
    return f'''<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><title>端到端科学报告</title>
<style>@page{{size:A4;margin:16mm}}body{{font:14px/1.65 -apple-system,BlinkMacSystemFont,"Noto Sans SC",sans-serif;color:#17212b;margin:0}}main{{max-width:980px;margin:auto}}h1{{margin-bottom:4px}}h2{{margin-top:28px;border-bottom:1px solid #ccd6dd}}.meta,.note{{color:#5d6973}}.notice{{padding:10px 12px;background:#f1f6f8;border-left:4px solid #168aad}}table{{width:100%;border-collapse:collapse;font-size:11px}}th,td{{border:1px solid #d7dfe5;padding:5px;text-align:left;vertical-align:top}}th{{background:#f3f6f8}}code{{font-size:10px;overflow-wrap:anywhere}}a{{color:#086788;text-decoration:none}}</style></head>
<body><main><h1>端到端科学报告</h1><p class="meta">任务：{html.escape(str(state.get("run_id") or "—"))} · 样本：{html.escape(sample_ids)} · 结果完成时间：{html.escape(generated)}</p>
<div class="notice">本报告为单样本描述统计，只解释本报告列出的数字与冻结文献条目；不构成临床诊断。数据库最佳命中不等同于亚型确认。</div>
<h2>结果事实</h2><table><thead><tr><th>ID</th><th>指标</th><th>值</th><th>单位</th><th>报告定位</th></tr></thead><tbody>{fact_rows}</tbody></table>
<h2>受控推理与总结</h2><ol>{reasoning_items or '<li>没有可由当前结果和本地文献共同支持的推理项，依据不足。</li>'}</ol>
<h2>本地文献库</h2><p class="note">Release：{html.escape(literature['release_id'])} · SHA-256：<code>{literature['release_sha256']}</code></p><ol>{reference_items}</ol>
<h2>复现身份</h2><ul><li>Facts SHA-256：<code>{packet['facts_sha256']}</code></li><li>Reasoning cache key：<code>{reasoning['cache_key']}</code></li><li>模型：{html.escape(str(reasoning['model']))}（模型只排序结论代号）</li></ul></main></body></html>'''


def _simple_table(rows: list[tuple[str, Any]]) -> str:
    return "".join(
        f"<tr><th>{html.escape(label)}</th><td>{html.escape(str(value if value is not None else '—'))}</td></tr>"
        for label, value in rows
    )


def render_tool_html(state: dict[str, Any], result: dict[str, Any]) -> str:
    inputs = result.get("inputs") or [{}]
    outputs = result.get("outputs") or [{}]
    rows = [
        ("Run", state.get("run_id")),
        ("工具", result.get("tool_id")),
        ("请求版本", result.get("requested_version")),
        ("实测版本", result.get("observed_version")),
        ("调用 adapter", result.get("invocation_adapter")),
        ("状态", result.get("status")),
        (
            "参数",
            json.dumps(
                result.get("parameters") or {}, ensure_ascii=False, sort_keys=True
            ),
        ),
        ("输入 SHA-256", inputs[0].get("sha256")),
        ("输出 SHA-256", outputs[0].get("sha256")),
    ]
    return f'''<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><title>本地工具库调用报告</title><style>body{{font:14px/1.6 -apple-system,"Noto Sans SC",sans-serif;max-width:900px;margin:30px auto;color:#17212b}}table{{border-collapse:collapse;width:100%}}th,td{{border:1px solid #d7dfe5;padding:7px;text-align:left}}th{{width:32%;background:#f3f6f8}}code{{overflow-wrap:anywhere}}</style></head><body><h1>本地工具库调用报告</h1><table>{_simple_table(rows)}</table><p>该结果由版本化工具条目调用并实际应用于 Workflow 1；未登记工具或版本不符会在排队前拒绝。</p></body></html>'''


def build_skill_comparison(
    baseline_state: dict[str, Any], modified_state: dict[str, Any]
) -> dict[str, Any]:
    baseline = (baseline_state.get("execution_spec") or {}).get("workflow_skill") or {}
    modified = (modified_state.get("execution_spec") or {}).get("workflow_skill") or {}
    if not baseline or not modified or baseline.get("skill_id") != modified.get("skill_id"):
        raise ValueError("skill comparison requires matching workflow skill invocations")
    before = baseline.get("effective_parameters") or {}
    after = modified.get("effective_parameters") or {}
    keys = sorted(set(before) | set(after))
    changes = [
        {"parameter": key, "before": before.get(key), "after": after.get(key)}
        for key in keys
        if before.get(key) != after.get(key)
    ]
    if changes != [{"parameter": "length_required", "before": 50, "after": 80}]:
        raise ValueError("skill demo must change only length_required from 50 to 80")
    payload = {
        "schema_version": SCHEMA_VERSION,
        "skill_id": baseline["skill_id"],
        "skill_version": baseline["skill_version"],
        "baseline_run_id": baseline_state.get("run_id"),
        "modified_run_id": modified_state.get("run_id"),
        "baseline_compiled_sha256": baseline.get("compiled_sha256"),
        "modified_compiled_sha256": modified.get("compiled_sha256"),
        "changes": changes,
        "baseline_tool_result": (baseline_state.get("tool_results") or [None])[0],
        "modified_tool_result": (modified_state.get("tool_results") or [None])[0],
        "status": "completed",
    }
    return {**payload, "comparison_sha256": _digest(payload)}


def render_skill_html(comparison: dict[str, Any]) -> str:
    change = comparison["changes"][0]
    return f'''<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><title>工作流技能修改报告</title><style>body{{font:14px/1.6 -apple-system,"Noto Sans SC",sans-serif;max-width:900px;margin:30px auto;color:#17212b}}table{{border-collapse:collapse;width:100%}}th,td{{border:1px solid #d7dfe5;padding:7px;text-align:left}}th{{background:#f3f6f8}}code{{overflow-wrap:anywhere}}</style></head><body><h1>工作流技能使用与修改报告</h1><table><tr><th>技能</th><td>{html.escape(comparison['skill_id'])}@{html.escape(comparison['skill_version'])}</td></tr><tr><th>原样试跑</th><td>{html.escape(str(comparison['baseline_run_id']))}</td></tr><tr><th>修改后试跑</th><td>{html.escape(str(comparison['modified_run_id']))}</td></tr><tr><th>受控修改</th><td>{html.escape(change['parameter'])}: {change['before']} → {change['after']}</td></tr><tr><th>原编译 digest</th><td><code>{comparison['baseline_compiled_sha256']}</code></td></tr><tr><th>新编译 digest</th><td><code>{comparison['modified_compiled_sha256']}</code></td></tr></table><p>两次执行均由同一版本化 skill 编译；修改记录保存在 execution spec，没有拼接临时命令。</p></body></html>'''


def _artifact(path: Path, run_dir: Path, media_type: str) -> dict[str, Any]:
    return {
        "path": path.relative_to(run_dir).as_posix(),
        "bytes": path.stat().st_size,
        "sha256": _sha256(path),
        "media_type": media_type,
    }


def generate_priority_reports(
    state_path: Path,
    *,
    reasoning_plan: dict[str, Any] | None,
    baseline_state_path: Path | None = None,
    weasyprint: str,
) -> dict[str, Any]:
    state = json.loads(state_path.read_text(encoding="utf-8"))
    run_dir = state_path.parent.parent.resolve()
    if state_path.resolve() != run_dir / ".bioagent" / "state.json":
        raise ValueError("state path does not match run directory")
    if (state.get("execution") or {}).get("status") != "completed":
        raise ValueError("priority reports require a completed run")
    telemetry_path = Path((state.get("technical_report") or {}).get("telemetry") or "")
    if not telemetry_path.is_absolute():
        raise ValueError("report telemetry path is invalid")
    resolved_telemetry = telemetry_path.resolve()
    if run_dir not in resolved_telemetry.parents or not resolved_telemetry.is_file():
        raise ValueError("report telemetry is outside the run directory")
    telemetry = json.loads(resolved_telemetry.read_text(encoding="utf-8"))
    output = run_dir / "reports" / "standard"
    output.mkdir(parents=True, exist_ok=True)
    generated_at = str((state.get("execution") or {}).get("finished_at") or _now())
    reports = dict(state.get("artifact_reports") or {})

    tool_results = state.get("tool_results") or []
    if tool_results:
        tool_dir = output / "tool-demo"
        result = tool_results[0]
        _write_json(tool_dir / "result.json", result)
        (tool_dir / "report.html").write_text(
            render_tool_html(state, result), encoding="utf-8"
        )
        reports["tool_demo"] = {
            "status": "ready",
            "generated_at": generated_at,
            "formats": {
                "html": _artifact(tool_dir / "report.html", run_dir, "text/html; charset=utf-8"),
                "json": _artifact(tool_dir / "result.json", run_dir, "application/json"),
            },
        }

    stage_ids = {
        stage.get("id")
        for stage in (state.get("execution_spec") or {}).get("stages") or []
    }
    if "virus_summary" in stage_ids:
        packet = build_fact_packet(state, telemetry)
        literature = load_literature()
        science_dir = output / "scientific"
        cached_path = science_dir / "reasoning-plan.json"
        if cached_path.is_file():
            cached = json.loads(cached_path.read_text(encoding="utf-8"))
            if reasoning_plan is None or cached.get("cache_key") == reasoning_plan.get(
                "cache_key"
            ):
                reasoning_plan = cached
        if not isinstance(reasoning_plan, dict):
            raise ValueError("scientific report requires a validated reasoning plan")
        expected_claim_ids = {claim["id"] for claim in applicable_claims(packet, literature)}
        actual_claim_ids = [claim.get("id") for claim in reasoning_plan.get("claims") or []]
        if (
            reasoning_plan.get("facts_sha256") != packet["facts_sha256"]
            or reasoning_plan.get("literature_sha256") != literature["release_sha256"]
            or len(actual_claim_ids) != len(expected_claim_ids)
            or set(actual_claim_ids) != expected_claim_ids
        ):
            raise ValueError("reasoning plan does not match frozen report evidence")
        _write_json(science_dir / "facts.json", packet)
        _write_json(science_dir / "literature.json", literature)
        _write_json(cached_path, reasoning_plan)
        html_path = science_dir / "report.html"
        pdf_path = science_dir / "report.pdf"
        html_path.write_text(
            render_scientific_html(state, packet, literature, reasoning_plan),
            encoding="utf-8",
        )
        completed = subprocess.run(
            [weasyprint, str(html_path), str(pdf_path)],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=900,
            check=False,
        )
        if completed.returncode or not pdf_path.is_file():
            raise RuntimeError("scientific PDF generation failed")
        reports["scientific"] = {
            "status": "ready",
            "generated_at": generated_at,
            "cache_key": reasoning_plan["cache_key"],
            "formats": {
                "html": _artifact(html_path, run_dir, "text/html; charset=utf-8"),
                "pdf": _artifact(pdf_path, run_dir, "application/pdf"),
                "json": _artifact(science_dir / "facts.json", run_dir, "application/json"),
                "facts_json": _artifact(
                    science_dir / "facts.json", run_dir, "application/json"
                ),
                "literature_json": _artifact(
                    science_dir / "literature.json", run_dir, "application/json"
                ),
                "reasoning_json": _artifact(
                    cached_path, run_dir, "application/json"
                ),
            },
        }

    if baseline_state_path is not None:
        baseline_state = json.loads(baseline_state_path.read_text(encoding="utf-8"))
        comparison = build_skill_comparison(baseline_state, state)
        skill_dir = output / "skill-demo"
        _write_json(skill_dir / "comparison.json", comparison)
        (skill_dir / "report.html").write_text(
            render_skill_html(comparison), encoding="utf-8"
        )
        reports["skill_demo"] = {
            "status": "ready",
            "generated_at": generated_at,
            "formats": {
                "html": _artifact(skill_dir / "report.html", run_dir, "text/html; charset=utf-8"),
                "json": _artifact(skill_dir / "comparison.json", run_dir, "application/json"),
            },
        }

    state["artifact_reports"] = reports
    _write_json(state_path, state)
    return reports


def report_artifact(
    state: dict[str, Any], run_dir: Path, *, kind: str, format_name: str
) -> tuple[dict[str, Any], bytes]:
    if kind not in REPORT_KINDS or format_name not in REPORT_FORMATS[kind]:
        raise ValueError("unsupported priority report artifact")
    report = (state.get("artifact_reports") or {}).get(kind) or {}
    artifact = (report.get("formats") or {}).get(format_name)
    if report.get("status") != "ready" or not isinstance(artifact, dict):
        raise ValueError("priority report artifact is not ready")
    relative = artifact.get("path")
    if not isinstance(relative, str) or Path(relative).is_absolute():
        raise ValueError("priority report artifact path is invalid")
    resolved_run = run_dir.resolve()
    path = (resolved_run / relative).resolve()
    if resolved_run not in path.parents or not path.is_file():
        raise ValueError("priority report artifact is unavailable")
    data = path.read_bytes()
    if len(data) != artifact.get("bytes") or hashlib.sha256(data).hexdigest() != artifact.get("sha256"):
        raise ValueError("priority report artifact validation failed")
    return artifact, data

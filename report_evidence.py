"""Extract report facts from artifacts and render numbers without model guesses."""

from __future__ import annotations

import hashlib
import html
import json
import math
from pathlib import Path
from typing import Any


class ReportEvidenceError(ValueError):
    pass


def _number(value: Any, field: str, *, rate: bool = False, integer: bool = False) -> int | float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ReportEvidenceError(f"报告字段 {field} 必须是有限数值。")
    if value < 0 or (rate and value > 1) or (integer and int(value) != value):
        raise ReportEvidenceError(f"报告字段 {field} 的数值不合法：{value!r}")
    return int(value) if integer else value


def extract_fastp(path: Path, run_dir: Path) -> dict:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
        summary = document["summary"]
        before, after = summary["before_filtering"], summary["after_filtering"]
        metric = {
            "source": str(path.relative_to(run_dir)),
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "sample": path.parent.name,
            "version": summary.get("fastp_version"),
            "before": {}, "after": {}, "evidence": {},
        }
        for side, data in [("before", before), ("after", after)]:
            for field in ["total_reads", "total_bases"]:
                metric[side][field] = _number(data[field], side + "." + field, integer=True)
            for field in ["q20_rate", "q30_rate", "gc_content"]:
                if field in data:
                    metric[side][field] = _number(data[field], side + "." + field, rate=True)
            if "q20_rate" in data and "q30_rate" in data and data["q30_rate"] > data["q20_rate"]:
                raise ReportEvidenceError("Q30 比例不能大于 Q20 比例。")
        for field in ["total_reads", "total_bases"]:
            if metric["after"][field] > metric["before"][field]:
                raise ReportEvidenceError("fastp 过滤后计数超过过滤前计数。")
        paired = "paired" in str(summary.get("sequencing", "")).lower()
        metric["paired"] = paired
        if paired:
            for side in ["before", "after"]:
                if metric[side]["total_reads"] % 2:
                    raise ReportEvidenceError("双端 fastp 报告的总 reads 数必须是偶数。")
                metric[side]["pairs"] = metric[side]["total_reads"] // 2
        total = metric["before"]["total_reads"]
        metric["retention_rate"] = metric["after"]["total_reads"] / total if total else None
        duplication = document.get("duplication", {})
        raw_rate = duplication.get("rate", duplication.get("duplication_rate"))
        metric["duplication_rate"] = None if raw_rate is None else _number(raw_rate, "duplication.rate", rate=True)
        if raw_rate is not None:
            key = "rate" if "rate" in duplication else "duplication_rate"
            metric["evidence"]["duplication_rate"] = "/duplication/" + key
        adapter = document.get("adapter_cutting", {})
        for name in ["adapter_trimmed_reads", "adapter_trimmed_bases"]:
            if name in adapter:
                metric[name] = _number(adapter[name], name, integer=True)
                metric["evidence"][name] = "/adapter_cutting/" + name
        filtering = document.get("filtering_result", {})
        metric["filtering_result"] = {key: _number(value, "filtering_result." + key, integer=True) for key, value in filtering.items() if isinstance(value, (int, float))}
        # The passed_filter_reads counter is deliberately separate from final output.
        return metric
    except (OSError, UnicodeError, KeyError, TypeError, json.JSONDecodeError) as exc:
        raise ReportEvidenceError(f"无法读取真实 fastp 指标：{path.name}：{exc}") from exc


def collect_evidence(run_dir: Path, state: dict) -> dict:
    run_dir = run_dir.resolve()
    metrics = []
    for path in sorted((run_dir / "results").rglob("01_fastp.json")):
        if not path.resolve().is_relative_to(run_dir):
            raise ReportEvidenceError("报告来源文件不在当前运行目录中。")
        metrics.append(extract_fastp(path, run_dir))
    selected = {rule for step in state.get("steps", []) for rule in step.get("rules", [])}
    if "fastp" in selected and not metrics:
        raise ReportEvidenceError("方案包含 fastp，但找不到真实的 01_fastp.json，不能生成质控数字。")
    artifacts = []
    for p in sorted((run_dir / "results").rglob("*")):
        if p.is_file() and p.suffix not in {".gz", ".bam", ".bai"}:
            artifacts.append({"path": str(p.relative_to(run_dir)), "bytes": p.stat().st_size})
    return {"version": 1, "validation": "passed", "run_id": state.get("run_id", run_dir.name), "fastp": metrics, "artifacts": artifacts}


def _escape(value: Any) -> str:
    return html.escape(str(value))


def _percent(value: float | None) -> str:
    return "未记录" if value is None else f"{value * 100:.2f}%"


def render_fragment(state: dict, evidence: dict) -> str:
    out = ['<article class="diagnostic-report"><header><h1>分析结果报告</h1>',
           '<p class="review-status">指标由结果文件提取并经程序校验</p></header>',
           '<section><h2>基本信息</h2><dl><dt>运行编号</dt><dd>' + _escape(evidence["run_id"]) + '</dd></dl></section>',
           '<section><h2>实际分析范围</h2><ul>']
    for step in state.get("steps", []):
        out.append('<li>' + _escape(step["label"]) + '：' + _escape(step.get("status", "unknown")) + '</li>')
    out.append('</ul></section>')
    if state.get("plan_revision") is not None:
        out.append('<section><h2>方案与参数</h2><p>确认方案版本：' + _escape(state["plan_revision"]) + '</p><ul>')
        for key, value in state.get("parameters", {}).items():
            out.append('<li><code>' + _escape(key) + '</code> = ' + _escape(value) + '</li>')
        if not state.get("parameters"):
            out.append('<li>采用工作流原有参数，未提交参数覆盖。</li>')
        out.append('</ul>')
        dependencies = state.get("confirmed_dependencies", [])
        if dependencies:
            out.append('<p>经用户确认增加的前置步骤：' + _escape('、'.join(dependencies)) + '</p>')
        out.append('</section>')
    for metric in evidence["fastp"]:
        before, after = metric["before"], metric["after"]
        out.append('<section><h2>质控结果：' + _escape(metric["sample"]) + '</h2><table><thead><tr><th>指标</th><th>过滤前</th><th>最终输出</th></tr></thead><tbody>')
        rows = [("reads 总数", before["total_reads"], after["total_reads"])]
        if metric["paired"]:
            rows.append(("reads 对数", before["pairs"], after["pairs"]))
        rows.extend([("碱基总数", before["total_bases"], after["total_bases"]),
                     ("Q20 碱基比例", _percent(before.get("q20_rate")), _percent(after.get("q20_rate"))),
                     ("Q30 碱基比例", _percent(before.get("q30_rate")), _percent(after.get("q30_rate")))])
        for label, a, b in rows:
            out.append('<tr><td>' + _escape(label) + '</td><td>' + _escape(a) + '</td><td>' + _escape(b) + '</td></tr>')
        out.append('</tbody></table><ul><li>最终 reads 保留比例：' + _percent(metric["retention_rate"]) + '</li>')
        if metric["duplication_rate"] is not None:
            out.append('<li>重复率：' + _percent(metric["duplication_rate"]) + '；证据字段：<code>' + _escape(metric["evidence"]["duplication_rate"]) + '</code></li>')
        else:
            out.append('<li>重复率：真实 JSON 未记录该字段。</li>')
        for field, label in [("adapter_trimmed_reads", "发生接头裁切的 reads 数"), ("adapter_trimmed_bases", "接头裁切的碱基数")]:
            if field in metric:
                out.append('<li>' + label + '：' + str(metric[field]) + '</li>')
        passed = metric["filtering_result"].get("passed_filter_reads")
        if passed is not None:
            out.append('<li>fastp 的 passed_filter_reads 计数：' + str(passed) + '；最终输出数量以 summary.after_filtering.total_reads 为准。</li>')
        out.append('</ul><p>数据来源：<code>' + _escape(metric["source"]) + '</code></p></section>')
    rules = {rule for step in state.get("steps", []) for rule in step.get("rules", [])}
    out.append('<section><h2>结果说明与局限</h2>')
    if not rules.intersection({"rna_virusidentification", "dna_virusidentification", "summarize_tables"}):
        out.append('<p>本次范围未包含病毒鉴定，不能从质控或组装结果推断病毒身份。</p>')
    else:
        out.append('<p>本报告列出已执行步骤与真实产物；病毒候选及分类结论需以对应证据表为准。</p>')
    out.append('<p>缺失字段按未记录处理；未使用未经验证的质量合格阈值或模型补写的数值。</p></section><section><h2>证据与产物</h2><ul>')
    for item in evidence["artifacts"][:100]:
        out.append('<li><code>' + _escape(item["path"]) + '</code></li>')
    out.append('</ul></section></article>')
    return ''.join(out)


def summary_text(evidence: dict) -> str:
    lines = ["分析执行完成，报告中的指标已按真实结果文件校验。"]
    for metric in evidence["fastp"]:
        before, after = metric["before"], metric["after"]
        unit = "对 reads" if metric["paired"] else "条 reads"
        key = "pairs" if metric["paired"] else "total_reads"
        lines.append(f"样本 {metric['sample']}：{before[key]:,} {unit} → {after[key]:,} {unit}，保留 {_percent(metric['retention_rate'])}。")
        if metric["duplication_rate"] is not None:
            lines.append("真实重复率：" + _percent(metric["duplication_rate"]) + "。")
    return "\n\n".join(lines)

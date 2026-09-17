"""Generate deterministic W1-W6 delivery reports from a completed BioAgent run."""

from __future__ import annotations

import argparse
import base64
import hashlib
import html
import json
import math
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1
LOW_DEPTH_READS = 5_000_000
WORKFLOWS = {
    "workflow1": ("W1", "上游质控与组装"),
    "workflow2": ("W2", "病毒鉴定"),
    "workflow3": ("W3", "宿主鉴定"),
    "workflow4": ("W4", "微生物谱（Kraken2/Bracken）"),
    "workflow5": ("W5", "病毒定量（RPM）"),
    "workflow6": ("W6", "病毒注释"),
}
GROUP_CANDIDATES = (
    "method",
    "host",
    "group",
    "treatment",
    "condition",
    "library",
    "batch",
    "sample_type",
    "phenotype",
    "control",
)
SAMPLE_KEYS = ("sample_id", "sample", "sample_name", "sn", "run_id")
FIGURES = (
    ("fig01_depth_by_sample", "W1", r"merge.*r1.*read|merged.*read.*r1|(raw|input).*read|depth"),
    ("fig02_w1_read_retention", "W1", r"retain|clean.*read|filtered.*read"),
    ("fig03_w1_base_quality", "W1", r"q30|q20"),
    ("fig04_w1_rrna", "W1", r"rrna"),
    ("fig05_w1_contig_yield", "W1", r"contig.*(count|num|seq|_n)|num.*contig"),
    ("fig06_w1_assembly_n50", "W1", r"n50"),
    ("fig07_w2_viral_contigs", "W2", r"viral.*contig|putative"),
    ("fig08_w2_checkv_quality", "W2", r"checkv|completeness|contamination"),
    ("fig09_w2_clusters", "W2", r"cluster"),
    ("fig10_w3_host_support", "W3", r"host|coverage|identity"),
    ("fig11_w4_species_richness", "W4", r"species|richness|tax"),
    ("fig12_w4_top_taxa", "W4", r"fraction|abundance|reads"),
    ("fig13_w4_classified_fraction", "W4", r"classified|unclassified"),
    ("fig14_w5_rpm", "W5", r"rpm"),
    ("fig15_w5_mapping", "W5", r"mapping|mapped|rate"),
    ("fig16_w6_checkv_quality", "W6", r"checkv|quality|completeness"),
    ("fig17_w6_annotation_strategy", "W6", r"strategy"),
    ("fig18_w6_annotation_outcomes", "W6", r"annotat|gene|amg|consistency"),
)


def _now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _safe_error(error: BaseException) -> str:
    text = re.sub(r"/[^\s:]+", "[path]", str(error))
    return text[:1000] or error.__class__.__name__


def _phase_map(state: dict[str, Any]) -> dict[str, dict[str, Any]]:
    phases: dict[str, dict[str, Any]] = {}
    for phase in state.get("phases") or []:
        if isinstance(phase, dict) and phase.get("workflow_id") in WORKFLOWS:
            phases[str(phase["workflow_id"])] = phase
    return phases


def _run_extractors(
    state: dict[str, Any], out_dir: Path, python: str
) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    payloads: dict[str, dict[str, Any]] = {}
    coverage: list[dict[str, Any]] = []
    module_root = Path(__file__).resolve().parent
    environment = os.environ.copy()
    environment["XVIRBENCH_ROOT"] = str(module_root)
    environment["PYTHONPATH"] = os.pathsep.join(
        [str(module_root), environment.get("PYTHONPATH", "")]
    )
    phases = _phase_map(state)
    for workflow_id, (short, _title) in WORKFLOWS.items():
        phase = phases.get(workflow_id)
        if not phase or phase.get("status") != "completed" or phase.get("skipped"):
            coverage.append(
                {"workflow": short, "status": "missing_workflow", "tables": 0}
            )
            continue
        workflow_dir = Path(str(phase.get("workflow_dir") or ""))
        results_dir = Path(str(phase.get("results_dir") or ""))
        script = workflow_dir / "scripts" / "extract_results.py"
        if not script.is_file() or not results_dir.is_dir():
            raise RuntimeError(f"{short} extractor or results directory is missing")
        target = out_dir / "extracted" / short.lower()
        command = [
            python,
            str(script),
            "--result-dir",
            str(results_dir),
            "--output-dir",
            str(target),
            "--format",
            "json",
            "--prefix",
            short.lower(),
        ]
        if workflow_id == "workflow1":
            sample_list = workflow_dir / "data" / "samples.txt"
            if sample_list.is_file():
                command.extend(["--sample-list", str(sample_list)])
        result = subprocess.run(
            command,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            env=environment,
            timeout=900,
            check=False,
        )
        if result.returncode:
            detail = (result.stderr or result.stdout).strip().splitlines()
            raise RuntimeError(
                f"{short} extractor failed: {detail[-1] if detail else result.returncode}"
            )
        payload_path = target / f"{short.lower()}_extracted.json"
        payload = json.loads(payload_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict) or not isinstance(payload.get("tables"), dict):
            raise TypeError(f"{short} extractor returned an invalid payload")
        payloads[short] = payload
        coverage.append(
            {
                "workflow": short,
                "status": "extracted",
                "tables": sum(
                    bool(rows) for rows in payload.get("tables", {}).values()
                ),
            }
        )
    return payloads, coverage


def _sample_id(row: dict[str, Any]) -> str | None:
    for key in SAMPLE_KEYS:
        value = row.get(key)
        if value not in {None, ""}:
            return str(value)
    return None


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)) and math.isfinite(float(value)):
        return float(value)
    if isinstance(value, str):
        try:
            parsed = float(value.replace(",", "").rstrip("%"))
            return parsed if math.isfinite(parsed) else None
        except ValueError:
            return None
    return None


def _metadata_samples(state: dict[str, Any]) -> dict[str, dict[str, Any]]:
    snapshot = state.get("metadata_snapshot")
    content = snapshot.get("content") if isinstance(snapshot, dict) else None
    samples = content.get("samples") if isinstance(content, dict) else None
    return {
        str(sample_id): dict(fields)
        for sample_id, fields in (samples or {}).items()
        if isinstance(fields, dict)
    }


def _build_tables(payloads: dict[str, dict[str, Any]], state: dict[str, Any], out_dir: Path):
    import pandas as pd

    table_dir = out_dir / "tables" / "extracted"
    metric_rows: list[dict[str, Any]] = []
    sample_metrics: dict[str, dict[str, list[float]]] = {}
    observed_samples: set[str] = set()
    for short, payload in payloads.items():
        for table_name, rows in payload.get("tables", {}).items():
            if not isinstance(rows, list):
                continue
            frame = pd.DataFrame([row for row in rows if isinstance(row, dict)])
            table_dir.mkdir(parents=True, exist_ok=True)
            frame.to_csv(table_dir / f"{short.lower()}__{table_name}.tsv", sep="\t", index=False)
            for row in rows:
                if not isinstance(row, dict):
                    continue
                sample_id = _sample_id(row)
                if sample_id:
                    observed_samples.add(sample_id)
                for key, raw in row.items():
                    value = _number(raw)
                    if value is None:
                        continue
                    metric_rows.append(
                        {
                            "workflow": short,
                            "sample_id": sample_id or "batch",
                            "table": str(table_name),
                            "metric": str(key),
                            "value": value,
                        }
                    )
                    if sample_id:
                        sample_metrics.setdefault(sample_id, {}).setdefault(
                            f"{short.lower()}__{key}", []
                        ).append(value)

    metadata = _metadata_samples(state)
    sample_ids = sorted(observed_samples | set(metadata))
    master_rows: list[dict[str, Any]] = []
    for sample_id in sample_ids:
        row: dict[str, Any] = {"sample_id": sample_id, **metadata.get(sample_id, {})}
        for metric, values in sample_metrics.get(sample_id, {}).items():
            row[metric] = float(pd.Series(values).median())
        master_rows.append(row)
    master = pd.DataFrame(master_rows)
    depth_columns = [
        column
        for column in master.columns
        if re.search(
            r"merge.*r1.*read|merged.*read.*r1|(raw|input).*read|depth",
            str(column),
            re.IGNORECASE,
        )
    ]
    depth_column = depth_columns[0] if depth_columns else None
    if depth_column:
        master["low_depth"] = pd.to_numeric(master[depth_column], errors="coerce") < LOW_DEPTH_READS
    else:
        master["low_depth"] = False
    tables_root = out_dir / "tables"
    tables_root.mkdir(parents=True, exist_ok=True)
    master.to_csv(tables_root / "master_samples.tsv", sep="\t", index=False)
    metrics = pd.DataFrame(metric_rows)
    metrics.to_csv(tables_root / "plot_metrics.tsv", sep="\t", index=False)
    return master, metrics


def _usable_groups(master: Any) -> tuple[list[str], list[str]]:
    usable: list[str] = []
    notes: list[str] = []
    for column in GROUP_CANDIDATES:
        if column not in master.columns:
            continue
        values = master[column].fillna("").astype(str).str.strip()
        counts = values[values != ""].value_counts()
        if len(counts) < 2:
            notes.append(f"{column}：少于两个非空水平，仅做总体统计")
        elif sum(counts >= 2) < 2:
            notes.append(f"{column}：不足两个 n≥2 的水平，不做组间检验")
        else:
            usable.append(column)
            notes.append(f"{column}：{len(counts)} 个水平，可做分组比较")
    if not usable:
        notes.append("没有可用分组因子，仅报告总体统计")
    return usable, notes


def _bh(values: list[float]) -> list[float]:
    ranked = sorted(enumerate(values), key=lambda item: item[1])
    adjusted = [1.0] * len(values)
    running = 1.0
    for reverse_index in range(len(ranked) - 1, -1, -1):
        original, value = ranked[reverse_index]
        rank = reverse_index + 1
        running = min(running, value * len(values) / rank)
        adjusted[original] = min(1.0, running)
    return adjusted


def _write_stats(master: Any, groups: list[str], out_dir: Path) -> tuple[Any, Any, Any]:
    import pandas as pd
    from scipy import stats

    numeric = [
        column
        for column in master.select_dtypes(include="number").columns
        if column != "low_depth"
    ]
    sets = {
        "core": master,
        "core_excl_lowdepth": master[~master["low_depth"].fillna(False)],
    }
    overall_rows: list[dict[str, Any]] = []
    grouped_rows: list[dict[str, Any]] = []
    test_rows: list[dict[str, Any]] = []
    for dataset, frame in sets.items():
        for metric in numeric:
            values = pd.to_numeric(frame[metric], errors="coerce").dropna()
            if values.empty:
                continue
            overall_rows.append(
                {
                    "dataset": dataset,
                    "metric": metric,
                    "n": len(values),
                    "median": values.median(),
                    "min": values.min(),
                    "max": values.max(),
                    "mean": values.mean(),
                }
            )
            for group in groups:
                arrays: dict[str, Any] = {}
                for level, subset in frame.groupby(group, dropna=True):
                    series = pd.to_numeric(subset[metric], errors="coerce").dropna()
                    if len(series):
                        grouped_rows.append(
                            {
                                "dataset": dataset,
                                "metric": metric,
                                "group_factor": group,
                                "group": level,
                                "n": len(series),
                                "median": series.median(),
                                "min": series.min(),
                                "max": series.max(),
                            }
                        )
                    if len(series) >= 2:
                        arrays[str(level)] = series.to_numpy()
                if len(arrays) < 2:
                    continue
                levels = list(arrays)
                h_value, p_value = stats.kruskal(*(arrays[level] for level in levels))
                n_value = sum(len(arrays[level]) for level in levels)
                epsilon = max(0.0, (h_value - len(levels) + 1) / (n_value - len(levels))) if n_value > len(levels) else 0.0
                test_rows.append(
                    {
                        "dataset": dataset,
                        "metric": metric,
                        "group_factor": group,
                        "test": "kruskal_wallis",
                        "comparison": "all",
                        "p": p_value,
                        "p_adj_bh": p_value,
                        "effect": epsilon,
                        "effect_name": "epsilon_squared",
                    }
                )
                pairs: list[tuple[str, str, float, float]] = []
                for index, left in enumerate(levels):
                    for right in levels[index + 1 :]:
                        u_value, pair_p = stats.mannwhitneyu(
                            arrays[left], arrays[right], alternative="two-sided"
                        )
                        rank_biserial = 1 - (2 * u_value) / (len(arrays[left]) * len(arrays[right]))
                        pairs.append((left, right, pair_p, rank_biserial))
                adjusted = _bh([pair[2] for pair in pairs])
                for pair, corrected in zip(pairs, adjusted, strict=True):
                    test_rows.append(
                        {
                            "dataset": dataset,
                            "metric": metric,
                            "group_factor": group,
                            "test": "mannwhitney_bh",
                            "comparison": f"{pair[0]} vs {pair[1]}",
                            "p": pair[2],
                            "p_adj_bh": corrected,
                            "effect": pair[3],
                            "effect_name": "rank_biserial",
                        }
                    )
    frames = tuple(pd.DataFrame(rows) for rows in (overall_rows, grouped_rows, test_rows))
    stats_dir = out_dir / "tables" / "stats"
    stats_dir.mkdir(parents=True, exist_ok=True)
    for name, frame in zip(("overall", "by_group", "tests"), frames, strict=True):
        frame.to_csv(stats_dir / f"{name}.tsv", sep="\t", index=False)
    return frames


def _r_script() -> str:
    specifications = ",\n".join(
        f'  list(name="{name}", workflow="{workflow}", pattern="{pattern.replace(chr(92), chr(92) * 2)}")'
        for name, workflow, pattern in FIGURES
    )
    return f'''suppressPackageStartupMessages({{
  library(ggplot2); library(readr); library(dplyr); library(stringr)
}})
args <- commandArgs(trailingOnly=TRUE)
metrics <- suppressMessages(read_tsv(args[[1]], show_col_types=FALSE))
out <- args[[2]]
dir.create(out, recursive=TRUE, showWarnings=FALSE)
specs <- list(
{specifications}
)
theme_set(theme_classic(base_size=10))
for (spec in specs) {{
  selected <- metrics %>% filter(workflow == spec$workflow, str_detect(metric, regex(spec$pattern, ignore_case=TRUE)))
  if (nrow(selected) == 0) next
  metric_name <- selected$metric[[1]]
  selected <- selected %>% filter(metric == metric_name) %>% group_by(sample_id) %>% summarise(value=median(value, na.rm=TRUE), .groups="drop")
  selected <- selected %>% filter(is.finite(value))
  if (nrow(selected) == 0) next
  p <- ggplot(selected, aes(x=reorder(sample_id, value), y=value)) +
    geom_col(fill="#168aad", width=.72) + coord_flip() +
    labs(x=NULL, y=metric_name, title=str_replace_all(spec$name, "_", " ")) +
    theme(plot.title=element_text(face="bold", size=11))
  ggsave(file.path(out, paste0(spec$name, ".png")), p, width=174, height=105, units="mm", dpi=300)
  ggsave(file.path(out, paste0(spec$name, ".pdf")), p, width=174, height=105, units="mm")
}}
'''


def _run_plots(metrics: Any, out_dir: Path, rscript: str) -> list[Path]:
    script_path = out_dir / "r" / "standard_figures.R"
    script_path.parent.mkdir(parents=True, exist_ok=True)
    script_path.write_text(_r_script(), encoding="utf-8")
    if metrics.empty:
        return []
    figures = out_dir / "figures"
    result = subprocess.run(
        [rscript, str(script_path), str(out_dir / "tables" / "plot_metrics.tsv"), str(figures)],
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=900,
        check=False,
    )
    (out_dir / "r" / "plot.log").write_text(
        (result.stdout or "") + (result.stderr or ""), encoding="utf-8"
    )
    if result.returncode:
        raise RuntimeError("R figure generation failed")
    return sorted(figures.glob("*.png"))


def _public_text(value: Any) -> str:
    return re.sub(r"(?<!\w)/[^\s<]+", "[路径已隐藏]", str(value))


def _table(frame: Any, *, limit: int = 200) -> str:
    if frame is None or frame.empty:
        return '<p class="muted">无可展示记录</p>'
    columns = list(frame.columns)
    head = "".join(f"<th>{html.escape(_public_text(column))}</th>" for column in columns)
    rows = []
    for _, row in frame.head(limit).iterrows():
        cells = []
        for value in row:
            if value is None or (isinstance(value, float) and math.isnan(value)):
                rendered = "—"
            elif isinstance(value, float):
                rendered = f"{value:.4g}"
            else:
                rendered = _public_text(value)
            cells.append(f"<td>{html.escape(rendered)}</td>")
        rows.append("<tr>" + "".join(cells) + "</tr>")
    note = f'<p class="muted">仅展示前 {limit} 行</p>' if len(frame) > limit else ""
    return f'<div class="table-wrap"><table><thead><tr>{head}</tr></thead><tbody>{"".join(rows)}</tbody></table></div>{note}'


def _figure(path: Path) -> str:
    encoded = base64.b64encode(path.read_bytes()).decode("ascii")
    title = path.stem.replace("_", " ")
    return f'<figure><img src="data:image/png;base64,{encoded}" alt="{html.escape(title)}"><figcaption>{html.escape(title)}</figcaption></figure>'


def _build_html(
    state: dict[str, Any], coverage: Any, master: Any, stats_frames: tuple[Any, Any, Any],
    groups: list[str], group_notes: list[str], payloads: dict[str, dict[str, Any]], out_dir: Path
) -> str:
    extracted = [row["workflow"] for row in coverage.to_dict("records") if row["status"] == "extracted"]
    skipped = [row["workflow"] for row in coverage.to_dict("records") if row["status"] != "extracted"]
    low = master.loc[master["low_depth"].fillna(False), "sample_id"].astype(str).tolist() if not master.empty else []
    figure_map = {path.stem: path for path in (out_dir / "figures").glob("*.png")}
    sections = []
    for short, title in WORKFLOWS.values():
        if short not in payloads:
            sections.append(f'<section><h2>{short} {title}</h2><div class="notice skip">本流程未运行，整章跳过。</div></section>')
            continue
        figures = [_figure(figure_map[name]) for name, workflow, _ in FIGURES if workflow == short and name in figure_map]
        table_count = sum(bool(rows) for rows in payloads[short].get("tables", {}).values())
        sections.append(
            f'<section><h2>{short} {title}</h2><h3>结果</h3><p>成功提取 {table_count} 张非空结构化表；这里只解释实际提取字段。</p>'
            f'<h3>图</h3><div class="figures">{"".join(figures) if figures else "<p class=muted>没有满足绘图条件的数据，不生成空图。</p>"}</div>'
            '<h3>生物学意义</h3><p>结果需结合测序深度、样本背景及其他流程交叉解读；流程输出不等同于临床诊断。</p></section>'
        )
    cross = ""
    if len(extracted) >= 2:
        cross = '<section><h2>跨流程衔接</h2><h3>结果</h3><p>下游相对丰度、RPM 与候选数量需先对照 W1 测序深度；宿主推断与微生物谱中的宿主相关信号应交叉复核。</p><h3>生物学意义</h3><p>低深度样本出现较高相对指标时优先考虑分母效应，不表述为绝对载量升高。</p></section>'
    script = (out_dir / "r" / "standard_figures.R").read_text(encoding="utf-8") if (out_dir / "r" / "standard_figures.R").is_file() else ""
    return f'''<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{html.escape(_public_text(state.get("title") or state.get("run_id") or "分析"))} · 交付报告</title><style>
@page{{size:A4 landscape;margin:12mm}}:root{{font:14px/1.55 -apple-system,BlinkMacSystemFont,"Segoe UI","Noto Sans SC",sans-serif;color:#17212b}}body{{margin:0;background:#eef2f5}}main{{max-width:1280px;margin:20px auto;padding:28px;background:#fff}}h1{{margin:0}}h2{{margin-top:28px;border-bottom:1px solid #d7dfe5;padding-bottom:6px}}h3{{margin:18px 0 8px}}.meta,.muted{{color:#65727e}}.notice{{padding:12px 14px;border-left:4px solid #168aad;background:#f5f8fa}}.skip{{border-color:#8a949c}}.table-wrap{{overflow-x:auto}}table{{width:100%;border-collapse:collapse;font-size:11px}}th,td{{padding:5px 7px;border:1px solid #d9e0e6;text-align:left;vertical-align:top;overflow-wrap:anywhere}}th{{background:#f4f7f9}}.figures{{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:16px}}figure{{margin:0}}img{{display:block;width:100%;height:auto}}figcaption{{color:#65727e;font-size:11px;text-align:center}}pre{{white-space:pre-wrap;font:9px/1.4 ui-monospace,monospace;overflow-wrap:anywhere}}@media print{{body{{background:#fff}}main{{margin:0;padding:0;max-width:none}}}}
</style></head><body><main><header><h1>工作流完成后交付报告</h1><div class="meta">任务：{html.escape(_public_text(state.get("title") or state.get("run_id")))} · 生成时间：{html.escape(_now())}</div></header>
<section><h2>1. 执行摘要</h2><h3>结果</h3><ul><li>核心队列 {len(master)} 个样本。</li><li>已提取流程：{html.escape(", ".join(extracted) or "无")}。</li><li>未运行并跳过：{html.escape(", ".join(skipped) or "无")}。</li><li>分组因子：{html.escape(", ".join(groups) or "无（仅总体分析）")}。</li><li>低深度样本（合并 R1 &lt; 5×10<sup>6</sup>）：{html.escape(", ".join(low) or "无或未提取深度")}。</li></ul><div class="notice">缺失流程或字段不判为失败；低深度样本的 RPM、物种数和 contig 数不可单独解释为生物学差异。本结果仅供科研参考，不构成临床诊断。</div><h3>生物学意义</h3><p>先检查测序深度和流程覆盖，再解读候选病原、宿主、微生物谱、相对定量与注释结果。小样本检验效能有限。</p></section>
<section><h2>2. 样本 Meta 与提取覆盖</h2><h3>结果</h3><ul>{''.join(f'<li>{html.escape(note)}</li>' for note in group_notes)}</ul>{_table(coverage)}<h3>核心样本主表</h3>{_table(master)}</section>
{''.join(sections)}{cross}
<section><h2>结构化统计</h2><h3>总体描述统计</h3>{_table(stats_frames[0])}<h3>分组描述统计</h3>{_table(stats_frames[1])}<h3>组间检验</h3>{_table(stats_frames[2])}</section>
<section><h2>方法学摘要</h2><ul><li>分组仅在至少两个水平且至少两个水平 n≥2 时启用。</li><li>组间检验使用 Kruskal–Wallis 与双侧 Mann–Whitney；成对 p 值以 BH 校正，并报告 ε² 与 rank-biserial。</li><li>总体与剔除低深度样本后的敏感性统计使用同一数据口径。</li><li>图由 R/ggplot2 生成，并以 base64 嵌入本报告。</li></ul></section>
<section><h2>附录：R 绘图脚本</h2><pre>{html.escape(script)}</pre></section></main></body></html>'''


def _manifest(out_dir: Path) -> dict[str, Any]:
    files = []
    for path in sorted(out_dir.rglob("*")):
        if not path.is_file() or path.name == "manifest.json":
            continue
        files.append(
            {
                "path": path.relative_to(out_dir).as_posix(),
                "bytes": path.stat().st_size,
                "sha256": _sha256(path),
            }
        )
    return {"schema_version": 1, "generated_at": _now(), "files": files}


def generate(state_path: Path) -> dict[str, Any]:
    import pandas as pd

    state = json.loads(state_path.read_text(encoding="utf-8"))
    run_dir = state_path.parent.parent.resolve()
    if state_path.resolve() != run_dir / ".bioagent" / "state.json":
        raise ValueError("state path does not match run directory")
    output = run_dir / "reports" / "standard"
    state["standard_delivery"] = {
        "schema_version": SCHEMA_VERSION,
        "status": "pending",
        "generated_at": _now(),
        "artifacts": {},
    }
    _write_json(state_path, state)
    if output.exists():
        shutil.rmtree(output)
    output.mkdir(parents=True, exist_ok=True)
    (output / "tables").mkdir(parents=True, exist_ok=True)
    python = os.getenv("BIOAGENT_REPORT_PYTHON", "/home/liufangzhou/.pixi/bin/bioagent-report-python")
    rscript = os.getenv("BIOAGENT_RSCRIPT", "/home/liufangzhou/.pixi/bin/bioagent-rscript")
    weasyprint = os.getenv("BIOAGENT_WEASYPRINT", "/home/liufangzhou/.pixi/bin/bioagent-weasyprint")
    payloads, coverage_rows = _run_extractors(state, output, python)
    coverage = pd.DataFrame(coverage_rows)
    coverage.to_csv(output / "tables" / "coverage.tsv", sep="\t", index=False)
    master, metrics = _build_tables(payloads, state, output)
    groups, group_notes = _usable_groups(master)
    stats_frames = _write_stats(master, groups, output)
    _run_plots(metrics, output, rscript)
    document = _build_html(
        state, coverage, master, stats_frames, groups, group_notes, payloads, output
    )
    html_path = output / "report.html"
    pdf_path = output / "report.pdf"
    html_path.write_text(document, encoding="utf-8")
    pdf_result = subprocess.run(
        [weasyprint, str(html_path), str(pdf_path)],
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=900,
        check=False,
    )
    if pdf_result.returncode or not pdf_path.is_file():
        raise RuntimeError("PDF generation failed")
    _write_json(output / "manifest.json", _manifest(output))
    artifacts = {}
    for format_name, path, media_type in (
        ("html", html_path, "text/html; charset=utf-8"),
        ("pdf", pdf_path, "application/pdf"),
    ):
        artifacts[format_name] = {
            "path": path.relative_to(run_dir).as_posix(),
            "bytes": path.stat().st_size,
            "sha256": _sha256(path),
            "media_type": media_type,
        }
    result = {
        "schema_version": SCHEMA_VERSION,
        "status": "ready",
        "generated_at": _now(),
        "artifacts": artifacts,
    }
    state["standard_delivery"] = result
    _write_json(state_path, state)
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate a standard W1-W6 delivery report")
    parser.add_argument("--state", required=True)
    args = parser.parse_args(argv)
    state_path = Path(args.state).resolve()
    try:
        generate(state_path)
    except Exception as error:  # noqa: BLE001 - report failure must not fail computation
        try:
            state = json.loads(state_path.read_text(encoding="utf-8"))
            state["standard_delivery"] = {
                "schema_version": SCHEMA_VERSION,
                "status": "failed",
                "generated_at": _now(),
                "error": _safe_error(error),
                "artifacts": {},
            }
            _write_json(state_path, state)
        except Exception as state_error:  # noqa: BLE001
            print(
                f"Could not record report failure: {_safe_error(state_error)}",
                file=sys.stderr,
            )
        print(f"Standard delivery report failed: {_safe_error(error)}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Generate deterministic W1-W6 standard delivery reports for BioAgent runs."""

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
import tempfile
from collections.abc import Iterable
from datetime import datetime
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1
TEMPLATE_VERSION = "bioagent-standard-delivery-v2"
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
SAMPLE_KEYS = ("sample_id", "source_sample", "sample", "sample_name", "sn")
OWNED_ENTRIES = (
    "extracted",
    "tables",
    "figures",
    "r",
    "report.html",
    "report.pdf",
    "manifest.json",
)
CAPTIONS = {
    "fig01_depth_by_sample": "各样本合并后 R1 深度；虚线为 5 million reads 低深度阈值。",
    "fig02_w1_fastp_retention": "fastp 读段保留率；单样本显示为点，多样本且有合格分组时显示分布。",
    "fig03_w1_q30": "fastp 后 Q30；数值按比例统一显示。",
    "fig04_w1_rrna_retention": "去 rRNA 后 R1 保留率；未产生该指标时不绘图。",
    "fig05_w1_depth_vs_contigs": "测序深度与不短于 1 kb 的 contig 数；点标注样本。",
    "fig06_w2_checkv_by_sample": "各样本候选病毒 contig 的 CheckV 质量组成。",
    "fig07_w2_contig_length": "候选病毒 contig 长度分布。",
    "fig08_w2_viral_contigs_by_method": "每样本候选病毒 contig 数；单样本以点表示。",
    "fig09_w3_host_species": "核心队列推断线粒体物种频次。",
    "fig10_w4_unclassified": "Kraken 未分类比例。",
    "fig11_w4_species_richness": "Bracken 物种分类单元数。",
    "fig12_w4_top_taxa": "Bracken 优势分类单元组成；仅表示提取表覆盖的分类空间。",
    "fig13_w5_mapping_rate": "比对到病毒 contig 的 reads 比例。",
    "fig14_w5_max_rpm": "每样本最大病毒 RPM。",
    "fig15_w5_depth_vs_mapping": "测序深度与病毒比对率。",
    "fig16_w6_checkv_quality": "W6 注释对象的 CheckV 质量组成。",
    "fig17_w6_annotation_strategy": "W6 注释策略分布。",
    "fig18_w6_annotation_outcomes": "W6 注释结果概览。",
}
WORKFLOW_FIGURES = {
    "W1": [
        "fig02_w1_fastp_retention",
        "fig03_w1_q30",
        "fig04_w1_rrna_retention",
        "fig05_w1_depth_vs_contigs",
    ],
    "W2": [
        "fig06_w2_checkv_by_sample",
        "fig07_w2_contig_length",
        "fig08_w2_viral_contigs_by_method",
    ],
    "W3": ["fig09_w3_host_species"],
    "W4": ["fig10_w4_unclassified", "fig11_w4_species_richness", "fig12_w4_top_taxa"],
    "W5": ["fig13_w5_mapping_rate", "fig14_w5_max_rpm", "fig15_w5_depth_vs_mapping"],
    "W6": ["fig16_w6_checkv_quality", "fig17_w6_annotation_strategy", "fig18_w6_annotation_outcomes"],
}


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
    text = re.sub(r"/[A-Za-z0-9_.@+%:=,/-]+", "[path]", str(error))
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
                "tables": sum(bool(rows) for rows in payload.get("tables", {}).values()),
            }
        )
    return payloads, coverage


def _sample_id(row: dict[str, Any]) -> str | None:
    for key in SAMPLE_KEYS:
        value = row.get(key)
        if value is None or isinstance(value, bool):
            continue
        if isinstance(value, float) and not math.isfinite(value):
            continue
        rendered = str(value).strip()
        if not rendered or rendered.lower() in {"nan", "none", "null", "na", "n/a"}:
            continue
        return rendered
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
    normalized: dict[str, dict[str, Any]] = {}
    for sample_id, fields in (samples or {}).items():
        sid = _sample_id({"sample_id": sample_id})
        if sid and isinstance(fields, dict):
            normalized[sid] = dict(fields)
    return normalized


def _rows(payloads: dict[str, dict[str, Any]], workflow: str, table: str) -> list[dict[str, Any]]:
    value = payloads.get(workflow, {}).get("tables", {}).get(table, [])
    return [row for row in value if isinstance(row, dict)] if isinstance(value, list) else []


def _aggregate(values: Iterable[Any], method: str) -> float | None:
    import pandas as pd

    numbers = pd.Series([value for value in (_number(v) for v in values) if value is not None])
    if numbers.empty:
        return None
    if method == "sum":
        return float(numbers.sum())
    if method == "max":
        return float(numbers.max())
    if method == "min":
        return float(numbers.min())
    return float(numbers.median())


def _assign_metric(
    records: dict[str, dict[str, Any]],
    rows: list[dict[str, Any]],
    source: str,
    target: str,
    method: str = "median",
) -> None:
    grouped: dict[str, list[Any]] = {}
    for row in rows:
        sid = _sample_id(row)
        if sid and source in row:
            grouped.setdefault(sid, []).append(row[source])
    for sid, values in grouped.items():
        value = _aggregate(values, method)
        if value is not None and target not in records.setdefault(sid, {}):
            records[sid][target] = value


def _first_text(rows: list[dict[str, Any]], source: str) -> dict[str, str]:
    found: dict[str, str] = {}
    for row in rows:
        sid = _sample_id(row)
        value = row.get(source)
        if sid and value not in {None, ""} and sid not in found:
            found[sid] = str(value)
    return found


def _build_tables(
    payloads: dict[str, dict[str, Any]], state: dict[str, Any], out_dir: Path
):
    """Build a curated sample-level table and preserve every extracted table verbatim."""
    import pandas as pd

    table_dir = out_dir / "tables" / "extracted"
    table_dir.mkdir(parents=True, exist_ok=True)
    observed_samples: set[str] = set()
    for short, payload in payloads.items():
        for table_name, table_rows in payload.get("tables", {}).items():
            if not isinstance(table_rows, list):
                continue
            clean = [row for row in table_rows if isinstance(row, dict)]
            pd.DataFrame(clean).to_csv(
                table_dir / f"{short.lower()}__{table_name}.tsv", sep="\t", index=False
            )
            observed_samples.update(sid for row in clean if (sid := _sample_id(row)))

    records: dict[str, dict[str, Any]] = {
        sid: {"sample_id": sid, **fields}
        for sid, fields in _metadata_samples(state).items()
    }
    for sid in observed_samples:
        records.setdefault(sid, {"sample_id": sid})

    w1_run = _rows(payloads, "W1", "run_qc")
    w1_samples = _rows(payloads, "W1", "samples")
    w1_summary = _rows(payloads, "W1", "summary")
    for source, target, method in (
        ("reads_before", "reads_before_sum", "sum"),
        ("reads_after_fastp", "reads_after_fastp_sum", "sum"),
        ("q30_after_fastp", "q30_after_fastp_median", "median"),
        ("read_retention_fastp", "read_retention_fastp_median", "median"),
        ("duplication_rate", "duplication_rate_median", "median"),
        ("clean_r1_reads", "clean_r1_reads_sum", "sum"),
        ("rrna_retention_r1", "rrna_retention_r1_median", "median"),
    ):
        _assign_metric(records, w1_run, source, target, method)
    for source, target in (
        ("merge_r1_reads", "merge_r1_reads"),
        ("merge_r1_q30_pct", "merge_r1_q30_pct"),
        ("contigs_all_n", "contigs_all_n"),
        ("contigs_ge1kb_n", "contigs_ge1kb_n"),
        ("contigs_ge1kb_bp", "contigs_ge1kb_bp"),
        ("contigs_ge1kb_avg_len", "contigs_ge1kb_avg_len"),
        ("contigs_ge1kb_max_len", "contigs_ge1kb_max_len"),
    ):
        _assign_metric(records, w1_samples, source, target)
    for source, target in (
        ("merged_reads_r1", "merge_r1_reads"),
        ("contigs_all_n", "contigs_all_n"),
        ("contigs_ge1kb_n", "contigs_ge1kb_n"),
        ("contigs_ge1kb_bp", "contigs_ge1kb_bp"),
    ):
        _assign_metric(records, w1_summary, source, target)

    w2_contigs = _rows(payloads, "W2", "contigs")
    w2_cluster = _rows(payloads, "W2", "sample_cluster")
    sole_sample = next(iter(records)) if len(records) == 1 else None
    contigs_by_sample: dict[str, list[dict[str, Any]]] = {}
    for row in w2_contigs:
        if sid := (_sample_id(row) or sole_sample):
            contigs_by_sample.setdefault(sid, []).append(row)
    for sid, rows in contigs_by_sample.items():
        record = records.setdefault(sid, {"sample_id": sid})
        record["n_putative_viral_contigs"] = float(len(rows))
        for quality, column in (
            ("complete", "n_complete"),
            ("high-quality", "n_high_quality"),
            ("medium-quality", "n_medium_quality"),
            ("low-quality", "n_low_quality"),
        ):
            record[column] = float(
                sum(str(row.get("checkv_quality", "")).strip().lower() == quality for row in rows)
            )
        for source, target in (
            ("length", "viral_contig_length_median"),
            ("completeness", "checkv_completeness_median"),
            ("contamination", "checkv_contamination_median"),
        ):
            value = _aggregate((row.get(source) for row in rows), "median")
            if value is not None:
                record[target] = value
    for source, target in (("n_contigs", "n_putative_viral_contigs"), ("n_clusters", "n_clusters")):
        _assign_metric(records, w2_cluster, source, target)
        if sole_sample and target not in records[sole_sample]:
            value = _aggregate((row.get(source) for row in w2_cluster), "median")
            if value is not None:
                records[sole_sample][target] = value

    mappings = {
        "W3": {
            "host_sample": (
                ("n_species_inferred", "n_species_inferred"),
                ("n_genus_inferred", "n_genus_inferred"),
                ("mean_coverage", "host_mean_coverage"),
                ("mean_identity", "host_mean_identity"),
            ),
        },
        "W4": {
            "sample_qc": (
                ("kraken_unclassified_frac", "kraken_unclassified_frac"),
                ("unclassified_fraction", "kraken_unclassified_frac"),
                ("bracken_species_taxa", "bracken_species_taxa"),
                ("species_taxa", "bracken_species_taxa"),
                ("bracken_species_reads_assigned", "bracken_species_reads_assigned"),
            ),
        },
        "W5": {
            "sample_summary": (
                ("mapping_rate_pct", "mapping_rate_pct"),
                ("mapping_rate", "mapping_rate_pct"),
                ("max_rpm", "max_rpm"),
                ("sum_rpm", "sum_rpm"),
            ),
        },
        "W6": {
            "sample_summary": (
                ("n_annotated", "n_annotated"),
                ("n_genes", "n_genes"),
                ("n_amg", "n_amg"),
                ("completeness", "w6_checkv_completeness"),
            ),
            "summary": (
                ("n_annotated", "n_annotated"),
                ("n_genes", "n_genes"),
                ("n_amg", "n_amg"),
            ),
        },
    }
    for short, tables in mappings.items():
        for table_name, fields in tables.items():
            table_rows = _rows(payloads, short, table_name)
            for source, target in fields:
                _assign_metric(records, table_rows, source, target)

    host_rows = _rows(payloads, "W3", "host_top_species") or _rows(payloads, "W3", "host_sample")
    for source in ("inferred_mt_species", "top_host_species", "species"):
        for sid, value in _first_text(host_rows, source).items():
            records.setdefault(sid, {"sample_id": sid}).setdefault("top_host_species", value)

    master = pd.DataFrame([records[sid] for sid in sorted(records)])
    if master.empty:
        master = pd.DataFrame(columns=["sample_id"])
    if "merge_r1_reads" in master:
        depth = pd.to_numeric(master["merge_r1_reads"], errors="coerce")
    else:
        depth = pd.Series(math.nan, index=master.index)
    master["low_depth"] = depth.notna() & (depth < LOW_DEPTH_READS)
    master["in_core_cohort"] = True
    tables_root = out_dir / "tables"
    tables_root.mkdir(parents=True, exist_ok=True)
    master.to_csv(tables_root / "master_samples.tsv", sep="\t", index=False)

    metric_rows: list[dict[str, Any]] = []
    for _, row in master.iterrows():
        for column, raw in row.items():
            value = _number(raw)
            if value is None or column in {"low_depth", "in_core_cohort"}:
                continue
            workflow = "W1"
            if column.startswith(("n_putative", "n_cluster", "n_complete", "n_high", "n_medium", "n_low", "viral_", "checkv_")):
                workflow = "W2"
            elif column.startswith(("n_species", "n_genus", "host_")):
                workflow = "W3"
            elif column.startswith(("kraken_", "bracken_")):
                workflow = "W4"
            elif column in {"mapping_rate_pct", "max_rpm", "sum_rpm"}:
                workflow = "W5"
            elif column.startswith(("n_annotated", "n_genes", "n_amg", "w6_")):
                workflow = "W6"
            metric_rows.append(
                {"workflow": workflow, "sample_id": row["sample_id"], "metric": column, "value": value}
            )
    metrics = pd.DataFrame(metric_rows, columns=["workflow", "sample_id", "metric", "value"])
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
        notes.append("没有可用分组因子，仅报告总体描述统计")
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


def _describe(values: Any, missing: int) -> dict[str, Any]:
    return {
        "n": len(values),
        "n_missing": int(missing),
        "mean": values.mean(),
        "sd": values.std(ddof=1) if len(values) > 1 else math.nan,
        "median": values.median(),
        "q25": values.quantile(0.25),
        "q75": values.quantile(0.75),
        "min": values.min(),
        "max": values.max(),
    }


def _write_stats(master: Any, groups: list[str], out_dir: Path) -> tuple[Any, Any, Any]:
    import pandas as pd
    from scipy import stats

    numeric = [
        column
        for column in master.select_dtypes(include="number").columns
        if column not in {"low_depth", "in_core_cohort"}
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
            series = pd.to_numeric(frame[metric], errors="coerce")
            values = series.dropna()
            if values.empty:
                continue
            overall_rows.append(
                {"dataset": dataset, "metric": metric, **_describe(values, series.isna().sum())}
            )
            for group in groups:
                arrays: dict[str, Any] = {}
                for level, subset in frame.groupby(group, dropna=True):
                    grouped_series = pd.to_numeric(subset[metric], errors="coerce")
                    grouped_values = grouped_series.dropna()
                    if len(grouped_values):
                        grouped_rows.append(
                            {
                                "dataset": dataset,
                                "metric": metric,
                                "group_factor": group,
                                "group": level,
                                **_describe(grouped_values, grouped_series.isna().sum()),
                            }
                        )
                    if len(grouped_values) >= 2:
                        arrays[str(level)] = grouped_values.to_numpy()
                if len(arrays) < 2:
                    continue
                levels = list(arrays)
                h_value, p_value = stats.kruskal(*(arrays[level] for level in levels))
                n_value = sum(len(arrays[level]) for level in levels)
                epsilon = (
                    max(0.0, (h_value - len(levels) + 1) / (n_value - len(levels)))
                    if n_value > len(levels)
                    else 0.0
                )
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
                        rank_biserial = 1 - (2 * u_value) / (
                            len(arrays[left]) * len(arrays[right])
                        )
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


def _asset_path(name: str) -> Path:
    path = Path(__file__).resolve().parent / "assets" / "standard-report" / name
    if not path.is_file():
        raise RuntimeError(f"bundled report asset is missing: {name}")
    return path


def _run_plots(metrics: Any, out_dir: Path, rscript: str) -> list[Path]:
    script_dir = out_dir / "r"
    script_dir.mkdir(parents=True, exist_ok=True)
    for name in ("00_sci_theme.R", "make_figures.R"):
        shutil.copy2(_asset_path(name), script_dir / name)
    if metrics.empty:
        return []
    figures = out_dir / "figures"
    result = subprocess.run(
        [rscript, str(script_dir / "make_figures.R"), str(out_dir / "tables"), str(figures)],
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=900,
        check=False,
    )
    (script_dir / "plot.log").write_text(
        (result.stdout or "") + (result.stderr or ""), encoding="utf-8"
    )
    if result.returncode:
        raise RuntimeError("R figure generation failed")
    return sorted(figures.glob("*.png"))


def _public_text(value: Any) -> str:
    text = str(value)
    return re.sub(r"(?<!\w)/(?:data|home|Users|private|tmp)/[^\s<]+", "[路径已隐藏]", text)


def _format_number(value: Any, column: str = "") -> str:
    if value is None or (isinstance(value, float) and not math.isfinite(value)):
        return "NA"
    if isinstance(value, bool):
        return "是" if value else "否"
    number = _number(value)
    if number is None:
        return _public_text(value)
    lower = column.lower()
    if lower in {"p", "p_adj_bh"}:
        return f"{number:.2e}" if number < 0.0001 else f"{number:.4f}"
    if lower in {"n", "n_missing", "k", "tables"} and abs(number - round(number)) < 1e-6:
        return f"{round(number):,}"
    if any(
        token in lower
        for token in ("reads", "bases", "contig", "cluster", "taxa", "count", "n_")
    ) and abs(number - round(number)) < 1e-6:
        return f"{round(number):,}"
    if any(token in lower for token in ("retention", "q30", "duplication", "fraction", "frac")):
        number = number * 100 if 0 <= number <= 1.5 else number
        return f"{number:.2f}%"
    if abs(number) >= 1000:
        return f"{number:,.2f}"
    if abs(number) < 1:
        return f"{number:.4f}"
    return f"{number:.2f}"


def _table(frame: Any, *, limit: int = 80) -> str:
    if frame is None or frame.empty:
        return '<p class="muted">（无表）</p>'
    safe_columns = [
        column
        for column in frame.columns
        if not any(token in str(column).lower() for token in ("path", "stdout", "stderr", "payload", "json"))
    ]
    view = frame.loc[:, safe_columns].head(limit)
    head = "".join(f"<th>{html.escape(_public_text(column))}</th>" for column in view.columns)
    rows = []
    for _, row in view.iterrows():
        cells = []
        for column, value in row.items():
            rendered = _format_number(value, str(column))
            css = ' class="num"' if _number(value) is not None else ""
            cells.append(f"<td{css}>{html.escape(rendered)}</td>")
        rows.append("<tr>" + "".join(cells) + "</tr>")
    note = (
        f'<p class="muted">显示前 {len(view)} / {len(frame)} 行；完整表见 tables/。</p>'
        if len(frame) > limit
        else ""
    )
    return (
        f'<div class="table-wrap"><table><thead><tr>{head}</tr></thead>'
        f'<tbody>{"".join(rows)}</tbody></table></div>{note}'
    )


def _figure(path: Path) -> str:
    encoded = base64.b64encode(path.read_bytes()).decode("ascii")
    caption = CAPTIONS.get(path.stem, path.stem)
    return (
        f'<figure id="{html.escape(path.stem)}"><img src="data:image/png;base64,{encoded}" '
        f'alt="{html.escape(caption)}"><figcaption><strong>{html.escape(path.stem)}</strong> '
        f'{html.escape(caption)} 绘图脚本 <code>r/make_figures.R</code>。</figcaption></figure>'
    )


def _figure_rows(out_dir: Path, stems: list[str]) -> str:
    blocks = []
    for stem in stems:
        path = out_dir / "figures" / f"{stem}.png"
        if path.is_file():
            blocks.append(_figure(path))
    if not blocks:
        return '<p class="muted">没有满足绘图条件的数据，不生成空图。</p>'
    rows: list[str] = []
    for index in range(0, len(blocks), 2):
        pair = blocks[index : index + 2]
        css = "fig-pair" if len(pair) == 2 else "fig-one"
        rows.append(
            f'<table class="{css}"><tr>'
            + "".join(f"<td>{block}</td>" for block in pair)
            + "</tr></table>"
        )
    return "".join(rows)


def _master_view(master: Any) -> Any:
    preferred = (
        "sample_id",
        "method",
        "host",
        "reads_before_sum",
        "reads_after_fastp_sum",
        "read_retention_fastp_median",
        "q30_after_fastp_median",
        "rrna_retention_r1_median",
        "merge_r1_reads",
        "contigs_ge1kb_n",
        "n_putative_viral_contigs",
        "n_clusters",
        "checkv_completeness_median",
        "low_depth",
    )
    columns = [column for column in preferred if column in master.columns]
    return master.loc[:, columns]


def _metric_sentence(master: Any, column: str, label: str) -> str | None:
    import pandas as pd

    if column not in master.columns:
        return None
    values = pd.to_numeric(master[column], errors="coerce").dropna()
    if values.empty:
        return None
    return (
        f"{label}中位数 {_format_number(values.median(), column)}"
        f"（范围 {_format_number(values.min(), column)} - {_format_number(values.max(), column)}）。"
    )


def _workflow_results(short: str, master: Any, payloads: dict[str, dict[str, Any]]) -> list[str]:
    table_count = sum(bool(rows) for rows in payloads.get(short, {}).get("tables", {}).values())
    results = [f"成功提取 {table_count} 张非空结构化表；本章只解释实际存在的字段。"]
    choices = {
        "W1": (
            ("merge_r1_reads", "合并后 R1 reads"),
            ("read_retention_fastp_median", "fastp 读段保留率"),
            ("q30_after_fastp_median", "fastp 后 Q30"),
            ("rrna_retention_r1_median", "去 rRNA 后 R1 保留率"),
            ("contigs_ge1kb_n", "不短于 1 kb 的 contig 数"),
        ),
        "W2": (
            ("n_putative_viral_contigs", "候选病毒 contig 数"),
            ("n_high_quality", "CheckV High-quality 数"),
            ("n_clusters", "聚类数"),
            ("checkv_completeness_median", "CheckV completeness"),
        ),
        "W3": (
            ("n_species_inferred", "推断宿主物种数"),
            ("host_mean_coverage", "宿主证据 coverage"),
            ("host_mean_identity", "宿主证据 identity"),
        ),
        "W4": (
            ("kraken_unclassified_frac", "Kraken 未分类比例"),
            ("bracken_species_taxa", "Bracken 物种分类单元数"),
        ),
        "W5": (("mapping_rate_pct", "病毒 contig 比对率"), ("max_rpm", "最大病毒 RPM")),
        "W6": (
            ("n_annotated", "已注释对象数"),
            ("n_genes", "注释基因数"),
            ("n_amg", "AMG 数"),
        ),
    }
    for column, label in choices.get(short, ()):
        if sentence := _metric_sentence(master, column, label):
            results.append(sentence)
    return results


def _biology(short: str, master: Any, figure_names: set[str]) -> list[str]:
    import pandas as pd

    notes: list[str] = []
    if short == "W1":
        if "fig01_depth_by_sample" in figure_names and "merge_r1_reads" in master:
            depth = pd.to_numeric(master["merge_r1_reads"], errors="coerce")
            low = master.loc[depth < LOW_DEPTH_READS, "sample_id"].astype(str).tolist()
            notes.append(
                "【fig01_depth_by_sample】低深度样本为 "
                + ("、".join(low) if low else "无")
                + "；下游相对指标应先与测序深度交叉解读。"
            )
        if "q30_after_fastp_median" in master:
            q30 = pd.to_numeric(master["q30_after_fastp_median"], errors="coerce").dropna()
            if not q30.empty:
                notes.append(
                    f"【fig03_w1_q30】本报告实际提取的 fastp 后 Q30 为 {_format_number(q30.median(), 'q30')}；"
                    "该指标说明碱基质量，不单独证明样本具备或缺少目标生物。"
                )
    elif short == "W2":
        if "n_putative_viral_contigs" in master:
            values = pd.to_numeric(master["n_putative_viral_contigs"], errors="coerce").dropna()
            if not values.empty:
                notes.append(
                    f"【fig08_w2_viral_contigs_by_method】实际提取候选病毒 contig 共 {int(values.sum())} 条；"
                    "候选、最佳命中或部分序列均不等同于已确认物种或亚型。"
                )
        notes.append("CheckV completeness 与分类命中只按提取表报告；缺失字段不推断、不补写。")
    elif short == "W3":
        notes.append("宿主结果是流程证据的摘要，不代替样本来源记录或临床鉴定。")
    elif short == "W4":
        notes.append("分类比例受数据库、未分类 reads 与测序深度影响，不等同于绝对丰度。")
    elif short == "W5":
        notes.append("RPM 和 mapping rate 是相对定量；低深度样本可能出现分母效应。")
    elif short == "W6":
        notes.append("功能注释依赖数据库与阈值，只陈述已提取的注释结果。")
    return notes


CSS = """
@page { size: A4 landscape; margin: 10mm; }
:root { --ink:#111; --muted:#555; --line:#c8c8c8; --bg:#f7f7f7; --accent:#1f4e79; }
* { box-sizing: border-box; }
body { margin:0; background:#ececec; color:var(--ink); font:12.5px/1.55 "Helvetica Neue",Helvetica,Arial,"Noto Sans CJK SC",sans-serif; }
main { max-width:1100px; margin:18px auto 48px; background:#fff; padding:28px 32px 48px; overflow-x:hidden; }
h1 { font-size:22px; margin:0 0 8px; font-weight:700; }
h2 { font-size:15px; margin:28px 0 8px; padding-bottom:4px; border-bottom:1.2px solid var(--ink); }
h3 { font-size:13px; margin:16px 0 6px; }
.kicker { letter-spacing:.08em; text-transform:uppercase; font-size:10px; color:var(--accent); font-weight:700; }
.meta,figcaption,.muted { color:var(--muted); }
.notice { background:var(--bg); border-left:3px solid var(--accent); padding:8px 10px; margin:10px 0; }
.warn { border-left-color:#9c3b2e; } .skip { border-left-color:#888; color:var(--muted); }
.table-wrap { overflow-x:auto; max-width:100%; margin:8px 0 14px; border:.6px solid var(--line); }
table { border-collapse:collapse; width:max-content; min-width:100%; font-size:11px; }
th,td { border-top:.6px solid var(--line); border-bottom:.6px solid var(--line); padding:3px 6px; text-align:left; vertical-align:top; }
th { border-top:1.1px solid var(--ink); border-bottom:1.1px solid var(--ink); background:#fff; font-weight:650; white-space:nowrap; }
td.num { font-variant-numeric:tabular-nums; white-space:nowrap; }
figure { margin:0; } figure img { width:100%; max-width:100%; height:auto; display:block; border:.6px solid var(--line); }
figcaption { margin-top:4px; font-size:10.5px; line-height:1.4; }
table.fig-pair { width:100%!important; min-width:0!important; border:none!important; border-collapse:separate; table-layout:fixed; margin:10px 0 16px; page-break-inside:avoid; }
table.fig-pair td { width:50%; border:none!important; background:transparent; padding:0 1.2%; vertical-align:top; }
table.fig-one { width:48%!important; min-width:0!important; margin:10px auto 16px!important; border:none!important; table-layout:fixed; page-break-inside:avoid; }
table.fig-one td { border:none!important; background:transparent; padding:0!important; }
nav a { color:var(--accent); text-decoration:none; } ul { margin:6px 0 10px 18px; } li { margin:3px 0; }
code,pre { font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace; font-size:10.5px; }
pre { background:var(--bg); padding:10px; overflow:auto; max-height:420px; border:.6px solid var(--line); white-space:pre-wrap; }
.tag { display:inline-block; font-size:10px; padding:1px 7px; border:.6px solid var(--line); margin-right:6px; }
footer { margin-top:28px; padding-top:10px; border-top:.6px solid var(--line); font-size:11px; color:var(--muted); }
@media print { body { background:#fff; } main { margin:0; max-width:none; padding:0; } .table-wrap { overflow:visible; border:none; }
table { font-size:8px; width:100%; } th,td { padding:2px 3px; white-space:normal; word-break:normal; overflow-wrap:normal; }
td.num { white-space:nowrap; } pre { max-height:none; font-size:7.5px; }
table.fig-pair { width:100%!important; min-width:0!important; table-layout:fixed!important; }
table.fig-pair img { width:100%!important; max-width:125mm!important; height:auto!important; }
table.fig-one { width:48%!important; min-width:0!important; margin:10px auto!important; } }
"""


def _build_html(
    state: dict[str, Any],
    coverage: Any,
    master: Any,
    stats_frames: tuple[Any, Any, Any],
    groups: list[str],
    group_notes: list[str],
    payloads: dict[str, dict[str, Any]],
    out_dir: Path,
) -> str:
    extracted = [row["workflow"] for row in coverage.to_dict("records") if row["status"] == "extracted"]
    skipped = [row["workflow"] for row in coverage.to_dict("records") if row["status"] != "extracted"]
    figures = {path.stem for path in (out_dir / "figures").glob("*.png")}
    run_title = _public_text(state.get("title") or state.get("run_id") or "分析任务")
    low = (
        master.loc[master["low_depth"].fillna(False), "sample_id"].astype(str).tolist()
        if not master.empty
        else []
    )
    sections: list[tuple[str, str, str]] = []
    exec_results = (
        f"<li>核心队列 {len(master)} 个样本。</li>"
        f"<li>已提取流程：{html.escape(', '.join(extracted) or '无')}。</li>"
        f"<li>未运行并跳过：{html.escape(', '.join(skipped) or '无')}。</li>"
        f"<li>分组因子：{html.escape(', '.join(groups) or '无（仅总体描述）')}。</li>"
        f"<li>低深度样本（合并 R1 &lt; 5×10<sup>6</sup>）：{html.escape('、'.join(low) or '无或未提取深度')}。</li>"
    )
    sections.append(
        (
            "exec",
            "执行摘要",
            "".join(
                (
                    f"<h3>结果</h3><ul>{exec_results}</ul>",
                    '<h3>生物学意义</h3><ul><li>先检查测序深度和流程覆盖，再解读候选病原、宿主、微生物谱、相对定量与注释结果。</li>',
                    '<li>本批次为单样本时只做描述统计；不进行没有信息量的组间推论。</li></ul>',
                )
            ),
        )
    )
    sections.append(
        (
            "cohort",
            "样本 Meta 与提取覆盖",
            "<h3>结果</h3><ul>"
            + "".join(f"<li>{html.escape(note)}</li>" for note in group_notes)
            + "</ul><h3>图</h3>"
            + _figure_rows(out_dir, ["fig01_depth_by_sample"]),
        )
    )
    for short, title in WORKFLOWS.values():
        if short not in payloads:
            content = '<div class="notice skip">本流程未运行或没有完成，整章跳过；不写成分析失败。</div>'
        else:
            results = "".join(
                f"<li>{html.escape(sentence)}</li>"
                for sentence in _workflow_results(short, master, payloads)
            )
            biology = "".join(
                f"<li>{html.escape(sentence)}</li>" for sentence in _biology(short, master, figures)
            )
            content = (
                f"<h3>结果</h3><ul>{results}</ul><h3>图</h3>"
                + _figure_rows(out_dir, WORKFLOW_FIGURES[short])
                + f"<h3>生物学意义</h3><ul>{biology}</ul>"
            )
        sections.append((short.lower(), f"{short} {title}", content))
    if len(extracted) >= 2:
        sections.append(
            (
                "cross",
                "跨流程衔接",
                '<h3>结果</h3><ul><li>下游候选数、coverage、identity、CheckV 与分类结果应先对照 W1 测序深度。</li>'
                + '<li>相对丰度或候选数量不被改写为绝对载量或已确认亚型。</li></ul>'
                + '<h3>生物学意义</h3><ul><li>单样本结果用于描述和生成后续验证假设，不支持群组层面的因果推断。</li></ul>',
            )
        )
    toc = []
    body = []
    for number, (section_id, title, content) in enumerate(sections, 1):
        toc.append(f'<a href="#{section_id}">{number}. {html.escape(title)}</a>')
        body.append(f'<section id="{section_id}"><h2>{number}. {html.escape(title)}</h2>{content}</section>')
    base = len(sections)
    script_path = out_dir / "r" / "make_figures.R"
    script = script_path.read_text(encoding="utf-8")[:20_000] if script_path.is_file() else ""
    body.append(
        f'<section id="tables"><h2>{base + 1}. 结构化表</h2><h3>提取覆盖</h3>{_table(coverage)}'
        f'<h3>核心样本主表</h3>{_table(_master_view(master))}'
        '<p class="muted">完整样本级字段见 tables/master_samples.tsv。</p>'
        f'<h3>总体描述统计</h3>{_table(stats_frames[0])}'
        f'<h3>分组描述统计</h3>{_table(stats_frames[1])}<h3>组间检验</h3>{_table(stats_frames[2])}</section>'
    )
    toc.append(f'<a href="#tables">{base + 1}. 结构化表</a>')
    body.append(
        f'<section id="methods"><h2>{base + 2}. 方法学摘要</h2><ul>'
        '<li>数据仅来自各 W1-W6 的 extract_results.py 结构化输出；缺失流程与字段保持缺失。</li>'
        '<li>描述统计报告 n、missing、mean、SD、median、Q25、Q75、min 与 max。</li>'
        '<li>仅在至少两个水平且至少两组 n≥2 时运行 Kruskal-Wallis；成对 Mann-Whitney p 值使用 BH 校正，并报告效应量。</li>'
        '<li>图由随 Agent worker 部署的 R/ggplot2 脚本生成；单样本退化为点图或柱图，不绘制无意义箱线。</li>'
        '</ul></section>'
    )
    toc.append(f'<a href="#methods">{base + 2}. 方法学摘要</a>')
    body.append(
        f'<section id="r-appendix"><h2>{base + 3}. 附录：受限 R 绘图脚本</h2>'
        '<p class="muted">展示随报告部署脚本的前 20,000 字符；完整副本见 r/make_figures.R。</p>'
        f'<pre>{html.escape(script)}</pre></section>'
    )
    toc.append(f'<a href="#r-appendix">{base + 3}. R 附录</a>')
    return f'''<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{html.escape(run_title)} · 标准交付报告</title><style>{CSS}</style></head><body><main>
<header><div class="kicker">BIOAGENT STANDARD DELIVERY · V2</div><h1>{html.escape(run_title)} 工作流完成后分析报告</h1>
<p class="meta">生成 {html.escape(_now())} · 指标口径对齐各流程结构化提取器 · 图已嵌入本文件</p>
<p><span class="tag">总体分析</span><span class="tag">缺失则跳过</span><span class="tag">SCI 风格 ggplot2</span><span class="tag">自包含 HTML</span><span class="tag">不构成诊断</span></p></header>
<nav class="muted">{' · '.join(toc)}</nav>
<div class="notice warn">缺失流程或字段不判为失败。数据库最佳命中不是正式定种或亚型确认。低深度样本的 RPM、物种数和 contig 数不可单独解释为生物学差异。本 HTML 图为内嵌 PNG，复制单文件即可打开。</div>
{''.join(body)}
<footer>BioAgent 标准交付报告 · {TEMPLATE_VERSION} · 结果仅供科研参考，不构成临床诊断；关键发现应结合原始证据与独立实验复核。</footer>
</main></body></html>'''


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
    return {
        "schema_version": SCHEMA_VERSION,
        "template_version": TEMPLATE_VERSION,
        "generated_at": _now(),
        "files": files,
    }


def _publish(staging: Path, output: Path) -> None:
    output.mkdir(parents=True, exist_ok=True)
    backup = Path(tempfile.mkdtemp(prefix=".delivery-backup-", dir=output))
    published: list[str] = []
    try:
        for name in OWNED_ENTRIES:
            destination = output / name
            if destination.exists():
                os.replace(destination, backup / name)
            source = staging / name
            if source.exists():
                os.replace(source, destination)
                published.append(name)
        shutil.rmtree(backup)
    except Exception:
        for name in reversed(published):
            destination = output / name
            if destination.is_dir():
                shutil.rmtree(destination)
            else:
                destination.unlink(missing_ok=True)
        for previous in backup.iterdir():
            os.replace(previous, output / previous.name)
        shutil.rmtree(backup, ignore_errors=True)
        raise


def generate(state_path: Path) -> dict[str, Any]:
    import pandas as pd

    state = json.loads(state_path.read_text(encoding="utf-8"))
    run_dir = state_path.parent.parent.resolve()
    if state_path.resolve() != run_dir / ".bioagent" / "state.json":
        raise ValueError("state path does not match run directory")
    output = run_dir / "reports" / "standard"
    output.mkdir(parents=True, exist_ok=True)
    state["standard_delivery"] = {
        "schema_version": SCHEMA_VERSION,
        "template_version": TEMPLATE_VERSION,
        "status": "pending",
        "generated_at": _now(),
        "artifacts": {},
    }
    _write_json(state_path, state)
    staging = Path(tempfile.mkdtemp(prefix=".delivery-staging-", dir=output))
    try:
        (staging / "tables").mkdir(parents=True, exist_ok=True)
        python = os.getenv(
            "BIOAGENT_REPORT_PYTHON", "/home/liufangzhou/.pixi/bin/bioagent-report-python"
        )
        rscript = os.getenv("BIOAGENT_RSCRIPT", "/home/liufangzhou/.pixi/bin/bioagent-rscript")
        weasyprint = os.getenv(
            "BIOAGENT_WEASYPRINT", "/home/liufangzhou/.pixi/bin/bioagent-weasyprint"
        )
        payloads, coverage_rows = _run_extractors(state, staging, python)
        coverage = pd.DataFrame(coverage_rows)
        coverage.to_csv(staging / "tables" / "coverage.tsv", sep="\t", index=False)
        master, metrics = _build_tables(payloads, state, staging)
        groups, group_notes = _usable_groups(master)
        stats_frames = _write_stats(master, groups, staging)
        _run_plots(metrics, staging, rscript)
        document = _build_html(
            state, coverage, master, stats_frames, groups, group_notes, payloads, staging
        )
        html_path = staging / "report.html"
        pdf_path = staging / "report.pdf"
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
        _write_json(staging / "manifest.json", _manifest(staging))
        _publish(staging, output)
    finally:
        shutil.rmtree(staging, ignore_errors=True)

    artifacts = {}
    for format_name, filename, media_type in (
        ("html", "report.html", "text/html; charset=utf-8"),
        ("pdf", "report.pdf", "application/pdf"),
    ):
        path = output / filename
        artifacts[format_name] = {
            "path": path.relative_to(run_dir).as_posix(),
            "bytes": path.stat().st_size,
            "sha256": _sha256(path),
            "media_type": media_type,
        }
    result = {
        "schema_version": SCHEMA_VERSION,
        "template_version": TEMPLATE_VERSION,
        "status": "ready",
        "generated_at": _now(),
        "artifacts": artifacts,
    }
    state = json.loads(state_path.read_text(encoding="utf-8"))
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
                "template_version": TEMPLATE_VERSION,
                "status": "failed",
                "generated_at": _now(),
                "error": _safe_error(error),
                "artifacts": {},
            }
            _write_json(state_path, state)
        except Exception as state_error:  # noqa: BLE001
            print(f"Could not record report failure: {_safe_error(state_error)}", file=sys.stderr)
        print(f"Standard delivery report failed: {_safe_error(error)}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

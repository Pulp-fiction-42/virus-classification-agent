"""Static public analysis stages and artifact contracts.

The catalog is the single source of truth shared by planning, execution and
progress reporting.  Workflow identifiers and Snakemake rules stay internal;
users select stable biological stages and typed artifacts instead.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from tool_library import public_tool_catalog
from workflow_skills import public_workflow_skill_catalog

PAIRED_READ_TYPES = {
    "raw_paired_reads",
    "qc_paired_reads",
    "clean_run_paired_reads",
    "clean_sample_paired_reads",
}


@dataclass(frozen=True)
class ArtifactSpec:
    id: str
    required_files: tuple[str, ...]
    identity_fields: tuple[str, ...] = ("sample_id",)
    collection: bool = True
    path_kinds: tuple[tuple[str, str], ...] = ()

    def kind(self, role: str) -> str:
        return dict(self.path_kinds).get(role, "file")


@dataclass(frozen=True)
class StageSpec:
    id: str
    label: str
    workflow_id: str
    requirements: tuple[tuple[str, ...], ...]
    produces: str
    rules: tuple[str, ...] | Callable[[dict[str, Any], dict[str, str]], tuple[str, ...]]

    def selected_rules(
        self, options: dict[str, Any], bindings: dict[str, str]
    ) -> tuple[str, ...]:
        return self.rules(options, bindings) if callable(self.rules) else self.rules


@dataclass(frozen=True)
class ParameterSpec:
    id: str
    label: str
    kind: str
    minimum: int | None
    maximum: int | None
    unit: str | None
    description: str
    flag: str


@dataclass(frozen=True)
class CapabilityManifest:
    id: str
    version: int
    stage_id: str
    tool_id: str
    label: str
    validation_status: str
    adapter_version: int
    impact: str
    software_dependencies: tuple[str, ...]
    reference_dependencies: tuple[str, ...]
    parser: str
    resources: tuple[tuple[str, int], ...]
    parameters: tuple[ParameterSpec, ...]
    presets: tuple[tuple[str, tuple[tuple[str, int | bool], ...]], ...]


PUBLIC_DELIVERABLES: dict[str, dict[str, str]] = {
    "read_qc": {
        "label": "仅序列质控",
        "description": "仅执行 fastp，输出质控后双端 reads 与质控报告",
        "group": "workflow1",
    },
    "rrna_filter": {
        "label": "质控与去 rRNA",
        "description": "执行 fastp 与 rRNA 去除，输出 clean reads",
        "group": "workflow1",
    },
    "sample_merge": {
        "label": "质控、去 rRNA 与样本合并",
        "description": "完成 run 级清理并合并同一样本的技术重复",
        "group": "workflow1",
    },
    "assembly": {
        "label": "质控与序列组装",
        "description": "完成上游清理、样本合并与 MEGAHIT 组装",
        "group": "workflow1",
    },
    "contig_filter": {
        "label": "质控与组装",
        "description": "完成 reads 质控、去 rRNA、组装与 contig 过滤",
        "group": "primary",
    },
    "virus_summary": {
        "label": "病毒识别",
        "description": "搜索病毒候选并汇总分类证据",
        "group": "primary",
    },
    "host_summary": {
        "label": "宿主识别",
        "description": "完成宿主比对、覆盖度、BLAST 与整合",
        "group": "primary",
    },
    "microbe_summary": {
        "label": "微生物谱",
        "description": "生成 Kraken2、Bracken 与分类摘要",
        "group": "primary",
    },
    "virus_rpm": {
        "label": "病毒定量",
        "description": "计算病毒覆盖度与 RPM 丰度",
        "group": "primary",
    },
    "annotation_delivery": {
        "label": "病毒注释",
        "description": "完成质量、结构与功能注释交付",
        "group": "primary",
    },
}

PUBLIC_DELIVERABLE_IDS = tuple(PUBLIC_DELIVERABLES)
FASTP_CAPABILITY_ID = "workflow1.read_qc.fastp"
FASTP_MANIFEST_VERSION = 1
WORKFLOW_ADAPTER_VERSION = 1
FASTP_PARAMETERS = (
    ParameterSpec(
        "length_required",
        "最短保留长度",
        "integer",
        1,
        1000,
        "bp",
        "短于该长度的 reads 会被丢弃；提高此值会减少保留 reads 数量。",
        "--length_required",
    ),
    ParameterSpec(
        "qualified_quality_phred",
        "合格碱基 Phred",
        "integer",
        0,
        40,
        "Phred",
        "定义单个碱基达到合格质量的 Phred 阈值。",
        "--qualified_quality_phred",
    ),
    ParameterSpec(
        "unqualified_percent_limit",
        "不合格碱基比例上限",
        "integer",
        0,
        100,
        "%",
        "超过该比例的不合格碱基会导致 read 或 read pair 被丢弃。",
        "--unqualified_percent_limit",
    ),
    ParameterSpec(
        "n_base_limit",
        "N 碱基上限",
        "integer",
        0,
        1000,
        "bases",
        "N 碱基数超过该值的 read 或 read pair 会被丢弃。",
        "--n_base_limit",
    ),
    ParameterSpec(
        "average_qual",
        "平均质量下限",
        "integer",
        0,
        40,
        "Phred",
        "平均质量低于该值的 read 或 read pair 会被丢弃；0 表示不限制。",
        "--average_qual",
    ),
    ParameterSpec(
        "detect_adapter_for_pe",
        "双端 adapter 自动检测",
        "boolean",
        None,
        None,
        None,
        "启用双端数据 adapter 自动检测。",
        "--detect_adapter_for_pe",
    ),
)
FASTP_MANIFEST = CapabilityManifest(
    id=FASTP_CAPABILITY_ID,
    version=FASTP_MANIFEST_VERSION,
    stage_id="read_qc",
    tool_id="fastp",
    label="fastp 序列质控",
    validation_status="validated",
    adapter_version=WORKFLOW_ADAPTER_VERSION,
    impact="参数变化可能改变保留 reads 的数量和后续分析输入；不会修改原始 FASTQ。",
    software_dependencies=("snakemake", "fastp"),
    reference_dependencies=(),
    parser="analysis_runner.fastp",
    resources=(("threads", 8),),
    parameters=FASTP_PARAMETERS,
    presets=(("workflow_default", ()),),
)
RRNA_FILTER_MANIFEST = CapabilityManifest(
    id="workflow1.rrna_filter.bowtie2",
    version=1,
    stage_id="rrna_filter",
    tool_id="bowtie2",
    label="Bowtie2 去除 rRNA",
    validation_status="dry_run_validated",
    adapter_version=WORKFLOW_ADAPTER_VERSION,
    impact="去除与已登记 rRNA 索引比对的 reads；不等同于宿主序列去除或宿主识别。",
    software_dependencies=("snakemake", "bowtie2", "samtools"),
    reference_dependencies=("workflow1.rrna_bowtie2",),
    parser="analysis_runner.rrna_filter",
    resources=(("threads", 8),),
    parameters=(),
    presets=(("workflow_default", ()),),
)
SAMPLE_MERGE_MANIFEST = CapabilityManifest(
    id="workflow1.sample_merge.seqkit",
    version=1,
    stage_id="sample_merge",
    tool_id="seqkit",
    label="SeqKit 技术重复合并",
    validation_status="code_exists",
    adapter_version=WORKFLOW_ADAPTER_VERSION,
    impact="按已确认的 run 到 sample 映射合并技术重复，不改变原始 FASTQ。",
    software_dependencies=("snakemake", "seqkit"),
    reference_dependencies=(),
    parser="analysis_runner.sample_merge",
    resources=(("threads", 1),),
    parameters=(),
    presets=(("workflow_default", ()),),
)
ASSEMBLY_MANIFEST = CapabilityManifest(
    id="workflow1.assembly.megahit",
    version=1,
    stage_id="assembly",
    tool_id="megahit",
    label="MEGAHIT 序列组装",
    validation_status="code_exists",
    adapter_version=WORKFLOW_ADAPTER_VERSION,
    impact="从合并后的 clean reads 生成组装 contigs。",
    software_dependencies=("snakemake", "megahit"),
    reference_dependencies=(),
    parser="analysis_runner.assembly",
    resources=(("threads", 12), ("memory_mb", 20000)),
    parameters=(),
    presets=(("workflow_default", ()),),
)
CONTIG_FILTER_MANIFEST = CapabilityManifest(
    id="workflow1.contig_filter.seqkit",
    version=1,
    stage_id="contig_filter",
    tool_id="seqkit",
    label="SeqKit Contig 重命名与过滤",
    validation_status="code_exists",
    adapter_version=WORKFLOW_ADAPTER_VERSION,
    impact="重命名并过滤组装 contigs，生成下游分析使用的固定 artifact。",
    software_dependencies=("snakemake", "seqkit"),
    reference_dependencies=(),
    parser="analysis_runner.contig_filter",
    resources=(("threads", 1),),
    parameters=(),
    presets=(("workflow_default", ()),),
)
VIRUS_SEQUENCE_PREP_MANIFEST = CapabilityManifest(
    id="workflow2.virus_sequence_prep.toolchain",
    version=1,
    stage_id="virus_sequence_prep",
    tool_id="seqkit_getorf",
    label="病毒识别序列准备工具链",
    validation_status="dry_run_validated",
    adapter_version=WORKFLOW_ADAPTER_VERSION,
    impact="统一 contig 标识并预测 ORF，为病毒候选搜索准备核酸与蛋白序列。",
    software_dependencies=("snakemake", "seqkit", "getorf", "python"),
    reference_dependencies=(),
    parser="analysis_runner.virus_sequence_prep",
    resources=(("threads", 1),),
    parameters=(),
    presets=(("workflow_default", ()),),
)
VIRUS_CANDIDATE_SEARCH_MANIFEST = CapabilityManifest(
    id="workflow2.virus_candidate_search.hmmsearch",
    version=1,
    stage_id="virus_candidate_search",
    tool_id="hmmsearch",
    label="HMM 病毒候选搜索",
    validation_status="dry_run_validated",
    adapter_version=WORKFLOW_ADAPTER_VERSION,
    impact="按 RNA／DNA 病毒类型使用已登记 HMM 资料搜索候选序列。",
    software_dependencies=("snakemake", "hmmsearch", "python"),
    reference_dependencies=(
        "workflow2.rdrpscan_hmm",
        "workflow2.rvmt_rdrpscan_hmm",
        "workflow2.dna_virus_marker_hmm",
    ),
    parser="analysis_runner.virus_candidate_search",
    resources=(("threads", 6),),
    parameters=(),
    presets=(("workflow_default", ()),),
)
VIRUS_CANDIDATE_EXTRACT_MANIFEST = CapabilityManifest(
    id="workflow2.virus_candidate_extract.seqkit",
    version=1,
    stage_id="virus_candidate_extract",
    tool_id="seqkit",
    label="病毒候选序列提取",
    validation_status="dry_run_validated",
    adapter_version=WORKFLOW_ADAPTER_VERSION,
    impact="按 HMM 搜索结果提取候选 contig、蛋白及覆盖度资料。",
    software_dependencies=("snakemake", "seqkit", "python"),
    reference_dependencies=(),
    parser="analysis_runner.virus_candidate_extract",
    resources=(("threads", 1),),
    parameters=(),
    presets=(("workflow_default", ()),),
)
VIRUS_EVIDENCE_MANIFEST = CapabilityManifest(
    id="workflow2.virus_evidence.toolchain",
    version=1,
    stage_id="virus_evidence",
    tool_id="virus_evidence_toolchain",
    label="病毒候选证据分析工具链",
    validation_status="dry_run_validated",
    adapter_version=WORKFLOW_ADAPTER_VERSION,
    impact="生成聚类、序列质量、NT／NR、病毒宿主库与 RdRpScan 等候选证据。",
    software_dependencies=(
        "snakemake",
        "mmseqs",
        "checkv",
        "blastn",
        "blastx",
        "diamond",
        "hmmsearch",
        "python",
    ),
    reference_dependencies=(
        "workflow2.checkv_db",
        "workflow2.nt",
        "workflow2.nr",
        "workflow2.virus_host_db",
        "workflow2.rdrpscan_hmm",
    ),
    parser="analysis_runner.virus_evidence",
    resources=(("threads", 24),),
    parameters=(),
    presets=(("workflow_default", ()),),
)
VIRUS_SUMMARY_MANIFEST = CapabilityManifest(
    id="workflow2.virus_summary.python",
    version=1,
    stage_id="virus_summary",
    tool_id="python",
    label="病毒识别结果汇总",
    validation_status="dry_run_validated",
    adapter_version=WORKFLOW_ADAPTER_VERSION,
    impact="汇总 Workflow 2 候选表；工程解析不等同于病毒分类结论或专家审核。",
    software_dependencies=("snakemake", "python"),
    reference_dependencies=(),
    parser="candidate_evidence.workflow2",
    resources=(("threads", 1),),
    parameters=(),
    presets=(("workflow_default", ()),),
)
HOST_MAPPING_MANIFEST = CapabilityManifest(
    id="workflow3.host_mapping.bowtie2",
    version=1,
    stage_id="host_mapping",
    tool_id="bowtie2",
    label="Bowtie2 宿主标记序列比对",
    validation_status="dry_run_validated",
    adapter_version=WORKFLOW_ADAPTER_VERSION,
    impact="将 clean reads 比对到已登记 EukaMarker 索引；宿主识别不等同于宿主序列去除。",
    software_dependencies=("snakemake", "bowtie2", "samtools", "perl"),
    reference_dependencies=("workflow3.eukamarker_bowtie2",),
    parser="analysis_runner.host_mapping",
    resources=(("threads", 12),),
    parameters=(),
    presets=(("workflow_default", ()),),
)
HOST_COVERAGE_MANIFEST = CapabilityManifest(
    id="workflow3.host_coverage.samtools",
    version=1,
    stage_id="host_coverage",
    tool_id="samtools",
    label="Samtools 宿主标记覆盖度",
    validation_status="dry_run_validated",
    adapter_version=WORKFLOW_ADAPTER_VERSION,
    impact="从宿主标记比对结果计算覆盖度，不单独形成宿主判定。",
    software_dependencies=("snakemake", "samtools", "awk", "sed"),
    reference_dependencies=(),
    parser="analysis_runner.host_coverage",
    resources=(("threads", 1),),
    parameters=(),
    presets=(("workflow_default", ()),),
)
HOST_BLAST_MANIFEST = CapabilityManifest(
    id="workflow3.host_blast.blastn",
    version=1,
    stage_id="host_blast",
    tool_id="blastn",
    label="BLASTn 宿主标记搜索",
    validation_status="dry_run_validated",
    adapter_version=WORKFLOW_ADAPTER_VERSION,
    impact="将组装 contig 与已登记 EukaMarker BLAST 数据库比对，作为宿主证据的一部分。",
    software_dependencies=("snakemake", "blastn", "sed"),
    reference_dependencies=("workflow3.eukamarker_blastdb",),
    parser="analysis_runner.host_blast",
    resources=(("threads", 6),),
    parameters=(),
    presets=(("workflow_default", ()),),
)
HOST_INTEGRATE_MANIFEST = CapabilityManifest(
    id="workflow3.host_integrate.posix",
    version=1,
    stage_id="host_integrate",
    tool_id="posix_text_tools",
    label="宿主标记证据整合",
    validation_status="dry_run_validated",
    adapter_version=WORKFLOW_ADAPTER_VERSION,
    impact="按 Workflow 3 固定过滤条件整合覆盖度与 BLAST 结果；工程汇总不等同于专家审核。",
    software_dependencies=("snakemake", "grep", "awk"),
    reference_dependencies=(),
    parser="analysis_runner.host_integrate",
    resources=(("threads", 1),),
    parameters=(),
    presets=(("workflow_default", ()),),
)
HOST_SUMMARY_MANIFEST = CapabilityManifest(
    id="workflow3.host_summary.python",
    version=1,
    stage_id="host_summary",
    tool_id="python",
    label="宿主识别结果汇总",
    validation_status="dry_run_validated",
    adapter_version=WORKFLOW_ADAPTER_VERSION,
    impact="使用已登记 taxonomy 资料生成宿主识别汇总；当前只完成 DAG dry-run。",
    software_dependencies=("snakemake", "python", "result_summarize_V3.py"),
    reference_dependencies=(
        "workflow3.eukamarker_taxonomy",
        "workflow3.eukamarker_taxonomy_chinese",
    ),
    parser="analysis_runner.host_summary",
    resources=(("threads", 1),),
    parameters=(),
    presets=(("workflow_default", ()),),
)
MICROBE_CLASSIFY_MANIFEST = CapabilityManifest(
    id="workflow4.microbe_classify.kraken2",
    version=1,
    stage_id="microbe_classify",
    tool_id="kraken2",
    label="Kraken2 微生物快速分类",
    validation_status="dry_run_validated",
    adapter_version=WORKFLOW_ADAPTER_VERSION,
    impact="将 clean reads 与已登记 Kraken2 数据库比对，生成分类报告；工程分类不等同于专家审核。",
    software_dependencies=("snakemake", "kraken2"),
    reference_dependencies=("workflow4.kraken2_standard_20240112",),
    parser="analysis_runner.microbe_classify",
    resources=(("threads", 12),),
    parameters=(),
    presets=(("workflow_default", ()),),
)
MICROBE_ABUNDANCE_MANIFEST = CapabilityManifest(
    id="workflow4.microbe_abundance.bracken",
    version=1,
    stage_id="microbe_abundance",
    tool_id="bracken",
    label="Bracken 微生物丰度估计",
    validation_status="dry_run_validated",
    adapter_version=WORKFLOW_ADAPTER_VERSION,
    impact="基于 Kraken2 报告和同一数据库的 k-mer 分布估计科、属、种丰度。",
    software_dependencies=("snakemake", "bracken"),
    reference_dependencies=("workflow4.kraken2_standard_20240112",),
    parser="analysis_runner.microbe_abundance",
    resources=(("threads", 8),),
    parameters=(),
    presets=(("workflow_default", ()),),
)
MICROBE_SUMMARY_MANIFEST = CapabilityManifest(
    id="workflow4.microbe_summary.bracken",
    version=1,
    stage_id="microbe_summary",
    tool_id="combine_bracken_outputs",
    label="Bracken 多样本结果汇总",
    validation_status="dry_run_validated",
    adapter_version=WORKFLOW_ADAPTER_VERSION,
    impact="合并已完成的 Bracken 科、属、种结果，不新增分类或丰度证据。",
    software_dependencies=(
        "snakemake",
        "python",
        "combine_bracken_outputs.py",
        "head",
        "tail",
        "sort",
    ),
    reference_dependencies=(),
    parser="analysis_runner.microbe_summary",
    resources=(("threads", 1),),
    parameters=(),
    presets=(("workflow_default", ()),),
)
VIRUS_READ_MAPPING_MANIFEST = CapabilityManifest(
    id="workflow5.virus_read_mapping.bowtie2",
    version=1,
    stage_id="virus_read_mapping",
    tool_id="bowtie2_samtools",
    label="Bowtie2／Samtools 病毒序列比对",
    validation_status="dry_run_validated",
    adapter_version=WORKFLOW_ADAPTER_VERSION,
    impact="从本次执行输入的病毒序列建立 run-local 索引并比对 clean reads；输入序列不是全局参考 release。",
    software_dependencies=(
        "snakemake",
        "bowtie2-build",
        "bowtie2",
        "samtools",
        "zcat",
        "awk",
        "sed",
    ),
    reference_dependencies=(),
    parser="analysis_runner.virus_read_mapping",
    resources=(("threads", 4),),
    parameters=(),
    presets=(("workflow_default", ()),),
)
VIRUS_COVERAGE_MANIFEST = CapabilityManifest(
    id="workflow5.virus_coverage.posix",
    version=1,
    stage_id="virus_coverage",
    tool_id="posix_text_tools",
    label="病毒覆盖度合并与筛选",
    validation_status="dry_run_validated",
    adapter_version=WORKFLOW_ADAPTER_VERSION,
    impact="按 Workflow 5 固定阈值合并并筛选 Samtools coverage 结果。",
    software_dependencies=("snakemake", "awk", "cat"),
    reference_dependencies=(),
    parser="analysis_runner.virus_coverage",
    resources=(("threads", 1),),
    parameters=(),
    presets=(("workflow_default", ()),),
)
VIRUS_RPM_MANIFEST = CapabilityManifest(
    id="workflow5.virus_rpm.posix",
    version=1,
    stage_id="virus_rpm",
    tool_id="posix_text_tools",
    label="病毒 RPM 丰度汇总",
    validation_status="dry_run_validated",
    adapter_version=WORKFLOW_ADAPTER_VERSION,
    impact="使用 clean read 数量与筛选后的比对计数计算并排序 RPM。",
    software_dependencies=("snakemake", "awk", "head", "tail", "sort"),
    reference_dependencies=(),
    parser="analysis_runner.virus_rpm",
    resources=(("threads", 1),),
    parameters=(),
    presets=(("workflow_default", ()),),
)
VIRUS_QUALITY_MANIFEST = CapabilityManifest(
    id="workflow6.virus_quality.checkv",
    version=1,
    stage_id="virus_quality",
    tool_id="checkv",
    label="CheckV 病毒序列质量筛选",
    validation_status="dry_run_validated",
    adapter_version=WORKFLOW_ADAPTER_VERSION,
    impact="运行 CheckV 并按 Workflow 6 固定质量和 taxonomy 条件筛选候选病毒序列。",
    software_dependencies=("snakemake", "checkv", "python"),
    reference_dependencies=("workflow6.checkv_db",),
    parser="analysis_runner.virus_quality",
    resources=(("threads", 6),),
    parameters=(),
    presets=(("workflow_default", ()),),
)
ANNOTATION_STRATEGY_MANIFEST = CapabilityManifest(
    id="workflow6.annotation_strategy.python",
    version=1,
    stage_id="annotation_strategy",
    tool_id="python",
    label="病毒结构注释策略分流",
    validation_status="dry_run_validated",
    adapter_version=WORKFLOW_ADAPTER_VERSION,
    impact="依据 run-local taxonomy、序列长度和随 workflow 发布的模型映射选择 VIGOR 或 Prodigal 路径。",
    software_dependencies=("snakemake", "python"),
    reference_dependencies=(),
    parser="analysis_runner.annotation_strategy",
    resources=(("threads", 1),),
    parameters=(),
    presets=(("workflow_default", ()),),
)
STRUCTURE_ANNOTATION_MANIFEST = CapabilityManifest(
    id="workflow6.structure_annotation.toolchain",
    version=1,
    stage_id="structure_annotation",
    tool_id="vigor_prodigal",
    label="VIGOR／Prodigal 病毒结构注释",
    validation_status="dry_run_validated",
    adapter_version=WORKFLOW_ADAPTER_VERSION,
    impact="按已选择策略运行 VIGOR、Prodigal 与固定 fallback 脚本并合并预测蛋白。",
    software_dependencies=("snakemake", "python", "vigor4", "prodigal"),
    reference_dependencies=("workflow6.vigor_reference_db",),
    parser="analysis_runner.structure_annotation",
    resources=(("threads", 1),),
    parameters=(),
    presets=(("workflow_default", ()),),
)
FUNCTIONAL_ANNOTATION_MANIFEST = CapabilityManifest(
    id="workflow6.functional_annotation.dramv",
    version=1,
    stage_id="functional_annotation",
    tool_id="dramv",
    label="DRAM-v 病毒功能注释",
    validation_status="dry_run_validated",
    adapter_version=WORKFLOW_ADAPTER_VERSION,
    impact="对合并后的预测蛋白执行 DRAM-v 功能注释；当前配置允许工具不可用时生成显式 stub。",
    software_dependencies=("snakemake", "python", "DRAM.py"),
    reference_dependencies=("workflow6.dramv_data", "workflow6.dram_config"),
    parser="analysis_runner.functional_annotation",
    resources=(("threads", 6),),
    parameters=(),
    presets=(("workflow_default", ()),),
)
ANNOTATION_DELIVERY_MANIFEST = CapabilityManifest(
    id="workflow6.annotation_delivery.python",
    version=1,
    stage_id="annotation_delivery",
    tool_id="python",
    label="病毒注释结果汇总",
    validation_status="dry_run_validated",
    adapter_version=WORKFLOW_ADAPTER_VERSION,
    impact="生成注释汇总、一致性报告、逐序列 GenBank 与待复核候选，不代表专家审核完成。",
    software_dependencies=("snakemake", "python"),
    reference_dependencies=("workflow6.reference_genbank",),
    parser="analysis_runner.annotation_delivery",
    resources=(("threads", 1),),
    parameters=(),
    presets=(("workflow_default", ()),),
)
CAPABILITY_MANIFESTS = {
    manifest.id: manifest
    for manifest in (
        FASTP_MANIFEST,
        RRNA_FILTER_MANIFEST,
        SAMPLE_MERGE_MANIFEST,
        ASSEMBLY_MANIFEST,
        CONTIG_FILTER_MANIFEST,
        VIRUS_SEQUENCE_PREP_MANIFEST,
        VIRUS_CANDIDATE_SEARCH_MANIFEST,
        VIRUS_CANDIDATE_EXTRACT_MANIFEST,
        VIRUS_EVIDENCE_MANIFEST,
        VIRUS_SUMMARY_MANIFEST,
        HOST_MAPPING_MANIFEST,
        HOST_COVERAGE_MANIFEST,
        HOST_BLAST_MANIFEST,
        HOST_INTEGRATE_MANIFEST,
        HOST_SUMMARY_MANIFEST,
        MICROBE_CLASSIFY_MANIFEST,
        MICROBE_ABUNDANCE_MANIFEST,
        MICROBE_SUMMARY_MANIFEST,
        VIRUS_READ_MAPPING_MANIFEST,
        VIRUS_COVERAGE_MANIFEST,
        VIRUS_RPM_MANIFEST,
        VIRUS_QUALITY_MANIFEST,
        ANNOTATION_STRATEGY_MANIFEST,
        STRUCTURE_ANNOTATION_MANIFEST,
        FUNCTIONAL_ANNOTATION_MANIFEST,
        ANNOTATION_DELIVERY_MANIFEST,
    )
}


def _virus_prep_rules(
    _options: dict[str, Any], bindings: dict[str, str]
) -> tuple[str, ...]:
    contigs_type = bindings.get("contigs")
    prefix = () if contigs_type == "filtered_contigs" else ("rename_contigs",)
    return (*prefix, "get_orfs", "merge_orfs", "merge_contigs")


def _virus_search_rules(
    options: dict[str, Any], _bindings: dict[str, str]
) -> tuple[str, ...]:
    viral_type = options.get("viral_type", "RNA+DNA")
    return {
        "RNA": ("rna_virusidentification",),
        "DNA": ("dna_virusidentification",),
        "RNA+DNA": ("rna_virusidentification", "dna_virusidentification"),
    }[viral_type]


ARTIFACTS: dict[str, ArtifactSpec] = {
    "raw_paired_reads": ArtifactSpec(
        "raw_paired_reads", ("read1", "read2"), ("run_id", "sample_id")
    ),
    "qc_paired_reads": ArtifactSpec(
        "qc_paired_reads", ("read1", "read2"), ("run_id", "sample_id")
    ),
    "clean_run_paired_reads": ArtifactSpec(
        "clean_run_paired_reads", ("read1", "read2"), ("run_id", "sample_id")
    ),
    "clean_sample_paired_reads": ArtifactSpec(
        "clean_sample_paired_reads", ("read1", "read2"), ("sample_id",)
    ),
    "assembled_contigs": ArtifactSpec("assembled_contigs", ("fasta",)),
    "filtered_contigs": ArtifactSpec("filtered_contigs", ("fasta",)),
    "contigs_fasta": ArtifactSpec("contigs_fasta", ("fasta",)),
    "virus_prepared_sequences": ArtifactSpec(
        "virus_prepared_sequences", ("contigs", "orfs"), (), False
    ),
    "virus_search_results": ArtifactSpec(
        "virus_search_results",
        ("workspace",),
        (),
        False,
        (("workspace", "directory"),),
    ),
    "virus_candidates": ArtifactSpec(
        "virus_candidates", ("contigs", "proteins", "coverage", "nucl2prot"), (), False
    ),
    "virus_evidence_bundle": ArtifactSpec(
        "virus_evidence_bundle",
        ("workspace",),
        (),
        False,
        (("workspace", "directory"),),
    ),
    "virus_summary_bundle": ArtifactSpec(
        "virus_summary_bundle",
        ("fasta", "table", "taxonomy"),
        (),
        False,
    ),
    "host_alignment_bundle": ArtifactSpec(
        "host_alignment_bundle",
        ("workspace", "sample_list"),
        (),
        False,
        (("workspace", "directory"),),
    ),
    "host_coverage_bundle": ArtifactSpec(
        "host_coverage_bundle",
        ("workspace", "sample_list"),
        (),
        False,
        (("workspace", "directory"),),
    ),
    "host_blast_bundle": ArtifactSpec(
        "host_blast_bundle",
        ("workspace", "sample_list"),
        (),
        False,
        (("workspace", "directory"),),
    ),
    "host_evidence_bundle": ArtifactSpec(
        "host_evidence_bundle",
        ("workspace", "sample_list"),
        (),
        False,
        (("workspace", "directory"),),
    ),
    "host_summary_bundle": ArtifactSpec("host_summary_bundle", ("report",), (), False),
    "kraken_profiles": ArtifactSpec(
        "kraken_profiles",
        ("workspace", "sample_list"),
        (),
        False,
        (("workspace", "directory"),),
    ),
    "bracken_profiles": ArtifactSpec(
        "bracken_profiles",
        ("workspace", "sample_list"),
        (),
        False,
        (("workspace", "directory"),),
    ),
    "microbe_summary_bundle": ArtifactSpec(
        "microbe_summary_bundle",
        ("workspace",),
        (),
        False,
        (("workspace", "directory"),),
    ),
    "virus_reference_fasta": ArtifactSpec(
        "virus_reference_fasta", ("fasta",), (), False
    ),
    "virus_mapping_bundle": ArtifactSpec(
        "virus_mapping_bundle",
        ("workspace", "sample_list", "reference"),
        (),
        False,
        (("workspace", "directory"),),
    ),
    "virus_coverage_bundle": ArtifactSpec(
        "virus_coverage_bundle",
        ("workspace", "sample_list"),
        (),
        False,
        (("workspace", "directory"),),
    ),
    "virus_quantification_bundle": ArtifactSpec(
        "virus_quantification_bundle", ("rpm", "summary"), (), False
    ),
    "virus_taxonomy": ArtifactSpec("virus_taxonomy", ("taxonomy",), (), False),
    "virus_quality_bundle": ArtifactSpec(
        "virus_quality_bundle",
        ("workspace", "taxonomy"),
        (),
        False,
        (("workspace", "directory"),),
    ),
    "annotation_strategy_bundle": ArtifactSpec(
        "annotation_strategy_bundle",
        ("workspace", "taxonomy"),
        (),
        False,
        (("workspace", "directory"),),
    ),
    "structure_annotation_bundle": ArtifactSpec(
        "structure_annotation_bundle",
        ("workspace", "taxonomy"),
        (),
        False,
        (("workspace", "directory"),),
    ),
    "functional_annotation_bundle": ArtifactSpec(
        "functional_annotation_bundle",
        ("workspace", "taxonomy"),
        (),
        False,
        (("workspace", "directory"),),
    ),
    "annotation_delivery_bundle": ArtifactSpec(
        "annotation_delivery_bundle",
        ("workspace",),
        (),
        False,
        (("workspace", "directory"),),
    ),
}


STAGES: tuple[StageSpec, ...] = (
    StageSpec(
        "read_qc",
        "序列质控",
        "workflow1",
        (("raw_paired_reads",),),
        "qc_paired_reads",
        ("fastp",),
    ),
    StageSpec(
        "rrna_filter",
        "去除 rRNA",
        "workflow1",
        (("qc_paired_reads",),),
        "clean_run_paired_reads",
        ("bowtie2",),
    ),
    StageSpec(
        "sample_merge",
        "样本合并",
        "workflow1",
        (("clean_run_paired_reads",),),
        "clean_sample_paired_reads",
        ("merge",),
    ),
    StageSpec(
        "assembly",
        "序列组装",
        "workflow1",
        (("clean_sample_paired_reads",),),
        "assembled_contigs",
        ("megahit",),
    ),
    StageSpec(
        "contig_filter",
        "Contig 重命名与过滤",
        "workflow1",
        (("assembled_contigs",),),
        "filtered_contigs",
        ("rename_filter",),
    ),
    StageSpec(
        "virus_sequence_prep",
        "病毒识别序列准备",
        "workflow2",
        (("filtered_contigs", "contigs_fasta"),),
        "virus_prepared_sequences",
        _virus_prep_rules,
    ),
    StageSpec(
        "virus_candidate_search",
        "病毒候选搜索",
        "workflow2",
        (("virus_prepared_sequences",),),
        "virus_search_results",
        _virus_search_rules,
    ),
    StageSpec(
        "virus_candidate_extract",
        "病毒候选提取",
        "workflow2",
        (("virus_search_results",),),
        "virus_candidates",
        ("coverage_calc", "fetch_contigs", "fetch_proteins"),
    ),
    StageSpec(
        "virus_evidence",
        "病毒分类证据分析",
        "workflow2",
        (("virus_candidates",),),
        "virus_evidence_bundle",
        (
            "mmseqs_nuclclust",
            "fetch_nuclrep_prot",
            "mmseqs_aaclust",
            "cluster_summarize",
            "checkv",
            "megablast_nt",
            "blastx_nr",
            "blastx_vhdb",
            "hmmsearch_rdrpscan",
            "parse_blastresult",
        ),
    ),
    StageSpec(
        "virus_summary",
        "病毒识别结果汇总",
        "workflow2",
        (("virus_evidence_bundle",),),
        "virus_summary_bundle",
        ("summarize_tables", "summarize_sequences"),
    ),
    StageSpec(
        "host_mapping",
        "宿主标记序列比对",
        "workflow3",
        (("clean_sample_paired_reads",),),
        "host_alignment_bundle",
        ("H1_bowtie2_mapping",),
    ),
    StageSpec(
        "host_coverage",
        "宿主标记覆盖度",
        "workflow3",
        (("host_alignment_bundle",),),
        "host_coverage_bundle",
        ("H2_samtools_coverage",),
    ),
    StageSpec(
        "host_blast",
        "宿主标记 BLAST",
        "workflow3",
        (("filtered_contigs", "assembled_contigs", "contigs_fasta"),),
        "host_blast_bundle",
        ("H3_blastn",),
    ),
    StageSpec(
        "host_integrate",
        "宿主证据整合",
        "workflow3",
        (("host_coverage_bundle",), ("host_blast_bundle",)),
        "host_evidence_bundle",
        ("H4_integrate_results",),
    ),
    StageSpec(
        "host_summary",
        "宿主识别结果",
        "workflow3",
        (("host_evidence_bundle",),),
        "host_summary_bundle",
        ("H5_summarize",),
    ),
    StageSpec(
        "microbe_classify",
        "微生物快速分类",
        "workflow4",
        (("clean_sample_paired_reads",),),
        "kraken_profiles",
        ("K1_kraken2_classify",),
    ),
    StageSpec(
        "microbe_abundance",
        "微生物丰度估计",
        "workflow4",
        (("kraken_profiles",),),
        "bracken_profiles",
        ("K2_bracken_species", "K2_bracken_genus", "K2_bracken_family"),
    ),
    StageSpec(
        "microbe_summary",
        "微生物谱汇总",
        "workflow4",
        (("bracken_profiles",),),
        "microbe_summary_bundle",
        ("K3_combine_species", "K3_combine_genus", "K3_combine_family"),
    ),
    StageSpec(
        "virus_read_mapping",
        "病毒序列比对",
        "workflow5",
        (
            ("clean_sample_paired_reads",),
            ("virus_reference_fasta", "virus_candidates", "virus_summary_bundle"),
        ),
        "virus_mapping_bundle",
        (
            "Q0_count_clean_reads",
            "Q1_build_bowtie2_index",
            "Q2_align_filter_sort_coverage",
        ),
    ),
    StageSpec(
        "virus_coverage",
        "病毒覆盖度筛选",
        "workflow5",
        (("virus_mapping_bundle",),),
        "virus_coverage_bundle",
        ("Q3_merge_coverage", "Q4_filter_coverage"),
    ),
    StageSpec(
        "virus_rpm",
        "病毒丰度定量",
        "workflow5",
        (("virus_coverage_bundle",),),
        "virus_quantification_bundle",
        ("Q5_calculate_rpm", "Q6_sort_rpm_and_summary"),
    ),
    StageSpec(
        "virus_quality",
        "病毒序列质量筛选",
        "workflow6",
        (
            ("virus_summary_bundle", "virus_reference_fasta", "virus_candidates"),
            ("virus_taxonomy",),
        ),
        "virus_quality_bundle",
        ("A1_checkv_end_to_end", "A2_filter_checkv"),
    ),
    StageSpec(
        "annotation_strategy",
        "注释策略分流",
        "workflow6",
        (("virus_quality_bundle",),),
        "annotation_strategy_bundle",
        ("A3_pick_structure_strategy", "A4_extract_split_fastas"),
    ),
    StageSpec(
        "structure_annotation",
        "病毒结构注释",
        "workflow6",
        (("annotation_strategy_bundle",),),
        "structure_annotation_bundle",
        (
            "A5_structure_by_vigor",
            "A5b_structure_fallback",
            "A6_structure_by_prodigal",
            "A7_merge_proteins",
        ),
    ),
    StageSpec(
        "functional_annotation",
        "病毒功能注释",
        "workflow6",
        (("structure_annotation_bundle",),),
        "functional_annotation_bundle",
        ("A8_function_by_dramv",),
    ),
    StageSpec(
        "annotation_delivery",
        "病毒注释结果汇总",
        "workflow6",
        (("functional_annotation_bundle",),),
        "annotation_delivery_bundle",
        ("A9_aggregate_summary", "A10_split_genbank", "A11_blast_review_candidates"),
    ),
)

STAGE_BY_ID = {stage.id: stage for stage in STAGES}
PRODUCER_BY_ARTIFACT = {stage.produces: stage for stage in STAGES}
STAGE_ORDER = {stage.id: index for index, stage in enumerate(STAGES)}


def stage_rules(
    stage: StageSpec, options: dict[str, Any], bindings: dict[str, str]
) -> tuple[str, ...]:
    return stage.selected_rules(options, bindings)


def _parameter_catalog(parameter: ParameterSpec) -> dict[str, Any]:
    return {
        "id": parameter.id,
        "label": parameter.label,
        "type": parameter.kind,
        **({"minimum": parameter.minimum} if parameter.minimum is not None else {}),
        **({"maximum": parameter.maximum} if parameter.maximum is not None else {}),
        **({"unit": parameter.unit} if parameter.unit else {}),
        "description": parameter.description,
    }


def capability_catalog() -> dict[str, Any]:
    """Return the public, path-free capability catalog used by UI and providers."""

    manifests = []
    for manifest in CAPABILITY_MANIFESTS.values():
        manifests.append(
            {
                "id": manifest.id,
                "version": manifest.version,
                "stage_id": manifest.stage_id,
                "tool_id": manifest.tool_id,
                "label": manifest.label,
                "validation_status": manifest.validation_status,
                "adapter_version": manifest.adapter_version,
                "impact": manifest.impact,
                "software_dependencies": list(manifest.software_dependencies),
                "reference_dependencies": list(manifest.reference_dependencies),
                "parser": manifest.parser,
                "resources": dict(manifest.resources),
                "input_artifacts": [
                    artifact
                    for alternatives in STAGE_BY_ID[manifest.stage_id].requirements
                    for artifact in alternatives
                ],
                "output_artifact": STAGE_BY_ID[manifest.stage_id].produces,
                "parameters": [
                    _parameter_catalog(parameter) for parameter in manifest.parameters
                ],
                "presets": [
                    {
                        "id": preset_id,
                        "values": dict(values),
                    }
                    for preset_id, values in manifest.presets
                ],
            }
        )
    return {
        "schema_version": 2,
        "deliverables": [
            {
                "id": deliverable_id,
                **metadata,
                "dependencies": [
                    PRODUCER_BY_ARTIFACT[alternatives[0]].id
                    for alternatives in STAGE_BY_ID[deliverable_id].requirements
                    if alternatives[0] in PRODUCER_BY_ARTIFACT
                ],
                "input_artifacts": [
                    artifact
                    for alternatives in STAGE_BY_ID[deliverable_id].requirements
                    for artifact in alternatives
                ],
                "output_artifact": STAGE_BY_ID[deliverable_id].produces,
            }
            for deliverable_id, metadata in PUBLIC_DELIVERABLES.items()
        ],
        "capabilities": manifests,
        "tools": public_tool_catalog(),
        "workflow_skills": public_workflow_skill_catalog(),
    }


def normalize_capability_parameters(raw: Any) -> list[dict[str, Any]]:
    """Validate and canonicalize user-controlled tool parameters."""

    if raw is None:
        return []
    if not isinstance(raw, list):
        raise ValueError("capability_parameters must be a list")  # noqa: TRY004
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, value in enumerate(raw):
        if not isinstance(value, dict):
            raise ValueError(  # noqa: TRY004
                f"capability_parameters[{index}] must be an object"
            )
        unsupported = set(value) - {
            "capability_id",
            "manifest_version",
            "preset_id",
            "values",
        }
        if unsupported:
            raise ValueError(
                "capability parameter selection contains unsupported fields: "
                + ", ".join(sorted(unsupported))
            )
        capability_id = value.get("capability_id")
        manifest = CAPABILITY_MANIFESTS.get(capability_id)
        if manifest is None:
            raise ValueError(f"unknown capability: {capability_id}")
        if capability_id in seen:
            raise ValueError(f"duplicate capability parameters: {capability_id}")
        seen.add(capability_id)
        version = value.get("manifest_version")
        if version != manifest.version:
            raise ValueError(
                f"capability manifest version is unavailable: {capability_id}@{version}"
            )
        preset_id = value.get("preset_id", "workflow_default")
        presets = {preset: dict(values) for preset, values in manifest.presets}
        if preset_id not in presets:
            raise ValueError(f"unknown capability preset: {capability_id}/{preset_id}")
        raw_values = value.get("values") or {}
        if not isinstance(raw_values, dict):
            raise ValueError(  # noqa: TRY004
                f"capability values must be an object: {capability_id}"
            )
        specs = {parameter.id: parameter for parameter in manifest.parameters}
        unknown = set(raw_values) - set(specs)
        if unknown:
            raise ValueError(
                f"unknown capability parameter for {capability_id}: "
                + ", ".join(sorted(unknown))
            )
        effective = presets[preset_id]
        for parameter_id, parameter_value in raw_values.items():
            spec = specs[parameter_id]
            if spec.kind == "boolean":
                if not isinstance(parameter_value, bool):
                    raise ValueError(
                        f"{capability_id}.{parameter_id} must be a boolean"
                    )
            elif (
                not isinstance(parameter_value, int)
                or isinstance(parameter_value, bool)
                or (spec.minimum is not None and parameter_value < spec.minimum)
                or (spec.maximum is not None and parameter_value > spec.maximum)
            ):
                raise ValueError(
                    f"{capability_id}.{parameter_id} is outside the allowed range"
                )
            effective[parameter_id] = parameter_value
        normalized.append(
            {
                "capability_id": capability_id,
                "manifest_version": manifest.version,
                "preset_id": preset_id,
                "values": {key: effective[key] for key in sorted(effective)},
            }
        )
    return sorted(normalized, key=lambda item: item["capability_id"])


def capability_parameter_arguments(
    capability_parameters: list[dict[str, Any]], capability_id: str
) -> tuple[str, ...]:
    selection = next(
        (
            item
            for item in capability_parameters
            if item.get("capability_id") == capability_id
        ),
        None,
    )
    if selection is None:
        return ()
    manifest = CAPABILITY_MANIFESTS[capability_id]
    specs = {parameter.id: parameter for parameter in manifest.parameters}
    arguments: list[str] = []
    for parameter_id, value in selection.get("values", {}).items():
        spec = specs[parameter_id]
        if spec.kind == "boolean":
            if value is True:
                arguments.append(spec.flag)
        else:
            arguments.extend((spec.flag, str(value)))
    return tuple(arguments)


def capability_parameter_values(
    capability_parameters: list[dict[str, Any]], capability_id: str
) -> dict[str, int | bool]:
    selection = next(
        (
            item
            for item in capability_parameters
            if item.get("capability_id") == capability_id
        ),
        None,
    )
    return dict(selection.get("values", {})) if selection else {}

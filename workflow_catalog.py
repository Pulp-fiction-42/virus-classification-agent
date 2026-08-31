"""Static public analysis stages and artifact contracts.

The catalog is the single source of truth shared by planning, execution and
progress reporting.  Workflow identifiers and Snakemake rules stay internal;
users select stable biological stages and typed artifacts instead.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


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

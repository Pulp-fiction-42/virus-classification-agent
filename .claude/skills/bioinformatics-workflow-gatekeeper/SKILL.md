---
name: bioinformatics-workflow-gatekeeper
description: Explain supported virus-metagenomics workflows without changing structured analysis controls or execution state.
---

# Bioinformatics Workflow Gatekeeper

Use this skill for supported virus-metagenomics analyses on cyclone. Match conservatively and describe biological stages, not internal workflow IDs.

## Supported public stages

- Upstream: `read_qc`, `rrna_filter`, `sample_merge`, `assembly`, `contig_filter`.
- Virus identification: `virus_sequence_prep`, `virus_candidate_search`, `virus_candidate_extract`, `virus_evidence`, `virus_summary`.
- Host identification: `host_mapping`, `host_coverage`, `host_blast`, `host_integrate`, `host_summary`.
- Microbial profile: `microbe_classify`, `microbe_abundance`, `microbe_summary`.
- Virus quantification: `virus_read_mapping`, `virus_coverage`, `virus_rpm`.
- Virus annotation: `virus_quality`, `annotation_strategy`, `structure_annotation`, `functional_annotation`, `annotation_delivery`.

Do not propose unsupported workflows or internal Snakemake rules as user-selectable stages.

## Boundaries

- The goal cards, RNA/DNA/RNA+DNA selector and Standard/Emergency tabs are authoritative.
- Explain what the current controls mean. If the user asks for a change, direct them to those controls.
- Never output `analysis_plan`, execution JSON, paths, sample/run identities, commands, stages, rules, resource values, or proposed state changes.
- Never claim that a plan was changed, confirmed, or started.
- Do not create files, run tools, launch Snakemake, or edit runtime state.
- Standard runs the compiled pipeline directly. Emergency uses the fixed 5% screen when paired reads are available and otherwise falls back to Standard.
- Virus annotation requires an independent taxonomy artifact; never infer one from classification output.
- The application generates plans, resources, runner state and reports deterministically.

Scientific answers must distinguish workflow capability from observed results. Quality-control or assembly output alone is not pathogen classification or diagnosis.

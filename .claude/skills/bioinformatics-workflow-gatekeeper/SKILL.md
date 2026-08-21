---
name: bioinformatics-workflow-gatekeeper
description: Use when a user requests an analysis covered by fixed Snakemake workflows 1–6. Produces one analysis_pipeline v2 execution spec and enforces the confirmation and runner boundaries.
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

Do not propose unsupported workflows or internal Snakemake rules as user-selectable stages. Any public stage may be the endpoint. A complete typed artifact bundle for a stage requirement may be the entry. Multiple endpoints are allowed.

## Confirmation gate

Ask for one explicit confirmation after presenting a concrete plan. Before confirmation, do not create a run directory, copy a workflow, write config, run a dry-run, or launch Snakemake.

After confirmation, execute only the runtime-provided runner command. The runtime compiles dependencies, prepares run-local phases, selects targets, and records state.

## Plan-mode contract

Append exactly one final `analysis_plan` fenced block when the goal, absolute paths, and sample/run relationships are clear:

````markdown
```analysis_plan
{
  "version": 2,
  "title": "病毒宏基因组分析",
  "ready_to_execute": true,
  "execution_spec": {
    "version": 2,
    "adapter": "analysis_pipeline",
    "inputs": [
      {
        "id": "primary",
        "type": "raw_paired_reads",
        "records": [
          {
            "run_id": "run1",
            "sample_id": "sample1",
            "files": {
              "read1": "/absolute/R1.fastq.gz",
              "read2": "/absolute/R2.fastq.gz"
            }
          }
        ]
      }
    ],
    "deliverables": ["virus_summary"],
    "options": {"viral_type": "RNA+DNA"},
    "policy": {"mode": "standard"}
  }
}
```
````

Rules:

- The only accepted adapter is `analysis_pipeline` version 2. Do not emit `workflow1`, `virus_classification`, or `adaptive_virus_analysis`.
- Do not emit model-authored `steps`, `entry_rule`, `exit_rule`, or exact rule lists. The runtime compiles canonical stages, dependencies, rules, and targets.
- Every input has a unique ID, supported artifact type, and non-empty `records`. Every required file role uses an absolute path. Never invent paths.
- Preserve all supplied `run_id` and `sample_id` values. Paired read records use `files.read1` and `files.read2`; contigs use `files.fasta`.
- Use multiple `deliverables` when the user requests branches. Shared prerequisites are compiled once.
- Default `options.viral_type` to `RNA+DNA`; narrow it to `RNA` or `DNA` only when requested.
- Virus annotation always requires an independent `virus_taxonomy` artifact with `files.taxonomy`. Never infer taxonomy TSV from workflow 2 output.
- Use existing workflow parameters and databases as defaults. These technical choices are not user requirements and do not block an otherwise complete plan.
- Do not output a resource estimate. The runtime returns explicit “not calibratable” values when history is insufficient.
- Set `ready_to_execute` false only when a user-level goal, required path, or sample relationship is missing. Explain what is missing before the control block.
- Plan mode must not create run directories or execute tools. The fenced block must be the final response content.

## Routing policy

The UI routing setting is authoritative:

- `standard`: run the compiled pipeline directly, with no screen.
- `auto`: when a used paired-read bundle exists, run the 5% Kraken2 screen. High confidence returns a rapid report and skips every planned pipeline stage; low confidence continues the full pipeline.
- `emergency`: when paired reads exist, run the screen and always stop, marking every planned pipeline stage skipped. A low-confidence screen is preliminary, not negative.
- If no paired reads are used, `auto` and `emergency` normalize to effective `standard`.
- `deep` is not executable.

The short-circuit scope is the entire pipeline, including non-virus branches.

## Execution boundary

- Never execute a stage omitted from the latest plan; the runtime-provided plan is already compiled.
- Do not edit source workflow directories under `/data/user/guojindan/Projects/Workflows`.
- Do not copy workflows, materialize inputs, choose targets, or add runner arguments yourself.
- Runtime state is `${RUN_DIR}/.bioagent/state.json`. Do not edit it manually.
- The runner owns `${RUN_DIR}/.bioagent/execution.log`, telemetry, benchmarks, status, and deterministic reports.
- Do not run the runner or Snakemake in the background.
- Do not output `analysis_plan` in execute mode.

Use only the injected command:

```bash
python "/data/user/liufangzhou/projects/bioagent/agent/analysis_runner.py" --state "<runtime injected state_path>"
```

The application generates the diagnostic report deterministically. Quality-control or assembly results alone must not be presented as pathogen classification or diagnosis.

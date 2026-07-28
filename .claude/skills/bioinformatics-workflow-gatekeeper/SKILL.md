---
name: bioinformatics-workflow-gatekeeper
description: Use when a user requests a bioinformatics analysis that may match one or more fixed Snakemake workflows workflow1-workflow6 on cyclone. Guides conservative internal workflow matching, plan-mode analysis_plan output, one explicit user confirmation gate, and execute-mode Snakemake execution through the Bash tool.
---

# Bioinformatics Workflow Gatekeeper

Use this skill when a user asks for a bioinformatics analysis that may match one or more fixed Snakemake workflows available on the cyclone server.

## Supported workflows

Only these workflows are in scope:

| ID        | Directory                      | Purpose                                                                                                                                           |
| --------- | ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| workflow1 | workflow1_upstream             | Upstream preprocessing for paired-end viral metagenomic reads: fastp QC, rRNA removal, sample merge, MEGAHIT assembly, contig renaming/filtering. |
| workflow2 | workflow2_virusid              | Virus identification workflow.                                                                                                                    |
| workflow3 | workflow3_hostid               | Host identification workflow.                                                                                                                     |
| workflow4 | workflow4_kraken_bracken       | Kraken/Bracken taxonomic profiling workflow.                                                                                                      |
| workflow5 | workflow5_virus_quantification | Virus quantification workflow.                                                                                                                    |
| workflow6 | workflow6_virus_annotation     | Virus annotation workflow.                                                                                                                        |

Do not propose workflow7 or any other workflow. You may internally compose supported workflows in series when the user's request clearly requires multiple supported phases, but do not expose workflow IDs as the main user-facing plan.

The deterministic execution adapter currently implements one contiguous
workflow1 segment only. Plans for workflow2-workflow6 or composed workflows may
be discussed, but must remain `ready_to_execute: false` until their adapters
exist.

## Core rule

This skill uses conversational confirmation gates, not Claude Agent SDK lifecycle hooks.

The agent must ask the user for one explicit confirmation before preparing a workflow run and launching the real Snakemake execution. A user message such as "确认启动分析，请按当前方案执行" counts as that confirmation when a concrete plan has already been discussed.

## Plan-mode contract

In plan mode, produce a concrete analysis plan and append exactly one internal `analysis_plan` fenced block whenever the user-facing flow is clear enough for the `/analysis` page:

````markdown
```analysis_plan
{
  "version": 1,
  "title": "病毒宏基因组上游流程",
  "ready_to_execute": true,
  "steps": [
    {"label": "质控", "tool": "fastp", "rules": ["fastp"]},
    {"label": "去 rRNA", "tool": "Bowtie2", "rules": ["bowtie2"]},
    {"label": "样本合并", "rules": ["merge"]},
    {"label": "组装", "tool": "MEGAHIT", "rules": ["megahit"]},
    {"label": "重命名过滤", "rules": ["rename_filter"]}
  ],
  "execution_spec": {
    "adapter": "workflow1",
    "entry_rule": "fastp",
    "exit_rule": "rename_filter",
    "inputs": [
      {
        "id": "primary",
        "type": "raw_paired_reads",
        "data": {
          "runs": [
            {
              "run_id": "sample-run",
              "sample_id": "sample",
              "read1": "/absolute/path/R1.fastq.gz",
              "read2": "/absolute/path/R2.fastq.gz"
            }
          ]
        }
      }
    ]
  },
  "resource_estimate": {
    "eta": "约 1-2 小时",
    "compute": "4-12 核，约 20 GB 内存",
    "confidence": "估算"
  }
}
```
````

Requirements:

- The fenced block content must be valid JSON only.
- The runtime removes this block before the user sees the reply.
- The fenced block is the final content in the response. Put every user-visible explanation or question before it.
- `steps` are user-understandable flow steps, not workflow names.
- `tool` is optional; omit it when uncertain.
- `rules` is internal progress metadata. For every fixed Snakemake workflow step, list the exact rule names represented by that user-facing step; inspect that workflow's Snakefile when necessary. One step may contain multiple rules.
- `execution_spec` is required for an executable plan. Its adapter owns input validation, run-local configuration, prerequisite materialization, and concrete targets.
- The workflow1 adapter accepts one contiguous segment delimited by `entry_rule` and `exit_rule`. The flattened `steps[].rules` must exactly equal that segment; the segment may start at any workflow1 rule.
- Workflow1 entry types are `raw_paired_reads` for fastp, `fastp_paired_reads` for bowtie2, `clean_paired_reads` for merge, `merged_paired_reads` for megahit, and `contigs_fasta` for rename_filter.
- Typed input data is adapter-specific and every file path must be absolute. Never invent file paths.
- Use the selected workflow's existing tool flags, databases, presets, and thresholds as defaults. These technical choices are not user requirements and must not make an otherwise executable plan unready.
- Set `ready_to_execute` to `true` once the supported analysis goal, input files, and required sample relationships are clear. Set it to `false` only when one of those user-level facts is missing or ambiguous, and ask for that information in plain language before the fenced block.
- If the user adjusts requirements in natural language, translate the request into the run-local workflow configuration and output a complete updated scheme, not a partial diff. Reflect qualitative changes in the step labels and remove omitted stages from the step list.
- Plan mode must not create run directories, write configs, run dry-runs, or execute Snakemake.
- BioAgent plan mode is not Claude Code Plan Mode. Never call EnterPlanMode or ExitPlanMode and never write `.claude/sessions/plans` files; finish with the `analysis_plan` block.

## Behavior

- Match conservatively. Suggest a workflow only when the user's request clearly fits workflow1-workflow6.
- Explain the concrete analysis flow briefly. Keep workflow IDs as internal implementation details unless the user explicitly asks about them.
- Before the user confirmation, do not call any workflow tool, do not create a run directory, do not write configuration files, and do not run Snakemake.
- After the user confirms the selected workflow, execute the runtime-provided runner command exactly once. The runner prepares the workflow and launches exactly the segment in the latest plan.
- Never execute a stage omitted from the latest plan. When the plan stops before a workflow's default final target, invoke Snakemake with the explicit output targets for the last selected stage instead of the default `all` target.
- In execute mode, summarize the execution result for the user as Markdown, including the run directory, execution log, return code, and any obvious next steps. Do not output `analysis_plan` in execute mode.
- After a successful execution summary, append the `diagnostic_report` control block described below. Failed or incomplete runs must not produce that block.
- If the user rejects the proposed workflow, exit this skill and continue normal conversation.

## Diagnostic report contract

When `execution.status` is `completed` and `execution.ok` is `true`, finish the response with exactly one internal report block:

````html
```diagnostic_report
<article class="diagnostic-report">
  <header>
    <h1>诊断意见书</h1>
    <p class="review-status">待专家审核</p>
  </header>
  <section>
    <h2>基本信息</h2>
    <dl>
      <dt>分析任务</dt>
      <dd>病毒宏基因组上游分析</dd>
    </dl>
  </section>
</article>
```
````

Requirements:

- The block contains one unstyled `<article class="diagnostic-report">` fragment, not a complete HTML document. The runtime hides it, applies the shared print style, and writes `diagnostic-opinion.html`; do not write that file yourself.
- Use only `article`, `header`, `section`, `h1`-`h3`, `p`, `ul`, `ol`, `li`, `dl`, `dt`, `dd`, `table`, `thead`, `tbody`, `tr`, `th`, `td`, `strong`, `em`, `small`, `code`, and `br`. Only `class` attributes are allowed.
- Do not include a doctype, `html`, `head`, `body`, `style`, `script`, links, images, external resources, inline styles, event attributes, Markdown, comments, or text outside the article.
- Include the available sections 基本信息、实际分析范围、关键结果、诊断意见、局限性与建议、证据与产物. Omit empty sections, unknown fields, and every analysis stage the user skipped.
- Base every metric, finding, conclusion, and evidence path on the completed run or the current conversation. Quality-control or assembly results alone must not be presented as a pathogen classification or etiological diagnosis.
- The fenced block is the final response content. Put the user-visible Markdown execution summary before it and write nothing after it.

## Tool boundaries

- Use the Bash tool only to run the exact `RUNTIME RUNNER COMMAND`.
- Do not use MCP custom tools for this workflow path.
- Do not edit the source workflow directories under `/data/user/guojindan/Projects/Workflows`.
- The run root is controlled by `BIOAGENT_RUN_ROOT`.
- Work only inside the runtime-injected run directory.
- Do not copy workflows, write sample/config files, materialize inputs, choose targets, or add runner arguments. The workflow adapter owns those actions.
- The real Snakemake execution must only happen after explicit user confirmation and execute-mode context.
- Runtime state has already been created at `${RUN_DIR}/.bioagent/state.json`. Do not edit it manually; the runner owns step timing, status, telemetry, logs, and terminal execution fields.
- Do not redirect Snakemake stdout/stderr yourself. The runner writes `${RUN_DIR}/.bioagent/execution.log` and streams the same output to the caller.
- Do not run the runner or Snakemake in the background.

## Bash execution pattern

Use only the command injected by the runtime after user confirmation:

```bash
python "/data/user/liufangzhou/projects/bioagent/agent/analysis_runner.py" --state "<runtime injected state_path>"
```

## Input expectations

Collect the adapter-specific input required by `entry_rule`. For paired reads,
record the run/sample relationship and absolute R1/R2 paths. For
`contigs_fasta`, record each sample ID and absolute contig path. If required
input details are missing, ask the user before producing a ready plan.

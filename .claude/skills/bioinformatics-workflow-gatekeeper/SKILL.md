---
name: bioinformatics-workflow-gatekeeper
description: Use when a user requests a bioinformatics analysis that may match one or more fixed Snakemake workflows workflow1-workflow6 on cyclone. Guides conservative internal workflow matching, plan-mode analysis_plan output, one explicit user confirmation gate, and execute-mode Snakemake execution through the Bash tool.
---

# Bioinformatics Workflow Gatekeeper

Use this skill when a user asks for a bioinformatics analysis that may match one or more fixed Snakemake workflows available on the cyclone server.

## Supported workflows

Only these workflows are in scope:

| ID | Directory | Purpose |
| --- | --- | --- |
| workflow1 | workflow1_upstream | Upstream preprocessing for paired-end viral metagenomic reads: fastp QC, rRNA removal, sample merge, MEGAHIT assembly, contig renaming/filtering. |
| workflow2 | workflow2_virusid | Virus identification workflow. |
| workflow3 | workflow3_hostid | Host identification workflow. |
| workflow4 | workflow4_kraken_bracken | Kraken/Bracken taxonomic profiling workflow. |
| workflow5 | workflow5_virus_quantification | Virus quantification workflow. |
| workflow6 | workflow6_virus_annotation | Virus annotation workflow. |

Do not propose workflow7 or any other workflow. You may internally compose supported workflows in series when the user's request clearly requires multiple supported phases, but do not expose workflow IDs as the main user-facing plan.

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
- Use the selected workflow's existing tool flags, databases, presets, and thresholds as defaults. These technical choices are not user requirements and must not make an otherwise executable plan unready.
- Set `ready_to_execute` to `true` once the supported analysis goal, input files, and required sample relationships are clear. Set it to `false` only when one of those user-level facts is missing or ambiguous, and ask for that information in plain language before the fenced block.
- If the user adjusts requirements in natural language, translate the request into the run-local workflow configuration and output a complete updated scheme, not a partial diff. Reflect qualitative changes in the step labels and remove omitted stages from the step list.
- Plan mode must not create run directories, write configs, run dry-runs, or execute Snakemake.
- BioAgent plan mode is not Claude Code Plan Mode. Never call EnterPlanMode or ExitPlanMode and never write `.claude/sessions/plans` files; finish with the `analysis_plan` block.

## Behavior

- Match conservatively. Suggest a workflow only when the user's request clearly fits workflow1-workflow6.
- Explain the concrete analysis flow briefly. Keep workflow IDs as internal implementation details unless the user explicitly asks about them.
- Before the user confirmation, do not call any workflow tool, do not create a run directory, do not write configuration files, and do not run Snakemake.
- After the user confirms the selected workflow, use the runtime-injected run_id and run_dir, copy the selected workflow source, prepare the run-local configuration, update state steps, and launch exactly the stages in the latest plan. Apply qualitative adjustments only to the run-local configuration.
- Never execute a stage omitted from the latest plan. When the plan stops before a workflow's default final target, invoke Snakemake with the explicit output targets for the last selected stage instead of the default `all` target.
- In execute mode, summarize the execution result for the user as Markdown, including the run directory, execution log, return code, and any obvious next steps. Do not output `analysis_plan` in execute mode.
- If the user rejects the proposed workflow, exit this skill and continue normal conversation.

## Tool boundaries

- Use the Bash tool directly for Snakemake workflow preparation and execution.
- Do not use MCP custom tools for this workflow path.
- Do not edit the source workflow directories under `/data/user/guojindan/Projects/Workflows`.
- The run root is controlled by `BIOAGENT_RUN_ROOT`.
- Work only inside the runtime-injected run directory.
- Copy the workflow source into `${RUN_DIR}/workflow` before running it.
- The real Snakemake execution must only happen after explicit user confirmation and execute-mode context.
- Runtime state has already been created at `${RUN_DIR}/.bioagent/state.json`; update it as each user-facing step starts or finishes. Step status is exactly `pending`, `running`, `completed`, or `failed`; execution status is exactly `running`, `completed`, or `failed`.
- Snakemake stdout/stderr should be captured in `${RUN_DIR}/.bioagent/execution.log`.
- Do not run Snakemake in the background. Before the Bash command exits, write terminal `execution.status`, `execution.ok`, and `execution.returncode`.

## Bash execution pattern

Use this structure only after the user confirmation. Replace placeholders before running.

```bash
set -euo pipefail

export WORKFLOW_ID="workflow1"
export WORKFLOW_SRC="/data/user/guojindan/Projects/Workflows/workflow1_upstream"
export SNAKEMAKE="/data/user/guojindan/softwares/xvirmin/bin/snakemake"
export RUN_ID="<runtime injected run_id>"
export RUN_DIR="<runtime injected run_dir>"
export STATE_PATH="${RUN_DIR}/.bioagent/state.json"
export LOG_PATH="${RUN_DIR}/.bioagent/execution.log"

mkdir -p "${RUN_DIR}/logs" "${RUN_DIR}/results" "${RUN_DIR}/.bioagent"
rsync -a \
  --exclude .git \
  --exclude .snakemake \
  --exclude logs \
  --exclude results \
  --exclude example \
  "${WORKFLOW_SRC}/" "${RUN_DIR}/workflow/"
```

For workflow1, prepare the run-local sample list and config inside `${RUN_DIR}/workflow`:

```bash
cd "${RUN_DIR}/workflow"

# If the user provided real samples, overwrite data/samples.txt with:
# run_id sample_name /absolute/path/to/R1.fastq.gz /absolute/path/to/R2.fastq.gz
#
# If this is only a workflow1 test and the user did not provide data, keep the
# workflow's existing example data/samples.txt.

python - <<'PY'
from pathlib import Path
config = Path("config/config.yaml")
text = config.read_text()
lines = []
for line in text.splitlines():
    if line.startswith("sample_list:"):
        lines.append('sample_list: "data/samples.txt"')
    elif line.startswith("output_dir:"):
        lines.append('output_dir: "../results"')
    else:
        lines.append(line)
config.write_text("\n".join(lines) + "\n")
PY
```

Update the run-local state and launch Snakemake:

```bash
cd "${RUN_DIR}/workflow"

python - <<'PY'
import json
import os
from pathlib import Path
path = Path(os.environ["STATE_PATH"])
state = json.loads(path.read_text())
state["workflow_id"] = os.environ["WORKFLOW_ID"]
state["workflow_dir"] = f'{os.environ["RUN_DIR"]}/workflow'
state["results_dir"] = f'{os.environ["RUN_DIR"]}/results'
state["execution"]["current_step_id"] = state["steps"][0]["id"]
state["steps"][0]["status"] = "running"
path.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n")
PY

set +e
"${SNAKEMAKE}" --snakefile Snakefile --configfile config/config.yaml --cores 4 \
  > "${LOG_PATH}" 2>&1
EXEC_STATUS=$?
set -e

python - <<PY
import json
from pathlib import Path
state_path = Path("${STATE_PATH}")
state = json.loads(state_path.read_text())
ok = ${EXEC_STATUS} == 0
for step in state.get("steps", []):
    if step.get("status") == "running":
        step["status"] = "completed" if ok else "failed"
    elif ok and step.get("status") == "pending":
        step["status"] = "completed"
state["execution"]["status"] = "completed" if ok else "failed"
state["execution"]["ok"] = ok
state["execution"]["returncode"] = ${EXEC_STATUS}
state["execution"]["current_step_id"] = None
state["execution"]["log"] = "${LOG_PATH}"
state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n")
PY

cat "${LOG_PATH}"
exit "${EXEC_STATUS}"
```

## Input expectations

For workflow1, collect paired-end sample information when the user provides real data:

- `run_id`
- `sample_name`
- absolute path to R1 FASTQ
- absolute path to R2 FASTQ

If required input details are missing, ask the user for them before preparing a run.

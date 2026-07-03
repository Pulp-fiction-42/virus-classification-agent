---
name: bioinformatics-workflow-gatekeeper
description: Use when a user requests a bioinformatics analysis that may match one or more fixed Snakemake workflows workflow1-workflow6 on cyclone. Guides conservative internal workflow matching, plan-mode analysis_scheme output, one explicit user confirmation gate, and execute-mode Snakemake execution through the Bash tool.
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

## Plan-mode card contract

In plan mode, produce a concrete analysis plan and append exactly one `analysis_scheme` fenced block whenever the user-facing flow is clear enough for the `/analysis` page:

````markdown
```analysis_scheme
{
  "version": 1,
  "title": "病毒宏基因组上游流程",
  "ready_to_execute": true,
  "steps": [
    {"label": "质控", "tool": "fastp"},
    {"label": "去 rRNA", "tool": "Bowtie2"},
    {"label": "样本合并"},
    {"label": "组装", "tool": "MEGAHIT"},
    {"label": "重命名过滤"}
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
- `steps` are user-understandable flow steps, not workflow names.
- `tool` is optional; omit it when uncertain.
- If required inputs or intent are missing, set `ready_to_execute` to `false`.
- If the user adjusts requirements in natural language, output a complete updated scheme, not a partial diff.
- Plan mode must not create run directories, write configs, run dry-runs, or execute Snakemake.

## Behavior

- Match conservatively. Suggest a workflow only when the user's request clearly fits workflow1-workflow6.
- Explain the concrete analysis flow briefly. Keep workflow IDs as internal implementation details unless the user explicitly asks about them.
- Before the user confirmation, do not call any workflow tool, do not create a run directory, do not write configuration files, and do not run Snakemake.
- After the user confirms the selected workflow, use the Bash tool to create a run directory, copy the selected workflow source, prepare the run-local configuration, and launch the real Snakemake execution.
- In execute mode, summarize the execution result for the user as Markdown, including the run directory, execution log, return code, and any obvious next steps. Do not output `analysis_scheme` in execute mode.
- If the user rejects the proposed workflow, exit this skill and continue normal conversation.

## Tool boundaries

- Use the Bash tool directly for Snakemake workflow preparation and execution.
- Do not use MCP custom tools for this workflow path.
- Do not edit the source workflow directories under `/data/user/guojindan/Projects/Workflows`.
- The run root is `/data/runs`.
- Work only inside a new run directory under `/data/runs/<run_id>`.
- Copy the workflow source into `/data/runs/<run_id>/workflow` before running it.
- The real Snakemake execution must only happen after explicit user confirmation and execute-mode context.
- Runtime state must be written to `${RUN_DIR}/.bioagent/state.json`.
- Snakemake stdout/stderr should be captured in `${RUN_DIR}/.bioagent/execution.log` so Open WebUI can parse progress.

## Bash execution pattern

Use this structure only after the user confirmation. Replace placeholders before running.

```bash
set -euo pipefail

WORKFLOW_ID="workflow1"
WORKFLOW_SRC="/data/user/guojindan/Projects/Workflows/workflow1_upstream"
SNAKEMAKE="/data/user/guojindan/softwares/xvirmin/bin/snakemake"
RUN_ID="workflow1-$(date +%Y%m%d-%H%M%S)"
RUN_DIR="/data/runs/${RUN_ID}"

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

Write the initial run-local state and launch Snakemake:

```bash
cd "${RUN_DIR}/workflow"

python - <<PY
import json
from pathlib import Path
state = {
    "workflow_id": "${WORKFLOW_ID}",
    "run_id": "${RUN_ID}",
    "run_dir": "${RUN_DIR}",
    "workflow_dir": "${RUN_DIR}/workflow",
    "results_dir": "${RUN_DIR}/results",
    "inputs": {"sample_name": "${RUN_ID}"},
    "steps": [
        {"label": "质控", "tool": "fastp"},
        {"label": "去 rRNA", "tool": "Bowtie2"},
        {"label": "样本合并"},
        {"label": "组装", "tool": "MEGAHIT"},
        {"label": "重命名过滤"},
    ],
    "execution": {
        "started": True,
        "ok": None,
        "returncode": None,
        "log": "${RUN_DIR}/.bioagent/execution.log",
    },
}
Path("${RUN_DIR}/.bioagent/state.json").write_text(
    json.dumps(state, ensure_ascii=False, indent=2) + "\n"
)
PY

set +e
"${SNAKEMAKE}" --snakefile Snakefile --configfile config/config.yaml --cores 4 \
  > "${RUN_DIR}/.bioagent/execution.log" 2>&1
EXEC_STATUS=$?
set -e

python - <<PY
import json
from pathlib import Path
state_path = Path("${RUN_DIR}/.bioagent/state.json")
state = json.loads(state_path.read_text())
state["execution"]["ok"] = ${EXEC_STATUS} == 0
state["execution"]["returncode"] = ${EXEC_STATUS}
state["execution"]["log"] = "${RUN_DIR}/.bioagent/execution.log"
state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n")
PY

cat "${RUN_DIR}/.bioagent/execution.log"
exit "${EXEC_STATUS}"
```

## Input expectations

For workflow1, collect paired-end sample information when the user provides real data:

- `run_id`
- `sample_name`
- absolute path to R1 FASTQ
- absolute path to R2 FASTQ

If required input details are missing, ask the user for them before preparing a run.

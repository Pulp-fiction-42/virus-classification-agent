---
name: bioinformatics-workflow-gatekeeper
description: Use when a user requests a bioinformatics analysis that may match one of the fixed Snakemake workflows workflow1-workflow6 on cyclone. Guides conservative workflow matching, one explicit user confirmation gate, and direct Snakemake execution through the Bash tool.
---

# Bioinformatics Workflow Gatekeeper

Use this skill when a user asks for a bioinformatics analysis that may match one of the fixed Snakemake workflows available on the cyclone server.

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

Do not propose workflow7 or any other workflow.

## Core rule

This skill uses conversational confirmation gates, not Claude Agent SDK lifecycle hooks.

The agent must ask the user for one explicit confirmation before preparing a workflow run and launching the real Snakemake execution.

## Behavior

- Match conservatively. Suggest a workflow only when the user's request clearly fits workflow1-workflow6.
- When suggesting a workflow, explain the match briefly and ask whether the user wants to use that workflow.
- Before the user confirmation, do not call any workflow tool, do not create a run directory, do not write configuration files, and do not run Snakemake.
- After the user confirms the selected workflow, use the Bash tool to create a run directory, copy the selected workflow source, prepare the run-local configuration, and launch the real Snakemake execution.
- Summarize the execution result for the user, including the selected workflow, run directory, execution log, return code, and any obvious next steps.
- If the user rejects the proposed workflow, exit this skill and continue normal conversation.

## Tool boundaries

- Use the Bash tool directly for Snakemake workflow preparation and execution.
- Do not use MCP custom tools for this workflow path.
- Do not edit the source workflow directories under `/data/user/guojindan/Projects/Workflows`.
- The run root is `/data/runs`.
- Work only inside a new run directory under `/data/runs/<run_id>`.
- Copy the workflow source into `/data/runs/<run_id>/workflow` before running it.
- The real Snakemake execution must only happen after explicit user confirmation.

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
    "execution": {"started": True, "ok": None},
}
Path("${RUN_DIR}/.bioagent/state.json").write_text(
    json.dumps(state, ensure_ascii=False, indent=2) + "\n"
)
PY

set +e
"${SNAKEMAKE}" --snakefile Snakefile --configfile config/config.yaml --cores 4 \
  > "${RUN_DIR}/logs/execute.log" 2>&1
EXEC_STATUS=$?
set -e

python - <<PY
import json
from pathlib import Path
state_path = Path("${RUN_DIR}/.bioagent/state.json")
state = json.loads(state_path.read_text())
state["execution"]["ok"] = ${EXEC_STATUS} == 0
state["execution"]["returncode"] = ${EXEC_STATUS}
state["execution"]["log"] = "${RUN_DIR}/logs/execute.log"
state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n")
PY

cat "${RUN_DIR}/logs/execute.log"
exit "${EXEC_STATUS}"
```

## Input expectations

For workflow1, collect paired-end sample information when the user provides real data:

- `run_id`
- `sample_name`
- absolute path to R1 FASTQ
- absolute path to R2 FASTQ

If required input details are missing, ask the user for them before preparing a run.

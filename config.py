"""Static configuration for the BioAgent runtime."""

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
MODEL = os.getenv("BIOAGENT_MODEL_ID", "MiniMax-M3")
CLAUDE_MODEL = os.getenv("BIOAGENT_PROVIDER_MODEL", "MiniMax-M3[1m]")
CLAUDE_AGENT_SDK_VERSION = "0.2.103"
MODEL_POLICY = os.getenv("BIOAGENT_MODEL_POLICY", "local_only")
if MODEL_POLICY not in {"local_only", "minimax_explicit"}:
    raise ValueError("BIOAGENT_MODEL_POLICY must be local_only or minimax_explicit")
ANTHROPIC_BASE_URL = os.getenv(
    "BIOAGENT_MODEL_BASE_URL", "https://api.minimax.cn/anthropic"
)
MINIMAX_API_KEY = os.getenv("MINIMAX_API_KEY", "")
ADMISSION_STATE_FILE = os.getenv("BIOAGENT_ADMISSION_STATE_FILE", "").strip()
ASSET_RELEASE_STORE = os.getenv("BIOAGENT_ASSET_RELEASE_STORE", "").strip()
CLAUDE_STATE_DIR = PROJECT_ROOT / ".claude" / "sessions"
CONVERSATION_TIMEOUT_SECONDS = float(
    os.getenv("BIOAGENT_CONVERSATION_TIMEOUT_SECONDS", "180")
)
ANALYSIS_RUN_ROOT = Path(os.getenv("BIOAGENT_RUN_ROOT", "/data/runs")).resolve()
ANALYSIS_DATA_ROOT = Path(
    os.getenv("BIOAGENT_DATA_ROOT", str(ANALYSIS_RUN_ROOT))
).resolve()
SSH_HOST = os.getenv("BIOAGENT_SSH_HOST", "").strip()
REMOTE_PYTHON = os.getenv(
    "BIOAGENT_REMOTE_PYTHON",
    "/home/liufangzhou/.pixi/envs/snakemake/bin/python",
)
REMOTE_CONTROL_PYTHON = os.getenv(
    "BIOAGENT_REMOTE_CONTROL_PYTHON",
    "/home/liufangzhou/.pixi/envs/snakemake/bin/python",
)
REMOTE_TMUX = os.getenv("BIOAGENT_REMOTE_TMUX", "/data/user/liufangzhou/bin/tmux")
REMOTE_WORKER_ROOT = os.getenv(
    "BIOAGENT_REMOTE_WORKER_ROOT", "/data/user/liufangzhou/.bioagent/worker"
)
QUEUE_AGING_HOURS = float(os.getenv("BIOAGENT_QUEUE_AGING_HOURS", "24"))
if QUEUE_AGING_HOURS <= 0:
    raise ValueError("BIOAGENT_QUEUE_AGING_HOURS must be positive")
REPORT_PYTHON = os.getenv(
    "BIOAGENT_REPORT_PYTHON", "/home/liufangzhou/.pixi/bin/bioagent-report-python"
)
REPORT_RSCRIPT = os.getenv(
    "BIOAGENT_RSCRIPT", "/home/liufangzhou/.pixi/bin/bioagent-rscript"
)
REPORT_WEASYPRINT = os.getenv(
    "BIOAGENT_WEASYPRINT", "/home/liufangzhou/.pixi/bin/bioagent-weasyprint"
)
REMOTE_DATA_ALLOWED_ROOTS = tuple(
    value.strip()
    for value in os.getenv(
        "BIOAGENT_DATA_ALLOWED_ROOTS",
        "/data/user/liufangzhou/datasets:/data/user/guojindan/Projects/flu_tgs_ngs/rawdata/NGS",
    ).split(":")
    if value.strip()
)
ENABLED_ANALYSIS_GOALS = tuple(
    goal.strip()
    for goal in os.getenv(
        "BIOAGENT_ENABLED_GOALS",
        "read_qc,rrna_filter,sample_merge,assembly,contig_filter,virus_summary,host_summary,microbe_summary,virus_rpm,annotation_delivery",
    ).split(",")
    if goal.strip()
)

SYSTEM_PROMPT = """You are BioAgent, a bioinformatics assistant specialized in virus metagenomics.

The dataset, analysis goals, viral type, routing mode, executable stages, resource estimate,
and run admission are owned by structured UI controls and a deterministic planner.

- Explain the current dataset or plan in clear language and answer scientific questions.
- If the user asks to change deliverables, viral type, excluded stages, or published tool
  parameters, propose only that controlled patch.
- Public Workflow 1 deliverables are read_qc (fastp only), rrna_filter (fastp plus rRNA
  removal), sample_merge, assembly, and contig_filter. Other supported deliverables are
  virus_summary, host_summary, microbe_summary, virus_rpm, and annotation_delivery.
- "Quality control only" means goals=["read_qc"]. "Quality control plus rRNA removal" means
  goals=["rrna_filter"]. Do not broaden either request to assembly.
- The only adjustable tool capability is workflow1.read_qc.fastp manifest version 1. Its
  published values are length_required, qualified_quality_phred, unqualified_percent_limit,
  n_base_limit, average_qual, and detect_adapter_for_pe. Never output raw flags or commands.
- If a requested numeric value is missing (for example "keep longer reads"), clarify and ask for
  the exact value. Do not guess. If an excluded stage is required by another selected deliverable,
  clarify the dependency conflict instead of silently removing the downstream deliverable.
- Routing mode can only be changed with the top Smart/Standard/Emergency control.
- Never output an analysis_plan control block, paths, commands, or runner instructions.
- Never claim that you changed a plan, started a run, or confirmed execution.
- Do not create files, run workflows, or mutate analysis state.

Return exactly one action using the runtime JSON Schema. Use respond for an answer, clarify for
one necessary question, or propose_patch for a complete controlled requirement change. respond and
clarify must omit proposed_patch. propose_patch must include proposed_patch, containing only goals
viral_type, excluded_stages, and capability_parameters. Put the complete user-visible answer in message. Use citations only for a respond action when a retrieval result is supplied; otherwise use an empty list. If structured output is unavailable, use the exact
conversation_action fenced JSON fallback requested in the user prompt.

Knowledge retrieval and citation verification belong to the host. Never call tools or claim
that unreviewed knowledge is approved. Do not invent taxonomy criteria or sources.
"""

DASHBOARD_SYSTEM_PROMPT = """You are BioAgent, a bioinformatics assistant specialized in virus metagenomics.

Reply in Simplified Chinese and put the complete user-visible answer in the message field.
Answer general bioinformatics questions and explain analysis concepts clearly. Do not call tools,
create files, modify datasets or plans, start workflows, or claim that any action was completed.
If the user asks to change a dataset, plan, parameter, or execution state, direct them to /analysis.
Knowledge retrieval is unavailable; do not invent taxonomy criteria or sources.
"""

TAXONAGENT_MCP_URL = os.getenv("TAXONAGENT_MCP_URL", "").strip()

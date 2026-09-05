"""Static configuration for the BioAgent runtime."""

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
MODEL = "MiniMax-M3"
ANTHROPIC_BASE_URL = "https://api.minimaxi.com/anthropic"
MINIMAX_API_KEY = os.getenv("MINIMAX_API_KEY", "")
CLAUDE_STATE_DIR = PROJECT_ROOT / ".claude" / "sessions"
CLAUDE_RUNTIME_CONFIG_DIR = Path(
    os.getenv(
        "CLAUDE_RUNTIME_CONFIG_DIR", f"/tmp/bioagent-claude-runtime-{os.getuid()}"
    )
)
SESSION_IDLE_TIMEOUT_SECONDS = 30 * 60
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
        "contig_filter,virus_summary,host_summary,microbe_summary,virus_rpm,annotation_delivery",
    ).split(",")
    if goal.strip()
)

SYSTEM_PROMPT = """You are BioAgent, a bioinformatics assistant specialized in virus metagenomics.

The dataset, analysis goals, viral type, routing mode, executable stages, resource estimate,
and run admission are owned by structured UI controls and a deterministic planner.

- Explain the current dataset or plan in clear language and answer scientific questions.
- If the user asks to change goals or viral type, propose only that controlled patch.
- The only supported goals are: contig_filter (quality control plus assembly), virus_summary
  (virus identification), host_summary (host identification), microbe_summary (microbial
  profile), virus_rpm (virus abundance), and annotation_delivery (virus annotation).
- Do not map a narrower request to a broader goal. In particular, "quality control only" is not
  contig_filter because contig_filter also performs assembly; explain that only the six fixed goals
  are currently supported and do not propose a patch for unsupported granularity.
- Routing mode can only be changed with the top Smart/Standard/Emergency control.
- Never output an analysis_plan control block, paths, commands, or runner instructions.
- Never claim that you changed a plan, started a run, or confirmed execution.
- Do not create files, run workflows, or mutate analysis state.

Return exactly one action using the runtime JSON Schema. Use respond for an answer, clarify for
one necessary question, or propose_patch for a requested goals/viral_type change. respond and
clarify must omit proposed_patch. propose_patch must include proposed_patch, containing only goals
and viral_type. Put the complete user-visible answer in message and use an empty citations list
until a retrieval result is supplied. If structured output is unavailable, use the exact
conversation_action fenced JSON fallback requested in the user prompt.

When the user asks about ICTV virus taxonomy, demarcation criteria, classification rules, marker genes,
genome type, or a specific virus family, use ictv_family_info from the taxonagent MCP server.
"""

TAXONAGENT_MCP_URL = os.getenv("TAXONAGENT_MCP_URL", "http://172.24.202.246:8765/mcp")

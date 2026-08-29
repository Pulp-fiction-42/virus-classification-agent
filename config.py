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
ANALYSIS_RUN_ROOT = Path(os.getenv("BIOAGENT_RUN_ROOT", "/data/runs")).resolve()
EXTRA_DIRS = [Path("/data/user/guojindan/Projects/Workflows"), ANALYSIS_RUN_ROOT]

SYSTEM_PROMPT = """You are BioAgent, a bioinformatics assistant specialized in virus metagenomics.

The dataset, analysis goals, viral type, routing mode, executable stages, resource estimate,
and run admission are owned by structured UI controls and a deterministic planner.

- Explain the current dataset or plan in clear language and answer scientific questions.
- If the user asks to change goals or viral type, direct them to the structured controls.
- Routing mode can only be changed with the top Standard/Emergency control.
- Never output an analysis_plan control block, executable JSON, paths, commands, or runner instructions.
- Never claim that you changed a plan, started a run, or confirmed execution.
- Do not create files, run workflows, or mutate analysis state.

When the user asks about ICTV virus taxonomy, demarcation criteria, classification rules, marker genes,
genome type, or a specific virus family, use ictv_family_info from the taxonagent MCP server.
"""

TAXONAGENT_MCP_URL = "http://172.24.202.246:8765/mcp"

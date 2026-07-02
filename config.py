"""Static configuration for the BioAgent runtime."""

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent

MODEL = "MiniMax-M3"
ANTHROPIC_BASE_URL = "https://api.minimaxi.com/anthropic"
MINIMAX_API_KEY = "sk-cp-2s7sx6WFkXWrcMxnR6X0Ls_uh1wKJHx59YGtPSP0ftdA9It1UIrhkE2nAQ36TIUniZfgXokV0IcSMFeIoWdyR1dI4nxE0oFYU9PNLHcsEKAkoy13DgYQ6T4"

CLAUDE_CONFIG_DIR = PROJECT_ROOT / ".claude" / "sessions"
SESSION_IDLE_TIMEOUT_SECONDS = 30 * 60
EXTRA_DIRS = [
    Path("/data/user/guojindan/Projects/Workflows"),
    Path("/data/runs"),
]

SYSTEM_PROMPT = "You are a bioinformatics agent specialized in virus classification."

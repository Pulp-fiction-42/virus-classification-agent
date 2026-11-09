from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


def test_model_identity_and_endpoint_are_environment_configurable() -> None:
    agent_root = Path(__file__).parents[1]
    environment = os.environ.copy()
    environment.update(
        {
            "BIOAGENT_MODEL_ID": "profile-model",
            "BIOAGENT_PROVIDER_MODEL": "provider-model",
            "BIOAGENT_MODEL_BASE_URL": "https://model.invalid/anthropic",
            "BIOAGENT_MODEL_POLICY": "minimax_explicit",
            "TAXONAGENT_MCP_URL": "",
        }
    )
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import json, config; "
                "print(json.dumps([config.MODEL, config.CLAUDE_MODEL, "
                "config.ANTHROPIC_BASE_URL, config.MODEL_POLICY]))"
            ),
        ],
        cwd=agent_root,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )

    assert json.loads(result.stdout) == [
        "profile-model",
        "provider-model",
        "https://model.invalid/anthropic",
        "minimax_explicit",
    ]

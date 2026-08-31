#!/usr/bin/env bash

AGENT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
if [ -f "$AGENT_DIR/.env" ]; then
    set -a
    . "$AGENT_DIR/.env"
    set +a
fi

AGENT_PORT="${AGENT_PORT:-8001}"
export BIOAGENT_RUN_ROOT="${BIOAGENT_RUN_ROOT:-"/data/runs"}"
mkdir -p "$BIOAGENT_RUN_ROOT"
cd "$AGENT_DIR"
exec uvicorn adapter:app --host 127.0.0.1 --port "$AGENT_PORT"

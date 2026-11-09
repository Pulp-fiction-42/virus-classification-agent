#!/usr/bin/env bash

AGENT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
if [ -f "$AGENT_DIR/.env" ]; then
    set -a
    . "$AGENT_DIR/.env"
    set +a
fi
if [ -n "${BIOAGENT_DEPLOYMENT_PROFILE:-}" ]; then
    PROFILE_LOADER="$AGENT_DIR/../deployment/load-profile.sh"
    if [ ! -f "$PROFILE_LOADER" ]; then
        echo "找不到 DeploymentProfile loader：$PROFILE_LOADER" >&2
        exit 1
    fi
    # shellcheck disable=SC1090
    . "$PROFILE_LOADER"
    bioagent_load_profile agent
fi

AGENT_HOST="${AGENT_HOST:-127.0.0.1}"
AGENT_PORT="${AGENT_PORT:-8001}"
export BIOAGENT_RUN_ROOT="${BIOAGENT_RUN_ROOT:-"/data/runs"}"
if [ -z "${BIOAGENT_SSH_HOST:-}" ]; then
    mkdir -p "$BIOAGENT_RUN_ROOT"
fi
cd "$AGENT_DIR"
exec uvicorn adapter:app --host "$AGENT_HOST" --port "$AGENT_PORT"

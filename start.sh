AGENT_PORT="${AGENT_PORT:-8001}"
export BIOAGENT_RUN_ROOT="${BIOAGENT_RUN_ROOT:-"/data/runs"}"
mkdir -p "$BIOAGENT_RUN_ROOT"
uvicorn adapter:app --host 127.0.0.1 --port $AGENT_PORT

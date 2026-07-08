AGENT_PORT="${AGENT_PORT:-8001}"
uvicorn adapter:app --host 127.0.0.1 --port $AGENT_PORT

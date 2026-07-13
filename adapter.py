"""OpenAI-compatible HTTP adapter for the BioAgent runtime."""

from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel

from runtime import (
    EXECUTE_MODE,
    PLAN_MODE,
    InvalidSessionError,
    OutputItemAdded,
    SessionError,
    SessionManager,
    SessionStateError,
    TextDelta,
)


app = FastAPI()
sessions = SessionManager()
log = logging.getLogger(__name__)


class ChatRequest(BaseModel):
    model: str = "BioAgent"
    messages: list[dict]
    stream: bool = True
    metadata: dict[str, Any] | None = None


def completion_id() -> str:
    return f"chatcmpl-{uuid.uuid4().hex[:8]}"


def sse(chunk: str, response_id: str) -> str:
    payload = {
        "id": response_id,
        "object": "chat.completion.chunk",
        "created": int(time.time()),
        "model": "BioAgent",
        "choices": [{"index": 0, "delta": {"content": chunk}, "finish_reason": None}],
    }
    return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"


def output_item_sse(item: dict, response_id: str) -> str:
    payload = {
        "type": "response.output_item.added",
        "response_id": response_id,
        "output_index": 0,
        "item": item,
    }
    return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"


def background_task(req: ChatRequest, request: Request) -> str | None:
    task = request.headers.get("X-OpenWebUI-Task")
    if not task and req.metadata:
        value = req.metadata.get("task")
        task = value if isinstance(value, str) else None
    return task.strip() if task and task.strip() else None


def error_sse(exc: Exception, response_id: str) -> str:
    payload = {
        "id": response_id,
        "object": "error",
        "created": int(time.time()),
        "model": "BioAgent",
        "error": {
            "message": str(exc) or exc.__class__.__name__,
            "type": exc.__class__.__name__,
        },
    }
    return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"


def latest_user_text(messages: list[dict]) -> str:
    for message in reversed(messages):
        if message.get("role") == "user":
            content = message.get("content")
            if isinstance(content, str):
                return content.strip()
            if isinstance(content, list):
                parts = []
                for item in content:
                    if isinstance(item, dict) and item.get("type") == "text":
                        text = item.get("text")
                        if isinstance(text, str) and text.strip():
                            parts.append(text.strip())
                return "\n".join(parts).strip()
    return ""


def completion_response(content: str, response_id: str) -> JSONResponse:
    return JSONResponse(
        {
            "id": response_id,
            "object": "chat.completion",
            "created": int(time.time()),
            "model": "BioAgent",
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": content},
                    "finish_reason": "stop",
                }
            ],
        }
    )


def streaming_text_response(content: str, response_id: str) -> StreamingResponse:
    async def stream():
        yield sse(content, response_id)
        yield "data: [DONE]\n\n"

    return StreamingResponse(stream(), media_type="text/event-stream")


@app.get("/v1/models")
@app.get("/models")
async def list_models():
    return {
        "object": "list",
        "data": [{"id": "BioAgent", "object": "model", "owned_by": "local"}],
    }


@app.post("/v1/chat/completions")
@app.post("/chat/completions")
async def chat(req: ChatRequest, request: Request):
    chat_id = request.headers.get("X-OpenWebUI-Chat-Id")
    task = background_task(req, request)
    command = latest_user_text(req.messages)
    response_id = completion_id()

    if not task and command in {"/plan", "/execute"}:
        mode = PLAN_MODE if command == "/plan" else EXECUTE_MODE
        try:
            await sessions.set_mode(chat_id, mode)
        except InvalidSessionError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except SessionStateError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

        content = f"已切换到 {mode} mode"
        if not req.stream:
            return completion_response(content, response_id)
        return streaming_text_response(content, response_id)

    if not req.stream:
        try:
            events = (
                sessions.stream_ephemeral_turn(req.messages)
                if task
                else sessions.stream_turn(req.messages, chat_id)
            )
            parts = []
            async for event in events:
                if isinstance(event, TextDelta):
                    parts.append(event.text)
            content = "".join(parts)
        except InvalidSessionError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except (SessionError, OSError) as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        return completion_response(content, response_id)

    if not task:
        try:
            await sessions.ensure_session(chat_id)
        except InvalidSessionError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except SessionStateError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    async def stream():
        try:
            events = (
                sessions.stream_ephemeral_turn(req.messages)
                if task
                else sessions.stream_turn(req.messages, chat_id)
            )
            async for event in events:
                if isinstance(event, TextDelta):
                    yield sse(event.text, response_id)
                elif isinstance(event, OutputItemAdded):
                    yield output_item_sse(event.item, response_id)
        except Exception as exc:
            log.exception("BioAgent stream failed")
            yield error_sse(exc, response_id)
        yield "data: [DONE]\n\n"

    return StreamingResponse(stream(), media_type="text/event-stream")


@app.on_event("shutdown")
async def shutdown_sessions():
    await sessions.close_all()

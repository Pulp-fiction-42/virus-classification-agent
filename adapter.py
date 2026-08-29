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

from deterministic_planner import DeterministicPlanner, PlannerError

from runtime import (
    InvalidSessionError,
    OutputItemAdded,
    SessionError,
    SessionManager,
    SessionStateError,
    TextDelta,
)


app = FastAPI()
sessions = SessionManager()
planner = DeterministicPlanner(sessions)
log = logging.getLogger(__name__)


class ChatRequest(BaseModel):
    model: str = "BioAgent"
    messages: list[dict]
    stream: bool = True
    metadata: dict[str, Any] | None = None


class CompilePlanRequest(BaseModel):
    dataset: dict[str, Any]
    goals: list[str]
    options: dict[str, Any] | None = None
    routing: dict[str, Any] | None = None


class StartRunRequest(BaseModel):
    plan: dict[str, Any]
    dataset: dict[str, Any]
    chat_id: str
    plan_id: str
    plan_revision: int
    run_id: str


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


def analysis_routing(req: ChatRequest) -> dict[str, Any] | None:
    metadata = req.metadata if isinstance(req.metadata, dict) else {}
    chat_meta = metadata.get("chat_meta")
    routing = chat_meta.get("analysis_routing") if isinstance(chat_meta, dict) else None
    return routing if isinstance(routing, dict) else None


def analysis_dataset(req: ChatRequest) -> dict[str, Any] | None:
    metadata = req.metadata if isinstance(req.metadata, dict) else {}
    chat_meta = metadata.get("chat_meta")
    dataset = chat_meta.get("analysis_dataset") if isinstance(chat_meta, dict) else None
    return dataset if isinstance(dataset, dict) else None


def analysis_plan_summary(req: ChatRequest) -> dict[str, Any] | None:
    metadata = req.metadata if isinstance(req.metadata, dict) else {}
    chat_meta = metadata.get("chat_meta")
    plan = (
        chat_meta.get("analysis_plan_summary") if isinstance(chat_meta, dict) else None
    )
    return plan if isinstance(plan, dict) else None


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


@app.post("/v1/analysis/plans/compile")
async def compile_analysis_plan(req: CompilePlanRequest):
    try:
        return planner.compile(
            dataset=req.dataset,
            goals=req.goals,
            options=req.options,
            routing=req.routing,
        )
    except PlannerError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@app.post("/v1/analysis/runs/start")
async def start_analysis_run(req: StartRunRequest):
    try:
        return await planner.start(
            persisted_plan=req.plan,
            dataset=req.dataset,
            chat_id=req.chat_id,
            plan_id=req.plan_id,
            plan_revision=req.plan_revision,
            run_id=req.run_id,
        )
    except PlannerError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except SessionStateError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/v1/chat/completions")
@app.post("/chat/completions")
async def chat(req: ChatRequest, request: Request):
    chat_id = request.headers.get("X-OpenWebUI-Chat-Id")
    task = background_task(req, request)
    routing = analysis_routing(req)
    dataset = analysis_dataset(req)
    plan_summary = analysis_plan_summary(req)
    response_id = completion_id()

    if not req.stream:
        try:
            events = (
                sessions.stream_ephemeral_turn(req.messages)
                if task
                else (
                    sessions.stream_turn(
                        req.messages, chat_id, routing, dataset, plan_summary
                    )
                    if routing
                    else sessions.stream_turn(
                        req.messages,
                        chat_id,
                        dataset_context=dataset,
                        plan_context=plan_summary,
                    )
                )
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
                else (
                    sessions.stream_turn(
                        req.messages, chat_id, routing, dataset, plan_summary
                    )
                    if routing
                    else sessions.stream_turn(
                        req.messages,
                        chat_id,
                        dataset_context=dataset,
                        plan_context=plan_summary,
                    )
                )
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

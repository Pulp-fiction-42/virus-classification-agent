"""OpenAI-compatible HTTP adapter for the BioAgent runtime."""

from __future__ import annotations

import base64
import json
import logging
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field

import config
from admission_control import AdmissionUnavailable, admission_status, require_admission
from asset_release_binding import (
    load_active_release_binding,
    release_binding_status,
)
from conversation_events import (
    InvalidSessionError,
    OutputItemAdded,
    SessionError,
    TextDelta,
)
from deterministic_planner import DeterministicPlanner, PlannerError
from deterministic_runtime import DeterministicRuntime, RemoteDeterministicRuntime
from priority_reports import (
    build_fact_packet,
    build_reasoning_plan,
    load_literature,
    reasoning_request,
)
from provider import (
    MiniMaxProvider,
    ProviderRequest,
    claude_agent_sdk_ready,
    claude_agent_sdk_version,
)
from remote_client import (
    RemoteConnectionError,
    RemoteExecutionError,
    RemoteWorkerClient,
)
from run_status_cache import RunStatusCache
from runtime_errors import ConversationPublicError, SessionStateError
from workflow_catalog import capability_catalog

app = FastAPI()
remote_worker = RemoteWorkerClient()
deterministic_runtime = (
    RemoteDeterministicRuntime(remote_worker)
    if remote_worker.enabled
    else DeterministicRuntime()
)
planner = DeterministicPlanner(
    deterministic_runtime,
    enabled_goals=config.ENABLED_ANALYSIS_GOALS,
    release_resolver=(
        (lambda: load_active_release_binding(Path(config.ASSET_RELEASE_STORE)))
        if config.ASSET_RELEASE_STORE
        else None
    ),
)
sessions: Any | None = None
log = logging.getLogger(__name__)
CONVERSATION_ERROR = "对话模型暂时不可用，请稍后重试；数据集、方案与执行功能不受影响。"
remote_snapshot_cache: dict[str, dict[str, Any]] = {}
run_status_cache = RunStatusCache()


def _warm_remote_worker() -> None:
    try:
        remote_worker.invoke_sync("health")
    except RemoteExecutionError:
        log.info("Cyclone worker warm-up did not complete; it will retry on demand")


@app.on_event("startup")
async def warm_remote_worker():
    conversation_sessions()
    if remote_worker.enabled:
        threading.Thread(
            target=_warm_remote_worker,
            name="bioagent-cyclone-warmup",
            daemon=True,
        ).start()


def conversation_sessions() -> Any:
    global sessions
    if sessions is None:
        from runtime import SessionManager

        sessions = SessionManager()
    return sessions


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


class QueueContext(BaseModel):
    model_config = ConfigDict(extra="forbid")

    project_id: str
    project_priority: Literal["low", "normal", "high"]


class StartRunRequest(BaseModel):
    plan: dict[str, Any]
    dataset: dict[str, Any]
    chat_id: str
    plan_id: str
    plan_revision: int
    run_id: str
    queue_context: QueueContext


class ResumeRunRequest(BaseModel):
    plan: dict[str, Any]
    dataset: dict[str, Any]
    chat_id: str
    plan_id: str
    plan_revision: int
    run_id: str


class RunStatusSummaryRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_ids: list[str] = Field(min_length=1, max_length=50)


class RemoteSourcesRequest(BaseModel):
    paths: list[str]


class RemotePreflightRequest(BaseModel):
    sources: list[dict[str, Any]]
    artifacts: list[dict[str, Any]]
    full: bool = False


class RemoteFingerprintRequest(BaseModel):
    sources: list[dict[str, Any]]


class RemoteEvidenceRequest(BaseModel):
    run_dir: str
    telemetry: dict[str, Any]


class CandidateAssessmentsRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_dir: str
    candidate_ids: list[str] = Field(min_length=1, max_length=5)


class PriorityReportRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_dir: str
    baseline_run_dir: str | None = None


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


def is_analysis_chat(req: ChatRequest) -> bool:
    metadata = req.metadata if isinstance(req.metadata, dict) else {}
    chat_meta = metadata.get("chat_meta")
    return isinstance(chat_meta, dict) and chat_meta.get("source") == "analysis"


def error_sse(exc: Exception, response_id: str) -> str:
    payload = {
        "id": response_id,
        "object": "error",
        "created": int(time.time()),
        "model": "BioAgent",
        "error": {
            "message": str(exc) or exc.__class__.__name__,
            "type": exc.__class__.__name__,
            **({"code": exc.code} if isinstance(exc, ConversationPublicError) else {}),
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


@app.get("/v1/analysis/health")
@app.get("/analysis/health")
async def analysis_health():
    knowledge = conversation_sessions().knowledge_status()
    remote = {"status": "local"}
    if remote_worker.enabled:
        try:
            remote = {"status": "ready", **(await remote_worker.invoke("health"))}
        except RemoteExecutionError:
            remote = {"status": "unavailable"}
    sdk_version = claude_agent_sdk_version()
    model_ready = (
        config.MODEL_POLICY == "minimax_explicit"
        and bool(config.MINIMAX_API_KEY)
        and claude_agent_sdk_ready()
    )
    return {
        "deterministic": {
            "status": "ready",
            "enabled_goals": list(planner.enabled_goals),
        },
        "conversation_model": {
            "id": "BioAgent",
            "status": "ready" if model_ready else "unavailable",
            "provider": config.MODEL,
            "policy": config.MODEL_POLICY,
            "sdk_version": sdk_version,
            "required_sdk_version": config.CLAUDE_AGENT_SDK_VERSION,
        },
        "knowledge": knowledge,
        "execution": remote,
        "admission": admission_status(config.ADMISSION_STATE_FILE),
        "asset_release": release_binding_status(
            Path(config.ASSET_RELEASE_STORE) if config.ASSET_RELEASE_STORE else None
        ),
    }


@app.get("/v1/analysis/capabilities")
@app.get("/analysis/capabilities")
async def analysis_capabilities():
    return capability_catalog()


@app.get("/v1/analysis/admission")
@app.get("/analysis/admission")
async def analysis_admission():
    return admission_status(config.ADMISSION_STATE_FILE)


def _remote_required() -> RemoteWorkerClient:
    if not remote_worker.enabled:
        raise HTTPException(
            status_code=503, detail="Cyclone SSH backend is not configured"
        )
    return remote_worker


async def _remote_call(operation: str, **payload: Any) -> dict[str, Any]:
    try:
        result = await _remote_required().invoke(operation, **payload)
        if operation == "snapshot" and result.get("found") is True:
            if isinstance(result.get("observation"), dict):
                run_status_cache.observe(result["observation"])
            state = result.get("state") if isinstance(result.get("state"), dict) else {}
            for key in (
                payload.get("run_id"),
                payload.get("run_dir"),
                state.get("run_id"),
                state.get("run_dir"),
            ):
                if isinstance(key, str) and key:
                    remote_snapshot_cache[key] = result
        return result
    except RemoteConnectionError as exc:
        if operation == "snapshot":
            cached = remote_snapshot_cache.get(
                str(payload.get("run_id") or payload.get("run_dir") or "")
            )
            if cached:
                result = {**cached, "connection_lost": True, "stale": True}
                if isinstance(cached.get("observation"), dict):
                    result["observation"] = {
                        **cached["observation"],
                        "connection_lost": True,
                        "stale": True,
                    }
                return result
        raise HTTPException(
            status_code=503,
            detail={
                "code": "CYCLONE_UNAVAILABLE",
                "message": str(exc),
                "retryable": True,
            },
        ) from exc
    except RemoteExecutionError as exc:
        if operation == "candidate_assessments":
            raw = str(exc)
            code, separator, message = raw.partition(":")
            code = code if separator else "ASSESSMENT_FAILED"
            raise HTTPException(
                status_code=503 if code.endswith("_UNAVAILABLE") else 422,
                detail={
                    "code": code,
                    "message": message.strip() if separator else raw,
                    "retryable": code.endswith("_UNAVAILABLE"),
                },
            ) from exc
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@app.get("/v1/analysis/files")
@app.get("/analysis/files")
async def remote_analysis_files(path: str = "/", cursor: int = 0, limit: int = 100):
    return await _remote_call("list_files", path=path, cursor=cursor, limit=limit)


@app.post("/v1/analysis/datasets/resolve")
@app.post("/analysis/datasets/resolve")
async def remote_analysis_sources(req: RemoteSourcesRequest):
    return await _remote_call("resolve_sources", paths=req.paths)


@app.post("/v1/analysis/datasets/preflight")
@app.post("/analysis/datasets/preflight")
async def remote_analysis_preflight(req: RemotePreflightRequest):
    return await _remote_call(
        "preflight", sources=req.sources, artifacts=req.artifacts, full=req.full
    )


@app.post("/v1/analysis/datasets/fingerprints")
@app.post("/analysis/datasets/fingerprints")
async def remote_analysis_fingerprints(req: RemoteFingerprintRequest):
    return await _remote_call("fingerprints_match", sources=req.sources)


@app.get("/v1/analysis/runs/snapshot")
@app.get("/analysis/runs/snapshot")
async def remote_analysis_snapshot(
    run_id: str | None = None,
    run_dir: str | None = None,
    report_kind: Literal["preliminary", "final"] | None = None,
):
    return await _remote_call(
        "snapshot", run_id=run_id, run_dir=run_dir, report_kind=report_kind
    )


@app.post("/v1/analysis/runs/status-summary")
@app.post("/analysis/runs/status-summary")
async def remote_analysis_status_summary(req: RunStatusSummaryRequest):
    from deterministic_planner import RUN_ID_RE

    if any(not RUN_ID_RE.fullmatch(run_id) for run_id in req.run_ids):
        raise HTTPException(status_code=422, detail="invalid analysis run ID")
    worker = _remote_required()
    return await run_status_cache.summary(list(dict.fromkeys(req.run_ids)), worker.invoke)


@app.post("/v1/analysis/runs/candidate-assessments")
@app.post("/analysis/runs/candidate-assessments")
async def remote_candidate_assessments(req: CandidateAssessmentsRequest):
    return await _remote_call(
        "candidate_assessments",
        run_dir=req.run_dir,
        candidate_ids=req.candidate_ids,
    )


@app.get("/v1/analysis/runs/delivery-report")
@app.get("/analysis/runs/delivery-report")
async def remote_analysis_delivery_report(run_dir: str, format: Literal["html", "pdf"]):
    result = await _remote_call("delivery_artifact", run_dir=run_dir, format=format)
    try:
        content = base64.b64decode(result["content_base64"], validate=True)
    except (KeyError, ValueError) as exc:
        raise HTTPException(
            status_code=502, detail="Cyclone returned an invalid report artifact"
        ) from exc
    return Response(
        content=content,
        media_type=str(
            result.get("media_type")
            or ("application/pdf" if format == "pdf" else "text/html; charset=utf-8")
        ),
        headers={"X-Content-Type-Options": "nosniff"},
    )


@app.post("/v1/analysis/runs/priority-reports")
@app.post("/analysis/runs/priority-reports")
async def generate_priority_report_artifacts(req: PriorityReportRequest):
    snapshot = await _remote_call(
        "snapshot", run_dir=req.run_dir, report_kind="final"
    )
    state = snapshot.get("state")
    telemetry = snapshot.get("telemetry")
    if not isinstance(state, dict) or not isinstance(telemetry, dict):
        raise HTTPException(status_code=404, detail="Analysis run is unavailable")
    if (state.get("execution") or {}).get("status") != "completed":
        raise HTTPException(status_code=409, detail="Analysis run is not completed")
    stage_ids = {
        stage.get("id")
        for stage in (state.get("execution_spec") or {}).get("stages") or []
        if isinstance(stage, dict)
    }
    existing = state.get("artifact_reports") or {}
    needs_scientific = "virus_summary" in stage_ids and (
        (existing.get("scientific") or {}).get("status") != "ready"
    )
    reasoning_plan = None
    if needs_scientific:
        packet = build_fact_packet(state, telemetry)
        literature = load_literature()
        context, schema, instructions = reasoning_request(packet, literature)
        provider = MiniMaxProvider(
            allow_cloud=config.MODEL_POLICY == "minimax_explicit"
        )
        generated = await provider.generate(
            ProviderRequest(
                task_kind="evidence_explanation",
                output_schema=schema,
                context=context,
                instructions=instructions,
                timeout_seconds=config.CONVERSATION_TIMEOUT_SECONDS,
            )
        )
        if generated.output is None:
            raise HTTPException(
                status_code=503,
                detail={
                    "code": generated.error_code or "MODEL_UNAVAILABLE",
                    "message": "Scientific reasoning generation is unavailable",
                    "retryable": True,
                },
            )
        reasoning_plan = build_reasoning_plan(
            packet,
            literature,
            list(generated.output.get("claim_ids") or []),
            provider=generated.provider,
            model=generated.model,
            backend_version=generated.backend_version,
        )
    return await _remote_call(
        "generate_artifact_reports",
        run_dir=req.run_dir,
        baseline_run_dir=req.baseline_run_dir,
        reasoning_plan=reasoning_plan,
    )


@app.get("/v1/analysis/runs/artifact-report")
@app.get("/analysis/runs/artifact-report")
async def remote_analysis_artifact_report(
    run_dir: str,
    kind: Literal["scientific", "tool_demo", "skill_demo"],
    format: Literal[
        "html",
        "pdf",
        "json",
        "facts_json",
        "literature_json",
        "reasoning_json",
    ],
):
    result = await _remote_call(
        "artifact_report", run_dir=run_dir, kind=kind, format=format
    )
    try:
        content = base64.b64decode(result["content_base64"], validate=True)
    except (KeyError, ValueError) as exc:
        raise HTTPException(
            status_code=502, detail="Cyclone returned an invalid report artifact"
        ) from exc
    return Response(
        content=content,
        media_type=str(result.get("media_type") or "application/octet-stream"),
        headers={"X-Content-Type-Options": "nosniff"},
    )


@app.get("/v1/analysis/resources")
@app.get("/analysis/resources")
async def remote_analysis_resources():
    return await _remote_call("resources")


@app.post("/v1/analysis/evidence/verify")
@app.post("/analysis/evidence/verify")
async def remote_analysis_evidence(req: RemoteEvidenceRequest):
    return await _remote_call(
        "verify_evidence", run_dir=req.run_dir, telemetry=req.telemetry
    )


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
        raise HTTPException(
            status_code=422,
            detail={"code": exc.code, "message": str(exc), "retryable": False},
        ) from exc


@app.post("/v1/analysis/runs/start")
async def start_analysis_run(req: StartRunRequest):
    try:
        require_admission(config.ADMISSION_STATE_FILE)
        result = await planner.start(
            persisted_plan=req.plan,
            dataset=req.dataset,
            chat_id=req.chat_id,
            plan_id=req.plan_id,
            plan_revision=req.plan_revision,
            run_id=req.run_id,
            queue_context=req.queue_context.model_dump(),
        )
        if isinstance(result.get("observation"), dict):
            run_status_cache.observe(result["observation"])
        return result
    except AdmissionUnavailable as exc:
        raise HTTPException(
            status_code=503,
            detail={"code": exc.code, "message": str(exc), "retryable": True},
        ) from exc
    except RemoteConnectionError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "code": "CYCLONE_UNAVAILABLE",
                "message": str(exc),
                "retryable": True,
            },
        ) from exc
    except RemoteExecutionError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except PlannerError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except SessionStateError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/v1/analysis/runs/resume")
async def resume_analysis_run(req: ResumeRunRequest):
    try:
        require_admission(config.ADMISSION_STATE_FILE)
        result = await planner.resume(
            persisted_plan=req.plan,
            dataset=req.dataset,
            chat_id=req.chat_id,
            plan_id=req.plan_id,
            plan_revision=req.plan_revision,
            run_id=req.run_id,
        )
        if isinstance(result.get("observation"), dict):
            run_status_cache.observe(result["observation"])
        return result
    except AdmissionUnavailable as exc:
        raise HTTPException(
            status_code=503,
            detail={"code": exc.code, "message": str(exc), "retryable": True},
        ) from exc
    except RemoteConnectionError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "code": "RECOVERY_CHECK_UNAVAILABLE",
                "message": str(exc),
                "retryable": True,
            },
        ) from exc
    except RemoteExecutionError as exc:
        raw = str(exc)
        code, separator, message = raw.partition(":")
        unavailable = separator and code == "RECOVERY_CHECK_UNAVAILABLE"
        raise HTTPException(
            status_code=503 if unavailable else 409,
            detail={
                "code": code if separator and code.startswith("RECOVERY_") else "RECOVERY_REJECTED",
                "message": message.strip() if separator else raw,
                "retryable": bool(unavailable),
            },
        ) from exc
    except (PlannerError, SessionStateError) as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "RECOVERY_CONTEXT_CHANGED",
                "message": str(exc),
                "retryable": False,
            },
        ) from exc


@app.post("/v1/chat/completions")
@app.post("/chat/completions")
async def chat(req: ChatRequest, request: Request):
    chat_id = request.headers.get("X-OpenWebUI-Chat-Id")
    task = background_task(req, request)
    routing = analysis_routing(req)
    dataset = analysis_dataset(req)
    plan_summary = analysis_plan_summary(req)
    analysis = is_analysis_chat(req)
    response_id = completion_id()
    sessions = conversation_sessions()

    if not req.stream:
        try:
            events = (
                sessions.stream_ephemeral_turn(req.messages)
                if task
                else (
                    sessions.stream_turn(
                        req.messages,
                        chat_id,
                        routing,
                        dataset,
                        plan_summary,
                        analysis=analysis,
                    )
                    if routing
                    else sessions.stream_turn(
                        req.messages,
                        chat_id,
                        dataset_context=dataset,
                        plan_context=plan_summary,
                        analysis=analysis,
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
        except ConversationPublicError as exc:
            log.info("BioAgent conversation rejected safely: %s", exc.code)
            raise HTTPException(
                status_code=502,
                detail={"code": exc.code, "message": str(exc)},
            ) from exc
        except (SessionError, SessionStateError, OSError) as exc:
            log.exception("BioAgent conversation failed")
            raise HTTPException(status_code=502, detail=CONVERSATION_ERROR) from exc
        except Exception as exc:
            log.exception("BioAgent conversation failed")
            raise HTTPException(status_code=502, detail=CONVERSATION_ERROR) from exc
        return completion_response(content, response_id)

    if not task:
        try:
            await sessions.ensure_session(chat_id)
        except InvalidSessionError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except ConversationPublicError as exc:
            log.info("BioAgent conversation session unavailable: %s", exc.code)
            raise HTTPException(
                status_code=503,
                detail={"code": exc.code, "message": str(exc)},
            ) from exc
        except SessionStateError as exc:
            log.exception("BioAgent conversation session failed")
            raise HTTPException(status_code=503, detail=CONVERSATION_ERROR) from exc

    async def stream():
        try:
            events = (
                sessions.stream_ephemeral_turn(req.messages)
                if task
                else (
                    sessions.stream_turn(
                        req.messages,
                        chat_id,
                        routing,
                        dataset,
                        plan_summary,
                        analysis=analysis,
                    )
                    if routing
                    else sessions.stream_turn(
                        req.messages,
                        chat_id,
                        dataset_context=dataset,
                        plan_context=plan_summary,
                        analysis=analysis,
                    )
                )
            )
            async for event in events:
                if isinstance(event, TextDelta):
                    yield sse(event.text, response_id)
                elif isinstance(event, OutputItemAdded):
                    yield output_item_sse(event.item, response_id)
        except ConversationPublicError as exc:
            log.info("BioAgent stream rejected safely: %s", exc.code)
            yield error_sse(exc, response_id)
        except Exception:
            log.exception("BioAgent stream failed")
            yield error_sse(SessionStateError(CONVERSATION_ERROR), response_id)
        yield "data: [DONE]\n\n"

    return StreamingResponse(stream(), media_type="text/event-stream")


@app.on_event("shutdown")
async def shutdown_sessions():
    remote_worker.close()
    if sessions is not None:
        await sessions.close_all()

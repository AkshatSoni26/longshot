"""FastAPI surface.

The handler does three things and three things only:
  * validates the inbound model (Pydantic)
  * pre-flight check: is *any* worker alive? — fail fast with a 503 if not
  * dispatches via TaskIQ and returns immediately with a typed response

It must NOT execute pipeline code. The decoupling is the whole point. The
import of ``summarize_url`` only gives us a typed *kicker* — calling
``.kiq(...)`` serializes a message to Redis; the function body never runs in
the API process.
"""

from __future__ import annotations

import asyncio
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from sse_starlette.sse import EventSourceResponse

from app import keys
from app.broker import broker, get_redis
from app.contract import (
    CancelResponse,
    HealthResponse,
    JobAccepted,
    JobRequest,
)
from app.events import EVENT_ADAPTER, TERMINAL_TYPES
from app.settings import get_settings
from app.tasks import run_job


@asynccontextmanager
async def lifespan(_app: FastAPI):
    # In the API process is_worker_process is False, so we always start the broker.
    if not broker.is_worker_process:
        await broker.startup()
    yield
    if not broker.is_worker_process:
        await broker.shutdown()


app = FastAPI(
    title="longshot",
    description="TaskIQ + Redis + SSE — decoupled background jobs with live progress.",
    version="0.1.0",
    lifespan=lifespan,
)


STATIC_DIR = Path(__file__).resolve().parent.parent / "static"
if STATIC_DIR.exists():
    app.mount("/ui", StaticFiles(directory=STATIC_DIR), name="ui")


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


@app.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    redis = await get_redis()
    redis_ok = False
    worker_ok = False
    try:
        await redis.ping()
        redis_ok = True
        worker_ok = bool(await redis.exists(keys.worker_heartbeat()))
    except Exception:
        pass
    return HealthResponse(
        redis=redis_ok, worker_alive=worker_ok, llm_mode=get_settings().llm_mode.value
    )


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------


@app.post("/jobs", response_model=JobAccepted, status_code=202)
async def create_job(payload: JobRequest, request: Request) -> JobAccepted:
    """Enqueue a job. Auto-detects URL vs chat mode from the input.

    Returns immediately with a ``session_id`` to subscribe to.
    """
    redis = await get_redis()

    # Pre-flight: if there is no worker, the request will queue forever. Tell the
    # caller now rather than have them watch an empty SSE stream.
    if not await redis.exists(keys.worker_heartbeat()):
        raise HTTPException(status_code=503, detail="No worker is currently running.")

    session_id = uuid.uuid4().hex
    # JobRequest.model_validator filled in `mode` if it was None.
    assert payload.mode is not None
    try:
        await run_job.kiq(
            session_id=session_id,
            input=payload.input,
            mode=payload.mode,
            chunk_size_chars=payload.chunk_size_chars,
            max_chunks=payload.max_chunks,
        )
    except Exception as exc:  # broker unreachable, etc.
        raise HTTPException(status_code=503, detail=f"Failed to enqueue task: {exc}") from exc

    base = str(request.base_url).rstrip("/")
    return JobAccepted(
        session_id=session_id,
        mode=payload.mode,
        stream_url=f"{base}/jobs/{session_id}/stream",
    )


# ---------------------------------------------------------------------------
# SSE: replay backlog, then tail live
# ---------------------------------------------------------------------------


@app.get("/jobs/{session_id}/stream")
async def stream_job(session_id: str, request: Request):
    redis = await get_redis()
    settings = get_settings()

    async def event_source():
        # Phase 1: REPLAY. Read everything that's already happened. The list is
        # capped by TTL but otherwise complete — clients reconnecting mid-task
        # see the full history before the live tail starts.
        replayed_seqs: set[int] = set()
        backlog_raw: list[bytes | str] = await redis.lrange(keys.events_list(session_id), 0, -1)
        for raw in backlog_raw:
            event = EVENT_ADAPTER.validate_json(raw)
            replayed_seqs.add(event.seq)
            yield {"event": event.type.value, "data": event.model_dump_json()}
            if event.type in TERMINAL_TYPES:
                return

        # Phase 2: TAIL. Subscribe and forward, deduping anything already replayed.
        pubsub = redis.pubsub()
        await pubsub.subscribe(keys.channel(session_id))
        loop = asyncio.get_event_loop()
        heartbeat_at = loop.time() + settings.sse_heartbeat_seconds
        try:
            while True:
                if await request.is_disconnected():
                    # Client gave up — set the cancel flag so the worker exits at the
                    # next checkpoint. The polite version of pulling the plug.
                    await redis.set(keys.cancel_flag(session_id), "1", ex=300)
                    return

                timeout = max(0.1, heartbeat_at - loop.time())
                msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=timeout)
                if msg is None:
                    yield {"event": "keepalive", "data": "{}"}
                    heartbeat_at = loop.time() + settings.sse_heartbeat_seconds
                    continue

                event = EVENT_ADAPTER.validate_json(msg["data"])
                if event.seq in replayed_seqs:
                    continue
                yield {"event": event.type.value, "data": event.model_dump_json()}
                if event.type in TERMINAL_TYPES:
                    return
        finally:
            await pubsub.unsubscribe(keys.channel(session_id))
            await pubsub.aclose()

    return EventSourceResponse(event_source())


# ---------------------------------------------------------------------------
# Cancel
# ---------------------------------------------------------------------------


@app.delete("/jobs/{session_id}", response_model=CancelResponse, status_code=202)
async def cancel_job(session_id: str) -> CancelResponse:
    redis = await get_redis()
    await redis.set(keys.cancel_flag(session_id), "1", ex=300)
    return CancelResponse(session_id=session_id, cancelled=True)


# ---------------------------------------------------------------------------
# Index — convenience: serve the demo UI at /
# ---------------------------------------------------------------------------


@app.get("/", include_in_schema=False)
async def index():
    index_html = STATIC_DIR / "index.html"
    if index_html.exists():
        return FileResponse(index_html)
    return JSONResponse({"hello": "longshot", "docs": "/docs", "ui": "/ui/index.html"})

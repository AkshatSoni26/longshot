"""The TaskIQ task — the only place pipeline code is reachable from.

Pattern checklist (every one of these is here on purpose):
  1. Idempotency lock first: SET NX EX. If we don't get the lock, this is a
     redelivery from Redis Streams — silently exit.
  2. Heartbeat: SETEX a worker key on a periodic cadence so the API can fail
     fast if no worker is alive when a request arrives.
  3. Cancel checks at safe boundaries: between stages, never mid-IO.
  4. Hard timeout via asyncio.wait_for — even runaway LLM calls must die.
  5. Every state transition emits a *typed* Event through the ordered publisher.
  6. Errors emit ErrorEvent (not just raise) so the SSE client always learns why.

Two pipeline branches:

  * URL  — fetch the page, chunk, summarize each chunk, synthesize.
  * CHAT — single streaming LLM call, ``TokenEvent`` per delta.

The branch is chosen by ``JobMode`` from the contract.
"""

from __future__ import annotations

import asyncio

from redis.asyncio import Redis
from taskiq import TaskiqDepends

from app import keys, pipeline
from app.broker import broker, redis_dependency
from app.contract import JobMode, TaskResult, TaskStatus
from app.events import (
    AnswerDoneEvent,
    AnswerStartedEvent,
    CancelledEvent,
    ChunkSummarizedEvent,
    DoneEvent,
    ErrorEvent,
    ErrorReason,
    FetchDoneEvent,
    FetchStartedEvent,
    FinalDoneEvent,
    FinalStartedEvent,
    StartedEvent,
    TokenEvent,
)
from app.publish import ProgressPublisher, get_publisher
from app.settings import get_settings


class TaskCancelled(Exception):
    """Raised when the cancel flag is observed at a checkpoint."""


async def _check_cancel(redis: Redis, session_id: str) -> None:
    if await redis.exists(keys.cancel_flag(session_id)):
        raise TaskCancelled()


async def _heartbeat_loop(redis: Redis) -> None:
    """Background coroutine: keep the worker:heartbeat key fresh."""
    while True:
        await redis.set(keys.worker_heartbeat(), "1", ex=10)
        await asyncio.sleep(3)


@broker.task(task_name="run_job")
async def run_job(
    session_id: str,
    input: str,  # noqa: A002  the payload field name in the API is also `input`
    mode: JobMode,
    chunk_size_chars: int,
    max_chunks: int,
    redis: Redis = TaskiqDepends(redis_dependency),  # noqa: B008  TaskIQ DI mirrors FastAPI Depends
) -> TaskResult:
    """Dispatch on ``mode``. Both branches share the lock + cancel + timeout
    skeleton — only the inner pipeline differs."""
    settings = get_settings()
    publisher = get_publisher(redis)

    # 1. Idempotency lock. If we lose the race, this is a redelivery — exit clean.
    got_lock = await redis.set(
        keys.idempotency_lock(session_id),
        "1",
        nx=True,
        ex=settings.idempotency_lock_ttl_seconds,
    )
    if not got_lock:
        return TaskResult(
            status=TaskStatus.DUPLICATE_DELIVERY, session_id=session_id, mode=mode
        )

    # 2. Wrap the pipeline in a hard timeout. Even if the LLM hangs.
    try:
        if mode is JobMode.CHAT:
            answer = await asyncio.wait_for(
                _run_chat(session_id, input, redis, publisher),
                timeout=settings.task_hard_timeout_seconds,
            )
            return TaskResult(
                status=TaskStatus.OK, session_id=session_id, mode=mode, summary=answer
            )

        summary, chunks = await asyncio.wait_for(
            _run_url_pipeline(session_id, input, chunk_size_chars, max_chunks, redis, publisher),
            timeout=settings.task_hard_timeout_seconds,
        )
        return TaskResult(
            status=TaskStatus.OK,
            session_id=session_id,
            mode=mode,
            summary=summary,
            chunks=chunks,
        )
    except TimeoutError:
        await publisher.emit(
            ErrorEvent(
                session_id=session_id,
                reason=ErrorReason.TIMEOUT,
                message=f"hard timeout after {settings.task_hard_timeout_seconds}s",
            )
        )
        return TaskResult(
            status=TaskStatus.TIMEOUT, session_id=session_id, mode=mode, error="timeout"
        )
    except TaskCancelled:
        await publisher.emit(CancelledEvent(session_id=session_id, by="client"))
        return TaskResult(status=TaskStatus.CANCELLED, session_id=session_id, mode=mode)
    except (pipeline.FetchError, pipeline.SummarizeError) as exc:
        return TaskResult(
            status=TaskStatus.ERROR, session_id=session_id, mode=mode, error=str(exc)
        )
    except Exception as exc:
        await publisher.emit(
            ErrorEvent(
                session_id=session_id,
                reason=ErrorReason.UNKNOWN,
                message=f"{type(exc).__name__}: {exc}",
            )
        )
        raise
    finally:
        await publisher.close(session_id)


# ---------------------------------------------------------------------------
# CHAT pipeline — one streaming LLM call
# ---------------------------------------------------------------------------


async def _run_chat(
    session_id: str,
    prompt: str,
    redis: Redis,
    publisher: ProgressPublisher,
) -> str:
    """Stream a chat answer token-by-token through the publisher."""
    await publisher.emit(StartedEvent(session_id=session_id, input=prompt[:200]))
    await _check_cancel(redis, session_id)
    await publisher.emit(AnswerStartedEvent(session_id=session_id))

    parts: list[str] = []
    try:
        async for delta in pipeline.chat_stream(
            prompt,
            system="You are a helpful assistant. Be concise and direct.",
        ):
            # Cancel check between deltas — keeps the pipeline responsive.
            if await redis.exists(keys.cancel_flag(session_id)):
                raise TaskCancelled()
            parts.append(delta)
            await publisher.emit(TokenEvent(session_id=session_id, delta=delta))
    except pipeline.SummarizeError as exc:
        await publisher.emit(
            ErrorEvent(
                session_id=session_id, reason=ErrorReason.SUMMARIZE, message=str(exc)
            )
        )
        raise

    answer = "".join(parts)
    await publisher.emit(AnswerDoneEvent(session_id=session_id, length=len(answer)))
    await publisher.emit(DoneEvent(session_id=session_id, summary=answer, chunks=0))
    return answer


# ---------------------------------------------------------------------------
# URL pipeline — fetch / chunk / summarize / synthesize
# ---------------------------------------------------------------------------


async def _run_url_pipeline(
    session_id: str,
    url: str,
    chunk_size_chars: int,
    max_chunks: int,
    redis: Redis,
    publisher: ProgressPublisher,
) -> tuple[str, int]:
    """Atomic-chunk URL summarization. Each stage emits its bracketing events."""
    await publisher.emit(StartedEvent(session_id=session_id, input=url))

    # FETCH ---------------------------------------------------------------
    await _check_cancel(redis, session_id)
    await publisher.emit(FetchStartedEvent(session_id=session_id, url=url))
    try:
        text = await pipeline.fetch_text(url)
    except pipeline.FetchError as exc:
        await publisher.emit(
            ErrorEvent(session_id=session_id, reason=ErrorReason.FETCH, message=str(exc))
        )
        raise
    await publisher.emit(FetchDoneEvent(session_id=session_id, chars=len(text)))

    # CHUNK ---------------------------------------------------------------
    await _check_cancel(redis, session_id)
    chunks = pipeline.chunk_text(text, size=chunk_size_chars, max_chunks=max_chunks)
    if not chunks:
        await publisher.emit(
            ErrorEvent(
                session_id=session_id,
                reason=ErrorReason.CHUNK,
                message="no text to summarize",
            )
        )
        raise pipeline.SummarizeError("no text to summarize")

    # SUMMARIZE EACH ------------------------------------------------------
    summaries: list[str] = []
    for index, chunk in enumerate(chunks):
        await _check_cancel(redis, session_id)
        try:
            summary = await pipeline.summarize_chunk(chunk, index=index)
        except pipeline.SummarizeError as exc:
            await publisher.emit(
                ErrorEvent(
                    session_id=session_id,
                    reason=ErrorReason.SUMMARIZE,
                    message=str(exc),
                )
            )
            raise
        summaries.append(summary)
        await publisher.emit(
            ChunkSummarizedEvent(
                session_id=session_id,
                index=index,
                total=len(chunks),
                summary=summary,
            )
        )

    # SYNTHESIZE — streams the final summary token-by-token ---------------
    await _check_cancel(redis, session_id)
    await publisher.emit(FinalStartedEvent(session_id=session_id, chunks=len(summaries)))
    await publisher.emit(AnswerStartedEvent(session_id=session_id))
    joined = "\n\n".join(f"- {s}" for s in summaries)
    final_parts: list[str] = []
    try:
        async for delta in pipeline.chat_stream(
            (
                "Combine these chunk summaries into one cohesive overall summary "
                "(3-5 sentences). Do not list the bullets back; produce flowing "
                f"prose.\n\n{joined}"
            ),
            system="You are a concise summarization assistant.",
        ):
            if await redis.exists(keys.cancel_flag(session_id)):
                raise TaskCancelled()
            final_parts.append(delta)
            await publisher.emit(TokenEvent(session_id=session_id, delta=delta))
    except pipeline.SummarizeError as exc:
        await publisher.emit(
            ErrorEvent(
                session_id=session_id, reason=ErrorReason.SYNTHESIZE, message=str(exc)
            )
        )
        raise

    final = "".join(final_parts)
    await publisher.emit(AnswerDoneEvent(session_id=session_id, length=len(final)))
    await publisher.emit(FinalDoneEvent(session_id=session_id, length=len(final)))
    await publisher.emit(
        DoneEvent(session_id=session_id, summary=final, chunks=len(summaries))
    )
    return final, len(summaries)


__all__ = ["run_job", "_heartbeat_loop"]

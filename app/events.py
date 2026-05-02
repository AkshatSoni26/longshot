"""Typed event hierarchy.

Every event the client can observe has its own Pydantic model. They share a
``BaseEvent`` (seq, session_id, ts) and a discriminator field (``type``) that
lets us validate inbound JSON back into the correct subclass on the SSE side.

Why typed (instead of a flat ``data: dict``):
  * The producer (worker) and consumer (SSE handler / web UI) get a real
    contract — adding a field to ``ChunkSummarizedEvent`` is a one-line change
    that fails fast everywhere if a key gets renamed.
  * No ``Any``. Schema is enforced at the publish boundary, not by convention.
  * The frontend payload is flat — no nested ``data`` indirection — and parses
    cleanly with discriminated unions on the JS side too if you want.
"""

from __future__ import annotations

import time
from enum import StrEnum
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter


class EventType(StrEnum):
    """Discriminator values. Kept as an enum so ``EventType.DONE`` stays usable
    in code, but the wire format is plain lowercase strings."""

    QUEUED = "queued"
    STARTED = "started"
    # URL pipeline
    FETCH_STARTED = "fetch_started"
    FETCH_DONE = "fetch_done"
    CHUNK_SUMMARIZED = "chunk_summarized"
    FINAL_STARTED = "final_started"
    FINAL_DONE = "final_done"
    # Chat pipeline (and any LLM-streaming stage)
    ANSWER_STARTED = "answer_started"
    TOKEN = "token"
    ANSWER_DONE = "answer_done"
    # Lifecycle
    HEARTBEAT = "heartbeat"
    CANCELLED = "cancelled"
    ERROR = "error"
    DONE = "done"


TERMINAL_TYPES: frozenset[EventType] = frozenset(
    {EventType.DONE, EventType.ERROR, EventType.CANCELLED}
)


class BaseEvent(BaseModel):
    """Common envelope. Every concrete event extends this and pins ``type``
    via a ``Literal`` (the discriminator for the union below)."""

    model_config = ConfigDict(frozen=True)

    # Declared on the base so attribute access is type-checked everywhere;
    # subclasses *must* narrow it to a single Literal[EventType.X] value.
    type: EventType
    # ``seq=0`` is a placeholder. The publisher's drainer (app/publish.py)
    # assigns the real Redis-INCR sequence via ``model_copy`` just before the
    # write, so producers never have to know about ordering.
    seq: int = Field(default=0, description="Monotonic per-session sequence (Redis INCR).")
    session_id: str
    ts: float = Field(default_factory=time.time)

    @property
    def is_terminal(self) -> bool:
        return self.type in TERMINAL_TYPES


# ---------------------------------------------------------------------------
# Concrete events. One per state transition.
# ---------------------------------------------------------------------------


class QueuedEvent(BaseEvent):
    type: Literal[EventType.QUEUED] = EventType.QUEUED


class StartedEvent(BaseEvent):
    """Marks the moment the worker picked up the job. ``input`` is whatever
    the user typed — a URL in URL mode, a prompt in chat mode (truncated)."""

    type: Literal[EventType.STARTED] = EventType.STARTED
    input: str


class FetchStartedEvent(BaseEvent):
    type: Literal[EventType.FETCH_STARTED] = EventType.FETCH_STARTED
    url: str


class FetchDoneEvent(BaseEvent):
    type: Literal[EventType.FETCH_DONE] = EventType.FETCH_DONE
    chars: int = Field(..., ge=0)


class ChunkSummarizedEvent(BaseEvent):
    type: Literal[EventType.CHUNK_SUMMARIZED] = EventType.CHUNK_SUMMARIZED
    index: int = Field(..., ge=0)
    total: int = Field(..., ge=1)
    summary: str


class FinalStartedEvent(BaseEvent):
    type: Literal[EventType.FINAL_STARTED] = EventType.FINAL_STARTED
    chunks: int = Field(..., ge=1)


class FinalDoneEvent(BaseEvent):
    type: Literal[EventType.FINAL_DONE] = EventType.FINAL_DONE
    length: int = Field(..., ge=0)


class HeartbeatEvent(BaseEvent):
    type: Literal[EventType.HEARTBEAT] = EventType.HEARTBEAT


# --- streaming chat / final-synthesis events -------------------------------


class AnswerStartedEvent(BaseEvent):
    """Opens a streaming bubble — used by chat mode and (optionally) the
    final-synthesis stage. Subsequent ``TokenEvent``s append to the bubble
    until ``AnswerDoneEvent``."""

    type: Literal[EventType.ANSWER_STARTED] = EventType.ANSWER_STARTED


class TokenEvent(BaseEvent):
    """One incremental delta from the LLM — append to the most recent
    ``AnswerStartedEvent`` bubble on the client."""

    type: Literal[EventType.TOKEN] = EventType.TOKEN
    delta: str


class AnswerDoneEvent(BaseEvent):
    """Closes the streaming bubble. ``length`` is the total char count emitted."""

    type: Literal[EventType.ANSWER_DONE] = EventType.ANSWER_DONE
    length: int = Field(..., ge=0)


class CancelledEvent(BaseEvent):
    type: Literal[EventType.CANCELLED] = EventType.CANCELLED
    by: Literal["client", "user", "timeout"] = "client"


class ErrorReason(StrEnum):
    FETCH = "fetch"
    CHUNK = "chunk"
    SUMMARIZE = "summarize"
    SYNTHESIZE = "synthesize"
    TIMEOUT = "timeout"
    UNKNOWN = "unknown"


class ErrorEvent(BaseEvent):
    type: Literal[EventType.ERROR] = EventType.ERROR
    reason: ErrorReason = ErrorReason.UNKNOWN
    message: str = ""


class DoneEvent(BaseEvent):
    type: Literal[EventType.DONE] = EventType.DONE
    summary: str
    chunks: int = Field(..., ge=0)


# ---------------------------------------------------------------------------
# Discriminated union — used by the SSE side to round-trip events.
# ---------------------------------------------------------------------------

AnyEvent = Annotated[
    QueuedEvent
    | StartedEvent
    | FetchStartedEvent
    | FetchDoneEvent
    | ChunkSummarizedEvent
    | FinalStartedEvent
    | FinalDoneEvent
    | AnswerStartedEvent
    | TokenEvent
    | AnswerDoneEvent
    | HeartbeatEvent
    | CancelledEvent
    | ErrorEvent
    | DoneEvent,
    Field(discriminator="type"),
]

# A reusable adapter so callers can ``EVENT_ADAPTER.validate_json(raw)``
# without paying a Pydantic-construction tax per call.
EVENT_ADAPTER: TypeAdapter[AnyEvent] = TypeAdapter(AnyEvent)

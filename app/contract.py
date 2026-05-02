"""The API ↔ worker contract.

Anything that crosses the Redis boundary or is exposed on HTTP is defined here.
The TaskIQ task signature consumes ``JobRequest`` and returns ``TaskResult``;
the API uses ``JobAccepted``, ``HealthResponse`` and ``CancelResponse`` for
HTTP I/O. No ``Any``, no untyped dicts.
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, Field, model_validator


class JobMode(StrEnum):
    """Which pipeline branch handles the input.

    * URL  — fetch → chunk → summarize each → synthesize. Atomic chunks.
    * CHAT — single streaming LLM call. Tokens stream through ``TokenEvent``s.
    """

    URL = "url"
    CHAT = "chat"


def detect_mode(text: str) -> JobMode:
    """Auto-pick a pipeline based on what the user typed.

    A URL is anything starting with ``http://`` or ``https://``. Everything
    else (a question, a prompt, naked words) is a chat-style prompt.
    """
    s = text.strip().lower()
    if s.startswith(("http://", "https://")):
        return JobMode.URL
    return JobMode.CHAT


# ---------------------------------------------------------------------------
# Inbound (HTTP)
# ---------------------------------------------------------------------------


class JobRequest(BaseModel):
    """POST /jobs payload — accepts either a URL to summarize or a chat prompt."""

    input: str = Field(..., min_length=1, max_length=8000)
    # ``None`` triggers auto-detect via ``detect_mode``.
    mode: JobMode | None = None
    # Only used in URL mode; ignored in CHAT mode but kept on the wire for
    # round-trip clarity.
    chunk_size_chars: int = Field(default=1500, ge=200, le=20000)
    max_chunks: int = Field(default=3, ge=1, le=50)

    @model_validator(mode="after")
    def _resolve_mode(self) -> JobRequest:
        if self.mode is None:
            self.mode = detect_mode(self.input)
        return self


# ---------------------------------------------------------------------------
# Outbound (HTTP)
# ---------------------------------------------------------------------------


class JobAccepted(BaseModel):
    """POST /jobs response."""

    session_id: str
    mode: JobMode
    stream_url: str


class HealthResponse(BaseModel):
    """GET /health response."""

    redis: bool
    worker_alive: bool
    llm_mode: str


class CancelResponse(BaseModel):
    """DELETE /jobs/{id} response."""

    session_id: str
    cancelled: bool = True


# ---------------------------------------------------------------------------
# Task return value (crosses Redis via the result backend)
# ---------------------------------------------------------------------------


class TaskStatus(StrEnum):
    OK = "ok"
    DUPLICATE_DELIVERY = "duplicate_delivery"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"
    ERROR = "error"


class TaskResult(BaseModel):
    """What the worker returns. Stored in the result backend with a TTL."""

    status: TaskStatus
    session_id: str
    mode: JobMode | None = None
    summary: str | None = None
    chunks: int | None = None
    error: str | None = None

"""Smoke tests — they run *without* a Redis instance.

The point isn't coverage; it's catching obvious wiring breakage in CI / pre-commit
without spinning up containers. The real verification is `bash demo/happy_path.sh`
against a running stack.
"""

from __future__ import annotations

import pytest


def test_imports() -> None:
    """All modules import without side-effecting Redis."""
    from app import (  # noqa: F401
        api,
        broker,
        contract,
        events,
        keys,
        pipeline,
        publish,
        settings,
        tasks,
        worker,
    )


def test_jobrequest_url_mode() -> None:
    from app.contract import JobMode, JobRequest

    r = JobRequest(input="https://example.com", chunk_size_chars=500, max_chunks=2)
    assert r.mode is JobMode.URL
    assert r.input == "https://example.com"


def test_jobrequest_chat_mode_autodetect() -> None:
    from app.contract import JobMode, JobRequest

    r = JobRequest(input="hello world, can you help me?")
    assert r.mode is JobMode.CHAT


def test_jobrequest_explicit_mode_overrides_autodetect() -> None:
    from app.contract import JobMode, JobRequest

    r = JobRequest(input="https://example.com", mode=JobMode.CHAT)
    assert r.mode is JobMode.CHAT


def test_detect_mode() -> None:
    from app.contract import JobMode, detect_mode

    assert detect_mode("https://example.com") is JobMode.URL
    assert detect_mode("HTTP://example.com") is JobMode.URL
    assert detect_mode("  http://x  ") is JobMode.URL
    assert detect_mode("what can you do?") is JobMode.CHAT
    assert detect_mode("explain redis to me") is JobMode.CHAT


def test_jobrequest_validation() -> None:
    from pydantic import ValidationError

    from app.contract import JobRequest

    with pytest.raises(ValidationError):
        JobRequest(input="")  # min_length=1
    with pytest.raises(ValidationError):
        JobRequest(input="x", chunk_size_chars=10)  # ge=200


def test_task_result_status() -> None:
    from app.contract import JobMode, TaskResult, TaskStatus

    tr = TaskResult(
        status=TaskStatus.OK, session_id="abc", mode=JobMode.URL, summary="x", chunks=1
    )
    assert tr.status is TaskStatus.OK
    assert tr.mode is JobMode.URL
    assert tr.model_dump()["status"] == "ok"


def test_event_discriminated_union_roundtrip() -> None:
    """Every concrete event must serialize and parse back to the same subclass."""
    from app.events import (
        EVENT_ADAPTER,
        TERMINAL_TYPES,
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
        HeartbeatEvent,
        QueuedEvent,
        StartedEvent,
        TokenEvent,
    )

    instances = [
        QueuedEvent(seq=1, session_id="s"),
        StartedEvent(seq=2, session_id="s", input="https://example.com"),
        FetchStartedEvent(seq=3, session_id="s", url="https://example.com"),
        FetchDoneEvent(seq=4, session_id="s", chars=1024),
        ChunkSummarizedEvent(seq=5, session_id="s", index=0, total=2, summary="hi"),
        FinalStartedEvent(seq=6, session_id="s", chunks=2),
        FinalDoneEvent(seq=7, session_id="s", length=10),
        AnswerStartedEvent(seq=8, session_id="s"),
        TokenEvent(seq=9, session_id="s", delta="hello "),
        AnswerDoneEvent(seq=10, session_id="s", length=5),
        HeartbeatEvent(seq=11, session_id="s"),
        CancelledEvent(seq=12, session_id="s", by="client"),
        ErrorEvent(seq=13, session_id="s", reason=ErrorReason.FETCH, message="404"),
        DoneEvent(seq=14, session_id="s", summary="done", chunks=2),
    ]

    for ev in instances:
        raw = ev.model_dump_json()
        back = EVENT_ADAPTER.validate_json(raw)
        assert type(back) is type(ev)
        assert back.seq == ev.seq

    assert {DoneEvent(seq=0, session_id="s", summary="x", chunks=1).type} <= TERMINAL_TYPES


def test_event_validation_rejects_bad_payload() -> None:
    from pydantic import ValidationError

    from app.events import ChunkSummarizedEvent

    with pytest.raises(ValidationError):
        ChunkSummarizedEvent(seq=1, session_id="s", index=-1, total=1, summary="x")
    with pytest.raises(ValidationError):
        ChunkSummarizedEvent(seq=1, session_id="s", index=0, total=0, summary="x")


def test_event_seq_default_and_model_copy() -> None:
    """Producers can omit seq; the publisher uses model_copy to assign it."""
    from app.events import StartedEvent

    ev = StartedEvent(session_id="s", input="https://x.test")
    assert ev.seq == 0  # placeholder

    bumped = ev.model_copy(update={"seq": 42})
    assert bumped.seq == 42
    assert bumped.input == "https://x.test"
    assert type(bumped) is StartedEvent  # round-trip preserves the subclass


def test_drainer_stop_sentinel_is_typed() -> None:
    """The stop sentinel is its own class — not ``object()`` — so the drainer
    queue can be typed strictly."""
    from app.publish import _DRAINER_STOP, _DrainerStop

    assert isinstance(_DRAINER_STOP, _DrainerStop)
    # Frozen + slots = no accidental attribute assignment.
    with pytest.raises((AttributeError, TypeError)):
        _DRAINER_STOP.foo = 1  # type: ignore[attr-defined]


def test_chunking() -> None:
    from app.pipeline import chunk_text

    text = "x" * 5000
    chunks = chunk_text(text, size=1000, max_chunks=10)
    assert len(chunks) == 5
    assert all(len(c) <= 1000 for c in chunks)

    capped = chunk_text(text, size=500, max_chunks=3)
    assert len(capped) == 3


def test_task_registered() -> None:
    """The broker must know about run_job, otherwise the worker won't pick it up."""
    from app.broker import broker
    from app.tasks import run_job  # noqa: F401  registers via decorator

    assert "run_job" in broker.local_task_registry, "run_job not registered with broker"


def test_keys_namespacing() -> None:
    from app import keys

    sid = "abc123"
    derived = [
        keys.events_list(sid),
        keys.progress_snapshot(sid),
        keys.channel(sid),
        keys.sequence(sid),
        keys.idempotency_lock(sid),
        keys.cancel_flag(sid),
    ]
    for k in derived:
        assert sid in k
    assert keys.worker_heartbeat() == "worker:heartbeat"


def test_settings_llm_mode_enum() -> None:
    from app.settings import LLMMode, Settings

    s = Settings()
    assert isinstance(s.llm_mode, LLMMode)
    # round-trip via env shape
    assert LLMMode("mock") is LLMMode.MOCK
    assert LLMMode("real") is LLMMode.REAL
    assert LLMMode("lmstudio") is LLMMode.LMSTUDIO


def test_lmstudio_strip_thinking_streaming() -> None:
    """The streaming variant threads ``in_think`` state across chunks."""
    from app.pipeline import _strip_thinking_streaming

    # Tag opens in chunk 1, body in chunk 2, closes in chunk 3, answer follows
    out1, in_think = _strip_thinking_streaming("hello <think>let", False)
    assert out1 == "hello "
    assert in_think is True

    out2, in_think = _strip_thinking_streaming(" me think", in_think)
    assert out2 == ""
    assert in_think is True

    out3, in_think = _strip_thinking_streaming(" carefully</think> Answer:", in_think)
    assert out3 == " Answer:"
    assert in_think is False

    out4, in_think = _strip_thinking_streaming(" ok", in_think)
    assert out4 == " ok"
    assert in_think is False


def test_lmstudio_strip_thinking() -> None:
    """Thinking models emit <think>...</think> blocks; we strip them by default."""
    from app.pipeline import _postprocess_lmstudio

    raw = "<think>Let me think about this. The user wants...</think>\n\nThe summary is X."
    assert _postprocess_lmstudio(raw, strip_thinking=True) == "The summary is X."

    # Off — preserve as-is (only trimmed).
    assert _postprocess_lmstudio(raw, strip_thinking=False) == raw.strip()

    # No think block — passthrough.
    assert _postprocess_lmstudio("just a summary", strip_thinking=True) == "just a summary"

    # Unbalanced <think> (model cut off mid-thought) — drop everything if no
    # closing tag was seen, else keep post-close content.
    cut_off = "<think>I am still thinking and was cut off"
    assert _postprocess_lmstudio(cut_off, strip_thinking=True) == ""

    multi = "<think>first</think>some text<think>second</think>final"
    assert _postprocess_lmstudio(multi, strip_thinking=True) == "some textfinal"


def test_pydantic_serializer_roundtrip() -> None:
    """The custom serializer must serialize a TaskResult model correctly."""
    from app.broker import PydanticJSONSerializer
    from app.contract import TaskResult, TaskStatus

    serializer = PydanticJSONSerializer()
    tr = TaskResult(status=TaskStatus.OK, session_id="x", summary="y", chunks=1)
    blob = serializer.dumpb(tr)
    parsed = serializer.loadb(blob)
    assert parsed["status"] == "ok"
    assert parsed["session_id"] == "x"

    # plain dicts still pass through
    assert serializer.loadb(serializer.dumpb({"k": 1}))["k"] == 1

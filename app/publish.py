"""Ordered, durable progress publishing.

Every event must
  1. land in a Redis list (so SSE reconnects can replay)
  2. land in a snapshot key (so a *late* joiner sees current state in one read)
  3. fan out to Pub/Sub (so live tails get it instantly)
  4. carry a strictly monotonic sequence number (so the client can detect gaps).

Naive implementation:

    seq = await r.incr(seq_key)
    await r.rpush(list_key, json)
    await r.publish(channel, json)

This races. If two coroutines emit simultaneously, the INCR returns in
declaration order but the RPUSH/PUBLISH may interleave — clients see seq=2
arrive before seq=1.

Fix: per-session ``asyncio.Queue`` + single drainer task. Producers enqueue
typed ``BaseEvent`` instances (with ``seq`` left at its default ``0``); the
drainer pops FIFO, ``model_copy``s in the real sequence, and pipelines the
four writes atomically.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from redis.asyncio import Redis

from app import keys
from app.events import TERMINAL_TYPES, BaseEvent
from app.settings import get_settings


@dataclass(frozen=True, slots=True)
class _DrainerStop:
    """Typed sentinel: tells a session's drainer to flush and exit.

    Single instance below — comparing identity (``is _DRAINER_STOP``) costs
    nothing and lets the queue stay strictly typed as
    ``BaseEvent | _DrainerStop`` (no ``object``, no ``Any``).
    """


_DRAINER_STOP: _DrainerStop = _DrainerStop()

_QueueItem = BaseEvent | _DrainerStop


@dataclass
class _SessionPublisher:
    queue: asyncio.Queue[_QueueItem]
    drainer: asyncio.Task[None]


class ProgressPublisher:
    """One instance per worker process. Holds a drainer per active session."""

    def __init__(self, redis: Redis) -> None:
        self._redis = redis
        self._sessions: dict[str, _SessionPublisher] = {}
        self._lock = asyncio.Lock()
        self._settings = get_settings()

    async def emit(self, event: BaseEvent) -> None:
        """Enqueue a typed event. ``event.session_id`` routes to the right drainer.

        ``event.seq`` is rewritten by the drainer just before the write — it's
        fine (and expected) to leave it at the ``0`` default when constructing.
        """
        publisher = await self._get_or_create(event.session_id)
        await publisher.queue.put(event)

    async def close(self, session_id: str) -> None:
        """Drain remaining items, stop the drainer. Call after a terminal event."""
        publisher = self._sessions.pop(session_id, None)
        if publisher is None:
            return
        await publisher.queue.put(_DRAINER_STOP)
        try:
            await asyncio.wait_for(publisher.drainer, timeout=5)
        except TimeoutError:
            publisher.drainer.cancel()

    async def _get_or_create(self, session_id: str) -> _SessionPublisher:
        if session_id in self._sessions:
            return self._sessions[session_id]
        async with self._lock:
            if session_id in self._sessions:
                return self._sessions[session_id]
            queue: asyncio.Queue[_QueueItem] = asyncio.Queue()
            drainer = asyncio.create_task(
                self._drain(session_id, queue), name=f"drain:{session_id}"
            )
            self._sessions[session_id] = _SessionPublisher(queue=queue, drainer=drainer)
            return self._sessions[session_id]

    async def _drain(self, session_id: str, queue: asyncio.Queue[_QueueItem]) -> None:
        """The single writer for this session. FIFO guarantees ordered seqs."""
        list_key = keys.events_list(session_id)
        snap_key = keys.progress_snapshot(session_id)
        chan_key = keys.channel(session_id)
        seq_key = keys.sequence(session_id)
        ttl = self._settings.progress_snapshot_ttl_seconds

        while True:
            item = await queue.get()
            if isinstance(item, _DrainerStop):
                return

            seq = int(await self._redis.incr(seq_key))
            event = item.model_copy(update={"seq": seq})
            payload = event.model_dump_json()

            # One pipelined round-trip for the durable writes + channel publish.
            pipe = self._redis.pipeline(transaction=False)
            pipe.rpush(list_key, payload)
            pipe.expire(list_key, ttl)
            pipe.set(snap_key, payload, ex=ttl)
            pipe.publish(chan_key, payload)
            await pipe.execute()

            if event.type in TERMINAL_TYPES:
                # Best-effort: keep terminal events around longer than mid-stream
                # ones so a late SSE client still sees the outcome.
                await self._redis.expire(list_key, ttl)


# Shared per-process instance. Built lazily so import order doesn't matter.
_publisher: ProgressPublisher | None = None


def get_publisher(redis: Redis) -> ProgressPublisher:
    global _publisher
    if _publisher is None:
        _publisher = ProgressPublisher(redis)
    return _publisher

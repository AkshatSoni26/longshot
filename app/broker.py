"""TaskIQ broker — Redis Streams for at-least-once delivery, with redelivery on worker crash.

Why Streams (not list/pubsub):
  * Pub/Sub: at-most-once. Worker crash mid-task = silent loss.
  * List (BLPOP): at-most-once unless you hand-roll ACKs.
  * Streams: consumer groups + XACK + XAUTOCLAIM = a real durable queue.

Why a separate result backend:
  Task return values are short-lived. Real state lives in app DB. Keep TTL tight.
"""

from __future__ import annotations

from pydantic import BaseModel, JsonValue
from redis.asyncio import Redis
from taskiq import TaskiqDepends
from taskiq.middlewares import SmartRetryMiddleware
from taskiq.serializers import JSONSerializer
from taskiq_redis import RedisAsyncResultBackend, RedisStreamBroker

from app.settings import get_settings

settings = get_settings()


def _pydantic_default(value: BaseModel) -> JsonValue:
    """``json.dumps(default=...)`` hook: serialize Pydantic models via ``model_dump``.

    Python's stdlib ``json`` only invokes ``default`` for values it cannot
    natively encode, so by the time we get here the value is already an
    instance of something exotic. We accept exactly ``BaseModel`` and let the
    type checker enforce that — anything else surfaces as a ``TypeError`` from
    the isinstance check below, which is what we want. The return type is
    ``pydantic.JsonValue`` — the precise recursive type for JSON-shaped data.
    """
    if not isinstance(value, BaseModel):
        raise TypeError(f"Object of type {type(value).__name__} is not JSON-serializable")
    return value.model_dump(mode="json")


class PydanticJSONSerializer(JSONSerializer):
    """``JSONSerializer`` configured to handle Pydantic models on the way out.

    Lets tasks return ``TaskResult`` (or any ``BaseModel``) directly. The
    parent's ``dumpb`` already calls ``json.dumps(value, default=self.default)``
    — we just supply the right ``default`` callback in ``__init__``. No need
    to override ``dumpb`` or ``loadb``.
    """

    def __init__(self) -> None:
        super().__init__(default=_pydantic_default)


# Streams broker — durable, ACK-based.
# `consumer_group_name` lets multiple workers split the stream cooperatively.
broker = (
    RedisStreamBroker(
        url=settings.redis_url,
        queue_name=settings.stream_name,
        consumer_group_name=settings.consumer_group,
        consumer_name=settings.worker_consumer_name,
        # If a worker holds a message longer than this without ACKing, XAUTOCLAIM
        # rescues it for another consumer. Tune to "longest legitimate task duration".
        xread_block=2000,
    )
    .with_result_backend(
        RedisAsyncResultBackend(
            redis_url=settings.redis_url,
            result_ex_time=settings.result_ttl_seconds,
        )
    )
    .with_serializer(PydanticJSONSerializer())
    .with_middlewares(
        # Exponential backoff with jitter, cap at 1 retry — the *idempotency lock*
        # in tasks.py is what really protects against duplicate work; retries are
        # for transient errors only.
        SmartRetryMiddleware(default_retry_count=1, use_jitter=True, use_delay_exponent=True),
    )
)


# A separate Redis client (asyncio) for everything *not* TaskIQ:
# event publishing, idempotency locks, cancel flags, heartbeat. Built lazily.
_redis_singleton: Redis | None = None


async def get_redis() -> Redis:
    """Singleton async Redis client. Used both inside tasks (via DI) and by FastAPI."""
    global _redis_singleton
    if _redis_singleton is None:
        _redis_singleton = Redis.from_url(settings.redis_url, decode_responses=True)
    return _redis_singleton


async def redis_dependency() -> Redis:
    """TaskIQ DI handle: ``redis: Redis = TaskiqDepends(redis_dependency)``."""
    return await get_redis()


__all__ = ["broker", "get_redis", "redis_dependency", "TaskiqDepends"]

"""Worker entry point.

Run with:
    uv run taskiq worker app.worker:broker --workers 1

The crucial detail: ``import app.tasks`` registers the task with the broker.
Without it the broker starts up, joins the consumer group, and never picks up
any work because no callable is mapped to the task name.
"""

import asyncio
import logging

from taskiq import TaskiqEvents, TaskiqState

from app import tasks  # noqa: F401  registers @broker.task functions
from app.broker import broker, get_redis
from app.tasks import _heartbeat_loop  # noqa: F401  (kept for clarity)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("longshot.worker")


@broker.on_event(TaskiqEvents.WORKER_STARTUP)
async def _on_startup(state: TaskiqState) -> None:
    redis = await get_redis()
    state.heartbeat_task = asyncio.create_task(_heartbeat_loop(redis), name="worker-heartbeat")
    log.info("worker startup complete; heartbeat running")


@broker.on_event(TaskiqEvents.WORKER_SHUTDOWN)
async def _on_shutdown(state: TaskiqState) -> None:
    hb = getattr(state, "heartbeat_task", None)
    if hb is not None:
        hb.cancel()
    log.info("worker shutdown")

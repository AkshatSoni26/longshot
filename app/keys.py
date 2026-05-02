"""All Redis keys live here. One place to audit naming + TTLs."""


def events_list(session_id: str) -> str:
    """RPUSH log of every event — used to replay on SSE reconnect."""
    return f"task_events:{session_id}"


def progress_snapshot(session_id: str) -> str:
    """SETEX latest snapshot — for late joiners who only need current state."""
    return f"task_progress:{session_id}"


def channel(session_id: str) -> str:
    """Pub/Sub channel — live tail."""
    return f"task_channel:{session_id}"


def sequence(session_id: str) -> str:
    """INCR counter — monotonic event sequence per session."""
    return f"task_seq:{session_id}"


def idempotency_lock(session_id: str) -> str:
    """SET NX EX — first-mover wins; redeliveries become silent no-ops."""
    return f"task_lock:{session_id}"


def cancel_flag(session_id: str) -> str:
    """SET when client disconnects or DELETE called — task checks at checkpoints."""
    return f"task_cancel:{session_id}"


def worker_heartbeat() -> str:
    """SETEX from worker; API checks before dispatching to fail fast if no worker."""
    return "worker:heartbeat"

# longshot

> A small, runnable demonstration of how to decouple long-running work from a
> FastAPI request handler using **TaskIQ + Redis Streams + SSE**, with the
> failure modes that matter in production demonstrated end-to-end.

## Why this repo exists

This is a sanitized, dependency-light extraction of an architecture I built in
production for a long-running AI-orchestration system. The patterns —
per-session ordered drainers, at-least-once delivery with an idempotency
lock, replay-then-tail SSE — were adapted from a production architecture
I built and shipped at work. This repo is the public, dependency-light
version: small enough to read end-to-end in one sitting.

It is intentionally one task type, no auth, no DB beyond Redis, no fan-out.
The goal is to explain the reliability patterns clearly enough that another
engineer can read, run, and adapt them — not to be a framework.

```
   client                       ┌──────────┐                        worker
  ───────►  POST /jobs   ───►   │  api.py  │                       ──────────
                                └────┬─────┘                       app.tasks
                                     │  .kiq()                      run_job
                                     ▼                                  ▲
                              ┌─────────────┐         XREADGROUP        │
                              │   Redis     │ ◄─────────────────────────┘
                              │  ┌───────┐  │
                              │  │stream │◄─┼── durable queue (ACK + redelivery)
                              │  ├───────┤  │
                              │  │ list  │──┼── replay log of progress events
                              │  ├───────┤  │
                              │  │pubsub │──┼── live tail of progress events
                              │  ├───────┤  │
                              │  │ KV    │──┼── locks, cancel flag, heartbeat
                              │  └───────┘  │
                              └─────────────┘
                                     ▲
                                     │  RPUSH + SET + PUBLISH (one pipeline)
                                     │
   client     ◄── SSE: replay + tail ── GET /jobs/{id}/stream
```

## The problem this solves

A FastAPI handler that runs for thirty seconds is a bug in three different
ways. It holds an HTTP connection (and a worker thread) hostage. It dies if
anything in front of it (load balancer, gateway, the user's wifi) drops the
socket. And it gives you no way to surface progress to the client.

The standard answer is "push it to a background queue." The standard answer is
also a trap — you trade one problem (long handler) for five smaller ones:

- **Workers crash mid-task.** Without ACK semantics the work is lost. Pub/Sub
  is at-most-once; a plain BLPOP list is too unless you hand-roll redelivery.
- **Tasks get delivered twice.** At-least-once is the correct trade, but it
  means your task code must be idempotent or you'll double-charge a card,
  double-summarize a doc, double-anything.
- **Clients want progress, not silence.** A 202 with a job id is fine; eight
  seconds of nothing followed by a result is not.
- **Reconnects.** A client refreshes the page mid-task. Did your "live"
  channel keep state? (Pub/Sub didn't.)
- **Cancellation.** The user closed the tab. Your worker is still going.

This repo demonstrates a compact solution to the core reliability problems
this raises: durable dispatch, duplicate protection, replayable progress,
cancellation, and typed failure visibility — in code small enough to read
end-to-end in a sitting. Where the demo intentionally stops short of a full
production answer (e.g., resumable-from-checkpoint workers), it says so
explicitly in *What this is NOT* below.

## Run it in 60 seconds

Requires Docker. The bundled demo is a URL summarizer (fetch → chunk → LLM
summary per chunk → final synthesis). Mock LLM by default, so no API key.

```bash
make up                   # docker compose up --build -d
bash demo/happy_path.sh
```

Open `http://localhost:8000/ui/index.html` for a browser view of the same
events streaming live. API docs are at `/docs`.

### Three LLM modes

The pipeline dispatches on the `LLM_MODE` env var. All three modes flow
through the same code path (`app/pipeline.py`) — only the leaf summarization
call differs.

| Mode | What it does | Setup |
|---|---|---|
| `mock` *(default)* | Sleep + canned summary. Fast, deterministic. | None — works out of the box. |
| `real` | Anthropic Messages API. | `LLM_MODE=real ANTHROPIC_API_KEY=sk-...`. Run `uv sync --extra real` for native. |
| `lmstudio` | Local LM Studio server (OpenAI-compatible). | Open LM Studio, click **+ Load Model**, confirm the **Local Server** tab shows `Status: Running`. Then `LLM_MODE=lmstudio make up`. |

LM Studio details: from inside Docker we reach LM Studio on the host via
`host.docker.internal:1234`. The compose files configure that automatically
(including the Linux `host-gateway` mapping). When running natively, set
`LMSTUDIO_BASE_URL=http://localhost:1234/v1`. If you want to pin to a
specific model, set `LMSTUDIO_MODEL=qwen/qwen3-4b` (whatever appears in LM
Studio's model picker); leaving it empty routes to the loaded model.

**Thinking models** (Qwen3-Thinking, DeepSeek-R1, GPT-OSS-Reasoning, …) emit
`<think>...</think>` blocks before the answer. Two settings handle them:
`LMSTUDIO_MAX_TOKENS` (default 2048) is generous so the chain-of-thought
fits, and `LMSTUDIO_STRIP_THINKING=true` (default) removes the think block
from what we treat as the summary. With a 30B-class thinking model expect
~10–30s per chunk on a workstation GPU — the demo defaults to 8 chunks, so
either drop `max_chunks` to 2–3 in `demo/happy_path.sh` or bump
`TASK_HARD_TIMEOUT_SECONDS` to 300+.

### Development mode (hot reload)

A separate compose file mounts `./app` and `./static` as bind mounts and runs
both uvicorn and taskiq with `--reload`. Edit a Python file → both services
restart automatically. No image rebuild unless `pyproject.toml` changes.

```bash
make dev-up         # build & start the dev stack
make dev-logs       # tail
# edit app/tasks.py — watch the worker restart
make dev-down       # stop
```

Use `make dev-rebuild` after changing dependencies in `pyproject.toml`.

## The contract

The whole point: **the API never *invokes* pipeline code — it only sends
messages.** The boundary lives in the broker, not in the import graph.

The API does import `run_job` from `app/tasks.py`, but the
`@broker.task` decorator turns it into a typed *kicker*: calling
`run_job.kiq(...)` serializes a message and pushes it to Redis. The
function body never runs in the API process. Replace `app.tasks` with a stub
re-exporting the same kickers (or move the worker to a separate repo
entirely) and zero API code changes.

The single source of truth is the typed task signature in `app/tasks.py`:

```python
@broker.task(task_name="run_job")
async def run_job(
    session_id: str,
    input: str,                          # URL or chat prompt
    mode: JobMode,                       # auto-detected on the API side
    chunk_size_chars: int,
    max_chunks: int,
    redis: Redis = TaskiqDepends(redis_dependency),
) -> TaskResult: ...
```

The return type is a `TaskResult` Pydantic model (see `app/contract.py`) and
the broker uses a `PydanticJSONSerializer` so models flow through the wire
without ad-hoc `model_dump()` calls. There are no `Any`-typed dicts crossing
the boundary in either direction. The API calls `run_job.kiq(...)` —
that proxies through the broker as a JSON-serialized message. The function
body never runs in the API process.

## The six patterns, with file:line refs

### 1. Broker config — at-least-once delivery, retries, scoped result TTL

**File:** `app/broker.py:55`

We use `RedisStreamBroker` (not the list-based or pub/sub-based variants)
because Streams give us ACK + consumer groups + `XAUTOCLAIM`. If a worker
crashes between `XREADGROUP` and `XACK`, the message becomes claimable by a
sibling worker after `idle_timeout`.

`RedisAsyncResultBackend` stores task return values with a tight TTL — those
values are debug-only; "real" state lives in the application database (or in
this case, in the events list). The broker is configured with
`SmartRetryMiddleware` (exponential backoff + jitter, cap of one retry) for
*unexpected* exceptions. Expected pipeline failures — `FetchError`,
`SummarizeError`, `TaskCancelled` — are caught inside the task body and
converted into typed `ErrorEvent`/`CancelledEvent` SSE events plus a
terminal `TaskResult`, so they never trip the retry path. The *idempotency
lock* (pattern 5) is what really protects against duplicate work when
retries do fire.

### 2. Typed events end-to-end (no `Any`, no untyped dicts)

**Files:** `app/events.py`, `app/contract.py`

Every state transition has its own Pydantic model (`StartedEvent`,
`FetchDoneEvent`, `ChunkSummarizedEvent`, …) sharing a `BaseEvent` envelope
(`seq`, `session_id`, `ts`). They're combined into a discriminated union:

```python
AnyEvent = Annotated[
    StartedEvent | FetchDoneEvent | ChunkSummarizedEvent | ...,
    Field(discriminator="type"),
]
EVENT_ADAPTER = TypeAdapter(AnyEvent)
```

The SSE endpoint (`app/api.py`) round-trips raw bytes from Redis through
`EVENT_ADAPTER.validate_json(...)` — schema is enforced at the boundary, not
by convention. Adding a field to one event = a one-line change that fails
fast everywhere if a key is misspelled.

### 3. Ordered events via per-session drainer

**File:** `app/publish.py:50`

The naive way to publish a progress event:

```python
seq = await r.incr(seq_key)
await r.rpush(list_key, payload)
await r.publish(channel, payload)
```

Two coroutines emitting concurrently can race past the `INCR`. The client sees
seq=2 arrive before seq=1.

The fix: a per-session `asyncio.Queue` plus exactly one drainer task. Producers
enqueue typed `BaseEvent` instances with `seq=0` placeholder; the drainer
pops FIFO, fetches the next `seq` via `INCR`, `model_copy`s the event with
the real seq, and pipelines four writes (RPUSH, EXPIRE, SET, PUBLISH) in a
single round-trip. Strictly monotonic seqs by construction — no
distributed lock, no Lua script. This is the most worth-reading file in the
repo.

### 4. SSE that survives reconnects: subscribe, replay, then tail

**File:** `app/api.py:113`

Order matters here. The SSE endpoint **subscribes to `task_channel:{id}`
first** (so the connection starts holding inbound messages for us), **then**
reads `task_events:{id}` from start (the durable history), **then** drains
the live tail — deduping by `seq` for events that landed in both the list
and the channel buffer. If you reverse the order (LRANGE first, SUBSCRIBE
second), there's a race window where events published in between are *lost
forever* — Pub/Sub has no replay, and the message is dropped because nobody
was listening yet. A keepalive every 30s lets the client distinguish "quiet"
from "dead". Every byte coming back through the SSE boundary is parsed by
the discriminated-union `TypeAdapter`, so a malformed event is rejected
instead of silently forwarded. Disconnecting the SSE client does **not**
cancel the worker — the work outlives the connection; reconnect to the same
session_id to resume tailing. Explicit cancel is `DELETE /jobs/{id}`.

### 5. Idempotency lock as the first line in the task

**File:** `app/tasks.py:65`

```python
got_lock = await redis.set(idempotency_lock(session_id), "1", nx=True, ex=...)
if not got_lock:
    return TaskResult(status=TaskStatus.DUPLICATE_DELIVERY, session_id=session_id)
```

Redis Streams don't redeliver by themselves — the broker periodically
calls `XAUTOCLAIM` to reclaim pending entries that have been held longer
than `idle_timeout`. TaskIQ does this for us. The retry middleware adds
another layer of redelivery on top, for transient errors caught after the
task body started. The `SET NX EX` lock is what makes both safe: only the
first delivery actually does work; subsequent ones observe the lock and
exit silently. This is what allows us to use at-least-once delivery
without making the business logic itself care about it.

### 6. Failure handling matrix

Every entry below has a code path *and* a way for the client to observe it.

| Failure | Where caught | Client sees |
|---|---|---|
| Worker not running at dispatch | `app/api.py:90` (heartbeat check) | HTTP 503 with retry hint |
| Broker unreachable on `.kiq()` | `app/api.py:106` (try/except) | HTTP 503 |
| Worker crash mid-task | Redis Streams pending-message claim (`XAUTOCLAIM` after `idle_timeout`) + `app/tasks.py:65` (lock) | No duplicate execution; checkpointed resume is out of scope (see *What this is NOT*) |
| Task raises | `app/tasks.py:106` emits `ErrorEvent` | SSE `error` event then close |
| Task hangs forever | `app/tasks.py:80` `asyncio.wait_for` | SSE `error` event with `reason=timeout` |
| LLM unreachable / errors | `app/pipeline.py` raises `SummarizeError` → `ErrorEvent` | SSE `error` event with `reason=summarize` |
| Client disconnects | `app/api.py:143` sets cancel flag | Worker exits at next checkpoint |
| User-initiated cancel | `DELETE /jobs/{id}` → cancel flag | SSE `cancelled` event |

## Failure demos

### Worker crash → resumption

```bash
make up
bash demo/chaos.sh
```

The script posts a job, kills the worker container three seconds in, restarts
it after five seconds. Because of the idempotency lock + Stream redelivery,
the task picks up exactly once. (Note: in this v1, "resumption" means the
redelivered worker observes the lock, exits silently, and the client times
out their SSE — *no double work*. A real-resumption-from-checkpoint variant
is in `FUTURE.md`.)

### User cancels mid-stream

```bash
bash demo/cancel.sh
```

Posts a job, then `DELETE`s it after four seconds. The worker observes the
cancel flag at its next checkpoint, emits a `CANCELLED` event, and exits
cleanly.

## What this is *not*

(See `FUTURE.md` for the long version. Highlights:)

- ❌ Authentication, users, billing.
- ❌ A frontend framework. The single-file `static/index.html` is intentional.
- ❌ Database persistence beyond Redis.
- ❌ Production deployment (no k8s, no Helm, no terraform).
- ❌ More than one task type or fan-out/fan-in topology.

If you find yourself adding any of those to *your* derivative work, that's
fine — but they're not what this repo is teaching.

## Things people get wrong

The things-people-get-wrong list is short and worth printing on a poster.

- **Putting business logic in the API handler "just for now".** The decoupling
  is the entire reason this repo exists. The `app/api.py` file is allowed to
  import `run_job` *only* because TaskIQ's decorator turns it into a
  message-sending proxy. Look at it again with that lens.
- **Using Pub/Sub alone.** No replay. Reconnect = lost state.
- **Using a list alone.** No live push. Clients have to poll.
- **Skipping the idempotency lock because "it's just a demo".** At-least-once
  delivery is real. A demo that doesn't show it teaches the wrong mental model.
- **Sending an HTTP error mid-stream.** SSE clients only see SSE. Pipeline
  failures must come through as SSE `error` events, not as a 500 on a stream
  that's already streaming.
- **Using TaskIQ's default in-memory broker for examples.** It has no
  redelivery, no ACK, no nothing. It hides every interesting failure mode.
  Use Streams.

## Project layout

```
longshot/
├── app/
│   ├── settings.py             env-driven config (LLMMode enum, timeouts, URLs)
│   ├── keys.py                 every Redis key string in one place
│   ├── broker.py               TaskIQ broker + PydanticJSONSerializer + retry
│   ├── contract.py             HTTP I/O Pydantic models + TaskResult
│   ├── events.py               typed event hierarchy + discriminated union
│   ├── publish.py              ordered drainer (the most interesting file)
│   ├── pipeline.py             fetch / chunk / summarize — mock | real | lmstudio
│   ├── tasks.py                @broker.task — idempotency, cancel, timeout, emit
│   ├── api.py                  FastAPI: POST /jobs, GET /jobs/{id}/stream, DELETE
│   └── worker.py               worker entry point + heartbeat
├── static/
│   └── index.html              vanilla-JS SSE viewer
├── demo/
│   ├── happy_path.sh           submit + tail until DONE
│   ├── chaos.sh                kill the worker mid-task, restart, observe
│   └── cancel.sh               submit + DELETE mid-stream
├── tests/
│   └── test_smoke.py           no-Redis-required wiring checks (10 tests)
├── docker-compose.yml          prod-style: build image + run
├── docker-compose.dev.yml      dev: bind-mount source + --reload everything
├── Dockerfile                  prod image
├── Dockerfile.dev              dev image (deps only; source via bind-mount)
├── Makefile                    up / dev-up / logs / demo / api / worker / test
├── pyproject.toml              uv-managed deps, MIT license
└── .env.example                every env var documented
```

## Run natively (no Docker)

You need a Redis instance reachable at `REDIS_URL` (default
`redis://localhost:6379/0`). Then, in two terminals:

```bash
# terminal 1
make api

# terminal 2
make worker
```

Then `bash demo/happy_path.sh` against `http://localhost:8000`.

## License

MIT — see `LICENSE`. Copy any of this freely; attribution welcome but not
required.

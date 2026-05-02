# FUTURE — explicitly out of scope

Things that would clearly be improvements, but that aren't part of the lesson
this repo exists to teach. Each is recorded so I (or anyone reading) don't
mistake their absence for an oversight.

## Architecture

- **Fan-out / fan-in tasks.** Summarize chunks in parallel sub-tasks, gather
  via a synthesis task. Demonstrates a different-shaped pipeline; doubles the
  control-flow complexity and obscures the core point.
- **Multi-worker scale-out demo.** Show two `worker` containers consuming the
  same Stream consumer group and balancing load. Easy to add (`docker compose
  up --scale worker=3`) but adds a "what does it prove" question to answer.
- **Dead-letter queue.** When a task exhausts retries, route it to a DLQ for
  manual inspection. Honestly important in production; deliberately omitted to
  keep the failure matrix readable.
- **Per-task priority lanes.** TaskIQ supports it; only matters at multi-tenant
  scale.

## Frontend

- A real React/HTMX viewer instead of the single-file vanilla page in `static/`.
  The vanilla page exists to prove no SPA framework is required — adding one
  would obscure that point.
- Streaming the final summary token-by-token (when LLM_MODE=real). Doable, just
  another event type; cut for v1.

## Ops

- Prometheus metrics endpoint, Grafana dashboard.
- Structured logging shipped to a sink (Loki / OpenTelemetry).
- A real Helm chart / Kubernetes manifests. Compose is the demo target.
- TLS / nginx in front of FastAPI.

## Hardening

- Auth on `/jobs` (an API key would be enough to block drive-by use). Skipped
  because authn is its own can of worms and the demo is local-only.
- Rate limiting per IP. Same reason.
- Body-size limits on URL fetch (we only check content-type).

## Tests

- Integration tests using `testcontainers-redis`. The smoke tests in `tests/`
  catch wiring breakage; integration would catch regressions in the publisher
  ordering and the SSE replay logic. Worth having; not on the critical path.
- Property-based tests on the publisher (concurrent emits → strictly increasing
  seqs at the consumer). The drainer makes this true *by construction*; a test
  would prove it under contention.

## Educational

- A second scenario (the CSV row processor from the brief) as a sibling
  example, to show the same pattern works for a different pipeline shape.
- A short blog post that walks through `app/publish.py` line by line —
  that's the most valuable file in here for someone doing the same in their
  own codebase.

#!/usr/bin/env bash
# Chaos demo: post a job, kill the worker mid-task, restart it, observe that
# the redelivered message is suppressed by the idempotency lock.
#
# What this proves: at-least-once delivery + an idempotency lock = no
# duplicate execution. What this does NOT prove: checkpointed resumption.
# The original task body never finishes — the lock just makes the second
# delivery a silent no-op. True resumption is a separate design (see
# FUTURE.md and the blog post).
#
# Run with `make up` first. The worker container must be named "longshot-worker-1"
# (compose default) — adjust WORKER_CONTAINER if you renamed it.

set -euo pipefail

API="${API:-http://localhost:8000}"
URL="${1:-https://en.wikipedia.org/wiki/Distributed_computing}"
WORKER_CONTAINER="${WORKER_CONTAINER:-longshot-worker-1}"

echo ">>> POST $API/jobs"
RESPONSE=$(curl -fsS -X POST "$API/jobs" \
  -H "content-type: application/json" \
  -d "{\"input\": \"$URL\", \"chunk_size_chars\": 1500, \"max_chunks\": 8}")
SESSION_ID=$(echo "$RESPONSE" | jq -r .session_id)
echo "session_id=$SESSION_ID"

echo
echo ">>> Watching SSE stream in the background..."
curl --no-buffer -fsS "$API/jobs/$SESSION_ID/stream" &
SSE_PID=$!

echo
echo ">>> Sleeping 3s, then killing the worker..."
sleep 3
docker kill "$WORKER_CONTAINER" || true

echo
echo ">>> Sleeping 5s, then restarting the worker..."
sleep 5
docker start "$WORKER_CONTAINER" || true

echo
echo ">>> Worker restarted. Redelivery observes the idempotency lock —"
echo ">>> no duplicate execution. The original task body did not finish;"
echo ">>> the second delivery is a silent no-op (TaskStatus.DUPLICATE_DELIVERY)."
echo ">>> Waiting for SSE to terminate (ctrl-c if it stalls)..."
wait "$SSE_PID" || true

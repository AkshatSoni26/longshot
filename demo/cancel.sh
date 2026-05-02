#!/usr/bin/env bash
# Cancel demo: post a long job, then DELETE it. Watch the worker exit cleanly.

set -euo pipefail
API="${API:-http://localhost:8000}"
URL="${1:-https://en.wikipedia.org/wiki/Cryptography}"

RESPONSE=$(curl -fsS -X POST "$API/jobs" \
  -H "content-type: application/json" \
  -d "{\"url\": \"$URL\", \"max_chunks\": 30, \"chunk_size_chars\": 800}")
SESSION_ID=$(echo "$RESPONSE" | jq -r .session_id)
echo "session_id=$SESSION_ID"

curl --no-buffer -fsS "$API/jobs/$SESSION_ID/stream" &
SSE_PID=$!

sleep 4
echo
echo ">>> DELETE /jobs/$SESSION_ID"
curl -fsS -X DELETE "$API/jobs/$SESSION_ID" | jq

wait "$SSE_PID" || true

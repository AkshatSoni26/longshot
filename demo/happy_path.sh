#!/usr/bin/env bash
# Happy-path demo: post a job, follow its SSE stream until DONE.
# Usage: bash demo/happy_path.sh [url]

set -euo pipefail

API="${API:-http://localhost:8000}"
URL="${1:-https://en.wikipedia.org/wiki/Redis}"
# Tunables — drop MAX_CHUNKS to 2 if you're running a slow local LM Studio
# model so the hard timeout doesn't hit you.
CHUNK_SIZE_CHARS="${CHUNK_SIZE_CHARS:-1500}"
MAX_CHUNKS="${MAX_CHUNKS:-4}"

if ! command -v jq >/dev/null 2>&1; then
  echo "This script wants 'jq' for pretty output. Install jq, or use curl directly."
  exit 1
fi

echo ">>> POST $API/jobs"
RESPONSE=$(curl -fsS -X POST "$API/jobs" \
  -H "content-type: application/json" \
  -d "{\"input\": \"$URL\", \"chunk_size_chars\": $CHUNK_SIZE_CHARS, \"max_chunks\": $MAX_CHUNKS}")
echo "$RESPONSE" | jq

SESSION_ID=$(echo "$RESPONSE" | jq -r .session_id)
STREAM_URL=$(echo "$RESPONSE" | jq -r .stream_url)

echo
echo ">>> SSE GET $STREAM_URL"
echo
# --no-buffer so events print as they arrive
curl --no-buffer -fsS "$STREAM_URL"

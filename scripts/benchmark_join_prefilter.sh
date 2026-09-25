#!/usr/bin/env bash
set -euo pipefail

# Benchmark helper for RFC #5093 (join key pre-filtering).
#
# Usage:
#   scripts/benchmark_join_prefilter.sh [QUERY_FILE]
#
# Env vars:
#   OS_ENDPOINT   (default: http://localhost:9200)
#   RUNS          (default: 10)
#   WARMUP        (default: 2)
#   USE_HYPERFINE (default: 1; set 0 to use curl loop)

QUERY_FILE="${1:-docs/dev/queries/join-key-prefiltering.ppl}"
OS_ENDPOINT="${OS_ENDPOINT:-http://localhost:9200}"
RUNS="${RUNS:-10}"
WARMUP="${WARMUP:-2}"
USE_HYPERFINE="${USE_HYPERFINE:-1}"

if [[ ! -f "$QUERY_FILE" ]]; then
  echo "Query file not found: $QUERY_FILE" >&2
  exit 1
fi

URL="${OS_ENDPOINT%/}/_plugins/_ppl"

if [[ "$USE_HYPERFINE" == "1" ]] && command -v hyperfine >/dev/null 2>&1; then
  hyperfine \
    --warmup "$WARMUP" \
    --runs "$RUNS" \
    "curl -sS -XPOST '$URL' --data-binary @$QUERY_FILE -H 'content-type: text/plain' >/dev/null"
  exit 0
fi

echo "hyperfine not available (or disabled). Falling back to curl loop..."
for i in $(seq 1 "$WARMUP"); do
  curl -sS -XPOST "$URL" --data-binary @"$QUERY_FILE" -H 'content-type: text/plain' >/dev/null
done

start=$(date +%s)
for i in $(seq 1 "$RUNS"); do
  curl -sS -XPOST "$URL" --data-binary @"$QUERY_FILE" -H 'content-type: text/plain' >/dev/null
done
end=$(date +%s)

elapsed=$((end - start))
echo "runs=$RUNS elapsed=${elapsed}s avg=$((elapsed / RUNS))s"

#!/usr/bin/env sh
set -eu

MODE="${MODE:-worker}"

if [ "$MODE" = "worker" ]; then
  : "${WORKER_NAME:?WORKER_NAME is required}"
  : "${DS_URL:?DS_URL is required}"
  : "${RELAY_URL:?RELAY_URL is required}"
  : "${LISTEN_ADDR:?LISTEN_ADDR is required}"

  exec worker \
    --name "$WORKER_NAME" \
    --ds-url "$DS_URL" \
    --relay-url "$RELAY_URL" \
    --listen-addr "$LISTEN_ADDR"
fi

if [ "$MODE" = "runner" ]; then
  exec benchmark_runner_http_staircase "$@"
fi

echo "Unknown MODE: $MODE" >&2
exit 1
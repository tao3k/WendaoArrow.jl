#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "$ROOT/../.." && pwd)"

exec direnv exec "$REPO_ROOT" julia --project="$ROOT" \
  "$ROOT/scripts/run_flight_example.jl" \
  "examples/stream_metadata_bad_response_flight_server.jl" \
  "$@"

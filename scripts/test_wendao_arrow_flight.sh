#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "$ROOT/../.." && pwd)"

exec direnv exec "$REPO_ROOT" julia --project="$ROOT" \
  "$ROOT/test/flight_grpcserver.jl"

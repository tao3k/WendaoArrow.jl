#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

exec "${JULIA:-julia}" --project="$ROOT" \
  "$ROOT/scripts/run_flight_example.jl" \
  "examples/list_route_roundtrip_flight_server.jl" \
  "$@"

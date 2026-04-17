#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -n "${PYTHON:-}" ]]; then
  read -r -a PYTHON_CMD <<<"${PYTHON}"
else
  PYTHON_CMD=(python3)
fi

if [[ -n "${JULIA:-}" ]]; then
  read -r -a JULIA_CMD <<<"${JULIA}"
else
  JULIA_CMD=(julia)
fi

"${PYTHON_CMD[@]}" -c "import pyarrow.flight" >/dev/null

HOST="${WENDAO_ARROW_STRESS_HOST:-127.0.0.1}"
PORT="$("${PYTHON_CMD[@]}" - <<'PY'
import socket

with socket.socket() as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
)"

DOC_BYTES="${WENDAO_ARROW_STRESS_DOC_BYTES:-2097152}"
WORKERS="${WENDAO_ARROW_STRESS_WORKERS:-4}"
SAMPLES="${WENDAO_ARROW_STRESS_SAMPLES:-4}"
REQUEST_ROWS="${WENDAO_ARROW_STRESS_REQUEST_ROWS:-32}"
WARMUP_SAMPLES="${WENDAO_ARROW_STRESS_WARMUP_SAMPLES:-1}"
WARMUP_REQUEST_ROWS="${WENDAO_ARROW_STRESS_WARMUP_REQUEST_ROWS:-4}"
MAX_ACTIVE_REQUESTS="${WENDAO_ARROW_STRESS_MAX_ACTIVE_REQUESTS:-8}"
REQUEST_CAPACITY="${WENDAO_ARROW_STRESS_REQUEST_CAPACITY:-8}"
RESPONSE_CAPACITY="${WENDAO_ARROW_STRESS_RESPONSE_CAPACITY:-8}"
PROCESSING_DELAY_MS="${WENDAO_ARROW_STRESS_PROCESSING_DELAY_MS:-5}"
DEADLINE="${WENDAO_ARROW_STRESS_DEADLINE:-30}"
THREADS="${JULIA_NUM_THREADS:-2}"

SERVER_LOG="$(mktemp)"

cleanup() {
  if [[ -n "${SERVER_PID:-}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    kill "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
  rm -f "${SERVER_LOG}"
}

trap cleanup EXIT

"${JULIA_CMD[@]}" \
  --threads="${THREADS}" \
  --project="${ROOT}" \
  "${ROOT}/scripts/run_packaged_flight_benchmark_server.jl" \
  --host "${HOST}" \
  --port "${PORT}" \
  --response-mode large_response \
  --large-doc-bytes "${DOC_BYTES}" \
  --processing-delay-ms "${PROCESSING_DELAY_MS}" \
  --max-active-requests "${MAX_ACTIVE_REQUESTS}" \
  --request-capacity "${REQUEST_CAPACITY}" \
  --response-capacity "${RESPONSE_CAPACITY}" \
  >"${SERVER_LOG}" 2>&1 &
SERVER_PID=$!

for _ in $(seq 1 60); do
  if grep -q '^READY ' "${SERVER_LOG}"; then
    break
  fi
  if ! kill -0 "${SERVER_PID}" 2>/dev/null; then
    cat "${SERVER_LOG}" >&2
    exit 1
  fi
  sleep 1
done

if ! grep -q '^READY ' "${SERVER_LOG}"; then
  cat "${SERVER_LOG}" >&2
  exit 1
fi

if [[ "${WARMUP_SAMPLES}" -gt 0 ]]; then
  "${PYTHON_CMD[@]}" \
    "${ROOT}/scripts/benchmark_packaged_flight_listener.py" \
    --host "${HOST}" \
    --port "${PORT}" \
    --workers 1 \
    --samples "${WARMUP_SAMPLES}" \
    --request-rows "${WARMUP_REQUEST_ROWS}" \
    --deadline "${DEADLINE}" >/dev/null
fi

CLIENT_OUTPUT="$("${PYTHON_CMD[@]}" \
  "${ROOT}/scripts/benchmark_packaged_flight_listener.py" \
  --host "${HOST}" \
  --port "${PORT}" \
  --workers "${WORKERS}" \
  --samples "${SAMPLES}" \
  --request-rows "${REQUEST_ROWS}" \
  --deadline "${DEADLINE}")"

printf '%s\n' "${CLIENT_OUTPUT}"

CLIENT_OUTPUT="${CLIENT_OUTPUT}" "${PYTHON_CMD[@]}" - <<'PY'
import os

lines = [line for line in os.environ["CLIENT_OUTPUT"].splitlines() if line.strip()]
if len(lines) < 2:
    raise SystemExit("expected benchmark output header and result row")

header = lines[0].split("\t")
row = lines[1].split("\t")
data = dict(zip(header, row))

attempts = int(data["attempts"])
success_count = int(data["success_count"])
failure_count = int(data["failure_count"])
throughput = float(data["throughput_mib_per_sec"])
response_bytes = int(data["response_bytes_per_request"])

if success_count != attempts:
    raise SystemExit(
        f"expected success_count == attempts, got {success_count} vs {attempts}"
    )
if failure_count != 0:
    raise SystemExit(f"expected zero benchmark failures, got {failure_count}")
if throughput <= 0:
    raise SystemExit(f"expected positive throughput, got {throughput}")
if response_bytes <= 0:
    raise SystemExit(
        f"expected positive response_bytes_per_request, got {response_bytes}"
    )
PY

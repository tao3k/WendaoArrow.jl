#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

ENV_PATH="${WENDAO_ARROW_BOOTSTRAP_ENV:-$(mktemp -d)}"
if [[ -z "${WENDAO_ARROW_BOOTSTRAP_ENV:-}" ]]; then
  trap 'rm -rf "${ENV_PATH}"' EXIT
fi
export WENDAO_ARROW_BOOTSTRAP_ENV="${ENV_PATH}"

if [[ ! -f "${ENV_PATH}/Project.toml" ]]; then
  "${JULIA:-julia}" "${ROOT}/scripts/prepare_wendao_arrow_env.jl"
fi

exec "${JULIA:-julia}" --project="${ENV_PATH}" -e \
  'using Pkg; Pkg.test("WendaoArrow"; coverage=false)'

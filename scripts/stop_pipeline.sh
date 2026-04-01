#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

DEFAULT_PROJECT_NAME="$(basename "${ROOT_DIR}" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9]+/-/g; s/^-+//; s/-+$//')"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-${DEFAULT_PROJECT_NAME}}"

docker compose down

echo "Pipeline stopped."

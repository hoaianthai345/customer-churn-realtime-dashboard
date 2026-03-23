#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

export COMPOSE_PROJECT_NAME="realtime-bi"

docker compose down -v --remove-orphans
rm -rf .checkpoints
mkdir -p .checkpoints

rm -f data/processed/*.csv || true

echo "Pipeline reset completed."

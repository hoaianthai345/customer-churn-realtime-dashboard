#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

COMPOSE_FILE="${DEMO_COMPOSE_FILE:-docker-compose.demo.yml}"
API_BASE_URL="${DEMO_API_BASE_URL:-http://localhost:8000}"
WEB_BASE_URL="${DEMO_WEB_BASE_URL:-http://localhost:3000}"
DEMO_MONTH="${DEMO_MONTH:-2017-04}"

required_paths=(
  "data/artifacts/feature_store"
  "data/artifacts_tab1_descriptive"
  "data/artifacts_tab2_predictive"
  "data/artifacts_tab3_prescriptive"
  "data/artifacts_tab3_monte_carlo"
)

wait_for_url() {
  local label="$1"
  local url="$2"
  local attempts="${3:-60}"
  local sleep_sec="${4:-2}"

  for attempt in $(seq 1 "${attempts}"); do
    if curl -fsS "${url}" >/dev/null 2>&1; then
      echo "${label} is ready: ${url}"
      return 0
    fi
    sleep "${sleep_sec}"
  done

  echo "${label} did not become ready in time: ${url}" >&2
  return 1
}

echo "Checking required artifact directories..."
missing_paths=0
for required_path in "${required_paths[@]}"; do
  if [[ -d "${required_path}" ]]; then
    echo "  OK  ${required_path}"
  else
    echo "  MISSING  ${required_path}" >&2
    missing_paths=1
  fi
done

if [[ "${missing_paths}" == "1" ]]; then
  echo "Demo artifacts are incomplete. Restore the missing directories before running the demo." >&2
  exit 1
fi

echo
echo "Starting artifact-backed demo stack..."
echo "Recreating demo containers so frontend and API share a fresh Compose network..."
docker compose -f "${COMPOSE_FILE}" down --remove-orphans >/dev/null 2>&1 || true

if ! docker compose -f "${COMPOSE_FILE}" up -d --build --force-recreate api web; then
  echo "Failed to start demo containers." >&2
  echo "If port 3000 or 8000 is already in use, stop the conflicting stack first." >&2
  exit 1
fi

echo
echo "Waiting for demo services..."
wait_for_url "FastAPI demo" "${API_BASE_URL}/health" 90 2
wait_for_url "Frontend demo" "${WEB_BASE_URL}" 90 2

echo
echo "Running demo validation..."
bash "${ROOT_DIR}/scripts/validate_demo.sh"

echo
echo "Demo stack is ready."
echo "Dashboard: ${WEB_BASE_URL}"
echo "API docs root: ${API_BASE_URL}/health"
echo "Demo month: ${DEMO_MONTH}"
echo "Mode: artifact-backed, stable for presentation."

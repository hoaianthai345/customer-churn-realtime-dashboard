#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

COMPOSE_FILE="${DEMO_COMPOSE_FILE:-docker-compose.demo.yml}"
API_BASE_URL="${DEMO_API_BASE_URL:-http://localhost:8000}"
WEB_BASE_URL="${DEMO_WEB_BASE_URL:-http://localhost:3000}"
DEMO_MONTH="${DEMO_MONTH:-2017-04}"
DEMO_YEAR="${DEMO_MONTH%%-*}"
DEMO_MONTH_NUM="${DEMO_MONTH##*-}"
CURL_MAX_TIME="${DEMO_CURL_MAX_TIME:-180}"

required_paths=(
  "data/artifacts/feature_store"
  "data/artifacts_tab1_descriptive"
  "data/artifacts_tab2_predictive"
  "data/artifacts_tab3_prescriptive"
  "data/artifacts_tab3_monte_carlo"
)

check_contains() {
  local label="$1"
  local url="$2"
  local expected="$3"
  local temp_file

  temp_file="$(mktemp)"
  curl -fsS --max-time "${CURL_MAX_TIME}" "${url}" -o "${temp_file}"

  if ! grep -Fq "${expected}" "${temp_file}"; then
    rm -f "${temp_file}"
    echo "Validation failed for ${label}. Expected response to contain: ${expected}" >&2
    echo "URL: ${url}" >&2
    exit 1
  fi

  rm -f "${temp_file}"
  echo "  OK  ${label}"
}

echo "Demo container status:"
docker compose -f "${COMPOSE_FILE}" ps

echo
echo "Checking artifact directories..."
for required_path in "${required_paths[@]}"; do
  if [[ ! -d "${required_path}" ]]; then
    echo "Missing required demo artifact directory: ${required_path}" >&2
    exit 1
  fi
  echo "  OK  ${required_path}"
done

echo
echo "Checking API endpoints..."
check_contains "API health" "${API_BASE_URL}/health" "\"status\":\"ok\""
check_contains "Month options" "${API_BASE_URL}/api/v1/month-options" "\"${DEMO_MONTH}\""
check_contains "Replay status" "${API_BASE_URL}/api/v1/replay/status" "\"status\""

echo
echo "Checking frontend..."
curl -fsSI --max-time "${CURL_MAX_TIME}" "${WEB_BASE_URL}" | head -n 1
check_contains "Frontend proxy" "${WEB_BASE_URL}/backend/api/v1/month-options" "\"${DEMO_MONTH}\""

echo
echo "Demo validation passed."

#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

mkdir -p data/raw data/processed data/sample .checkpoints

if command -v python3 >/dev/null 2>&1; then
  python3 -m pip install -r requirements.txt
else
  echo "python3 not found. Please install Python 3 first." >&2
  exit 1
fi

if [[ ! -f data/raw/members_v3.csv || ! -f data/raw/transactions_v2.csv || ! -f data/raw/user_logs_v2.csv ]]; then
  echo "Raw CSV files are missing in data/raw."
  echo "Extract from data/kkbox-churn-prediction-challenge.zip (.7z files) before running full pipeline."
fi

echo "Local setup completed."

#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from notebooks.team_code.notebook_lib.tab1_dashboard_chart_features import run_tab1_dashboard_chart_features
from notebooks.team_code.notebook_lib.tab2_dashboard_chart_features import run_tab2_dashboard_chart_features
from scripts.build_demo_payload_cache import build_tab1_cache, build_tab2_cache, collect_segment_filters


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the missing Tab 1 / Tab 2 chart artifacts and refresh demo payload cache.")
    parser.add_argument("--root", default=".", help="Project root. Defaults to the current working directory.")
    parser.add_argument("--month", default="201704", help="Month in YYYYMM format. Defaults to 201704.")
    parser.add_argument(
        "--no-refresh-cache",
        dest="refresh_cache",
        action="store_false",
        help="Only build parquet artifacts and skip JSON payload cache refresh.",
    )
    parser.set_defaults(refresh_cache=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    root_hint = Path(args.root).resolve()
    month_text = str(args.month).strip()
    if len(month_text) != 6 or not month_text.isdigit():
        raise ValueError("--month must be in YYYYMM format")

    score_month = int(month_text)
    month_start = date(int(month_text[:4]), int(month_text[4:6]), 1)

    tab1_result = run_tab1_dashboard_chart_features(
        feature_store_root_hint=root_hint,
        tab1_artifacts_root_hint=root_hint,
        score_month=score_month,
    )
    tab2_result = run_tab2_dashboard_chart_features(
        tab2_artifacts_root_hint=root_hint,
        score_month=score_month,
    )

    cache_payload: dict[str, object] | None = None
    if args.refresh_cache:
        filters = collect_segment_filters(root_hint)
        cache_payload = {
            "tab1": build_tab1_cache(root_hint, month_start, filters),
            "tab2": build_tab2_cache(root_hint, month_start, filters),
        }

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "score_month": score_month,
        "tab1": {
            "artifact_dir": str(tab1_result["artifact_dir"]),
            "output_dir": str(tab1_result["output_dir"]),
            "trend_rows": int(len(tab1_result["trend_monthly_summary"])),
            "risk_heatmap_rows": int(len(tab1_result["snapshot_risk_heatmap_all"])),
            "outputs": {name: str(path) for name, path in tab1_result["output_paths"].items()},
        },
        "tab2": {
            "artifact_dir": str(tab2_result["artifact_dir"]),
            "output_dir": str(tab2_result["output_dir"]),
            "executive_matrix_rows": int(len(tab2_result["executive_value_risk_matrix"])),
            "outputs": {name: str(path) for name, path in tab2_result["output_paths"].items()},
        },
        "payload_cache": cache_payload,
    }
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

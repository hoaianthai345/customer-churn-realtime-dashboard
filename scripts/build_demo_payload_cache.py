#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from apps.api_fastapi.artifact_backend import (
    ALLOWED_SEGMENT_TYPES,
    _payload_cache_dir,
    _snapshot_payload_cache_path,
    _tab1_payload_cache_path,
    _tab2_payload_cache_path,
    build_dashboard_snapshot_payload,
    build_tab1_descriptive_payload,
    build_tab2_predictive_payload,
    read_parquet_copy,
    resolve_feature_store_dir,
    resolve_tab1_artifacts_dir,
    resolve_tab2_artifacts_dir,
    yyyymm_to_label,
)


TAB1_DIMENSIONS = ("age", "gender", "txn_freq", "skip_ratio")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Precompute JSON payload cache for the artifact-backed demo.")
    parser.add_argument("--root", default=".", help="Project root. Defaults to the current working directory.")
    parser.add_argument("--month", default="201704", help="Month in YYYYMM format. Defaults to 201704.")
    return parser.parse_args()


def to_jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (datetime, date, pd.Timestamp)):
        return value.isoformat()
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, dict):
        return {str(key): to_jsonable(inner) for key, inner in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_jsonable(inner) for inner in value]
    if isinstance(value, pd.Series):
        return [to_jsonable(inner) for inner in value.tolist()]
    if isinstance(value, pd.DataFrame):
        return [to_jsonable(row) for row in value.to_dict(orient="records")]
    if isinstance(value, np.ndarray):
        return [to_jsonable(inner) for inner in value.tolist()]
    return value


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(to_jsonable(payload), ensure_ascii=False, indent=2), encoding="utf-8")


def collect_segment_filters(root_hint: Path) -> list[tuple[str | None, str | None]]:
    feature_store_dir = resolve_feature_store_dir(root_hint=root_hint, score_month=201704)
    feature_df = read_parquet_copy(feature_store_dir / "train_features_bi_all.parquet", columns=tuple(ALLOWED_SEGMENT_TYPES))

    filters: list[tuple[str | None, str | None]] = [(None, None)]
    for segment_type in sorted(ALLOWED_SEGMENT_TYPES):
        values = sorted(feature_df[segment_type].dropna().astype(str).unique().tolist())
        filters.extend((segment_type, value) for value in values)
    return filters


def build_tab1_cache(root_hint: Path, month_start: date, filters: Iterable[tuple[str | None, str | None]]) -> dict[str, Any]:
    target_month = month_start.year * 100 + month_start.month
    tab1_dir = resolve_tab1_artifacts_dir(root_hint=root_hint, score_month=target_month)
    cache_dir = _payload_cache_dir(tab1_dir)
    count = 0

    for dimension in TAB1_DIMENSIONS:
        for segment_type, segment_value in filters:
            payload = build_tab1_descriptive_payload(
                month_start,
                dimension=dimension,
                segment_type=segment_type,
                segment_value=segment_value,
                root_hint=root_hint,
                prefer_cache=False,
            )
            write_json(
                _tab1_payload_cache_path(
                    tab1_dir,
                    target_month=target_month,
                    dimension=dimension,
                    segment_type=segment_type,
                    segment_value=segment_value,
                ),
                payload,
            )
            count += 1

    snapshot_payload = build_dashboard_snapshot_payload(month_start, root_hint=root_hint, prefer_cache=False)
    write_json(_snapshot_payload_cache_path(tab1_dir, target_month=target_month), snapshot_payload)

    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "month": yyyymm_to_label(target_month),
        "payload_count": count + 1,
        "tab1_dimensions": list(TAB1_DIMENSIONS),
        "segment_filters": [
            {"segment_type": segment_type, "segment_value": segment_value}
            for segment_type, segment_value in filters
        ],
        "files": {
            "snapshot": _snapshot_payload_cache_path(tab1_dir, target_month=target_month).name,
        },
    }
    write_json(cache_dir / "payload_cache_manifest.json", manifest)
    return manifest


def build_tab2_cache(root_hint: Path, month_start: date, filters: Iterable[tuple[str | None, str | None]]) -> dict[str, Any]:
    target_month = month_start.year * 100 + month_start.month
    tab2_dir = resolve_tab2_artifacts_dir(root_hint=root_hint, score_month=target_month)
    cache_dir = _payload_cache_dir(tab2_dir)
    count = 0

    for segment_type, segment_value in filters:
        payload = build_tab2_predictive_payload(
            month_start,
            segment_type=segment_type,
            segment_value=segment_value,
            sample_limit=None,
            root_hint=root_hint,
            prefer_cache=False,
        )
        write_json(
            _tab2_payload_cache_path(
                tab2_dir,
                target_month=target_month,
                segment_type=segment_type,
                segment_value=segment_value,
            ),
            payload,
        )
        count += 1

    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "month": yyyymm_to_label(target_month),
        "payload_count": count,
        "segment_filters": [
            {"segment_type": segment_type, "segment_value": segment_value}
            for segment_type, segment_value in filters
        ],
    }
    write_json(cache_dir / "payload_cache_manifest.json", manifest)
    return manifest


def main() -> None:
    args = parse_args()
    root_hint = Path(args.root).resolve()
    month_text = str(args.month).strip()
    if len(month_text) != 6 or not month_text.isdigit():
        raise ValueError("--month must be in YYYYMM format")

    month_start = date(int(month_text[:4]), int(month_text[4:6]), 1)
    filters = collect_segment_filters(root_hint)

    tab1_manifest = build_tab1_cache(root_hint, month_start, filters)
    tab2_manifest = build_tab2_cache(root_hint, month_start, filters)

    print("Payload cache generated")
    print(json.dumps({"tab1": tab1_manifest, "tab2": tab2_manifest}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

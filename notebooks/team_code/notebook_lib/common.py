from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Iterable, Sequence

import numpy as np
import pandas as pd


def discover_project_dir(start: Path | None = None) -> Path:
    start = (start or Path.cwd()).resolve()
    for candidate in [start, *start.parents]:
        if (candidate / "team_code").exists() or (candidate / "project-realtime-bi").exists():
            return candidate
    return start


def build_upward_candidates(base_dir: Path, relative_parts: Sequence[str]) -> list[Path]:
    candidates: list[Path] = []
    for parent in [base_dir.resolve(), *base_dir.resolve().parents]:
        candidates.append(parent.joinpath(*relative_parts))
    return candidates


def resolve_feature_store_dir(
    root_hint: str | Path | None = None,
    score_month: int = 201704,
    required_files: Sequence[str] | None = None,
) -> Path:
    if required_files is None:
        required_files = (
            "train_features_bi_all.parquet",
            f"test_features_bi_{score_month}_full.parquet",
            "feature_columns.csv",
            "bi_dimension_columns.csv",
        )

    base_dir = discover_project_dir()
    candidates: list[Path] = []

    if root_hint is not None:
        root_hint = Path(root_hint)
        candidates.extend([root_hint, root_hint / "feature_store", root_hint / "data" / "artifacts" / "feature_store"])

    candidates.extend(
        [
            Path("/kaggle/input/kkbox-feature-store"),
            Path("/kaggle/input/kkbox-feature-store/feature_store"),
            Path("/kaggle/input/kkbox-churn-feature-store"),
            Path("/kaggle/input/kkbox-churn-feature-store/feature_store"),
            Path("/kaggle/input/kkbox-churn-output"),
            Path("/kaggle/input/kkbox-churn-output/feature_store"),
        ]
    )
    candidates.extend(build_upward_candidates(base_dir, ("artifacts", "feature_store")))
    candidates.extend(build_upward_candidates(base_dir, ("data", "artifacts", "feature_store")))
    candidates.extend(build_upward_candidates(base_dir, ("feature_store",)))

    seen: set[Path] = set()
    for candidate in candidates:
        candidate = candidate.resolve()
        if candidate in seen or not candidate.exists():
            continue
        seen.add(candidate)
        if all((candidate / name).exists() for name in required_files):
            return candidate

    required = ", ".join(required_files)
    raise FileNotFoundError(
        f"Khong tim thay feature store canonical. Can cac file: {required}."
    )


def resolve_tab2_artifacts_dir(
    root_hint: str | Path | None = None,
    score_month: int = 201704,
    required_files: Sequence[str] | None = None,
) -> Path:
    if required_files is None:
        required_files = (
            f"tab2_test_scored_{score_month}.parquet",
            "tab2_validation_metrics.json",
            "tab2_model_summary.json",
        )

    base_dir = discover_project_dir()
    candidates: list[Path] = []

    if root_hint is not None:
        root_hint = Path(root_hint)
        candidates.extend([root_hint, root_hint / "artifacts_tab2_predictive", root_hint / "data" / "artifacts_tab2_predictive"])

    candidates.extend(
        [
            Path("/kaggle/input/kkbox-tab2-predictive"),
            Path("/kaggle/input/kkbox-tab2-predictive/artifacts_tab2_predictive"),
            Path("/kaggle/input/kkbox-tab2-predictive-artifacts"),
            Path("/kaggle/input/kkbox-tab2-predictive-artifacts/artifacts_tab2_predictive"),
        ]
    )
    candidates.extend(build_upward_candidates(base_dir, ("artifacts_tab2_predictive",)))
    candidates.extend(build_upward_candidates(base_dir, ("data", "artifacts_tab2_predictive")))
    candidates.extend(build_upward_candidates(base_dir, ("artifacts", "tab2_predictive")))

    seen: set[Path] = set()
    for candidate in candidates:
        candidate = candidate.resolve()
        if candidate in seen or not candidate.exists():
            continue
        seen.add(candidate)
        if all((candidate / name).exists() for name in required_files):
            return candidate

    required = ", ".join(required_files)
    raise FileNotFoundError(
        f"Khong tim thay Tab 2 predictive artifacts. Can cac file: {required}."
    )


def resolve_tab1_artifacts_dir(
    root_hint: str | Path | None = None,
    score_month: int = 201704,
    required_files: Sequence[str] | None = None,
) -> Path:
    if required_files is None:
        required_files = (
            f"tab1_snapshot_{score_month}.parquet",
            "tab1_kpis_monthly.parquet",
            "tab1_km_curves.parquet",
            "manifest.json",
        )

    base_dir = discover_project_dir()
    candidates: list[Path] = []

    if root_hint is not None:
        root_hint = Path(root_hint)
        candidates.extend([root_hint, root_hint / "artifacts_tab1_descriptive", root_hint / "data" / "artifacts_tab1_descriptive"])

    candidates.extend(
        [
            Path("/kaggle/input/kkbox-tab1-descriptive"),
            Path("/kaggle/input/kkbox-tab1-descriptive/artifacts_tab1_descriptive"),
            Path("/kaggle/input/kkbox-tab1-descriptive-artifacts"),
            Path("/kaggle/input/kkbox-tab1-descriptive-artifacts/artifacts_tab1_descriptive"),
        ]
    )
    candidates.extend(build_upward_candidates(base_dir, ("artifacts_tab1_descriptive",)))
    candidates.extend(build_upward_candidates(base_dir, ("data", "artifacts_tab1_descriptive")))
    candidates.extend(build_upward_candidates(base_dir, ("artifacts", "tab1_descriptive")))

    seen: set[Path] = set()
    for candidate in candidates:
        candidate = candidate.resolve()
        if candidate in seen or not candidate.exists():
            continue
        seen.add(candidate)
        if all((candidate / name).exists() for name in required_files):
            return candidate

    required = ", ".join(required_files)
    raise FileNotFoundError(
        f"Khong tim thay Tab 1 descriptive artifacts. Can cac file: {required}."
    )


def ensure_columns(df: pd.DataFrame, required: Iterable[str], label: str) -> None:
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"{label} thieu cot bat buoc: {missing}")


def ensure_output_dir(output_dir: str | Path) -> Path:
    path = Path(output_dir).resolve()
    path.mkdir(parents=True, exist_ok=True)
    return path


def to_json_serializable(value):
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): to_json_serializable(val) for key, val in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_json_serializable(item) for item in value]
    return value


def write_json(path: str | Path, payload: dict) -> Path:
    path = Path(path).resolve()
    path.write_text(
        json.dumps(to_json_serializable(payload), indent=2, ensure_ascii=True),
        encoding="utf-8",
    )
    return path


def write_manifest(
    output_dir: str | Path,
    notebook_name: str,
    artifact_type: str,
    input_paths: dict[str, str | Path],
    output_paths: dict[str, str | Path],
    metadata: dict | None = None,
) -> Path:
    output_dir = ensure_output_dir(output_dir)
    payload = {
        "notebook_name": notebook_name,
        "artifact_type": artifact_type,
        "created_at_utc": datetime.now(UTC).isoformat(),
        "inputs": {key: str(value) for key, value in input_paths.items()},
        "outputs": {key: str(value) for key, value in output_paths.items()},
        "metadata": metadata or {},
    }
    return write_json(output_dir / "manifest.json", payload)


def risk_band_from_probability(probabilities: pd.Series) -> pd.Series:
    probs = probabilities.astype("float32").clip(lower=0.0, upper=1.0)
    return pd.Series(
        np.select(
            [
                probs >= 0.80,
                probs >= 0.60,
                probs >= 0.40,
                probs >= 0.20,
            ],
            ["Very High", "High", "Medium", "Low"],
            default="Very Low",
        ),
        index=probabilities.index,
    )


def make_yyyymm_label(target_month: pd.Series) -> pd.Series:
    month = target_month.astype("int32").astype(str).str.zfill(6)
    return month.str.slice(0, 4) + "-" + month.str.slice(4, 6)

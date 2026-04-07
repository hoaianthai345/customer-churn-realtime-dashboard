from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional
import gc
import json
import shutil
import subprocess

import numpy as np
import pandas as pd
from tqdm.auto import tqdm


KAGGLE_INPUT_DIR = Path("/kaggle/input/competitions/kkbox-churn-prediction-challenge")
SEVEN_ZIP_BIN = shutil.which("7z") or shutil.which("7za") or shutil.which("7zr")

ALL_TARGET_MONTHS = [
    201601,
    201602,
    201603,
    201604,
    201605,
    201606,
    201607,
    201608,
    201609,
    201610,
    201611,
    201612,
    201701,
    201702,
    201703,
    201704,
]
TRAIN_MONTHS = [201701, 201702, 201703]
TEST_MONTH = 201704
PREVIOUS_MONTH = {
    month: ALL_TARGET_MONTHS[idx - 1]
    for idx, month in enumerate(ALL_TARGET_MONTHS)
    if idx > 0
}
BI_DIMENSION_COLUMNS = [
    "target_month",
    "last_expire_month",
    "age_segment",
    "gender_profile",
    "renewal_segment",
    "price_segment",
    "loyalty_segment",
    "active_segment",
    "skip_segment",
    "discovery_segment",
    "rfm_segment",
    "bi_segment_name",
]
USER_LOG_FEATURE_COLS = [
    "num_25",
    "num_50",
    "num_75",
    "num_985",
    "num_100",
    "num_unq",
    "total_secs",
]
LOG_AGG_CACHE_VERSION = "v1"
FEATURE_STORE_COMPRESSION = "zstd"


def find_project_dir(start: Optional[Path] = None) -> Path:
    current = (start or Path.cwd()).resolve()
    search_roots = [current, *current.parents]
    for candidate in search_roots:
        if candidate.name == "project-realtime-bi":
            return candidate
        nested_repo = candidate / "project-realtime-bi"
        if nested_repo.exists() and nested_repo.is_dir():
            return nested_repo.resolve()
    return current


def choose_data_dir(project_dir: Path) -> Path:
    candidates = []
    if KAGGLE_INPUT_DIR.exists():
        candidates.append(KAGGLE_INPUT_DIR)
    candidates.extend([project_dir / "data" / "raw", project_dir / "input"])
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"Khong tim thay data dir hop le. Da thu: {[str(path) for path in candidates]}"
    )


def choose_artifact_root(project_dir: Path) -> Path:
    candidates = [project_dir / "data" / "artifacts", project_dir / "artifacts"]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


def choose_log_cache_dir(project_dir: Path, artifact_root: Path) -> Path:
    candidates = [
        artifact_root / "feature_store" / "log_agg_cache",
        project_dir / "data" / "artifacts" / "feature_store" / "log_agg_cache",
        project_dir / "artifacts" / "feature_store" / "log_agg_cache",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


def resolve_source_file(file_name: str, data_dir: Path, required: bool = True) -> Optional[Path]:
    csv_path = data_dir / file_name
    archive_path = data_dir / f"{file_name}.7z"
    if csv_path.exists():
        return csv_path
    if archive_path.exists():
        return archive_path
    if required:
        raise FileNotFoundError(f"Khong tim thay {file_name} hoac {file_name}.7z trong {data_dir}")
    return None


def read_csv_from_source(data_dir: Path, file_name: str, **kwargs) -> pd.DataFrame:
    source_path = resolve_source_file(file_name, data_dir)
    assert source_path is not None
    if source_path.suffix == ".csv":
        return pd.read_csv(source_path, **kwargs)

    if SEVEN_ZIP_BIN is None:
        raise RuntimeError(
            f"Cannot read {source_path} without a 7z binary. Kaggle usually has 7z preinstalled."
        )

    process = subprocess.Popen(
        [SEVEN_ZIP_BIN, "x", "-so", str(source_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        df = pd.read_csv(process.stdout, **kwargs)
        if process.stdout is not None:
            process.stdout.close()
        stderr_text = process.stderr.read().decode("utf-8", errors="ignore") if process.stderr is not None else ""
        return_code = process.wait()
        if return_code != 0:
            raise RuntimeError(f"7z failed for {source_path}: {stderr_text}")
        return df
    except Exception:
        process.kill()
        process.wait()
        raise


def load_transactions(data_dir: Path) -> pd.DataFrame:
    transaction_files = ["transactions.csv", "transactions_v2.csv"]
    parts = []
    usecols = [
        "msno",
        "transaction_date",
        "membership_expire_date",
        "payment_method_id",
        "payment_plan_days",
        "plan_list_price",
        "actual_amount_paid",
        "is_auto_renew",
        "is_cancel",
    ]
    dtypes = {
        "transaction_date": "int32",
        "membership_expire_date": "int32",
        "payment_method_id": "int16",
        "payment_plan_days": "int16",
        "plan_list_price": "float32",
        "actual_amount_paid": "float32",
        "is_auto_renew": "int8",
        "is_cancel": "int8",
    }

    available_files = []
    for file_name in transaction_files:
        if resolve_source_file(file_name, data_dir, required=False) is not None:
            available_files.append(file_name)
    if not available_files:
        raise FileNotFoundError(f"Khong tim thay transactions.csv hoac transactions_v2.csv trong {data_dir}")
    print(f"Using transaction files: {available_files}")
    missing_files = [file_name for file_name in transaction_files if file_name not in available_files]
    if missing_files:
        print(f"Warning: missing transaction sources: {missing_files}")

    for file_name in available_files:
        parts.append(read_csv_from_source(data_dir, file_name, usecols=usecols, dtype=dtypes))

    transactions = pd.concat(parts, ignore_index=True)
    invalid_expire_mask = transactions["membership_expire_date"] < transactions["transaction_date"]
    transactions["invalid_expire_before_txn"] = invalid_expire_mask.astype("int8")
    transactions.loc[invalid_expire_mask, "membership_expire_date"] = transactions.loc[
        invalid_expire_mask, "transaction_date"
    ]

    transactions["transaction_month"] = (transactions["transaction_date"] // 100).astype("int32")
    transactions["expire_month"] = (transactions["membership_expire_date"] // 100).astype("int32")
    transactions["transaction_dt"] = pd.to_datetime(
        transactions["transaction_date"].astype(str), format="%Y%m%d", errors="coerce"
    )
    transactions["expire_dt"] = pd.to_datetime(
        transactions["membership_expire_date"].astype(str), format="%Y%m%d", errors="coerce"
    )
    transactions = transactions.dropna(subset=["transaction_dt", "expire_dt"])
    transactions = transactions.sort_values(
        ["msno", "transaction_date", "membership_expire_date"]
    ).reset_index(drop=True)
    return transactions


def _same_day_order_key(
    idx: int,
    membership_expire_dates: np.ndarray,
    plan_list_prices: np.ndarray,
    payment_plan_days: np.ndarray,
    payment_method_ids: np.ndarray,
    is_cancels: np.ndarray,
) -> tuple:
    is_cancel = int(is_cancels[idx])
    expire_order = -int(membership_expire_dates[idx]) if is_cancel == 1 else int(membership_expire_dates[idx])
    return (
        -float(plan_list_prices[idx]),
        -int(payment_plan_days[idx]),
        -int(payment_method_ids[idx]),
        is_cancel,
        expire_order,
    )


def _select_effective_history_idx(
    history_end_idx: int,
    transaction_dates: np.ndarray,
    membership_expire_dates: np.ndarray,
    plan_list_prices: np.ndarray,
    payment_plan_days: np.ndarray,
    payment_method_ids: np.ndarray,
    is_cancels: np.ndarray,
) -> int | None:
    if history_end_idx <= 0:
        return None

    last_txn_date = int(transaction_dates[history_end_idx - 1])
    block_start = int(np.searchsorted(transaction_dates, last_txn_date, side="left"))
    best_idx = block_start
    best_key = _same_day_order_key(
        best_idx,
        membership_expire_dates,
        plan_list_prices,
        payment_plan_days,
        payment_method_ids,
        is_cancels,
    )

    for idx in range(block_start + 1, history_end_idx):
        candidate_key = _same_day_order_key(
            idx,
            membership_expire_dates,
            plan_list_prices,
            payment_plan_days,
            payment_method_ids,
            is_cancels,
        )
        if candidate_key >= best_key:
            best_idx = idx
            best_key = candidate_key

    return int(best_idx)


def _calculate_renewal_gap_days(
    future_start_idx: int,
    anchor_expire_dt: np.datetime64,
    transaction_dates: np.ndarray,
    transaction_dts: np.ndarray,
    expire_dts: np.ndarray,
    membership_expire_dates: np.ndarray,
    plan_list_prices: np.ndarray,
    payment_plan_days: np.ndarray,
    payment_method_ids: np.ndarray,
    is_cancels: np.ndarray,
) -> float | None:
    if future_start_idx >= len(transaction_dates):
        return None

    effective_expire_dt = anchor_expire_dt
    current_idx = future_start_idx
    total_rows = len(transaction_dates)
    while current_idx < total_rows:
        current_txn_date = int(transaction_dates[current_idx])
        day_end_idx = int(np.searchsorted(transaction_dates, current_txn_date, side="right"))
        day_indices = list(range(current_idx, day_end_idx))
        day_indices.sort(
            key=lambda idx: _same_day_order_key(
                idx,
                membership_expire_dates,
                plan_list_prices,
                payment_plan_days,
                payment_method_ids,
                is_cancels,
            )
        )

        for idx in day_indices:
            if int(is_cancels[idx]) == 1:
                if expire_dts[idx] < effective_expire_dt:
                    effective_expire_dt = expire_dts[idx]
                continue

            gap_days = float((transaction_dts[idx] - effective_expire_dt) / np.timedelta64(1, "D"))
            return gap_days

        current_idx = day_end_idx

    return None


def build_month_labels(
    transactions: pd.DataFrame,
    history_months: List[int],
    output_months: List[int],
) -> Dict[int, pd.DataFrame]:
    output_months_set = set(output_months)
    outputs: Dict[int, List[dict]] = {month: [] for month in output_months}
    grouped = transactions.groupby("msno", sort=False)
    ordered_history_months = [month for month in history_months if month in PREVIOUS_MONTH]

    for msno, user_txn in tqdm(grouped, total=transactions["msno"].nunique(), desc="Building labels"):
        user_txn = user_txn.reset_index(drop=True)
        transaction_dates = user_txn["transaction_date"].to_numpy()
        membership_expire_dates = user_txn["membership_expire_date"].to_numpy()
        transaction_months = user_txn["transaction_month"].to_numpy()
        transaction_dts = user_txn["transaction_dt"].to_numpy()
        expire_dts = user_txn["expire_dt"].to_numpy()
        is_cancels = user_txn["is_cancel"].to_numpy(dtype=np.int8)
        payment_method_ids = user_txn["payment_method_id"].to_numpy()
        payment_plan_days_all = user_txn["payment_plan_days"].to_numpy()
        plan_list_prices = user_txn["plan_list_price"].to_numpy()
        actual_amount_paids = user_txn["actual_amount_paid"].to_numpy()
        is_auto_renews = user_txn["is_auto_renew"].to_numpy(dtype=np.int8)
        invalid_expire_flags = user_txn["invalid_expire_before_txn"].to_numpy(dtype=np.int8)

        churn_history: List[int] = []

        for target_month in ordered_history_months:
            history_end_idx = int(np.searchsorted(transaction_months, target_month, side="left"))
            if history_end_idx <= 0:
                continue

            effective_idx = _select_effective_history_idx(
                history_end_idx,
                transaction_dates,
                membership_expire_dates,
                plan_list_prices,
                payment_plan_days_all,
                payment_method_ids,
                is_cancels,
            )
            if effective_idx is None:
                continue

            effective_expire_month = int(membership_expire_dates[effective_idx] // 100)
            if effective_expire_month != target_month:
                continue

            renewal_gap_days = _calculate_renewal_gap_days(
                history_end_idx,
                expire_dts[effective_idx],
                transaction_dates,
                transaction_dts,
                expire_dts,
                membership_expire_dates,
                plan_list_prices,
                payment_plan_days_all,
                payment_method_ids,
                is_cancels,
            )
            is_churn = int(renewal_gap_days is None or renewal_gap_days >= 30)

            history_len = len(churn_history)
            history_tail = [churn_history[-offset] if history_len >= offset else -1 for offset in range(1, 6)]
            churn_count = int(sum(churn_history)) if history_len > 0 else -1
            churn_rate = (sum(churn_history) / history_len) if history_len > 0 else -1
            transaction_count = history_len if history_len > 0 else -1

            payment_plan_days = payment_plan_days_all[effective_idx]
            actual_amount_paid = actual_amount_paids[effective_idx]
            plan_list_price = plan_list_prices[effective_idx]
            discount = plan_list_price - actual_amount_paid
            amt_per_day = actual_amount_paid / payment_plan_days if payment_plan_days not in [0, -1] else -1

            historical_transaction_rows = int(history_end_idx)
            history_slice = slice(0, history_end_idx)
            historical_cancel_count = int(is_cancels[history_slice].sum()) if historical_transaction_rows > 0 else -1
            historical_cancel_rate = (
                historical_cancel_count / historical_transaction_rows if historical_transaction_rows > 0 else -1
            )
            historical_auto_renew_rate = (
                float(is_auto_renews[history_slice].mean()) if historical_transaction_rows > 0 else -1
            )
            historical_paid_total = (
                float(actual_amount_paids[history_slice].sum()) if historical_transaction_rows > 0 else -1
            )
            historical_paid_mean = (
                float(actual_amount_paids[history_slice].mean()) if historical_transaction_rows > 0 else -1
            )
            historical_list_price_mean = (
                float(plan_list_prices[history_slice].mean()) if historical_transaction_rows > 0 else -1
            )

            if effective_idx > 0:
                prev_idx = effective_idx - 1
                days_since_previous_transaction = float(
                    (transaction_dts[effective_idx] - transaction_dts[prev_idx]) / np.timedelta64(1, "D")
                )
            else:
                days_since_previous_transaction = -1

            churn_history.append(is_churn)
            if target_month in output_months_set:
                outputs[target_month].append(
                    {
                        "msno": msno,
                        "target_month": target_month,
                        "is_churn": is_churn,
                        "expire_date": int(membership_expire_dates[effective_idx]),
                        "transaction_date": int(transaction_dates[effective_idx]),
                        "payment_method_id": payment_method_ids[effective_idx],
                        "payment_plan_days": payment_plan_days,
                        "plan_list_price": plan_list_price,
                        "actual_amount_paid": actual_amount_paid,
                        "is_auto_renew": is_auto_renews[effective_idx],
                        "invalid_expire_before_txn": invalid_expire_flags[effective_idx],
                        "last_1_is_churn": history_tail[0],
                        "last_2_is_churn": history_tail[1],
                        "last_3_is_churn": history_tail[2],
                        "last_4_is_churn": history_tail[3],
                        "last_5_is_churn": history_tail[4],
                        "churn_rate": churn_rate,
                        "churn_count": churn_count,
                        "transaction_count": transaction_count,
                        "discount": discount,
                        "is_discount": int(discount > 0),
                        "amt_per_day": amt_per_day,
                        "historical_transaction_rows": historical_transaction_rows,
                        "historical_paid_total": historical_paid_total,
                        "historical_paid_mean": historical_paid_mean,
                        "historical_list_price_mean": historical_list_price_mean,
                        "historical_cancel_count": historical_cancel_count,
                        "historical_cancel_rate": historical_cancel_rate,
                        "historical_auto_renew_rate": historical_auto_renew_rate,
                        "days_since_previous_transaction": days_since_previous_transaction,
                    }
                )

    output_columns = [
        "msno",
        "target_month",
        "is_churn",
        "expire_date",
        "transaction_date",
        "payment_method_id",
        "payment_plan_days",
        "plan_list_price",
        "actual_amount_paid",
        "is_auto_renew",
        "invalid_expire_before_txn",
        "last_1_is_churn",
        "last_2_is_churn",
        "last_3_is_churn",
        "last_4_is_churn",
        "last_5_is_churn",
        "churn_rate",
        "churn_count",
        "transaction_count",
        "discount",
        "is_discount",
        "amt_per_day",
        "historical_transaction_rows",
        "historical_paid_total",
        "historical_paid_mean",
        "historical_list_price_mean",
        "historical_cancel_count",
        "historical_cancel_rate",
        "historical_auto_renew_rate",
        "days_since_previous_transaction",
    ]

    result = {}
    for month, rows in outputs.items():
        frame = pd.DataFrame(rows, columns=output_columns)
        if not frame.empty:
            frame = frame.sort_values("msno").reset_index(drop=True)
        result[month] = frame
    return result


def _unpack_log_agg_frame(frame: pd.DataFrame) -> Dict[str, pd.DataFrame | pd.Series]:
    indexed = frame.set_index("msno").sort_index()
    return {
        "sum": indexed[USER_LOG_FEATURE_COLS].copy(),
        "count": indexed["count"].rename("count").copy(),
        "last_log_date": indexed["last_log_date"].rename("last_log_date").copy(),
        "capped_count": indexed["capped_log_count"].rename("capped_log_count").copy(),
    }


def load_cached_user_logs(
    cache_dir: Path,
    labels_by_month: Dict[int, pd.DataFrame],
    train_months: List[int],
    test_month: int,
) -> tuple[Dict[int, Dict[str, pd.DataFrame | pd.Series]], pd.DataFrame]:
    month_to_msnos = {
        PREVIOUS_MONTH[month]: set(labels_by_month[month]["msno"].tolist())
        for month in train_months + [test_month]
    }
    required_months = sorted(month_to_msnos.keys())

    aggregated: Dict[int, Dict[str, pd.DataFrame | pd.Series]] = {}
    coverage_rows = []
    for month in required_months:
        cache_path = cache_dir / f"user_log_agg_{LOG_AGG_CACHE_VERSION}_{month}.parquet"
        if not cache_path.exists():
            raise FileNotFoundError(f"Khong tim thay cache log cho month {month}: {cache_path}")

        frame = pd.read_parquet(cache_path)
        required_index = pd.Index(sorted(month_to_msnos[month]))
        cached_index = pd.Index(frame["msno"])
        missing_required_msnos = required_index.difference(cached_index)
        unexpected_msnos = cached_index.difference(required_index)
        filtered = frame[frame["msno"].isin(required_index)].copy()
        aggregated[month] = _unpack_log_agg_frame(filtered)
        coverage_rows.append(
            {
                "log_month": month,
                "required_msnos": len(required_index),
                "cached_rows": len(frame),
                "usable_rows": len(filtered),
                "missing_required_msnos": len(missing_required_msnos),
                "unexpected_cached_msnos": len(unexpected_msnos),
                "cache_path": str(cache_path),
            }
        )

    coverage_df = pd.DataFrame(coverage_rows)
    return aggregated, coverage_df


def safe_divide(numerator: pd.Series, denominator: pd.Series, default: float = 0.0) -> pd.Series:
    numerator = numerator.astype("float32")
    denominator = denominator.astype("float32")
    result = numerator.div(denominator.replace(0, np.nan))
    return result.replace([np.inf, -np.inf], np.nan).fillna(default).astype("float32")


def build_user_log_features(
    base_df: pd.DataFrame,
    prev_month: int,
    aggregated_user_logs: Dict[int, Dict[str, pd.DataFrame | pd.Series]],
) -> pd.DataFrame:
    payload = aggregated_user_logs.get(prev_month)
    if payload is None or payload["sum"] is None:
        raise ValueError(f"Missing aggregated user log payload for month {prev_month}")

    sum_df = payload["sum"].copy()
    count_series = payload["count"].copy()
    last_log_date = payload["last_log_date"].copy()
    capped_count = payload["capped_count"].copy()

    mean_df = sum_df.div(count_series, axis=0)
    mean_df = mean_df.rename(columns={col: f"{col}_mean" for col in mean_df.columns}).reset_index()
    sum_df = sum_df.rename(columns={col: f"{col}_sum" for col in sum_df.columns}).reset_index()
    count_df = count_series.reset_index()
    last_log_date_df = last_log_date.reset_index()
    capped_count_df = capped_count.reset_index()

    feature_df = base_df.merge(mean_df, on="msno", how="left")
    feature_df = feature_df.merge(sum_df, on="msno", how="left")
    feature_df = feature_df.merge(count_df, on="msno", how="left")
    feature_df = feature_df.merge(last_log_date_df, on="msno", how="left")
    feature_df = feature_df.merge(capped_count_df, on="msno", how="left")
    return feature_df


def load_members(data_dir: Path) -> pd.DataFrame:
    members = read_csv_from_source(
        data_dir,
        "members_v3.csv",
        usecols=["msno", "city", "bd", "gender", "registered_via", "registration_init_time"],
        dtype={
            "city": "float32",
            "bd": "float32",
            "registered_via": "float32",
            "registration_init_time": "int32",
        },
    )
    members["gender"] = members["gender"].map({"male": 1, "female": 2}).fillna(0).astype("int8")
    members["bd"] = members["bd"].where(members["bd"].between(15, 65))
    return members


def add_feature_layers(df: pd.DataFrame) -> pd.DataFrame:
    enriched = df.copy()

    transaction_dt = pd.to_datetime(
        enriched["transaction_date"].astype("Int64").astype(str),
        format="%Y%m%d",
        errors="coerce",
    )
    expire_dt = pd.to_datetime(
        enriched["expire_date"].astype("Int64").astype(str),
        format="%Y%m%d",
        errors="coerce",
    )
    registration_dt = pd.to_datetime(
        enriched["registration_init_time"].astype("Int64").astype(str),
        format="%Y%m%d",
        errors="coerce",
    )
    last_log_dt = pd.to_datetime(
        enriched["last_log_date"].astype("Int64").astype(str),
        format="%Y%m%d",
        errors="coerce",
    )
    snapshot_month_start = pd.to_datetime(
        enriched["target_month"].astype("Int64").astype(str) + "01",
        format="%Y%m%d",
        errors="coerce",
    )
    snapshot_dt = snapshot_month_start - pd.Timedelta(days=1)
    invalid_registration_after_snapshot = registration_dt.notna() & snapshot_dt.notna() & (registration_dt > snapshot_dt)
    enriched["invalid_registration_after_snapshot"] = invalid_registration_after_snapshot.astype("int8")
    if invalid_registration_after_snapshot.any():
        enriched.loc[invalid_registration_after_snapshot, "registration_init_time"] = np.nan
        registration_dt = registration_dt.where(~invalid_registration_after_snapshot)

    enriched["age"] = enriched["bd"].fillna(-1).astype("float32")
    enriched["has_valid_age"] = (enriched["age"] >= 0).astype("int8")
    enriched["transaction_month"] = (transaction_dt.dt.year * 100 + transaction_dt.dt.month).astype("Int32")
    enriched["expire_month"] = (expire_dt.dt.year * 100 + expire_dt.dt.month).astype("Int32")
    enriched["last_expire_month"] = enriched["expire_month"]
    enriched["transaction_day"] = transaction_dt.dt.day.astype("Int16")
    enriched["expire_day"] = expire_dt.dt.day.astype("Int16")
    enriched["registration_year"] = registration_dt.dt.year.astype("Int16")
    enriched["registration_month"] = registration_dt.dt.month.astype("Int16")
    enriched["registration_day"] = registration_dt.dt.day.astype("Int16")

    enriched["days_to_expire"] = (expire_dt - snapshot_dt).dt.days.astype("float32")
    enriched["membership_age_days"] = (snapshot_dt - registration_dt).dt.days.astype("float32")
    enriched["days_since_last_listen"] = (snapshot_dt - last_log_dt).dt.days.astype("float32")
    enriched["tenure_months"] = safe_divide(
        enriched["membership_age_days"], pd.Series(30.0, index=enriched.index), default=-1
    )

    enriched["expected_renewal_amount"] = np.where(
        enriched["actual_amount_paid"].fillna(0) > 0,
        enriched["actual_amount_paid"],
        enriched["plan_list_price"],
    ).astype("float32")
    enriched["price_gap"] = (enriched["plan_list_price"] - enriched["actual_amount_paid"]).astype("float32")
    enriched["discount_ratio"] = safe_divide(
        enriched["plan_list_price"] - enriched["actual_amount_paid"],
        enriched["plan_list_price"],
        default=0.0,
    )
    enriched["payment_to_list_ratio"] = safe_divide(
        enriched["actual_amount_paid"],
        enriched["plan_list_price"],
        default=0.0,
    )
    enriched["secs_per_log"] = safe_divide(enriched["total_secs_sum"], enriched["count"], default=0.0)
    enriched["unique_per_log"] = safe_divide(enriched["num_unq_sum"], enriched["count"], default=0.0)
    enriched["num100_per_log"] = safe_divide(enriched["num_100_sum"], enriched["count"], default=0.0)

    weighted_completion = (
        enriched["num_25_sum"] * 0.25
        + enriched["num_50_sum"] * 0.50
        + enriched["num_75_sum"] * 0.75
        + enriched["num_985_sum"] * 0.985
        + enriched["num_100_sum"] * 1.0
    ).astype("float32")
    enriched["weighted_completion_sum"] = weighted_completion
    enriched["weighted_completion_per_log"] = safe_divide(weighted_completion, enriched["count"], default=0.0)
    enriched["listen_events_sum"] = (
        enriched["num_25_sum"]
        + enriched["num_50_sum"]
        + enriched["num_75_sum"]
        + enriched["num_985_sum"]
        + enriched["num_100_sum"]
    ).astype("float32")
    enriched["skip_events_sum"] = (
        enriched["num_25_sum"] + enriched["num_50_sum"] + enriched["num_75_sum"]
    ).astype("float32")
    enriched["listen_events_per_log"] = safe_divide(enriched["listen_events_sum"], enriched["count"], default=0.0)
    enriched["avg_secs_per_unique"] = safe_divide(
        enriched["total_secs_sum"], enriched["num_unq_sum"], default=0.0
    )
    enriched["secs_per_plan_day"] = safe_divide(
        enriched["total_secs_sum"], enriched["payment_plan_days"], default=0.0
    )
    enriched["uniques_per_plan_day"] = safe_divide(
        enriched["num_unq_sum"], enriched["payment_plan_days"], default=0.0
    )
    enriched["logs_per_plan_day"] = safe_divide(
        enriched["count"], enriched["payment_plan_days"], default=0.0
    )
    enriched["remaining_plan_ratio"] = safe_divide(
        enriched["days_to_expire"], enriched["payment_plan_days"], default=0.0
    )
    enriched["completion_ratio"] = safe_divide(
        enriched["weighted_completion_sum"], enriched["listen_events_sum"], default=0.0
    )
    enriched["skip_ratio"] = safe_divide(
        enriched["skip_events_sum"], enriched["listen_events_sum"], default=0.0
    )
    enriched["discovery_ratio"] = safe_divide(
        enriched["num_unq_sum"], enriched["listen_events_sum"], default=0.0
    )
    enriched["replay_ratio"] = (1 - enriched["discovery_ratio"]).clip(lower=0, upper=1).astype("float32")
    enriched["price_gap_per_plan_day"] = safe_divide(
        enriched["price_gap"], enriched["payment_plan_days"], default=0.0
    )
    enriched["secs_per_paid_amount"] = safe_divide(
        enriched["total_secs_sum"], enriched["actual_amount_paid"], default=0.0
    )
    enriched["capped_log_share"] = safe_divide(
        enriched["capped_log_count"], enriched["count"], default=0.0
    )

    history_cols = [
        col
        for col in ["last_1_is_churn", "last_2_is_churn", "last_3_is_churn", "last_4_is_churn", "last_5_is_churn"]
        if col in enriched.columns
    ]
    if history_cols:
        history_frame = enriched[history_cols].replace(-1, 0).astype("float32")
        weights = np.arange(len(history_cols), 0, -1, dtype="float32")
        enriched["recent_churn_events"] = history_frame.sum(axis=1).astype("float32")
        enriched["weighted_recent_churn"] = history_frame.to_numpy(dtype="float32") @ weights

    enriched["is_expiring_user"] = 1
    enriched["is_manual_renew"] = (enriched["is_auto_renew"] == 0).astype("int8")
    enriched["high_skip_flag"] = (
        (enriched["listen_events_sum"] > 0) & (enriched["skip_ratio"] >= 0.5)
    ).astype("int8")
    enriched["low_discovery_flag"] = (
        (enriched["listen_events_sum"] > 0) & (enriched["discovery_ratio"] < 0.2)
    ).astype("int8")
    enriched["deal_hunter_flag"] = (
        (enriched["amt_per_day"] > 0) & (enriched["amt_per_day"] < 4.5)
    ).astype("int8")
    enriched["free_trial_flag"] = (enriched["expected_renewal_amount"] <= 0).astype("int8")
    enriched["content_fatigue_flag"] = (
        (enriched["listen_events_sum"] > 0)
        & (enriched["skip_ratio"] >= 0.5)
        & (enriched["discovery_ratio"] < 0.2)
    ).astype("int8")
    enriched["auto_renew_discount_interaction"] = (
        enriched["is_auto_renew"].astype("float32") * enriched["is_discount"].astype("float32")
    )
    enriched["churn_rate_x_transaction_count"] = (
        enriched["churn_rate"].astype("float32") * enriched["transaction_count"].astype("float32")
    )

    recency_score = np.select(
        [
            (enriched["listen_events_sum"] > 0) & (enriched["days_since_last_listen"] <= 7),
            (enriched["listen_events_sum"] > 0) & (enriched["days_since_last_listen"] <= 21),
            enriched["listen_events_sum"] > 0,
        ],
        [3, 2, 1],
        default=0,
    ).astype("int8")
    frequency_score = np.select(
        [enriched["count"] > 15, enriched["count"] > 5, enriched["count"] > 0],
        [3, 2, 1],
        default=0,
    ).astype("int8")
    monetary_score = np.select(
        [
            enriched["expected_renewal_amount"] >= 150,
            enriched["expected_renewal_amount"] >= 100,
            enriched["expected_renewal_amount"] > 0,
        ],
        [3, 2, 1],
        default=0,
    ).astype("int8")
    enriched["rfm_recency_score"] = recency_score
    enriched["rfm_frequency_score"] = frequency_score
    enriched["rfm_monetary_score"] = monetary_score
    enriched["rfm_total_score"] = (recency_score + frequency_score + monetary_score).astype("int8")

    age_segment_code = pd.Series(
        np.select(
            [
                enriched["age"].between(15, 20),
                enriched["age"].between(21, 25),
                enriched["age"].between(26, 35),
                enriched["age"].between(36, 50),
                enriched["age"].between(51, 65),
            ],
            [1, 2, 3, 4, 5],
            default=0,
        ).astype("int8"),
        index=enriched.index,
    )
    price_segment_code = pd.Series(
        np.select(
            [
                enriched["amt_per_day"] <= 0,
                (enriched["amt_per_day"] > 0) & (enriched["amt_per_day"] < 4.5),
                (enriched["amt_per_day"] >= 4.5) & (enriched["amt_per_day"] < 6.5),
                enriched["amt_per_day"] >= 6.5,
            ],
            [1, 2, 3, 4],
            default=0,
        ).astype("int8"),
        index=enriched.index,
    )
    loyalty_segment_code = pd.Series(
        np.select(
            [
                (enriched["membership_age_days"] >= 0) & (enriched["membership_age_days"] < 30),
                (enriched["membership_age_days"] >= 30) & (enriched["membership_age_days"] < 180),
                (enriched["membership_age_days"] >= 180) & (enriched["membership_age_days"] < 365),
                enriched["membership_age_days"] >= 365,
            ],
            [1, 2, 3, 4],
            default=0,
        ).astype("int8"),
        index=enriched.index,
    )
    active_segment_code = pd.Series(
        np.select(
            [
                enriched["count"] <= 0,
                (enriched["count"] > 0) & (enriched["count"] <= 5),
                (enriched["count"] > 5) & (enriched["count"] <= 15),
                enriched["count"] > 15,
            ],
            [1, 2, 3, 4],
            default=0,
        ).astype("int8"),
        index=enriched.index,
    )
    skip_segment_code = pd.Series(
        np.select(
            [
                enriched["listen_events_sum"] <= 0,
                enriched["skip_ratio"] < 0.2,
                enriched["skip_ratio"] < 0.5,
                enriched["skip_ratio"] >= 0.5,
            ],
            [0, 1, 2, 3],
            default=0,
        ).astype("int8"),
        index=enriched.index,
    )
    discovery_segment_code = pd.Series(
        np.select(
            [
                enriched["listen_events_sum"] <= 0,
                enriched["discovery_ratio"] < 0.2,
                enriched["discovery_ratio"] < 0.5,
                enriched["discovery_ratio"] >= 0.5,
            ],
            [0, 1, 2, 3],
            default=0,
        ).astype("int8"),
        index=enriched.index,
    )
    renewal_segment_code = pd.Series(
        np.select(
            [enriched["is_auto_renew"] == 1, enriched["is_auto_renew"] == 0],
            [1, 2],
            default=0,
        ).astype("int8"),
        index=enriched.index,
    )
    rfm_segment_code = pd.Series(
        np.select(
            [
                enriched["rfm_total_score"] >= 8,
                enriched["rfm_total_score"] >= 5,
                enriched["rfm_total_score"] > 0,
            ],
            [3, 2, 1],
            default=0,
        ).astype("int8"),
        index=enriched.index,
    )

    enriched["age_segment_code"] = age_segment_code
    enriched["price_segment_code"] = price_segment_code
    enriched["loyalty_segment_code"] = loyalty_segment_code
    enriched["active_segment_code"] = active_segment_code
    enriched["skip_segment_code"] = skip_segment_code
    enriched["discovery_segment_code"] = discovery_segment_code
    enriched["renewal_segment_code"] = renewal_segment_code
    enriched["rfm_segment_code"] = rfm_segment_code

    age_label_map = {
        0: "Unknown",
        1: "15-20",
        2: "21-25",
        3: "26-35",
        4: "36-50",
        5: "51-65",
    }
    price_label_map = {
        0: "Unknown",
        1: "Free Trial / Zero Pay",
        2: "Deal Hunter < 4.5",
        3: "Standard 4.5-6.5",
        4: "Premium >= 6.5",
    }
    loyalty_label_map = {
        0: "Unknown",
        1: "New < 30d",
        2: "Growing 30-179d",
        3: "Established 180-364d",
        4: "Loyal >= 365d",
    }
    active_label_map = {
        0: "Unknown",
        1: "Inactive",
        2: "Light 1-5 logs",
        3: "Active 6-15 logs",
        4: "Heavy > 15 logs",
    }
    skip_label_map = {
        0: "No Listening Data",
        1: "Low < 20%",
        2: "Medium 20-50%",
        3: "High >= 50%",
    }
    discovery_label_map = {
        0: "No Listening Data",
        1: "Habit < 20%",
        2: "Balanced 20-50%",
        3: "Explore >= 50%",
    }
    renewal_label_map = {
        0: "Unknown",
        1: "Pay_Auto-Renew",
        2: "Pay_Manual",
    }
    rfm_label_map = {
        0: "Unclassified",
        1: "Low Value",
        2: "Mid Value",
        3: "High Value",
    }
    gender_label_map = {
        0: "Unknown",
        1: "Male",
        2: "Female",
    }

    enriched["age_segment"] = age_segment_code.map(age_label_map)
    enriched["price_segment"] = price_segment_code.map(price_label_map)
    enriched["loyalty_segment"] = loyalty_segment_code.map(loyalty_label_map)
    enriched["active_segment"] = active_segment_code.map(active_label_map)
    enriched["skip_segment"] = skip_segment_code.map(skip_label_map)
    enriched["discovery_segment"] = discovery_segment_code.map(discovery_label_map)
    enriched["renewal_segment"] = renewal_segment_code.map(renewal_label_map)
    enriched["rfm_segment"] = rfm_segment_code.map(rfm_label_map)
    enriched["gender"] = enriched["gender"].fillna(0).astype("int8")
    enriched["gender_profile"] = enriched["gender"].map(gender_label_map)

    enriched["bi_segment_name"] = (
        enriched["loyalty_segment"].astype(str)
        + " | "
        + enriched["renewal_segment"].astype(str)
        + " | "
        + enriched["price_segment"].astype(str)
        + " | "
        + enriched["discovery_segment"].astype(str)
    )

    enriched = enriched.replace([np.inf, -np.inf], np.nan)
    numeric_cols = enriched.select_dtypes(include=[np.number]).columns
    enriched[numeric_cols] = enriched[numeric_cols].fillna(-1)
    return enriched


def validate_snapshot_df(bi_snapshot_df: pd.DataFrame) -> None:
    invalid_snapshot_recency = bi_snapshot_df[
        (bi_snapshot_df["last_log_date"] > 0)
        & (bi_snapshot_df["count"] > 0)
        & (bi_snapshot_df["days_since_last_listen"] < 0)
    ]
    if not invalid_snapshot_recency.empty:
        raise ValueError(
            "days_since_last_listen < 0 cho mot so dong co log thang truoc. Kiem tra lai moc snapshot_dt."
        )

    invalid_days_to_expire = bi_snapshot_df[
        (bi_snapshot_df["days_to_expire"] < 0) | (bi_snapshot_df["days_to_expire"] > 31)
    ]
    if not invalid_days_to_expire.empty:
        raise ValueError(
            "days_to_expire nam ngoai khoang snapshot hop ly [0, 31]. Kiem tra lai cong thuc days_to_expire."
        )

    invalid_membership_age = bi_snapshot_df[
        (bi_snapshot_df["registration_init_time"] > 0) & (bi_snapshot_df["membership_age_days"] < 0)
    ]
    if not invalid_membership_age.empty:
        raise ValueError(
            "membership_age_days < 0 cho mot so user co registration_init_time hop le. Kiem tra lai moc snapshot_dt."
        )


def build_submission_alignment(
    data_dir: Path,
    test_feature_df: pd.DataFrame,
    strict_test_cohort_match: bool = False,
) -> tuple[Optional[pd.DataFrame], Optional[dict]]:
    sample_source = resolve_source_file("sample_submission_v2.csv", data_dir, required=False)
    if sample_source is None:
        return None, None

    sample_submission = read_csv_from_source(data_dir, "sample_submission_v2.csv", usecols=["msno"])
    if not sample_submission["msno"].is_unique:
        duplicate_count = int(sample_submission["msno"].duplicated().sum())
        raise ValueError(f"sample_submission_v2.csv co {duplicate_count} msno bi duplicate.")
    if not test_feature_df["msno"].is_unique:
        duplicate_count = int(test_feature_df["msno"].duplicated().sum())
        raise ValueError(f"test_feature_df co {duplicate_count} msno bi duplicate truoc khi merge test cohort.")

    sample_msnos = pd.Index(sample_submission["msno"])
    test_feature_msnos = pd.Index(test_feature_df["msno"])
    missing_test_msnos = sample_msnos.difference(test_feature_msnos)
    unexpected_test_msnos = test_feature_msnos.difference(sample_msnos)
    report = {
        "sample_submission_rows": int(len(sample_submission)),
        "built_test_rows": int(len(test_feature_df)),
        "missing_from_built_test": int(len(missing_test_msnos)),
        "unexpected_in_built_test": int(len(unexpected_test_msnos)),
        "exact_match": bool(len(missing_test_msnos) == 0 and len(unexpected_test_msnos) == 0),
        "strict_mode": bool(strict_test_cohort_match),
        "sample_source": str(sample_source),
    }
    if strict_test_cohort_match and not report["exact_match"]:
        raise ValueError(
            "Test cohort tu build khong khop sample_submission_v2.csv: "
            f"missing={len(missing_test_msnos)}, unexpected={len(unexpected_test_msnos)}"
        )

    submission_df = sample_submission.merge(test_feature_df, on="msno", how="left")
    submission_df["target_month"] = submission_df["target_month"].fillna(TEST_MONTH).astype("Int32")
    submission_df["missing_from_built_test"] = submission_df["transaction_date"].isna().astype("int8")
    return submission_df, report


def backfill_member_columns(test_df: pd.DataFrame, members: pd.DataFrame) -> pd.DataFrame:
    member_columns = [column for column in members.columns if column != "msno"]
    member_lookup = members[["msno", *member_columns]].copy()
    aligned = test_df.merge(member_lookup, on="msno", how="left", suffixes=("", "_member"))
    for column in member_columns:
        member_column = f"{column}_member"
        if member_column in aligned.columns:
            aligned[column] = aligned[column].combine_first(aligned[member_column])
            aligned = aligned.drop(columns=[member_column])
    numeric_cols = aligned.select_dtypes(include=[np.number]).columns
    aligned[numeric_cols] = aligned[numeric_cols].fillna(-1)
    return aligned


def run_feature_prep_from_cache(
    output_subdir: str = "feature_store_from_cache",
    strict_test_cohort_match: bool = False,
) -> dict:
    project_dir = find_project_dir()
    data_dir = choose_data_dir(project_dir)
    artifact_root = choose_artifact_root(project_dir)
    cache_dir = choose_log_cache_dir(project_dir, artifact_root)
    feature_store_dir = artifact_root / output_subdir
    feature_store_dir.mkdir(parents=True, exist_ok=True)

    print(f"PROJECT_DIR = {project_dir}")
    print(f"DATA_DIR = {data_dir}")
    print(f"ARTIFACT_ROOT = {artifact_root}")
    print(f"LOG_AGG_CACHE_DIR = {cache_dir}")
    print(f"FEATURE_STORE_DIR = {feature_store_dir}")

    transactions = load_transactions(data_dir)
    labels_by_month = build_month_labels(
        transactions=transactions,
        history_months=ALL_TARGET_MONTHS,
        output_months=TRAIN_MONTHS + [TEST_MONTH],
    )
    del transactions
    gc.collect()

    aggregated_user_logs, cache_coverage_df = load_cached_user_logs(
        cache_dir=cache_dir,
        labels_by_month=labels_by_month,
        train_months=TRAIN_MONTHS,
        test_month=TEST_MONTH,
    )
    print(cache_coverage_df)

    monthly_feature_frames: Dict[int, pd.DataFrame] = {}
    for month in TRAIN_MONTHS + [TEST_MONTH]:
        prev_month = PREVIOUS_MONTH[month]
        frame = build_user_log_features(labels_by_month[month], prev_month, aggregated_user_logs)
        monthly_feature_frames[month] = frame
        print(f"{month} raw feature frame: {frame.shape}")

    members = load_members(data_dir)
    monthly_bi_frames: Dict[int, pd.DataFrame] = {}
    for month, frame in monthly_feature_frames.items():
        enriched_frame = frame.merge(members, on="msno", how="left")
        enriched_frame = add_feature_layers(enriched_frame)
        monthly_bi_frames[month] = enriched_frame
        print(f"{month} enriched frame: {enriched_frame.shape}")

    bi_snapshot_df = pd.concat(
        [monthly_bi_frames[month] for month in TRAIN_MONTHS + [TEST_MONTH]],
        ignore_index=True,
    )
    validate_snapshot_df(bi_snapshot_df)

    train_bi_df = pd.concat(
        [monthly_bi_frames[month] for month in TRAIN_MONTHS],
        ignore_index=True,
    )
    test_cohort_bi_df = monthly_bi_frames[TEST_MONTH].drop(columns=["is_churn"])
    submission_aligned_df, submission_report = build_submission_alignment(
        data_dir=data_dir,
        test_feature_df=test_cohort_bi_df,
        strict_test_cohort_match=strict_test_cohort_match,
    )

    if submission_aligned_df is not None:
        submission_aligned_df = backfill_member_columns(submission_aligned_df, members)

    model_exclude = {"msno", "is_churn", "transaction_date", "expire_date"}
    model_feature_cols = [
        col
        for col in train_bi_df.columns
        if col not in model_exclude and pd.api.types.is_numeric_dtype(train_bi_df[col])
    ]
    train_model_df = train_bi_df[["msno", "is_churn", "transaction_date", "expire_date", *model_feature_cols]].copy()
    test_cohort_model_df = test_cohort_bi_df[["msno", "transaction_date", "expire_date", *model_feature_cols]].copy()

    train_model_df.to_parquet(
        feature_store_dir / "train_features_all.parquet",
        index=False,
        compression=FEATURE_STORE_COMPRESSION,
    )
    test_cohort_model_df.to_parquet(
        feature_store_dir / f"test_features_{TEST_MONTH}_cohort.parquet",
        index=False,
        compression=FEATURE_STORE_COMPRESSION,
    )
    bi_snapshot_df.to_parquet(
        feature_store_dir / "bi_feature_master.parquet",
        index=False,
        compression=FEATURE_STORE_COMPRESSION,
    )
    train_bi_df.to_parquet(
        feature_store_dir / "train_features_bi_all.parquet",
        index=False,
        compression=FEATURE_STORE_COMPRESSION,
    )
    test_cohort_bi_df.to_parquet(
        feature_store_dir / f"test_features_bi_{TEST_MONTH}_cohort.parquet",
        index=False,
        compression=FEATURE_STORE_COMPRESSION,
    )
    pd.Series(model_feature_cols, name="feature").to_csv(
        feature_store_dir / "feature_columns.csv",
        index=False,
    )
    pd.Series(BI_DIMENSION_COLUMNS, name="dimension").to_csv(
        feature_store_dir / "bi_dimension_columns.csv",
        index=False,
    )
    cache_coverage_df.to_csv(
        feature_store_dir / "log_cache_coverage.csv",
        index=False,
    )

    if submission_aligned_df is not None:
        submission_aligned_df.to_parquet(
            feature_store_dir / f"test_features_bi_{TEST_MONTH}_submission_aligned.parquet",
            index=False,
            compression=FEATURE_STORE_COMPRESSION,
        )
        submission_model_df = submission_aligned_df[
            ["msno", "transaction_date", "expire_date", *model_feature_cols]
        ].copy()
        submission_model_df.to_parquet(
            feature_store_dir / f"test_features_{TEST_MONTH}_submission_aligned.parquet",
            index=False,
            compression=FEATURE_STORE_COMPRESSION,
        )
    else:
        submission_model_df = None

    summary = {
        "project_dir": str(project_dir),
        "data_dir": str(data_dir),
        "artifact_root": str(artifact_root),
        "log_cache_dir": str(cache_dir),
        "feature_store_dir": str(feature_store_dir),
        "train_rows": int(len(train_bi_df)),
        "test_cohort_rows": int(len(test_cohort_bi_df)),
        "bi_snapshot_rows": int(len(bi_snapshot_df)),
        "model_feature_count": int(len(model_feature_cols)),
    }
    if submission_report is not None:
        summary.update(
            {
                "submission_rows": submission_report["sample_submission_rows"],
                "submission_exact_match": submission_report["exact_match"],
                "missing_from_built_test": submission_report["missing_from_built_test"],
                "unexpected_in_built_test": submission_report["unexpected_in_built_test"],
            }
        )
        with (feature_store_dir / f"submission_alignment_{TEST_MONTH}.json").open("w", encoding="utf-8") as file:
            json.dump(submission_report, file, ensure_ascii=False, indent=2)

    with (feature_store_dir / "feature_prep_from_cache_summary.json").open("w", encoding="utf-8") as file:
        json.dump(summary, file, ensure_ascii=False, indent=2)

    print(json.dumps(summary, ensure_ascii=False, indent=2))

    del members
    del monthly_feature_frames
    del monthly_bi_frames
    gc.collect()

    return {
        "summary": summary,
        "cache_coverage_df": cache_coverage_df,
        "train_bi_df": train_bi_df,
        "test_cohort_bi_df": test_cohort_bi_df,
        "submission_aligned_df": submission_aligned_df,
        "bi_snapshot_df": bi_snapshot_df,
        "train_model_df": train_model_df,
        "test_cohort_model_df": test_cohort_model_df,
        "submission_model_df": submission_model_df,
        "submission_report": submission_report,
    }

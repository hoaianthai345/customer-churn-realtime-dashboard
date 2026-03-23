import csv
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _resolve_project_path(path_value: str) -> Path:
    path = Path(path_value)
    if path.exists():
        return path
    if path.is_absolute() and str(path).startswith("/opt/project"):
        relative = str(path).replace("/opt/project/", "", 1)
        local_path = PROJECT_ROOT / relative
        if local_path.exists():
            return local_path
    return path


def data_file(base_dir_env: str, file_env: str, default_file: str) -> Path:
    base_dir_value = os.getenv(base_dir_env, f"{PROJECT_ROOT}/data/raw")
    filename = os.getenv(file_env, default_file)
    return _resolve_project_path(str(Path(base_dir_value) / filename))


def processed_file(filename_env: str, default_file: str) -> Path:
    base_dir_value = os.getenv("PROCESSED_DATA_DIR", f"{PROJECT_ROOT}/data/processed")
    filename = os.getenv(filename_env, default_file)
    path = _resolve_project_path(str(Path(base_dir_value) / filename))
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def read_csv_rows(path: Path) -> Iterable[Dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            yield row


def write_csv_rows(path: Path, rows: List[Dict[str, object]], fieldnames: List[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def parse_date_yyyymmdd(value: str) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y%m%d").date().isoformat()
    except ValueError:
        return None


def parse_date_flexible(value: str) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    for pattern in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, pattern).date().isoformat()
        except ValueError:
            continue
    return None


def to_int(value: object) -> Optional[int]:
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def to_float(value: object) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    try:
        return float(text)
    except ValueError:
        return None

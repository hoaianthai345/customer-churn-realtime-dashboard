from __future__ import annotations

import json
import time
from pathlib import Path

import pandas as pd

from apps.api_fastapi.artifact_backend import read_json_copy, read_parquet_copy


def test_read_json_copy_refreshes_when_file_changes(tmp_path: Path) -> None:
    path = tmp_path / "payload.json"
    path.write_text(json.dumps({"value": "old"}), encoding="utf-8")

    first = read_json_copy(path)
    assert first["value"] == "old"

    time.sleep(0.01)
    path.write_text(json.dumps({"value": "new", "month": "2017-03"}), encoding="utf-8")

    second = read_json_copy(path)
    assert second["value"] == "new"
    assert second["month"] == "2017-03"


def test_read_parquet_copy_refreshes_when_file_changes(tmp_path: Path) -> None:
    path = tmp_path / "payload.parquet"
    pd.DataFrame([{"value": 1}]).to_parquet(path, index=False)

    first = read_parquet_copy(path)
    assert first["value"].tolist() == [1]

    time.sleep(0.01)
    pd.DataFrame([{"value": 3}, {"value": 5}]).to_parquet(path, index=False)

    second = read_parquet_copy(path)
    assert second["value"].tolist() == [3, 5]

import os
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[3]
load_dotenv(PROJECT_ROOT / ".env")


def checkpoint_path(job_name: str) -> str:
    base = os.getenv("SPARK_CHECKPOINT_BASE", "/tmp/realtime-bi-checkpoints")
    base_path = Path(base)
    if str(base_path).startswith("/opt/project"):
        local = Path(str(base_path).replace("/opt/project/", f"{PROJECT_ROOT}/", 1))
        local.mkdir(parents=True, exist_ok=True)
    return str(base_path / job_name)

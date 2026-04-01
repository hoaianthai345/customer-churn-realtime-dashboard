import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[3]
load_dotenv(PROJECT_ROOT / ".env")


def _resolve_path(path_value: str) -> Path:
    path = Path(path_value)
    if path.exists():
        return path
    if path.is_absolute() and str(path).startswith("/opt/project"):
        relative = str(path).replace("/opt/project/", "", 1)
        local_path = PROJECT_ROOT / relative
        if local_path.exists() or local_path.parent.exists():
            return local_path
    return path


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


def _env_optional_int(name: str) -> Optional[int]:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return None
    return int(raw)


@dataclass(frozen=True)
class ProducerSettings:
    kafka_bootstrap_servers: str
    topic_member_events: str
    topic_transaction_events: str
    topic_user_log_events: str
    replay_sleep_seconds: int
    max_replay_days: Optional[int]
    replay_start_date: str
    clickhouse_host: str
    clickhouse_port: int
    clickhouse_user: str
    clickhouse_password: str
    clickhouse_db: str
    processed_data_dir: Path
    members_clean_file: str
    transactions_clean_file: str
    user_logs_clean_file: str

    @property
    def members_clean_path(self) -> Path:
        return _resolve_path(str(self.processed_data_dir / self.members_clean_file))

    @property
    def transactions_clean_path(self) -> Path:
        return _resolve_path(str(self.processed_data_dir / self.transactions_clean_file))

    @property
    def user_logs_clean_path(self) -> Path:
        return _resolve_path(str(self.processed_data_dir / self.user_logs_clean_file))


def get_settings() -> ProducerSettings:
    return ProducerSettings(
        kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
        topic_member_events=os.getenv("TOPIC_MEMBER_EVENTS", "member_events"),
        topic_transaction_events=os.getenv("TOPIC_TRANSACTION_EVENTS", "transaction_events"),
        topic_user_log_events=os.getenv("TOPIC_USER_LOG_EVENTS", "user_log_events"),
        replay_sleep_seconds=_env_int("REPLAY_SLEEP_SECONDS", 2),
        max_replay_days=_env_optional_int("MAX_REPLAY_DAYS"),
        replay_start_date=os.getenv("REPLAY_START_DATE", "2017-03-01").strip() or "2017-03-01",
        clickhouse_host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        clickhouse_port=_env_int("CLICKHOUSE_HTTP_PORT", 8123),
        clickhouse_user=os.getenv("CLICKHOUSE_USER", "default"),
        clickhouse_password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        clickhouse_db=os.getenv("CLICKHOUSE_DB", "realtime_bi"),
        processed_data_dir=_resolve_path(os.getenv("PROCESSED_DATA_DIR", str(PROJECT_ROOT / "data/processed"))),
        members_clean_file=os.getenv("MEMBERS_CLEAN_FILE", "members_clean.csv"),
        transactions_clean_file=os.getenv("TRANSACTIONS_CLEAN_FILE", "transactions_clean.csv"),
        user_logs_clean_file=os.getenv("USER_LOGS_CLEAN_FILE", "user_logs_clean.csv"),
    )

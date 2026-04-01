import csv
import json
from pathlib import Path
from types import SimpleNamespace

from apps.producers import replay_user_logs


class FakeProducer:
    def __init__(self) -> None:
        self.sent = []
        self.flush_count = 0
        self.closed = False

    def send(self, topic, key, value):
        self.sent.append((topic, key, value))

    def flush(self):
        self.flush_count += 1

    def close(self):
        self.closed = True


def _write_user_logs_csv(path: Path, rows) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "msno",
                "date",
                "num_25",
                "num_50",
                "num_75",
                "num_985",
                "num_100",
                "num_unq",
                "total_secs",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


def test_replay_user_logs_orders_days_and_respects_max_days(tmp_path, monkeypatch):
    csv_path = tmp_path / "user_logs_clean.csv"
    _write_user_logs_csv(
        csv_path,
        [
            {
                "msno": "u1",
                "date": "2017-03-31",
                "num_25": "1",
                "num_50": "2",
                "num_75": "3",
                "num_985": "4",
                "num_100": "5",
                "num_unq": "8",
                "total_secs": "20.5",
            },
            {
                "msno": "u2",
                "date": "2017-03-30",
                "num_25": "0",
                "num_50": "1",
                "num_75": "0",
                "num_985": "0",
                "num_100": "2",
                "num_unq": "3",
                "total_secs": "10.0",
            },
            {
                "msno": "u3",
                "date": "2017-03-31",
                "num_25": "2",
                "num_50": "1",
                "num_75": "1",
                "num_985": "0",
                "num_100": "2",
                "num_unq": "4",
                "total_secs": "11.0",
            },
            {
                "msno": "u4",
                "date": "2017-04-01",
                "num_25": "1",
                "num_50": "0",
                "num_75": "0",
                "num_985": "0",
                "num_100": "1",
                "num_unq": "2",
                "total_secs": "6.0",
            },
            {
                "msno": "u5",
                "date": "2017-03-29",
                "num_25": "1",
                "num_50": "0",
                "num_75": "0",
                "num_985": "0",
                "num_100": "1",
                "num_unq": "2",
                "total_secs": "5.0",
            },
        ],
    )

    settings = SimpleNamespace(
        user_logs_clean_path=csv_path,
        replay_start_date="2017-03-30",
        max_replay_days=2,
        kafka_bootstrap_servers="unused",
        topic_user_log_events="user_log_events",
        replay_sleep_seconds=0,
    )

    producer = FakeProducer()
    monkeypatch.setattr(replay_user_logs, "get_settings", lambda: settings)
    monkeypatch.setattr(replay_user_logs, "build_kafka_producer", lambda _: producer)
    monkeypatch.setattr(replay_user_logs.time, "sleep", lambda _: None)

    replay_user_logs.main()

    sent_dates = [json.loads(value.decode("utf-8"))["date"] for _, _, value in producer.sent]
    sent_msnos = [key.decode("utf-8") for _, key, _ in producer.sent]

    assert sent_dates == ["2017-03-30", "2017-03-31", "2017-03-31"]
    assert sent_msnos == ["u2", "u1", "u3"]
    assert producer.flush_count == 2
    assert producer.closed is True

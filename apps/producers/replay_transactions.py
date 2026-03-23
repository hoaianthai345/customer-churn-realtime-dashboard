#!/usr/bin/env python3
import logging
import time

from apps.producers.common.config import get_settings
from apps.producers.common.serializers import to_value_bytes, transaction_payload
from apps.producers.common.utils import build_kafka_producer, group_rows_by_date, read_csv

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    settings = get_settings()
    rows = read_csv(settings.transactions_clean_path)
    batches = group_rows_by_date(rows, "transaction_date")

    if settings.max_replay_days is not None:
        batches = batches[: settings.max_replay_days]

    producer = build_kafka_producer(settings.kafka_bootstrap_servers)

    total_events = 0
    for idx, (event_date, day_rows) in enumerate(batches, start=1):
        for row in day_rows:
            payload = transaction_payload(row)
            if not payload["msno"]:
                continue
            producer.send(
                topic=settings.topic_transaction_events,
                key=payload["msno"].encode("utf-8"),
                value=to_value_bytes(payload),
            )
            total_events += 1

        producer.flush()
        logger.info("Replayed transaction batch %s day=%s rows=%s", idx, event_date, len(day_rows))
        time.sleep(settings.replay_sleep_seconds)

    producer.close()
    logger.info("Transaction replay done. total_events=%s", total_events)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
import argparse
import logging

import clickhouse_connect

from apps.producers.common.config import get_settings
from apps.producers.common.serializers import member_payload, to_value_bytes
from apps.producers.common.utils import build_kafka_producer, read_csv

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def bootstrap_to_clickhouse(rows, settings) -> None:
    client = clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_db,
    )

    data = []
    for row in rows:
        payload = member_payload(row)
        if not payload["msno"] or not payload["registration_init_time"]:
            continue
        data.append(
            [
                payload["msno"],
                payload["city"],
                payload["bd"],
                payload["gender"],
                payload["registered_via"],
                payload["registration_init_time"],
            ]
        )

    if not data:
        logger.warning("No valid member rows to insert.")
        return

    client.insert(
        table="dim_members",
        data=data,
        column_names=[
            "msno",
            "city",
            "bd",
            "gender",
            "registered_via",
            "registration_init_time",
        ],
    )
    logger.info("Inserted %s members into ClickHouse dim_members", len(data))


def publish_member_events(rows, settings) -> None:
    producer = build_kafka_producer(settings.kafka_bootstrap_servers)
    sent = 0
    for row in rows:
        payload = member_payload(row)
        if not payload["msno"]:
            continue
        producer.send(
            topic=settings.topic_member_events,
            key=payload["msno"].encode("utf-8"),
            value=to_value_bytes(payload),
        )
        sent += 1
    producer.flush()
    producer.close()
    logger.info("Published %s member events to topic=%s", sent, settings.topic_member_events)


def main() -> None:
    parser = argparse.ArgumentParser(description="Bootstrap member dimension")
    parser.add_argument(
        "--publish-events",
        action="store_true",
        help="Publish member events to Kafka topic after ClickHouse bootstrap",
    )
    args = parser.parse_args()

    settings = get_settings()
    rows = read_csv(settings.members_clean_path)
    logger.info("Loaded %s rows from %s", len(rows), settings.members_clean_path)

    bootstrap_to_clickhouse(rows, settings)

    if args.publish_events:
        publish_member_events(rows, settings)


if __name__ == "__main__":
    main()

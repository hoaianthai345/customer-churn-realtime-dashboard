import csv
import logging
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

from kafka import KafkaProducer

logger = logging.getLogger(__name__)


def build_kafka_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        linger_ms=20,
        retries=5,
        acks="all",
    )


def read_csv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def group_rows_by_date(rows: Iterable[Dict[str, str]], date_column: str) -> List[Tuple[str, List[Dict[str, str]]]]:
    grouped: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for row in rows:
        date_value = (row.get(date_column) or "").strip()
        if not date_value:
            continue
        grouped[date_value].append(row)
    return sorted(grouped.items(), key=lambda item: item[0])


def publish_rows(
    producer: KafkaProducer,
    topic: str,
    rows: Iterable[Dict[str, str]],
    key_field: str,
    value_serializer,
) -> int:
    sent = 0
    for row in rows:
        key = (row.get(key_field) or "").encode("utf-8")
        value = value_serializer(row)
        producer.send(topic=topic, key=key, value=value)
        sent += 1
    producer.flush()
    logger.info("Published %s messages to %s", sent, topic)
    return sent

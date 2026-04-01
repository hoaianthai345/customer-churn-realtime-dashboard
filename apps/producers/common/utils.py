import csv
import logging
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

logger = logging.getLogger(__name__)


def build_kafka_producer(bootstrap_servers: str) -> Any:
    from kafka import KafkaProducer

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


def _parse_iso_date(value: str) -> Optional[date]:
    text = (value or "").strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        return None


def filter_batches_from_start_date(
    batches: List[Tuple[str, List[Dict[str, str]]]],
    start_date: Optional[str],
) -> List[Tuple[str, List[Dict[str, str]]]]:
    if not start_date:
        return batches

    parsed_start_date = _parse_iso_date(start_date)
    if parsed_start_date is None:
        logger.warning("Invalid REPLAY_START_DATE=%s. Expected YYYY-MM-DD. Skip date filter.", start_date)
        return batches

    filtered: List[Tuple[str, List[Dict[str, str]]]] = []
    invalid_batch_dates = 0
    for event_date, day_rows in batches:
        parsed_event_date = _parse_iso_date(event_date)
        if parsed_event_date is None:
            invalid_batch_dates += 1
            filtered.append((event_date, day_rows))
            continue
        if parsed_event_date >= parsed_start_date:
            filtered.append((event_date, day_rows))

    if invalid_batch_dates:
        logger.warning(
            "Found %s batch date values not in YYYY-MM-DD format. Those batches were kept unfiltered.",
            invalid_batch_dates,
        )

    logger.info(
        "Applied replay date filter from %s. batches_before=%s batches_after=%s",
        parsed_start_date.isoformat(),
        len(batches),
        len(filtered),
    )
    return filtered


def publish_rows(
    producer: Any,
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

#!/usr/bin/env python3
import csv
import logging
import os
import tempfile
import time
from datetime import date, datetime
from pathlib import Path
from typing import BinaryIO, Dict, List, Optional, Set

from apps.producers.common.config import get_settings
from apps.producers.common.serializers import to_value_bytes, user_log_payload
from apps.producers.common.utils import build_kafka_producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


LOG_PROGRESS_EVERY_ROWS = 1_000_000
DEFAULT_REPLAY_FLUSH_EVERY = 20_000


def _parse_iso_date(value: str) -> Optional[date]:
    text = (value or "").strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        return None


def _open_day_payload_file(
    base_dir: Path,
    event_date: str,
    file_handles: Dict[str, BinaryIO],
    file_paths: Dict[str, Path],
) -> BinaryIO:
    handle = file_handles.get(event_date)
    if handle is not None:
        return handle

    path = base_dir / f"{event_date}.jsonl"
    handle = path.open("ab")
    file_handles[event_date] = handle
    file_paths[event_date] = path
    return handle


def _close_payload_files(file_handles: Dict[str, BinaryIO]) -> None:
    for handle in file_handles.values():
        handle.close()


def _discover_replay_dates(
    csv_path: Path,
    replay_start: Optional[date],
    max_replay_days: Optional[int],
) -> List[str]:
    scanned_rows = 0
    invalid_date_rows = 0
    empty_msno_rows = 0
    filtered_before_start = 0
    unique_dates: Set[str] = set()

    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            scanned_rows += 1
            payload = user_log_payload(row)
            if not payload["msno"]:
                empty_msno_rows += 1
                continue

            event_date = payload["date"]
            parsed_date = _parse_iso_date(event_date)
            if parsed_date is None:
                invalid_date_rows += 1
                continue
            if replay_start is not None and parsed_date < replay_start:
                filtered_before_start += 1
                continue

            unique_dates.add(event_date)
            if scanned_rows % LOG_PROGRESS_EVERY_ROWS == 0:
                logger.info(
                    "Discovering replay dates... scanned_rows=%s unique_dates=%s",
                    scanned_rows,
                    len(unique_dates),
                )

    replay_dates = sorted(unique_dates)
    if max_replay_days is not None:
        replay_dates = replay_dates[:max_replay_days]

    logger.info(
        "Replay day discovery done. scanned_rows=%s replay_days=%s filtered_before_start=%s invalid_date_rows=%s empty_msno_rows=%s",
        scanned_rows,
        len(replay_dates),
        filtered_before_start,
        invalid_date_rows,
        empty_msno_rows,
    )
    return replay_dates


def _spool_selected_days(
    csv_path: Path,
    replay_dates: List[str],
    spool_dir: Path,
) -> Dict[str, Path]:
    selected_dates = set(replay_dates)
    file_handles: Dict[str, BinaryIO] = {}
    file_paths: Dict[str, Path] = {}
    scanned_rows = 0
    spooled_rows = 0

    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            scanned_rows += 1
            payload = user_log_payload(row)
            msno = payload["msno"]
            if not msno:
                continue

            event_date = payload["date"]
            if event_date not in selected_dates:
                continue

            # Guard malformed event_date rows even if selected set is valid.
            if _parse_iso_date(event_date) is None:
                continue

            payload_file = _open_day_payload_file(
                base_dir=spool_dir,
                event_date=event_date,
                file_handles=file_handles,
                file_paths=file_paths,
            )
            payload_file.write(msno.encode("utf-8"))
            payload_file.write(b"\t")
            payload_file.write(to_value_bytes(payload))
            payload_file.write(b"\n")
            spooled_rows += 1

            if scanned_rows % LOG_PROGRESS_EVERY_ROWS == 0:
                logger.info(
                    "Spooling replay payloads... scanned_rows=%s spooled_rows=%s selected_days=%s",
                    scanned_rows,
                    spooled_rows,
                    len(selected_dates),
                )

    _close_payload_files(file_handles)
    logger.info(
        "Spooling done. scanned_rows=%s spooled_rows=%s selected_days=%s",
        scanned_rows,
        spooled_rows,
        len(selected_dates),
    )
    return file_paths


def main() -> None:
    settings = get_settings()
    replay_start = _parse_iso_date(settings.replay_start_date)
    if settings.replay_start_date and replay_start is None:
        logger.warning(
            "Invalid REPLAY_START_DATE=%s. Expected YYYY-MM-DD. Skip date filter.",
            settings.replay_start_date,
        )

    replay_dates = _discover_replay_dates(
        csv_path=settings.user_logs_clean_path,
        replay_start=replay_start,
        max_replay_days=settings.max_replay_days,
    )
    if not replay_dates:
        logger.info("No user-log batches eligible for replay.")
        return

    flush_every_raw = (
        os.getenv("REPLAY_FLUSH_EVERY", str(DEFAULT_REPLAY_FLUSH_EVERY)).strip() or str(DEFAULT_REPLAY_FLUSH_EVERY)
    )
    try:
        flush_every = max(1, int(flush_every_raw))
    except ValueError:
        flush_every = DEFAULT_REPLAY_FLUSH_EVERY
        logger.warning(
            "Invalid REPLAY_FLUSH_EVERY=%s. Fallback to %s.",
            flush_every_raw,
            flush_every,
        )

    with tempfile.TemporaryDirectory(prefix="replay_user_logs_") as temp_dir:
        spool_dir = Path(temp_dir)
        file_paths = _spool_selected_days(
            csv_path=settings.user_logs_clean_path,
            replay_dates=replay_dates,
            spool_dir=spool_dir,
        )

        producer = build_kafka_producer(settings.kafka_bootstrap_servers)
        total_events = 0
        try:
            for idx, event_date in enumerate(replay_dates, start=1):
                payload_path = file_paths[event_date]
                day_rows = 0

                with payload_path.open("rb") as payload_stream:
                    for line in payload_stream:
                        if not line:
                            continue
                        row = line.rstrip(b"\n")
                        if not row:
                            continue
                        parts = row.split(b"\t", 1)
                        if len(parts) != 2:
                            continue
                        key_bytes, value_bytes = parts
                        if not key_bytes:
                            continue
                        producer.send(
                            topic=settings.topic_user_log_events,
                            key=key_bytes,
                            value=value_bytes,
                        )
                        day_rows += 1
                        if day_rows % flush_every == 0:
                            producer.flush()

                producer.flush()
                total_events += day_rows
                logger.info("Replayed user log batch %s day=%s rows=%s", idx, event_date, day_rows)
                time.sleep(settings.replay_sleep_seconds)
        finally:
            producer.close()

    logger.info(
        "User log replay done. total_events=%s replay_days=%s flush_every=%s",
        total_events,
        len(replay_dates),
        flush_every,
    )


if __name__ == "__main__":
    main()

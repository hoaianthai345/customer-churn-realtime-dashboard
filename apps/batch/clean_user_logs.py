#!/usr/bin/env python3
import logging

from apps.batch.common import (
    data_file,
    parse_date_flexible,
    processed_file,
    read_csv_rows,
    to_float,
    to_int,
    write_csv_rows,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


OUTPUT_FIELDS = [
    "msno",
    "date",
    "num_25",
    "num_50",
    "num_75",
    "num_985",
    "num_100",
    "num_unq",
    "total_secs",
]


def main() -> None:
    source = data_file("RAW_DATA_DIR", "USER_LOGS_RAW_FILE", "user_logs_v2.csv")
    target = processed_file("USER_LOGS_CLEAN_FILE", "user_logs_clean.csv")

    cleaned_rows = []
    dropped = 0

    for row in read_csv_rows(source):
        msno = (row.get("msno") or "").strip()
        log_date = parse_date_flexible(row.get("date", ""))

        num_25 = to_int(row.get("num_25"))
        num_50 = to_int(row.get("num_50"))
        num_75 = to_int(row.get("num_75"))
        num_985 = to_int(row.get("num_985"))
        num_100 = to_int(row.get("num_100"))
        num_unq = to_int(row.get("num_unq"))
        total_secs = to_float(row.get("total_secs"))

        if not msno or log_date is None:
            dropped += 1
            continue

        if None in (num_25, num_50, num_75, num_985, num_100, num_unq, total_secs):
            dropped += 1
            continue

        if min(num_25, num_50, num_75, num_985, num_100, num_unq) < 0 or total_secs < 0:
            dropped += 1
            continue

        cleaned_rows.append(
            {
                "msno": msno,
                "date": log_date,
                "num_25": num_25,
                "num_50": num_50,
                "num_75": num_75,
                "num_985": num_985,
                "num_100": num_100,
                "num_unq": num_unq,
                "total_secs": total_secs,
            }
        )

    write_csv_rows(target, cleaned_rows, OUTPUT_FIELDS)
    logging.info("user_logs cleaned: %s rows, dropped=%s, output=%s", len(cleaned_rows), dropped, target)


if __name__ == "__main__":
    main()

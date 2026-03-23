#!/usr/bin/env python3
import logging

from apps.batch.common import (
    data_file,
    parse_date_flexible,
    processed_file,
    read_csv_rows,
    to_int,
    write_csv_rows,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


OUTPUT_FIELDS = ["msno", "is_churn", "churn_event_date"]


def main() -> None:
    source = data_file("RAW_DATA_DIR", "TRAIN_RAW_FILE", "train_v2.csv")
    target = processed_file("TRAIN_CLEAN_FILE", "train_clean.csv")

    cleaned_rows = []
    dropped = 0

    for row in read_csv_rows(source):
        msno = (row.get("msno") or "").strip()
        is_churn = to_int(row.get("is_churn"))

        if not msno or is_churn not in (0, 1):
            dropped += 1
            continue

        churn_event_date = parse_date_flexible(
            row.get("transaction_date")
            or row.get("membership_expire_date")
            or row.get("date")
            or ""
        )

        cleaned_rows.append(
            {
                "msno": msno,
                "is_churn": is_churn,
                "churn_event_date": churn_event_date or "",
            }
        )

    write_csv_rows(target, cleaned_rows, OUTPUT_FIELDS)
    logging.info("train cleaned: %s rows, dropped=%s, output=%s", len(cleaned_rows), dropped, target)


if __name__ == "__main__":
    main()

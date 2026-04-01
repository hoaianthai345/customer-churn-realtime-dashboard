#!/usr/bin/env python3
import logging

from apps.batch.common import (
    data_file,
    parse_date_yyyymmdd,
    processed_file,
    read_csv_rows,
    to_int,
    write_csv_rows,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


OUTPUT_FIELDS = [
    "msno",
    "city",
    "bd",
    "gender",
    "registered_via",
    "registration_init_time",
]


def main() -> None:
    source = data_file("RAW_DATA_DIR", "MEMBERS_RAW_FILE", "members_v3.csv")
    target = processed_file("MEMBERS_CLEAN_FILE", "members_clean.csv")

    cleaned_rows = []
    dropped = 0

    for row in read_csv_rows(source):
        msno = (row.get("msno") or "").strip()
        if not msno:
            dropped += 1
            continue

        registration_init_time = parse_date_yyyymmdd(row.get("registration_init_time", ""))
        if registration_init_time is None:
            dropped += 1
            continue

        city = to_int(row.get("city"))
        bd = to_int(row.get("bd"))
        registered_via = to_int(row.get("registered_via"))
        gender = (row.get("gender") or "").strip().lower() or "unknown"

        # Rule-based cleanup: replace bd=0 with null.
        if bd <= 0:
            bd = None

        cleaned_rows.append(
            {
                "msno": msno,
                "city": city,
                "bd": bd,
                "gender": gender,
                "registered_via": registered_via,
                "registration_init_time": registration_init_time,
            }
        )

    write_csv_rows(target, cleaned_rows, OUTPUT_FIELDS)
    logging.info("members cleaned: %s rows, dropped=%s, output=%s", len(cleaned_rows), dropped, target)


if __name__ == "__main__":
    main()

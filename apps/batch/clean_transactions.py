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
    "payment_method_id",
    "payment_plan_days",
    "plan_list_price",
    "actual_amount_paid",
    "is_auto_renew",
    "transaction_date",
    "membership_expire_date",
    "is_cancel",
]


def main() -> None:
    source = data_file("RAW_DATA_DIR", "TRANSACTIONS_RAW_FILE", "transactions_v2.csv")
    target = processed_file("TRANSACTIONS_CLEAN_FILE", "transactions_clean.csv")

    cleaned_rows = []
    dropped = 0

    for row in read_csv_rows(source):
        msno = (row.get("msno") or "").strip()
        transaction_date = parse_date_flexible(row.get("transaction_date", ""))
        membership_expire_date = parse_date_flexible(row.get("membership_expire_date", ""))

        payment_method_id = to_int(row.get("payment_method_id"))
        payment_plan_days = to_int(row.get("payment_plan_days"))
        plan_list_price = to_float(row.get("plan_list_price"))
        actual_amount_paid = to_float(row.get("actual_amount_paid"))
        is_auto_renew = to_int(row.get("is_auto_renew"))
        is_cancel = to_int(row.get("is_cancel"))

        if not msno or transaction_date is None or membership_expire_date is None:
            dropped += 1
            continue

        if None in (
            payment_method_id,
            payment_plan_days,
            plan_list_price,
            actual_amount_paid,
            is_auto_renew,
            is_cancel,
        ):
            dropped += 1
            continue

        if plan_list_price < 0 or actual_amount_paid < 0 or payment_plan_days < 0:
            dropped += 1
            continue

        cleaned_rows.append(
            {
                "msno": msno,
                "payment_method_id": payment_method_id,
                "payment_plan_days": payment_plan_days,
                "plan_list_price": plan_list_price,
                "actual_amount_paid": actual_amount_paid,
                "is_auto_renew": int(is_auto_renew),
                "transaction_date": transaction_date,
                "membership_expire_date": membership_expire_date,
                "is_cancel": int(is_cancel),
            }
        )

    write_csv_rows(target, cleaned_rows, OUTPUT_FIELDS)
    logging.info("transactions cleaned: %s rows, dropped=%s, output=%s", len(cleaned_rows), dropped, target)


if __name__ == "__main__":
    main()

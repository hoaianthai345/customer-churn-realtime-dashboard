#!/usr/bin/env python3
import logging
import shutil

from apps.batch.common import processed_file

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def main() -> None:
    source = processed_file("MEMBERS_CLEAN_FILE", "members_clean.csv")
    target = source.parent / "member_dimension.csv"
    shutil.copyfile(source, target)
    logging.info("member dimension exported: %s", target)


if __name__ == "__main__":
    main()

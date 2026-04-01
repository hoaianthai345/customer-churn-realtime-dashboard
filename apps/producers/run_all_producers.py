#!/usr/bin/env python3
import logging
import subprocess
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def run(cmd):
    logger.info("Running: %s", " ".join(cmd))
    subprocess.run(cmd, check=True)


def main() -> None:
    python_bin = sys.executable

    run([python_bin, "-m", "apps.producers.bootstrap_members"])
    run([python_bin, "-m", "apps.batch.bootstrap_transactions"])
    run([python_bin, "-m", "apps.producers.replay_user_logs"])

    logger.info("All producer jobs finished.")


if __name__ == "__main__":
    main()

#!/usr/bin/env python

"""
Run the queue client
"""

import argparse
from time import sleep
import logging
import sys
import config
from supervisor import Supervisor

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description='Run the queue client',
    )

    # Parse args
    args = parser.parse_args()

    logger.info("Starting queue client...")

    # Create and run supervisor
    supervisor = Supervisor()
    supervisor.run()

    try:
        # Run until keyboard exit
        while True:
            # Display supervisor status
            supervisor.status()
            sleep(config.QUEUE_MONITOR_INTERVAL)
    except KeyboardInterrupt:
        # Call stop on all workers
        supervisor.stop()

    logger.info("Exiting...")

    # Kill the queue
    sys.exit(0)


if __name__ == "__main__":
    main()

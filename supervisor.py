#!/usr/bin/env python

"""
Supervisor manages worker processes
"""

import logging
import multiprocessing
from workers import Worker

logger = logging.getLogger(__name__)


class Supervisor(object):
    """
    Supervisor will manage queue workers
    """

    def __init__(self):
        """
        Construct a Supervisor
        """

        # Store the workers
        self.workers = []

        # Display banner and status
        logger.info("*** QUEUE PROCESSING APP ***")
        self.status()

    def run(self):
        """
        Method called by run_app to start
        processing the queue
        """

        num_workers = multiprocessing.cpu_count()

        logger.info('Starting %d workers...' % num_workers)

        for ii in range(0, num_workers):
            # Create and store
            worker = Worker(id=ii)
            self.workers.append(worker)

            # Start the process
            worker.daemon = True
            worker.start()

    def status(self):
        """
        Called by run_client
        """

        logger.debug(self)
        for worker in self.workers:
            logger.debug('Worker - ' + worker.name + ':' + str(worker.pid))

    def stop(self):
        """
        Method called by run_app to stop workers
        """

        logger.info('Stopping %d workers...' % len(self.workers))

        for worker in self.workers:
            # Stop the thread, close connections
            worker.stop()
            worker.terminate()

        self.workers = []
        self.status()

    def __repr__(self):
        """
        String repr for a Supervisor
        """

        return 'Supervisor - ' + str(len(self.workers)) + ' workers'

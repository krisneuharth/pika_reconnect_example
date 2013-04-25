#!/usr/bin/env python

"""
Workers to process queue messages
"""

import multiprocessing
import logging
import time
import json
from os import _exit

import pika
import config

logger = logging.getLogger(__name__)


class Worker(multiprocessing.Process):
    """
    Base worker to abstract all connection and processing
    logic away for simplification
    """

    def __init__(self, id):
        """
        Construct the worker
        """

        # Call super process init
        multiprocessing.Process.__init__(self)

        # Set the process name
        self.id = id
        self.name = 'Worker(' + str(id) + ')'

        self.channel = None
        self.connection = None

        self.parameters = pika.ConnectionParameters(
            host=config.QUEUE_HOST,
        )

        logger.debug(str(self) + ' - Created')

        self.connect()

    def run(self):
        """
        Implement process start method, kicks off message
        processing in the ioloop
        """
        logger.info(str(self) + ' - Waiting for messages...')

        try:
            # Loop so we can communicate with RabbitMQ
            self.connection.ioloop.start()
        except Exception:
            # Don't care
            pass

    def stop(self):
        """
        Provide a way to stop this process and message processing
        """

        logger.debug(str(self) + ' - Stopping')

        try:
            # Gracefully close the connection
            self.channel.stop_consuming()
            self.connection.close()

            # Loop until we're fully closed, will stop on its own
            self.connection.ioloop.start()
        except Exception:
            # Don't care
            pass

    def connect(self):
        """
        Make a connection to the queue or exit
        """

        attempt = 1
        while attempt < config.QUEUE_CONNECTION_ATTEMPTS:
            try:
                logger.debug(
                    str(self) + ' - Connecting - Attempt ' + str(attempt))

                self.connection = pika.SelectConnection(
                    self.parameters,
                    self.on_connected
                )

                logger.debug(str(self) + ' - Connected!')

                # Stop trying
                return

            except Exception:
                # Sleep then try again
                time.sleep(config.QUEUE_RETRY_DELAY)

            attempt += 1

        logger.error(str(self) + ' - Failed to connect, max attempts reached')

        # Kill the worker
        _exit(1)

    def on_connected(self, new_connection):
        """
        Callback for when a connection is made
        """

        # Save new connection
        self.connection = new_connection

        # Add callbacks
        self.connection.channel(self.on_channel_open)
        self.connection.add_on_close_callback(self.on_connection_closed)

    def on_channel_open(self, new_channel):
        """
        Callback for when a channel is opened
        """

        # Save new channel
        self.channel = new_channel

        # Setup channel
        self.channel.queue_declare(
            queue=config.QUEUE_NAME,
            durable=config.QUEUE_DURABLE,
            callback=self.on_queue_declared,
        )

    def on_queue_declared(self, frame):
        """
        Callback for when a queue is declared
        """

        # Declare callback for consuming from queue
        self.channel.basic_consume(self.on_message, queue=config.QUEUE_NAME)

        # Set additional options on queue: 1 msg at a time
        self.channel.basic_qos(prefetch_count=config.QUEUE_PREFETCH_COUNT)

    def on_message(self, channel, method, header, body):
        """
        Callback for when a message is received
        """

        logger.debug(str(self) + " - Received:\n" + str(body))

        # Parse JSON into dict
        body = json.loads(body)

        # Break when num retries is reached
        if ('retries' in body and
                body['retries'] == config.QUEUE_NUM_RETRIES):
            logger.error(str(self) + " - Max retries failed...")

            # ACK - Finished with message, fail or retry
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        try:
            # Dispatch the processing of the message
            self.process(body)

        except Exception:
            logger.error(str(self) + " - Retrying...")

            # Upsert retries
            if not 'retries' in body:
                body['retries'] = 1
            else:
                retries = body['retries']
                body['retries'] = retries + 1

            try:
                # Put the same message back on the queue with metadata
                self.channel.basic_publish(
                    exchange='',
                    routing_key=config.QUEUE_NAME,
                    body=json.dumps(body),
                    properties=pika.BasicProperties(delivery_mode=2),
                )
            except Exception as e:
                logger.error(e)

        # Always ack, finished with message, fail or retry
        channel.basic_ack(delivery_tag=method.delivery_tag)

    def process(self, message):
        """
        Method called to do something with the received message,
        to be implemented by extending classes
        """

        raise NotImplemented

    def on_connection_closed(self, frame):
        """
        Callback for when a connection is closed
        """

        logger.error(str(self) + ' - Connection lost!')

        self.stop()
        self.connect()
        self.run()

    def __repr__(self):
        """
        String repr for a Worker
        """

        return self.name


class ExampleWorker(Worker):
    """
    Example worker that subclasses base Worker
    """

    def process(self, message):
        """
        Override process
        """

        # Do stuff...
        pass

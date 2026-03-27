"""
Abstract base class for Kafka producers.

Encapsulates all shared Kafka plumbing — connection, callbacks,
main run loop, signal handling, and graceful shutdown. Subclasses
only need to implement three methods:
    - connect_source()  — connect to the upstream data source
    - transform()       — convert a raw message to (key, value)
    - get_topic()       — return the Kafka topic name
"""
import asyncio
import json
import logging
import signal
import sys
from abc import ABC, abstractmethod
from typing import Optional

from kafka import KafkaProducer

from .config import BaseConfig
from .kafka_client import create_kafka_producer


class _JsonFormatter(logging.Formatter):
    """Emit each log record as a single JSON line for Loki ingestion."""

    def format(self, record):
        entry = {
            "ts": self.formatTime(record, datefmt="%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            entry["exc"] = self.formatException(record.exc_info)
        return json.dumps(entry)


_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(_JsonFormatter())
logging.basicConfig(level=logging.INFO, handlers=[_handler])

logger = logging.getLogger(__name__)


class BaseProducer(ABC):
    """Abstract base for all Kafka producers.

    All the Kafka plumbing — connect, retry, batch, shutdown, logging.
    Subclasses implement the data-source-specific logic.
    """

    def __init__(self, config: BaseConfig):
        self.config = config
        self.producer: Optional[KafkaProducer] = None
        self.running = True
        self.messages_produced = 0

    # ------------------------------------------------------------------ #
    # Abstract methods — subclasses must implement these
    # ------------------------------------------------------------------ #

    @abstractmethod
    async def connect_source(self):
        """Connect to the upstream data source and yield raw messages.

        This should be an async generator or an async context manager
        that provides messages. Implementation is source-specific
        (WebSocket, HTTP polling, queue consumer, etc.).
        """
        ...

    @abstractmethod
    def transform(self, raw_message) -> tuple:
        """Transform a raw message into (partition_key, kafka_value).

        Args:
            raw_message: The raw message from the data source.

        Returns:
            A tuple of (key: str, value: dict) for the Kafka record.
        """
        ...

    @abstractmethod
    def get_topic(self) -> str:
        """Return the Kafka topic to produce to."""
        ...

    # ------------------------------------------------------------------ #
    # Shared Kafka plumbing
    # ------------------------------------------------------------------ #

    def _on_send_success(self, record_metadata):
        """Callback for successful message delivery."""
        self.messages_produced += 1
        if self.messages_produced % 1000 == 0:
            logger.info(
                f"Produced {self.messages_produced} messages to {record_metadata.topic}"
            )

    def _on_send_error(self, excp):
        """Callback for failed message delivery."""
        logger.error(f"Failed to produce message: {excp}")

    def send(self, key: str, value: dict):
        """Send a message to Kafka with callbacks.

        Args:
            key: Partition key for the record.
            value: Message value (will be JSON-serialized).
        """
        self.producer.send(
            self.get_topic(),
            key=key,
            value=value,
        ).add_callback(
            self._on_send_success
        ).add_errback(
            self._on_send_error
        )

    async def run(self):
        """Initialize Kafka producer, set up signal handlers, and start consuming.

        This is the main entry point. Subclasses should NOT override this;
        implement connect_source(), transform(), and get_topic() instead.
        """
        logger.info(f"Starting {self.__class__.__name__}...")
        logger.info(f"Topic: {self.get_topic()}")

        # Initialize Kafka producer
        self.producer = create_kafka_producer(self.config)

        # Set up signal handlers for graceful shutdown
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self.shutdown)

        try:
            await self.connect_source()
        finally:
            self.cleanup()

    def shutdown(self):
        """Signal handler for graceful shutdown."""
        logger.info("Shutdown signal received...")
        self.running = False

    def cleanup(self):
        """Flush and close the Kafka producer."""
        if self.producer:
            logger.info("Flushing Kafka producer...")
            self.producer.flush(timeout=10)
            self.producer.close()
            logger.info(
                f"Producer closed. Total messages produced: {self.messages_produced}"
            )

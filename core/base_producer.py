"""
Abstract base class for Kafka producers.

Encapsulates all shared Kafka plumbing — connection, callbacks,
main run loop, signal handling, and graceful shutdown. Subclasses
only need to implement three methods:
    - connect_source()  — connect to the upstream data source
    - transform()       — convert a raw message to (key, value)
    - get_topic()       — return the Kafka topic name

Failure handling is configurable via FAILURE_STRATEGY env var:
    - "log"         — log and drop (default, current behavior)
    - "retry"       — extended kafka-python retries + backoff
    - "disk_buffer" — write to a local JSONL file for later replay
"""
import asyncio
import json
import logging
import os
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
        self.messages_failed = 0
        self.messages_buffered = 0

    # ------------------------------------------------------------------ #
    # Abstract methods — subclasses must implement these
    # ------------------------------------------------------------------ #

    @abstractmethod
    async def connect_source(self):
        """Connect to the upstream data source and yield raw messages."""
        ...

    @abstractmethod
    def transform(self, raw_message) -> tuple:
        """Transform a raw message into (partition_key, kafka_value)."""
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

    def _on_send_error(self, excp, key=None, value=None):
        """Callback for failed message delivery. Dispatches to configured strategy."""
        self.messages_failed += 1
        strategy = self.config.FAILURE_STRATEGY

        if strategy == "log":
            logger.error(f"Failed to produce message: {excp}")

        elif strategy == "retry":
            # kafka-python already retried with extended settings.
            # If we're here, all retries were exhausted.
            logger.error(
                f"Failed to produce after extended retries: {excp} "
                f"(total failures: {self.messages_failed})"
            )

        elif strategy == "disk_buffer":
            self._write_to_disk_buffer(key, value, excp)

        else:
            logger.error(f"Unknown failure strategy '{strategy}', dropping message: {excp}")

    def _write_to_disk_buffer(self, key, value, error):
        """Append a failed message to a local JSONL file for later replay."""
        buffer_path = self.config.DISK_BUFFER_PATH
        max_size = self.config.DISK_BUFFER_MAX_SIZE_MB * 1024 * 1024

        # Check file size before writing
        try:
            current_size = os.path.getsize(buffer_path) if os.path.exists(buffer_path) else 0
        except OSError:
            current_size = 0

        if current_size >= max_size:
            if self.messages_failed % 1000 == 1:  # Log once per 1000 to avoid spam
                logger.error(
                    f"Disk buffer full ({self.config.DISK_BUFFER_MAX_SIZE_MB}MB). "
                    f"Dropping message."
                )
            return

        record = {
            "topic": self.get_topic(),
            "key": key,
            "value": value,
            "error": str(error),
        }

        try:
            with open(buffer_path, "a") as f:
                f.write(json.dumps(record) + "\n")
            self.messages_buffered += 1
            if self.messages_buffered % 100 == 1:
                logger.warning(
                    f"Buffered {self.messages_buffered} messages to {buffer_path}"
                )
        except OSError as e:
            logger.error(f"Failed to write to disk buffer: {e}")

    def replay_buffer(self):
        """Replay messages from the disk buffer file back to Kafka.

        Call this after Kafka is healthy again. Reads the JSONL file,
        re-sends each message, and removes the file when done.

        Returns:
            Tuple of (replayed_count, failed_count).
        """
        buffer_path = self.config.DISK_BUFFER_PATH

        if not os.path.exists(buffer_path):
            logger.info("No disk buffer to replay.")
            return 0, 0

        replayed = 0
        failed = 0

        logger.info(f"Replaying messages from {buffer_path}...")

        with open(buffer_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    self.producer.send(
                        record["topic"],
                        key=record.get("key"),
                        value=record["value"],
                    ).add_callback(
                        self._on_send_success
                    ).add_errback(
                        self._on_send_error
                    )
                    replayed += 1
                except Exception as e:
                    logger.error(f"Failed to replay buffered message: {e}")
                    failed += 1

        # Flush to ensure all replayed messages are sent
        if self.producer:
            self.producer.flush(timeout=30)

        # Remove the buffer file
        try:
            os.remove(buffer_path)
            logger.info(f"Disk buffer cleared. Replayed {replayed}, failed {failed}.")
        except OSError as e:
            logger.error(f"Failed to remove buffer file: {e}")

        return replayed, failed

    def send(self, key: str, value: dict):
        """Send a message to Kafka with callbacks.

        Args:
            key: Partition key for the record.
            value: Message value (will be JSON-serialized).
        """
        future = self.producer.send(
            self.get_topic(),
            key=key,
            value=value,
        )
        future.add_callback(self._on_send_success)

        # For strategies that need the original message on failure,
        # capture key/value in the errback closure
        strategy = self.config.FAILURE_STRATEGY
        if strategy == "disk_buffer":
            future.add_errback(lambda excp: self._on_send_error(excp, key=key, value=value))
        else:
            future.add_errback(self._on_send_error)

    async def run(self):
        """Initialize Kafka producer, set up signal handlers, and start consuming.

        This is the main entry point. Subclasses should NOT override this;
        implement connect_source(), transform(), and get_topic() instead.
        """
        logger.info(f"Starting {self.__class__.__name__}...")
        logger.info(f"Topic: {self.get_topic()}")
        logger.info(f"Failure strategy: {self.config.FAILURE_STRATEGY}")

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
            self.producer.flush(timeout=30)
            self.producer.close()
            logger.info(
                f"Producer closed. "
                f"Produced: {self.messages_produced}, "
                f"Failed: {self.messages_failed}, "
                f"Buffered: {self.messages_buffered}"
            )

"""
Shared Kafka client utilities.

Provides a reusable factory for creating KafkaProducer instances
with connection retry logic. Extracted from BlueskyJetstreamProducer._create_kafka_producer().
"""
import asyncio
import json
import logging

from kafka import KafkaProducer
from kafka.errors import KafkaError

from .config import BaseConfig

logger = logging.getLogger(__name__)


def create_kafka_producer(config: BaseConfig) -> KafkaProducer:
    """Create a KafkaProducer with retry logic.

    Args:
        config: A BaseConfig (or subclass) instance with connection settings.

    Returns:
        A connected KafkaProducer.

    Raises:
        KafkaError: If all connection attempts are exhausted.
    """
    max_retries = config.MAX_CONNECT_RETRIES
    retry_delay = config.CONNECT_RETRY_DELAY

    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                batch_size=config.BATCH_SIZE * 1024,  # Convert KB to bytes
                linger_ms=config.LINGER_MS,
                acks=config.ACKS,
                retries=config.RETRIES,
            )
            logger.info(f"Connected to Kafka at {config.KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except KafkaError as e:
            logger.warning(
                f"Kafka connection attempt {attempt + 1}/{max_retries} failed: {e}"
            )
            if attempt < max_retries - 1:
                asyncio.get_event_loop().run_until_complete(
                    asyncio.sleep(retry_delay)
                )
            else:
                raise

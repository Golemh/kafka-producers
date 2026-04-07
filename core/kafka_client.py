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

    When FAILURE_STRATEGY is "retry", uses higher retries and backoff
    to hold messages in the buffer longer during transient outages.

    Args:
        config: A BaseConfig (or subclass) instance with connection settings.

    Returns:
        A connected KafkaProducer.

    Raises:
        KafkaError: If all connection attempts are exhausted.
    """
    max_retries = config.MAX_CONNECT_RETRIES
    retry_delay = config.CONNECT_RETRY_DELAY

    # When retry strategy is active, configure kafka-python for longer retention
    use_retry_strategy = config.FAILURE_STRATEGY == "retry"
    producer_retries = config.RETRY_MAX_RETRIES if use_retry_strategy else config.RETRIES
    retry_backoff = config.RETRY_BACKOFF_MS if use_retry_strategy else 100

    producer_kwargs = dict(
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        batch_size=config.BATCH_SIZE * 1024,
        linger_ms=config.LINGER_MS,
        acks=config.ACKS,
        retries=producer_retries,
        retry_backoff_ms=retry_backoff,
    )

    if use_retry_strategy:
        producer_kwargs["delivery_timeout_ms"] = config.DELIVERY_TIMEOUT_MS
        logger.info(
            f"Retry strategy active: retries={producer_retries}, "
            f"backoff={retry_backoff}ms, delivery_timeout={config.DELIVERY_TIMEOUT_MS}ms"
        )

    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(**producer_kwargs)
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

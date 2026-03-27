"""
Base configuration for Kafka producers.
Loads shared settings from environment variables with sensible defaults.

Individual producers extend this with source-specific config.
"""
import os


class BaseConfig:
    # Kafka connection
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

    # Producer batching
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))        # KB
    LINGER_MS = int(os.getenv("LINGER_MS", "10"))

    # Kafka producer reliability
    ACKS = os.getenv("KAFKA_ACKS", "all")
    RETRIES = int(os.getenv("KAFKA_RETRIES", "3"))

    # Connection retry
    MAX_CONNECT_RETRIES = int(os.getenv("MAX_CONNECT_RETRIES", "10"))
    CONNECT_RETRY_DELAY = int(os.getenv("CONNECT_RETRY_DELAY", "5"))  # seconds

    # Reconnection to data source
    RECONNECT_DELAY_SECONDS = int(os.getenv("RECONNECT_DELAY_SECONDS", "5"))
    MAX_RECONNECT_ATTEMPTS = int(os.getenv("MAX_RECONNECT_ATTEMPTS", "0"))  # 0 = infinite

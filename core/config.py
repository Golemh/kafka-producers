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

    # Failure handling strategy: "log", "retry", "disk_buffer"
    #   log         — current default: log error, drop message
    #   retry       — increase kafka-python retries + backoff (holds messages longer in buffer)
    #   disk_buffer — write failed messages to a local JSONL file for later replay
    FAILURE_STRATEGY = os.getenv("FAILURE_STRATEGY", "log")

    # retry strategy settings (also applies as kafka-python config when strategy is "retry")
    RETRY_BACKOFF_MS = int(os.getenv("RETRY_BACKOFF_MS", "100"))
    RETRY_MAX_RETRIES = int(os.getenv("RETRY_MAX_RETRIES", "10"))
    DELIVERY_TIMEOUT_MS = int(os.getenv("DELIVERY_TIMEOUT_MS", "120000"))  # 2 minutes

    # disk_buffer strategy settings
    DISK_BUFFER_PATH = os.getenv("DISK_BUFFER_PATH", "/tmp/kafka-buffer.jsonl")
    DISK_BUFFER_MAX_SIZE_MB = int(os.getenv("DISK_BUFFER_MAX_SIZE_MB", "100"))

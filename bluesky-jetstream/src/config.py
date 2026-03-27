"""
Configuration for Bluesky Jetstream Producer.
Extends BaseConfig with Bluesky-specific settings.
"""
import os

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from core.config import BaseConfig


class BlueskyConfig(BaseConfig):
    # Jetstream WebSocket settings
    JETSTREAM_URL = os.getenv(
        "JETSTREAM_URL",
        "wss://jetstream2.us-east.bsky.network/subscribe",
    )

    # Comma-separated list of AT Protocol collections to subscribe to
    JETSTREAM_COLLECTIONS = os.getenv(
        "JETSTREAM_COLLECTIONS",
        "app.bsky.feed.post",
    ).split(",")

    # Kafka topic for Bluesky events
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "bluesky-posts")

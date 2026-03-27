"""
Bluesky Jetstream Producer

Connects to Bluesky's Jetstream WebSocket API and publishes events to Kafka.
Extends BaseProducer — only implements the data-source-specific logic:
    - connect_source()  → WebSocket connection + message loop
    - transform()       → JSON parse + DID extraction
    - get_topic()       → returns the configured topic
"""
import asyncio
import json
import logging
import os
import sys

import websockets

# Allow imports from the repo root so `core` package is accessible
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from core.base_producer import BaseProducer
from config import BlueskyConfig

logger = logging.getLogger(__name__)


class BlueskyProducer(BaseProducer):
    """Consumes Bluesky Jetstream and produces to Kafka."""

    def __init__(self):
        super().__init__(config=BlueskyConfig)

    def get_topic(self) -> str:
        return self.config.KAFKA_TOPIC

    def _build_jetstream_url(self) -> str:
        """Build the Jetstream WebSocket URL with collection filters."""
        url = self.config.JETSTREAM_URL
        collections = self.config.JETSTREAM_COLLECTIONS

        if collections:
            params = "&".join(
                f"wantedCollections={c.strip()}" for c in collections
            )
            url = f"{url}?{params}"

        return url

    def transform(self, raw_message) -> tuple:
        """Parse a Jetstream JSON message and extract the DID as partition key.

        Args:
            raw_message: Raw WebSocket text frame (JSON string).

        Returns:
            (did, event) tuple — DID as partition key, parsed event as value.
        """
        event = json.loads(raw_message)
        did = event.get("did", "unknown")
        return did, event

    async def connect_source(self):
        """Connect to Bluesky Jetstream WebSocket and produce messages.

        Handles automatic reconnection with configurable delay and max attempts.
        """
        url = self._build_jetstream_url()
        reconnect_attempts = 0

        logger.info(f"Collections: {self.config.JETSTREAM_COLLECTIONS}")

        while self.running:
            try:
                logger.info(f"Connecting to Jetstream: {url}")

                async with websockets.connect(
                    url,
                    ping_interval=30,
                    ping_timeout=10,
                    close_timeout=5,
                ) as ws:
                    logger.info("Connected to Bluesky Jetstream!")
                    reconnect_attempts = 0  # Reset on successful connection

                    async for message in ws:
                        if not self.running:
                            break

                        try:
                            key, value = self.transform(message)
                            self.send(key, value)
                        except json.JSONDecodeError as e:
                            logger.warning(f"Failed to parse message: {e}")
                        except Exception as e:
                            logger.error(f"Error processing message: {e}")

            except websockets.ConnectionClosed as e:
                logger.warning(f"WebSocket connection closed: {e}")
            except Exception as e:
                logger.error(f"Jetstream connection error: {e}")

            # Reconnection logic
            if self.running:
                reconnect_attempts += 1
                max_attempts = self.config.MAX_RECONNECT_ATTEMPTS

                if max_attempts > 0 and reconnect_attempts >= max_attempts:
                    logger.error(
                        f"Max reconnection attempts ({max_attempts}) reached. Exiting."
                    )
                    break

                delay = self.config.RECONNECT_DELAY_SECONDS
                logger.info(
                    f"Reconnecting in {delay} seconds "
                    f"(attempt {reconnect_attempts})..."
                )
                await asyncio.sleep(delay)


async def main():
    producer = BlueskyProducer()
    await producer.run()


if __name__ == "__main__":
    asyncio.run(main())

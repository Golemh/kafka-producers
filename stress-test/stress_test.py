"""
Kafka stress test producer.

A standalone script that sends messages at a controlled rate to a Kafka topic,
measures per-message produce latency, and reports throughput and latency
percentiles at regular intervals.

Usage:
    python stress_test.py --bootstrap-servers kafka:9092 --rate 500 --duration 120
"""

import argparse
import logging
import os
import signal
import statistics
import sys
import time
from threading import Lock

from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="Kafka stress test producer")
    parser.add_argument(
        "--bootstrap-servers",
        required=True,
        help="Kafka bootstrap servers (comma-separated)",
    )
    parser.add_argument(
        "--topic",
        default="stress-test",
        help="Target Kafka topic (default: stress-test)",
    )
    parser.add_argument(
        "--rate",
        type=int,
        default=100,
        help="Target messages per second (default: 100)",
    )
    parser.add_argument(
        "--message-size",
        type=int,
        default=1024,
        help="Payload size in bytes (default: 1024)",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=60,
        help="Test duration in seconds (default: 60)",
    )
    parser.add_argument(
        "--acks",
        default="all",
        help="Producer acks setting: 0, 1, or 'all' (default: all)",
    )
    parser.add_argument(
        "--report-interval",
        type=int,
        default=5,
        help="Seconds between progress reports (default: 5)",
    )
    return parser.parse_args()


def coerce_acks(value: str):
    """Convert the --acks CLI string to the type kafka-python expects."""
    if value in ("0", "1"):
        return int(value)
    return value  # "all"


def build_payload(size: int) -> bytes:
    """Generate a random-ish payload of exactly `size` bytes.

    Uses os.urandom for speed; the content doesn't matter for a stress test.
    """
    return os.urandom(size)


def percentile(sorted_data, pct):
    """Return the pct-th percentile from an already-sorted list."""
    if not sorted_data:
        return 0.0
    k = (len(sorted_data) - 1) * (pct / 100.0)
    f = int(k)
    c = f + 1
    if c >= len(sorted_data):
        return sorted_data[f]
    return sorted_data[f] + (k - f) * (sorted_data[c] - sorted_data[f])


class StressTest:
    """Drives a sustained send workload against a Kafka cluster."""

    def __init__(self, args):
        self.bootstrap_servers = args.bootstrap_servers
        self.topic = args.topic
        self.target_rate = args.rate
        self.message_size = args.message_size
        self.duration = args.duration
        self.acks = coerce_acks(args.acks)
        self.report_interval = args.report_interval

        # Counters (protected by lock because callbacks fire from sender thread)
        self._lock = Lock()
        self._sent = 0
        self._acked = 0
        self._errors = 0
        self._latencies: list[float] = []  # seconds

        # Interval-scoped counters for progress reports
        self._interval_acked = 0
        self._interval_errors = 0
        self._interval_latencies: list[float] = []

        self._running = True
        self._producer: KafkaProducer | None = None

    # ------------------------------------------------------------------ #
    # Signal handling
    # ------------------------------------------------------------------ #

    def _install_signal_handlers(self):
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, self._handle_signal)

    def _handle_signal(self, signum, frame):
        logger.info("Received signal %s, stopping...", signum)
        self._running = False

    # ------------------------------------------------------------------ #
    # Kafka callbacks
    # ------------------------------------------------------------------ #

    def _make_callback(self, send_time: float):
        """Return success/error callbacks that capture the send timestamp."""

        def on_success(record_metadata):
            latency = time.monotonic() - send_time
            with self._lock:
                self._acked += 1
                self._latencies.append(latency)
                self._interval_acked += 1
                self._interval_latencies.append(latency)

        def on_error(exc):
            with self._lock:
                self._errors += 1
                self._interval_errors += 1
            logger.debug("Produce error: %s", exc)

        return on_success, on_error

    # ------------------------------------------------------------------ #
    # Reporting
    # ------------------------------------------------------------------ #

    def _report_interval(self, elapsed: float):
        """Print a progress report for the most recent interval."""
        with self._lock:
            acked = self._interval_acked
            errors = self._interval_errors
            latencies = sorted(self._interval_latencies)
            # Reset interval counters
            self._interval_acked = 0
            self._interval_errors = 0
            self._interval_latencies = []

        actual_rate = acked / self.report_interval if self.report_interval > 0 else 0
        p50 = percentile(latencies, 50) * 1000 if latencies else 0
        p95 = percentile(latencies, 95) * 1000 if latencies else 0
        p99 = percentile(latencies, 99) * 1000 if latencies else 0

        logger.info(
            "[%.0fs] rate: %.1f/s (target %d/s) | "
            "latency p50=%.1fms p95=%.1fms p99=%.1fms | "
            "errors: %d",
            elapsed,
            actual_rate,
            self.target_rate,
            p50,
            p95,
            p99,
            errors,
        )

    def _report_final(self, wall_time: float):
        """Print the end-of-test summary."""
        with self._lock:
            total_acked = self._acked
            total_errors = self._errors
            latencies = sorted(self._latencies)

        overall_rate = total_acked / wall_time if wall_time > 0 else 0
        p50 = percentile(latencies, 50) * 1000 if latencies else 0
        p95 = percentile(latencies, 95) * 1000 if latencies else 0
        p99 = percentile(latencies, 99) * 1000 if latencies else 0
        avg = (statistics.mean(latencies) * 1000) if latencies else 0
        mn = (min(latencies) * 1000) if latencies else 0
        mx = (max(latencies) * 1000) if latencies else 0

        print("\n" + "=" * 60)
        print("STRESS TEST COMPLETE")
        print("=" * 60)
        print(f"  Duration:          {wall_time:.1f}s")
        print(f"  Messages sent:     {self._sent}")
        print(f"  Messages acked:    {total_acked}")
        print(f"  Errors:            {total_errors}")
        print(f"  Throughput:        {overall_rate:.1f} msg/s")
        print(f"  Latency avg:       {avg:.2f}ms")
        print(f"  Latency min:       {mn:.2f}ms")
        print(f"  Latency p50:       {p50:.2f}ms")
        print(f"  Latency p95:       {p95:.2f}ms")
        print(f"  Latency p99:       {p99:.2f}ms")
        print(f"  Latency max:       {mx:.2f}ms")
        print("=" * 60)

    # ------------------------------------------------------------------ #
    # Main loop
    # ------------------------------------------------------------------ #

    def run(self):
        self._install_signal_handlers()

        logger.info("Connecting to Kafka at %s", self.bootstrap_servers)
        try:
            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                acks=self.acks,
                # Use raw bytes — no serializer overhead in a stress test
                value_serializer=None,
                key_serializer=None,
                linger_ms=5,
                batch_size=64 * 1024,
                buffer_memory=128 * 1024 * 1024,
                max_in_flight_requests_per_connection=5,
            )
        except KafkaError as exc:
            logger.error("Failed to connect to Kafka: %s", exc)
            sys.exit(1)

        payload = build_payload(self.message_size)

        logger.info(
            "Starting stress test: topic=%s rate=%d msg/s size=%d bytes "
            "duration=%ds acks=%s",
            self.topic,
            self.target_rate,
            self.message_size,
            self.duration,
            self.acks,
        )

        interval_between_msgs = 1.0 / self.target_rate if self.target_rate > 0 else 0
        test_start = time.monotonic()
        last_report = test_start
        next_send = test_start

        try:
            while self._running:
                now = time.monotonic()
                elapsed = now - test_start

                # Check if duration has been reached
                if elapsed >= self.duration:
                    break

                # Progress report
                if now - last_report >= self.report_interval:
                    self._report_interval(elapsed)
                    last_report = now

                # Rate limiting: only send when it's time for the next message
                if now < next_send:
                    # Busy-wait in very short bursts for accuracy
                    continue

                # Send one message
                send_time = time.monotonic()
                on_success, on_error = self._make_callback(send_time)
                try:
                    future = self._producer.send(self.topic, value=payload)
                    future.add_callback(on_success)
                    future.add_errback(on_error)
                    self._sent += 1
                except KafkaError as exc:
                    with self._lock:
                        self._errors += 1
                        self._interval_errors += 1
                    logger.debug("Send failed: %s", exc)

                next_send += interval_between_msgs

                # If we've fallen behind, reset the schedule instead of bursting
                if next_send < now - 1.0:
                    next_send = now

        finally:
            logger.info("Flushing producer...")
            self._producer.flush(timeout=30)
            self._producer.close(timeout=10)

            wall_time = time.monotonic() - test_start
            self._report_final(wall_time)


def main():
    args = parse_args()
    test = StressTest(args)
    test.run()


if __name__ == "__main__":
    main()

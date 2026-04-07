# kafka-producers

A shared framework for building Kafka producers, with the Bluesky Jetstream producer as the first implementation.

## Architecture

```
kafka-producers/
├── core/                        # Shared producer framework
│   ├── base_producer.py         # Abstract BaseProducer class
│   ├── kafka_client.py          # Kafka connection factory with retry
│   └── config.py                # Base configuration (env vars)
│
├── bluesky-jetstream/           # Bluesky Jetstream → Kafka
│   ├── Dockerfile
│   ├── requirements.txt
│   └── src/
│       ├── producer.py          # Extends BaseProducer
│       └── config.py            # Bluesky-specific config
│
└── stress-test/                 # Load testing tool
    ├── Dockerfile
    ├── docker-compose.yml
    └── stress_test.py
```

## How It Works

`BaseProducer` handles all Kafka plumbing (connection, batching, callbacks, failure handling, shutdown). Each producer subclass only implements:

| Method | Purpose |
|---|---|
| `connect_source()` | Connect to the upstream data source and yield messages |
| `transform(raw)` | Convert a raw message to `(partition_key, value)` |
| `get_topic()` | Return the Kafka topic name |

## Adding a New Producer

1. Create `<name>/src/producer.py` extending `BaseProducer`
2. Create `<name>/src/config.py` extending `BaseConfig`
3. Create `<name>/Dockerfile` and `<name>/requirements.txt`
4. Implement `connect_source()`, `transform()`, `get_topic()`

## Failure Handling

By default, failed messages are logged and dropped. Set `FAILURE_STRATEGY` to enable alternative handling:

| Strategy | Env value | Behavior | Best for |
|----------|-----------|----------|----------|
| Log and drop | `log` (default) | Log error, drop message | Fire-and-forget, acceptable loss |
| Extended retry | `retry` | Higher kafka-python retries + backoff, holds messages in buffer longer | Brief outages (< 2 min) |
| Disk buffer | `disk_buffer` | Write to a local JSONL file for later replay | Full Kafka outages (spot recovery window) |

### Enabling a strategy

```bash
# Via environment variable
FAILURE_STRATEGY=disk_buffer

# Or in docker-compose
environment:
  FAILURE_STRATEGY: disk_buffer
  DISK_BUFFER_PATH: /data/kafka-buffer.jsonl
  DISK_BUFFER_MAX_SIZE_MB: 100
```

### Replaying buffered messages

After Kafka is healthy again, call `replay_buffer()` on the producer instance. This reads the JSONL file, re-sends all messages, and removes the file.

For the Bluesky producer, replay can be triggered by restarting the container — or by adding a replay step to the startup logic. The buffer file persists across container restarts if mounted to a volume.

### Strategy details

**`retry`** — Configures kafka-python with `RETRY_MAX_RETRIES` (default 10), `RETRY_BACKOFF_MS` (default 100ms), and `DELIVERY_TIMEOUT_MS` (default 120s / 2 minutes). Messages are held in the kafka-python internal buffer until retries exhaust or the delivery timeout expires. If all retries fail, the message is logged and dropped.

**`disk_buffer`** — Appends failed messages as JSONL to `DISK_BUFFER_PATH`. Stops writing when the file exceeds `DISK_BUFFER_MAX_SIZE_MB` (default 100MB). Messages include the original topic, key, value, and error for replay.

## Environment Variables

### Shared (BaseConfig)
| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka broker address |
| `BATCH_SIZE` | `100` | Batch size in KB |
| `LINGER_MS` | `10` | Max wait before sending a batch |
| `KAFKA_ACKS` | `all` | Producer ack mode (`0`, `1`, `all`) |
| `KAFKA_RETRIES` | `3` | Kafka send retries (for `log` strategy) |
| `MAX_CONNECT_RETRIES` | `10` | Kafka connection retry attempts |
| `CONNECT_RETRY_DELAY` | `5` | Seconds between connection retries |
| `FAILURE_STRATEGY` | `log` | Failure handling: `log`, `retry`, `dead_letter`, `disk_buffer` |

### Retry strategy
| Variable | Default | Description |
|---|---|---|
| `RETRY_MAX_RETRIES` | `10` | Max send retries (when strategy is `retry`) |
| `RETRY_BACKOFF_MS` | `100` | Backoff between retries in ms |
| `DELIVERY_TIMEOUT_MS` | `120000` | Max time to hold a message before dropping (ms) |

### Disk buffer strategy
| Variable | Default | Description |
|---|---|---|
| `DISK_BUFFER_PATH` | `/tmp/kafka-buffer.jsonl` | Path to buffer file |
| `DISK_BUFFER_MAX_SIZE_MB` | `100` | Max buffer file size before dropping |

### Bluesky Producer (BlueskyConfig)
| Variable | Default | Description |
|---|---|---|
| `KAFKA_TOPIC` | `bluesky-posts` | Target Kafka topic |
| `JETSTREAM_URL` | `wss://jetstream2.us-east.bsky.network/subscribe` | Jetstream WebSocket URL |
| `JETSTREAM_COLLECTIONS` | `app.bsky.feed.post` | Comma-separated AT Protocol collections |

## Running

```bash
cd bluesky-jetstream
docker build -t bluesky-producer .
docker run -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 bluesky-producer
```

### With failure handling enabled

```bash
docker run \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  -e FAILURE_STRATEGY=disk_buffer \
  -e DISK_BUFFER_PATH=/data/kafka-buffer.jsonl \
  -v /data:/data \
  bluesky-producer
```

## Stress Test

See `stress-test/` for a load testing tool with configurable rate, message size, duration, and acks mode.

```bash
cd stress-test
docker compose run --rm stress-test \
  --bootstrap-servers kafka:9092 \
  --rate 1000 \
  --duration 60
```

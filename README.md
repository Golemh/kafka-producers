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
└── bluesky-jetstream/           # Bluesky Jetstream → Kafka
    ├── Dockerfile
    ├── requirements.txt
    └── src/
        ├── producer.py          # Extends BaseProducer
        └── config.py            # Bluesky-specific config
```

## How It Works

`BaseProducer` handles all Kafka plumbing (connection, batching, callbacks, shutdown). Each producer subclass only implements:

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

## Environment Variables

### Shared (BaseConfig)
| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka broker address |
| `BATCH_SIZE` | `100` | Batch size in KB |
| `LINGER_MS` | `10` | Max wait before sending a batch |
| `MAX_CONNECT_RETRIES` | `10` | Kafka connection retry attempts |

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

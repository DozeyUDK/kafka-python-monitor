# Kafka Python Monitor

A small Python service that consumes records from Apache Kafka and optionally indexes them into Elasticsearch. It exposes liveness, readiness, JSON status, and Prometheus metrics over HTTP.

This project is a Kafka **consumer and monitor**, not a Kafka broker. The Elasticsearch ingestion path is designed for **at-least-once delivery**, with deterministic Elasticsearch document IDs to make replay safe for indexed records. Real Kafka/Elasticsearch integration tests run in CI; production rollout still requires target-environment validation and representative load testing.

## Architecture

Kafka topics -> consumer group -> initial-position checkpoint (if missing) -> optional callback -> Elasticsearch bulk -> processed-offset commit

For a new consumer group or newly assigned partition without committed offsets, the monitor **synchronously checkpoints the first unprocessed offset before any callback or Elasticsearch export**. This prevents `auto_offset_reset=latest` from skipping a failed first batch on restart. Assigned partitions with no returned records are checkpointed at their current position. These initial commits **do not acknowledge message processing**.

Processed offsets are committed for **exactly the topic partitions returned in that poll** and only after Elasticsearch confirms the complete bulk operation. The consumer never implicitly commits unrelated assigned partitions or positions. If indexing fails, the monitor exits nonzero, and a container restart can replay messages from the last committed offsets. Partial Elasticsearch bulk writes are safe to replay: the document ID is derived from topic, partition and offset, and the index date is derived from the Kafka record timestamp.

Callbacks that perform external side effects are not guaranteed to be idempotent. No distributed transaction exists between Kafka, Elasticsearch, and custom callback services.

## Quick start

Requires Python 3.10+, an accessible Kafka broker, and Elasticsearch 8.x if indexing is enabled.

    python -m venv .venv
    . .venv/bin/activate
    pip install -r requirements.lock
    cp env.example .env

Set KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPICS, and KAFKA_GROUP_ID in .env. For Elasticsearch export, also set:

    ELASTICSEARCH_ENABLED=true
    ELASTICSEARCH_HOSTS=https://elasticsearch.example:9200
    ELASTICSEARCH_API_KEY=replace-with-real-api-key
    ELASTICSEARCH_CA_CERTS=/run/secrets/elastic-ca.pem

Then run:

    python program.py

With ELASTICSEARCH_ENABLED=false, the monitor only consumes records, emits metrics and runs callbacks. It **does not** persist events to Elasticsearch.

Do not commit real credentials to Git. Use secrets injection or mounted read-only files in production.

## Docker

    docker build -t kafka-python-monitor:local .
    docker run -d --name kafka-python-monitor --restart unless-stopped \
      --env-file .env \
      -e HEALTH_SERVER_HOST=0.0.0.0 \
      -p 127.0.0.1:8080:8080 \
      kafka-python-monitor:local

The provided env.example binds the HTTP server to localhost for local Python usage. Docker must override HEALTH_SERVER_HOST to 0.0.0.0 inside the container. Do **not** publish the metrics endpoints to the internet; the HTTP interface has no authentication. Use a trusted monitoring network or reverse proxy.

Kafka and Elasticsearch addresses in a container must be reachable **from inside that container**. localhost refers to the container itself, not the Docker host.

Docker HEALTHCHECK uses /live. Container health does not imply successful ingestion; scrape /ready and the metrics endpoint.

## Endpoints

| Route | Behavior |
| --- | --- |
| GET /live | 200 while the HTTP server is responding |
| GET /ready | 200 after a successful recent Kafka poll while the consumer runs, provided the exporter has not failed |
| GET /health | JSON health status, 200/503 |
| GET /metrics | Prometheus text exposition |
| GET /metrics?format=json | JSON statistics |
| GET /stats | JSON statistics |

The default HTTP bind address is 127.0.0.1 outside the container; default port is 8080. The HTTP server is optional: set HEALTH_CHECK_PORT=0 to disable it, but remove or adjust the Docker HEALTHCHECK in that case.

Metrics reset on process restart. Kafka consumer lag is sampled periodically and is keyed by topic and partition. `KAFKA_POLL_STALE_SECONDS` (default: 120) controls the readiness timeout: before the first poll, or after an abnormally long gap without a successful poll, `/ready` returns 503. Tune this higher than expected worst-case bulk processing and poll durations. An idle topic is not unhealthy as long as Kafka `poll()` continues returning successfully. This is a **poll freshness check**, not an independent broker-wide network connectivity guarantee.

Prometheus metrics `kafka_monitor_poll_age_seconds` and `kafka_monitor_processed_commit_age_seconds` expose the age of the most recent successful poll and processed-batch commit. Those time series are absent until the first respective successful observation.

## Configuration

The supplied env.example documents the supported environment variables. Important settings:

| Variable | Purpose | Default |
| --- | --- | --- |
| KAFKA_BOOTSTRAP_SERVERS | Comma-separated broker addresses | localhost:9092 |
| KAFKA_TOPICS | Comma-separated topic list | test-topic |
| KAFKA_GROUP_ID | Persistent consumer group | kafka-monitor-group |
| KAFKA_AUTO_OFFSET_RESET | Behavior without committed offsets: earliest/latest | latest |
| KAFKA_BATCH_SIZE | Maximum records requested per poll | 100 |
| KAFKA_POLL_STALE_SECONDS | Readiness poll freshness timeout | 120 |
| MONITOR_INTERVAL_SECONDS | Kafka poll timeout | 5 |
| METRICS_INTERVAL_SECONDS | Lag/Elasticsearch metrics export interval | 30 |
| KAFKA_MAX_RETRIES | Connection attempt limit per reconnect cycle | 5 |
| KAFKA_RECONNECT_INTERVAL_SECONDS | Delay between reconnect attempts | 5 |
| KAFKA_SECURITY_PROTOCOL | PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL | PLAINTEXT |
| KAFKA_DLQ_ENABLED | Acknowledge failed custom callback events via DLQ | false |
| ELASTICSEARCH_ENABLED | Enable record indexing | false |
| ELASTICSEARCH_HOSTS | Comma-separated http(s) endpoint URLs | localhost:9200 |
| ELASTICSEARCH_BATCH_SIZE | Documents per bulk request | 100 |
| HEALTH_CHECK_PORT | HTTP endpoint port; 0 disables | 8080 |
| HEALTH_SERVER_HOST | HTTP bind address | 127.0.0.1 |

Kafka TLS can be configured with KAFKA_SSL_CAFILE, KAFKA_SSL_CERTFILE, and KAFKA_SSL_KEYFILE. SASL supports PLAIN, SCRAM-SHA-256, and SCRAM-SHA-512. Set a TLS-capable protocol for remote connections. Elasticsearch supports basic auth and API keys, TLS certificate validation and a custom CA.

KAFKA_AUTO_OFFSET_RESET applies only when the consumer group has no committed offset (or its stored offset is invalid). It does not rewind existing groups.

## Failure handling and delivery guarantees

- Auto-commit is disabled. Only initial start positions for partitions without stored offsets may be committed before processing; these checkpoints point to the first **unprocessed** offset.
- Processed offsets are committed synchronously only after confirmed Elasticsearch indexing. On failed first-batch indexing, restart resumes at the initial checkpoint rather than at the new log end.
- Any Elasticsearch bulk failure aborts processing and exits nonzero rather than silently dropping queued events.
- Elasticsearch actions use deterministic IDs, so reprocessing a Kafka offset overwrites its previous document instead of creating another copy.
- The Kafka record timestamp fixes the index date across retries. Records without a valid timestamp use the stable 1970-01-01 fallback index date.
- Batches are bounded by KAFKA_BATCH_SIZE. The exporter sends confirmed bulk requests without an in-memory deferred event queue.
- The optional DLQ is for failed **custom callbacks**. The producer waits for broker acknowledgement. Elasticsearch infrastructure failures do not go to the DLQ: the consumer stops without committing the offset.
- Kafka retains the original events according to its retention settings. Recovery requires source events and offsets still to exist.
- Metrics exports are best-effort and do not control commits. Elasticsearch indexing of Kafka records is fail-closed.
- At-least-once is **not exactly-once**. Callback side effects may repeat after a retry, Kafka commit failure, or consumer group rebalance.

## Operational checklist

Before production rollout:

1. Configure Kafka ACLs granting READ on topics and consumer group, and DLQ WRITE only when enabled.
2. Use TLS, verify Elasticsearch certificates, and use limited-scope credentials.
3. Set Elasticsearch index templates, mappings, retention/ILM, disk limits, and alerting independently. This project does not provision them.
4. Configure container CPU/memory limits, restart policy, log retention, and alerting on /ready, Kafka lag, error counters, and Elasticsearch indexing failures.
5. Test network failures, authentication failures, rejected Elasticsearch bulk items, process termination, broker reconnects, and consumer group rebalance in a real test environment.
6. Test sustained workload and record sizes against max poll interval, memory budget, throughput and index capacity before declaring production readiness.
7. Keep this service's health/metrics endpoint on a private network.

## Source

- program.py: process entrypoint and signal handlers.
- src/kafka_monitor.py: Kafka consumption, callbacks, offset handling and metrics.
- src/elasticsearch_exporter.py: synchronous Elasticsearch indexing.
- src/health_server.py: operational HTTP endpoints.
- env.example: configuration reference.
- Dockerfile: non-root container.

## Integration, security and operations

The repository includes disposable Docker Compose fixtures for Kafka 3.9.1 and Elasticsearch 8.19.2, real-client integration tests, a CI dependency audit, Kafka/Elasticsearch fault-injection scenarios, and an opt-in throughput/memory benchmark.

Elasticsearch lifecycle policies and strict mappings are versioned under `ops/elasticsearch/`. Applying them is a separate, explicit administrative operation. `deploy/compose.example.yml` provides resource and security boundaries for the monitor container; it does not provision or expose Kafka/Elasticsearch.

The Dockerfile pins an immutable Python base-image digest, and `requirements.lock` pins direct and transitive runtime dependencies. Updates require passing unit/integration CI and the dependency vulnerability audit.

See [docs/OPERATIONS.md](docs/OPERATIONS.md) for integration commands (including the three-broker leader-failover workflow), ILM setup, resource limits, alerts, ACL/TLS guidance, and remaining release gates. The testing cluster disables authentication by design and is never a production deployment.

## Tests and audit

Run unit tests with the actual installed client packages (unit tests do not contact external services):

    python -m pip install pytest
    python -m pytest -q

The GitHub Actions workflow runs the unit suite and builds the container. Read [AUDIT.md](AUDIT.md) for known production blockers and the required integration tests.

## License

MIT. See LICENSE.

# Operations and release gates

This monitor reads Kafka records and optionally indexes them into Elasticsearch 8.x. It does not run or administer the Kafka brokers or Elasticsearch cluster. Production connection details, credentials, Kafka ACLs and index lifecycles are owned by operators.

## Data safety

- The consumer group ID must be stable across restarts and unique to the monitor's workstream.
- For an uncommitted partition, the initial Kafka position is saved synchronously before records are processed. For committed partitions, the saved offset is never overridden.
- Processed Kafka offsets are synchronously committed after the entire Elasticsearch bulk operation completes. The explicit commit mapping includes only offsets from successfully processed records in the current poll; unrelated assigned partitions are not advanced.
- A rejected Elasticsearch bulk item aborts the batch. A restart replays events using stable Elasticsearch document IDs.
- Kafka source retention must exceed the **maximum expected downtime plus catch-up duration**. If brokers delete uncommitted events, no consumer can restore them.
- The system does not provide a distributed transaction across Kafka, Elasticsearch and callbacks. Custom callbacks must be idempotent.
- A DLQ is only for failed custom callbacks and must be configured with its own retention, topic authorization and operational ownership.

## Elasticsearch lifecycle setup (opt-in)

Runtime installation uses `requirements.lock` with exact direct and transitive versions; the Docker base image is pinned to an immutable SHA256 digest verified during CI. Changes to these pins require a new security scan, image build and integration run. This runtime lock does not contain individual wheel hashes and does not replace artifact provenance checks.

The source includes an ILM policy and index template for both streams:

| Index pattern | Policy | Age before delete |
| --- | --- | --- |
| kafka-events-* | kafka-monitor-events-30d | 30 days |
| kafka-metrics-* | kafka-monitor-metrics-7d | 7 days |

Policies **delete indices** when they reach their retention age. They do not roll over aliases, because the exporter uses a deterministic daily index derived from each Kafka record timestamp. Before enabling, verify legal and business retention obligations. Retention age is based on the index's lifecycle age, not the embedded Kafka timestamp.

The templates explicitly type Kafka fields and prevent uncontrolled dynamic field creation; default shards=1 and replicas=1. A disposable one-node Elasticsearch will have yellow index health because the replica cannot be assigned. That is expected during testing; production requires capacity across at least two data nodes.

To inspect the policy payloads without writing:

    python ops/elasticsearch/apply_templates.py

To intentionally apply them after reviewing retention and selecting the target cluster:

    export ELASTICSEARCH_HOSTS=https://YOUR-ELASTIC-CLUSTER:9200
    export ELASTICSEARCH_CA_CERTS=/run/secrets/elastic-ca.pem
    # Read a limited-privilege API key from the secret provider, not this repo.
    python ops/elasticsearch/apply_templates.py --apply

The apply command requires an authorized administrator/service identity able to update ILM policies and index templates. It is never automatically executed by the monitor. Do not apply test policies to an existing production cluster without reviewing name conflicts. The monitor itself should have only required index write permissions, not lifecycle-administration rights.

## Local integration test (isolated)

The integration compose file runs Kafka 3.9.1 in KRaft mode and Elasticsearch 8.19.2 with **authentication disabled**. It is disposable, listens only on localhost and **must not be deployed to production**.

Requires Docker Compose v2 and a host able to run both Java services. On Linux Elasticsearch requires vm.max_map_count of at least 262144.

    python -m pip install -r requirements.lock pytest
    docker compose -f tests/integration/compose.yml up -d
    python tests/integration/wait_services.py
    KAFKA_INTEGRATION=1 python -m pytest -q -m integration tests/integration
    docker compose -f tests/integration/compose.yml down -v --remove-orphans

Real integration scenarios include successful indexing, an uncommitted first batch with a partial Elasticsearch success and restart replay under auto.offset.reset=latest, bulk item rejection and recovery, index template/ILM, SIGTERM, and a two-consumer **group rebalance with replay**. The group rebalance test observes `PreparingRebalance` via the broker coordinator and then deliberately injects rejection of a processed-offset commit; it asserts the uncommitted, already-indexed record reappears on restart and all per-partition offsets advance. Commit rejection itself is fault-injected because native broker acceptance at that precise race window is not deterministic. The test runs against a single Kafka broker; it does not simulate a broker leader or controller failure. The local default unit suite skips integration tests unless explicitly enabled.

The GitHub Actions integration workflow starts and destroys these containers automatically on PRs. An integration CI failure blocks release readiness; passing does not establish production SLAs.

### Performance smoke test (opt-in)

After the disposable Kafka/Elasticsearch compose services are ready:

    KAFKA_INTEGRATION=1 python -m pytest -q -s -m performance tests/integration/test_performance.py

The benchmark reports records/s, elapsed seconds and peak resident memory for the process. Default: 500 records of 1024 bytes, distributed across two partitions. Set KAFKA_PERF_EVENTS (1..10000) and KAFKA_PERF_BYTES (1..1048576) to model the intended workload. The GitHub Actions integration workflow also supports a manual **performance** input. Measure sustained peak workloads separately; this benchmark is a smoke test, not a production capacity forecast.

## Container deployment

The example deploy/compose.example.yml starts the monitor **only**, not brokers or Elasticsearch. It enables:

- Non-root image user; read-only root filesystem and capped temporary filesystem.
- Dropped Linux capabilities, no-new-privileges, process count limit, and 512 MiB / 1 CPU starting limits.
- Localhost-only metrics binding on host port 8080.
- Restart policy and SIGTERM grace period.

Adjust resource ceilings after profiling. Deploy using:

    cp env.example .env
    # Edit .env locally and inject credentials outside Git.
    docker compose -f deploy/compose.example.yml up --build -d

Kafka and Elasticsearch URLs must resolve from within the container, not from the Docker host's localhost unless intended. Keep .env readable by the deployment account only. Prefer an orchestrator secret store or root-owned read-only secret files over plaintext in environment variables.

For an orchestrated cluster, reproduce the container's security boundaries and configure /live as liveness and /ready as readiness. Monitor the application's nonzero exits and restart loops. Exposing /metrics on the public Internet is unsupported.

## Network and credentials

Production recommendations:

- Kafka: TLS or SASL_SSL with certificate verification. Restrict topics and consumer-group permissions to the monitor's own resources. DLQ publishing requires separate write authorization.
- Elasticsearch: HTTPS with verified CA; a limited API key scoped to kafka-events-* and kafka-metrics-* write/create/index privileges. Templates and ILM are installed separately with administrative authorization.
- Do not store tokens, passwords, TLS private keys, test cluster dumps, or real event payloads in Git.
- Restrict /health, /metrics and /stats to an internal monitoring network. Even aggregate topic names and group IDs may be sensitive.

## Suggested monitoring and runbook

Scrape /metrics on a private network and alert on:

- `/ready` returns 503 until Kafka has completed its first poll or whenever the most recent successful poll is older than `KAFKA_POLL_STALE_SECONDS` (default 120). Tune the threshold to exceed worst-case processing/poll duration. A successful empty poll is healthy; the freshness check does not establish broker-wide availability.
- Use `kafka_monitor_poll_age_seconds` to identify polling stalls. `kafka_monitor_processed_commit_age_seconds` shows time since the last acknowledged record batch; on idle topics it can grow indefinitely, so combine it with lag and traffic rather than alerting on that age alone.

- Kafka monitor readiness false for longer than deployment startup allowance.
- Sustained consumer lag growth by topic/partition, with a threshold based on your expected throughput.
- Any increase in kafka_monitor_errors_by_type and kafka_monitor_events_failed.
- Repeated restarts, failed Elasticsearch writes, and unexpected missing documents.
- Elasticsearch index lifecycle failures, blocked writes, low free disk, and expired TLS/API credentials.
- Broker source retention approaching longest unprocessed interval.

Failure response: verify Kafka broker accessibility and group committed offsets, Elasticsearch write health, index template mapping, disk pressure, API key and TLS settings. Do not manually advance Kafka offsets to "fix" indexing without accounting for dropped records. Use Kafka retention to replay and verify deterministic Elasticsearch IDs.

## Release checklist

A release is not production-ready until:

1. Unit tests, CI integration tests and dependency safety checks pass.
2. Container image builds, with resolved dependency versions and reviewed base-image digest recorded for the release.
3. Failure injection covers connection loss, rejected/partial Elasticsearch bulk, restart, SIGTERM and group rebalance against a representative Kafka cluster. A disposable two-member group test is in CI, but **multi-broker leader/controller failure** still needs a separate representative environment.
4. A load test with representative event payload sizes, peak throughput, target partitions and deployment resource limits passes.
5. Actual production Kafka ACLs, Elasticsearch permissions, TLS certificates, index templates/retention and monitoring alerts are validated by the operating team.
6. Rollback to the previous image and recovery from Kafka retained events are documented and practiced.

No code-level test substitutes for operational approval of retention, access control and reliability targets.

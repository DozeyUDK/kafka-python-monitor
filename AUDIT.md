# Production readiness audit

Audit target: Kafka Python Monitor, source tree at the repository root.
Scope: Kafka consumer delivery, Elasticsearch indexing, metrics, security, packaging, Docker and tests.

## Verdict

**Not yet approved for production deployment.**

The first hardening pass removed the data-loss hazards in the source. Tests now exercise the critical acknowledgement path. CI has completed end-to-end checks against disposable Kafka 3.9.1 and Elasticsearch 8.19.2, including replay and fault scenarios. This is not a substitute for deployment-specific resilience, security and representative load testing; those release gates remain open.

## Findings and remediation

| Severity | Original finding | Status after hardening |
| --- | --- | --- |
| Critical | Kafka auto-commit could advance offsets before Elasticsearch had indexed the events | Fixed: synchronous manual commit after Elasticsearch bulk success |
| High | General consumer commit could include partitions/positions unrelated to a successfully exported batch | Fixed: only explicitly specified last-processed offsets for partitions in the current poll are committed |
| Critical | A new consumer group with auto_offset_reset=latest could skip its failed first batch after restart without any previously committed offset | Fixed: explicitly checkpoint the first unprocessed offset of newly assigned partitions (including idle partitions) before any event processing; fail closed if the checkpoint fails |
| Critical | Failed Elasticsearch bulk discarded the event buffer | Fixed: no deferred event buffer; bulk exceptions propagate; offsets are not committed |
| Critical | Elasticsearch connection/indexing errors were hidden and exporter silently disabled itself | Fixed: enabled exporter fails on startup connectivity error and aborts ingestion on bulk failures |
| High | Kafka SSL settings used an invalid ssl_context dictionary; security protocol was not applied consistently | Fixed: kafka-python ssl_cafile/ssl_certfile/ssl_keyfile and security_protocol for producer and consumer |
| High | Callback DLQ producer send was asynchronous and counted success before broker confirmation | Fixed: waits for future.get; no commit if DLQ delivery fails |
| High | Replays could create duplicate Elasticsearch documents, especially across day boundaries | Fixed: stable hashed ID based on topic/partition/offset and Kafka-timestamp-based index date |
| High | Metrics were emitted using event-count modulo and could be skipped on idle workloads | Fixed: fixed periodic interval |
| High | Consumer lag identified only partition number, conflating topics | Fixed: topic and partition metrics separated |
| Medium | HTTP readiness could report healthy before Kafka completed polling or during a processing stall | Fixed: initial-poll gating, configurable poll freshness threshold and associated metrics. A recent successful poll does not prove broker-wide availability |
| Medium | Elasticsearch connection parameters used obsolete client arguments | Fixed: explicit scheme URLs, basic_auth, api_key and verify_certs |
| Medium | Source pinned very old Kafka client and used an import hack for six | Fixed: kafka-python 2.3.2, removed compatibility shim, Elasticsearch client pinned to 8.19.2; python-dotenv bumped to 1.2.2 after CI vulnerability audit |
| Medium | The service did not have automated tests | Fixed for baseline: regression unit tests plus successful disposable Kafka/Elasticsearch integration CI. Representative load and multi-broker failover still open |
| Medium | README overclaimed production readiness, duplicated config and used decorative emojis | Fixed: concise technical README with documented guarantees and deployment prerequisites |
| Medium | HTTP endpoint defaults were not suitable for all environments | Fixed: local bind by default, explicit Docker override, limited host port exposure in example |
| Medium | setup.py pointed to a placeholder GitHub URL and missed the CLI entrypoint module | Fixed |
| Open | No end-to-end Kafka+Elasticsearch test against the intended production versions | CI tests now authored with disposable services; need successful CI run and deployment-specific compatibility evidence |
| Open | No tested recovery with Kafka leader change, ES 429/503, partial bulk rejection and process SIGTERM | CI now covers injected partial bulk, real ES bulk rejection and SIGTERM/restart. Two-consumer CI integration test now waits for broker-confirmed PreparingRebalance, injects a processed-commit rejection, and verifies replay and final offsets. Multi-broker leader change and network partitions still require validation |
| Open | No committed infrastructure config for index templates, ILM, resource requests/limits or security ACLs | Versioned templates/ILM policies and a hardened Compose example now included. Actual deployment ACLs and resources remain deployment-specific |
| Open | No measured throughput/memory/performance budget for real message sizes | Opt-in benchmark now available; representative target measurements and SLOs still required |
| Resolved for baseline | Docker image was not pinned to an immutable digest and transitive dependencies were not locked | Python 3.11.17 Bookworm base image now pinned to a GitHub Actions-observed SHA256 digest; tested transitive versions pinned in requirements.lock and audited in CI. Hash-locking and periodic refresh remain future supply-chain improvements |

## Data handling contract

1. Consume at most KAFKA_BATCH_SIZE messages from Kafka per poll.
2. On each new partition assignment without a committed offset, synchronously commit its initial position, not its post-poll position. Abort if this checkpoint fails. No records are considered processed by this commit.
3. Execute callbacks. For individual callback errors, optionally publish to DLQ and wait for Kafka acknowledgement. If DLQ is disabled or unavailable, fail without committing the current poll.
4. Index all remaining events through synchronous Elasticsearch bulk. Reject partial failures.
5. Commit the processed Kafka consumer offset synchronously after indexing succeeds.
6. On failures before a processed-offset commit, stop the process with a nonzero exit code. Restart replays from the last committed position (including the initial checkpoint), indexed under deterministic identifiers.

No atomic commit spans Kafka, Elasticsearch and external callbacks. Custom callbacks should be idempotent.

## Validation performed

- Source review covering program.py, src/kafka_monitor.py, src/elasticsearch_exporter.py, src/health_server.py, Dockerfile and environment configuration.
- Offline regression suite: 33 tests passing on the server's test interpreter at time of audit.
- Runtime test caveat: the server's test interpreter did not include Kafka or Elasticsearch client modules, so the unit suite provided minimal dependency stubs. Real package imports and network behavior **have not** been validated by this result.

## Release gates

Before calling this production-ready:

1. Create an isolated Python environment, install exactly the pinned dependencies, and rerun pytest without dependency-stub warnings.
2. Run an integration test with the intended Kafka version and Elasticsearch 8.x, including SASL/TLS.
3. Inject ES bulk rejection including a partial success. Verify: no Kafka commit for the failed poll, nonzero process exit, restart replay, exactly one Elasticsearch document per topic/partition/offset.
4. Stop the monitor during ingestion and immediately restart it; verify no missing offsets.
5. Run a load and resource test representative of average/max message size, rate and partition count; verify consumer max poll interval and Elasticsearch retry budgets.
6. Configure an index template, ILM retention, Kafka permissions, Elasticsearch API key privileges, private health/metrics access and alerting.
7. Pin the Docker base image to a reviewed digest, lock transitive package versions, scan the image for vulnerabilities and run as non-root.

PR #2 has been merged. A subsequent PR contains integration, operations and security checks; it has not been deployed.

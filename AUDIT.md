# Production readiness audit

Audit target: Kafka Python Monitor, source tree at the repository root.
Scope: Kafka consumer delivery, Elasticsearch indexing, metrics, security, packaging, Docker and tests.

## Verdict

**Not yet approved for production deployment.**

The first hardening pass removed the data-loss hazards in the source. Tests now exercise the critical acknowledgement path. However, no integration test against a real Kafka cluster and Elasticsearch 8.x has been completed, and the deployment-specific operational requirements listed below remain open.

## Findings and remediation

| Severity | Original finding | Status after hardening |
| --- | --- | --- |
| Critical | Kafka auto-commit could advance offsets before Elasticsearch had indexed the events | Fixed: synchronous manual commit after Elasticsearch bulk success |
| Critical | Failed Elasticsearch bulk discarded the event buffer | Fixed: no deferred event buffer; bulk exceptions propagate; offsets are not committed |
| Critical | Elasticsearch connection/indexing errors were hidden and exporter silently disabled itself | Fixed: enabled exporter fails on startup connectivity error and aborts ingestion on bulk failures |
| High | Kafka SSL settings used an invalid ssl_context dictionary; security protocol was not applied consistently | Fixed: kafka-python ssl_cafile/ssl_certfile/ssl_keyfile and security_protocol for producer and consumer |
| High | Callback DLQ producer send was asynchronous and counted success before broker confirmation | Fixed: waits for future.get; no commit if DLQ delivery fails |
| High | Replays could create duplicate Elasticsearch documents, especially across day boundaries | Fixed: stable hashed ID based on topic/partition/offset and Kafka-timestamp-based index date |
| High | Metrics were emitted using event-count modulo and could be skipped on idle workloads | Fixed: fixed periodic interval |
| High | Consumer lag identified only partition number, conflating topics | Fixed: topic and partition metrics separated |
| Medium | Elasticsearch connection parameters used obsolete client arguments | Fixed: explicit scheme URLs, basic_auth, api_key and verify_certs |
| Medium | Source pinned very old Kafka client and used an import hack for six | Fixed: kafka-python 2.3.2, removed compatibility shim, Elasticsearch client pinned to 8.19.2 |
| Medium | The service did not have automated tests | Partial: regression unit tests with mocked clients; integration tests still required |
| Medium | README overclaimed production readiness, duplicated config and used decorative emojis | Fixed: concise technical README with documented guarantees and deployment prerequisites |
| Medium | HTTP endpoint defaults were not suitable for all environments | Fixed: local bind by default, explicit Docker override, limited host port exposure in example |
| Medium | setup.py pointed to a placeholder GitHub URL and missed the CLI entrypoint module | Fixed |
| Open | No end-to-end Kafka+Elasticsearch test against the intended production versions | BLOCKER |
| Open | No tested recovery with Kafka leader change, ES 429/503, partial bulk rejection and process SIGTERM | BLOCKER |
| Open | No committed infrastructure config for index templates, ILM, resource requests/limits or security ACLs | Deployment-specific blocker |
| Open | No measured throughput/memory/performance budget for real message sizes | Deployment-specific blocker |
| Open | Docker image is not pinned to an immutable digest and transitive dependencies are not locked | Supply chain hardening required |

## Data handling contract

1. Consume at most KAFKA_BATCH_SIZE messages from Kafka per poll.
2. Execute callbacks. For individual callback errors, optionally publish to DLQ and wait for Kafka acknowledgement. If DLQ is disabled or unavailable, fail without committing the current poll.
3. Index all remaining events through synchronous Elasticsearch bulk. Reject partial failures.
4. Commit the Kafka consumer offset synchronously after indexing succeeds.
5. On failures before commit, stop the process with a nonzero exit code. Restart replays uncommitted messages, indexed under deterministic identifiers.

No atomic commit spans Kafka, Elasticsearch and external callbacks. Custom callbacks should be idempotent.

## Validation performed

- Source review covering program.py, src/kafka_monitor.py, src/elasticsearch_exporter.py, src/health_server.py, Dockerfile and environment configuration.
- Offline regression suite: 14 tests passing on the server's test interpreter at time of audit.
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

Audit changes are only in the working snapshot on dozeyserver. They have not been pushed to GitHub.

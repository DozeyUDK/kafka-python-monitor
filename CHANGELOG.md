# Changelog

## Unreleased (2026-10-21)

Initial operational hardening pass:

- Replace Kafka consumer auto-commit with a synchronous commit after confirmed Elasticsearch indexing.
- Before the first event processing of new/uncommitted partitions, durably checkpoint their initial positions so latest-based restart cannot skip a failed first batch; include idle assignments and fail closed if the checkpoint is rejected.
- Remove Elasticsearch event buffering that discarded records on bulk failure.
- Use deterministic Elasticsearch document IDs and index dates derived from Kafka timestamps.
- Fail fast on Elasticsearch connectivity failures and stop on bulk indexing failure.
- Fix Kafka TLS/SASL configuration and wait for DLQ producer acknowledgement.
- Refresh Kafka and Elasticsearch 8.x Python client dependencies; remove legacy import workaround.
- Improve liveness/readiness, metric refresh cadence and Kafka lag labeling.
- Add unit regression tests, basic CI checks and the AUDIT.md release checklist.
- Rewrite README without unsupported production-readiness claims.

Production release remains blocked by integration, failure-injection and load testing.

## 1.0.0 (previous repository state)

Initial Kafka event monitor with health endpoints, DLQ support, Elasticsearch export, Prometheus metrics, and Docker packaging.

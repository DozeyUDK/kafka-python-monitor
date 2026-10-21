# Changelog

## Unreleased (2026-10-21)

Additional reliability/operations work (next PR):

- Add real-client Kafka 3.9.1 and Elasticsearch 8.19.2 integration workflow with disposable Docker Compose services.
- Exercise first-batch replay after partial Elasticsearch success, rejected Elasticsearch bulk items, and SIGTERM/restart recovery.
- Add Elasticsearch index templates, field mappings, 30-day event and 7-day metric lifecycle policies with opt-in administrative application.
- Add optional throughput/memory smoke benchmark and hardened Docker Compose deployment example.
- Add known-secret pattern scanning and a Python dependency vulnerability audit to CI.
- Upgrade python-dotenv to 1.2.2 after CI vulnerability scan identified an advisory against 1.0.1.
- Pin the verified Python base image SHA256 and runtime transitive dependency versions in requirements.lock.
- Document network isolation, least-privilege ACL/API keys, retention, metrics, resource limits and remaining release gates.

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

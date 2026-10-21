"""Concurrent Kafka producer -> monitor -> Elasticsearch load/correctness gate.

A short PR smoke test is automatic; a longer paced profile is manual. All
traffic is restricted to ephemeral loopback-only Kafka/Elasticsearch services.
This is NOT a production throughput/SLO certification.
"""
import json
import os
import resource
import threading
import time
import uuid

import pytest

from ops.performance_profile import LoadProfile

pytestmark = [
    pytest.mark.performance,
    pytest.mark.skipif(
        os.getenv("KAFKA_INTEGRATION") != "1",
        reason="Only runs against the explicitly enabled disposable integration stack",
    ),
]


def test_paced_load_has_complete_indexed_records_and_committed_offsets(monkeypatch):
    from elasticsearch import Elasticsearch
    from kafka import KafkaAdminClient, KafkaProducer
    from kafka.admin import NewTopic
    from kafka.structs import TopicPartition
    from ops.elasticsearch.apply_templates import apply_templates
    from src.kafka_monitor import KafkaMonitor

    profile = LoadProfile.from_env()
    broker = os.getenv("KAFKA_TEST_BROKER", "localhost:29092")
    url = os.getenv("ELASTICSEARCH_TEST_URL", "http://localhost:19200")
    profile.validate_target(broker, url)  # Must happen before any network call.

    topic = "monitor-load-" + uuid.uuid4().hex[:12]
    group = topic + "-consumer"
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", broker)
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    monkeypatch.setenv("KAFKA_TOPICS", topic)
    monkeypatch.setenv("KAFKA_GROUP_ID", group)
    monkeypatch.setenv("KAFKA_AUTO_OFFSET_RESET", "earliest")
    monkeypatch.setenv("KAFKA_BATCH_SIZE", str(profile.batch_size))
    monkeypatch.setenv("MONITOR_INTERVAL_SECONDS", "1")
    monkeypatch.setenv("METRICS_INTERVAL_SECONDS", "60")
    monkeypatch.setenv("ELASTICSEARCH_ENABLED", "true")
    monkeypatch.setenv("ELASTICSEARCH_HOSTS", url)

    admin = KafkaAdminClient(bootstrap_servers=broker, request_timeout_ms=10000)
    producer = KafkaProducer(bootstrap_servers=broker, acks="all", retries=3)
    elastic = Elasticsearch(url, request_timeout=15)
    monitor = None
    timer = None
    producer_thread = None
    producer_errors = []
    producer_complete = threading.Event()
    indexed = set()
    latencies = []
    bytes_sent = profile.events * profile.payload_bytes
    started = None
    completed = None

    try:
        apply_templates(elastic)
        admin.create_topics([
            NewTopic(topic, num_partitions=profile.partitions, replication_factor=1)
        ])
        monitor = KafkaMonitor()
        assert monitor.connect()
        original_export = monitor.es_exporter.export_events

        def verified_export(events):
            original_export(events)  # Return only after Elasticsearch acknowledges bulk.
            indexed_at_ms = time.time() * 1000
            for event in events:
                identity = (event["partition"], event["offset"])
                if identity in indexed:
                    raise AssertionError("Duplicate record processed in load run")
                indexed.add(identity)
                assert event["timestamp"] is not None
                latencies.append(max(0, (indexed_at_ms - event["timestamp"]) / 1000))
            if len(indexed) == profile.events:
                monitor.stop()  # The monitor still commits this completed batch.

        monitor.es_exporter.export_events = verified_export

        def generate():
            try:
                value = b"x" * profile.payload_bytes
                futures = []
                send_started = time.monotonic()
                for i in range(profile.events):
                    futures.append(producer.send(
                        topic, partition=i % profile.partitions, value=value
                    ))
                    # Bounded in-flight requests; never retain unbounded producer
                    # futures. Confirm each group rather than trusting flush only.
                    if len(futures) == 50:
                        for future in futures:
                            future.get(timeout=20)
                        futures.clear()
                    scheduled = send_started + profile.duration_seconds * (i + 1) / profile.events
                    delay = scheduled - time.monotonic()
                    if delay > 0:
                        time.sleep(delay)
                for future in futures:
                    future.get(timeout=20)
                producer.flush(timeout=30)
            except Exception as exc:
                producer_errors.append(exc)
                monitor.stop()
            finally:
                producer_complete.set()

        started = time.monotonic()
        timer = threading.Timer(profile.duration_seconds + 120, monitor.stop)
        timer.daemon = True
        producer_thread = threading.Thread(target=generate, name="load-producer", daemon=True)
        timer.start()
        producer_thread.start()
        monitor.monitor()
        completed = time.monotonic()

        assert producer_complete.wait(30), "Producer did not finish"
        producer_thread.join(timeout=5)
        assert not producer_errors, f"Kafka producer failure: {producer_errors!r}"
        assert len(indexed) == profile.events, (
            f"Indexed {len(indexed)} out of {profile.events}; timed out or missed records"
        )

        expected = {
            (partition, offset)
            for partition in range(profile.partitions)
            for offset in range(len(range(partition, profile.events, profile.partitions)))
        }
        assert indexed == expected, "A Kafka partition contains missing/extra offsets"

        # Synchronous export plus commit must advance every partition to its
        # real end, not merely the callback's last seen position.
        for partition in range(profile.partitions):
            tp = TopicPartition(topic, partition)
            expected_offset = len(range(partition, profile.events, profile.partitions))
            assert monitor.consumer.committed(tp) == expected_offset

        elastic.indices.refresh(index="kafka-events-*", ignore_unavailable=True)
        actual_documents = elastic.count(
            index="kafka-events-*",
            query={"term": {"kafka_topic": topic}},
        )["count"]
        assert actual_documents == profile.events, (
            f"Expected {profile.events} Elasticsearch docs, got {actual_documents}"
        )

        elapsed = max(completed - started, 0.001)
        report = {
            "events": profile.events,
            "unique_indexed": len(indexed),
            "elasticsearch_documents": actual_documents,
            "partitions": profile.partitions,
            "payload_bytes": profile.payload_bytes,
            "bytes_sent": bytes_sent,
            "duration_target_seconds": profile.duration_seconds,
            "elapsed_seconds": round(elapsed, 3),
            "events_per_second": round(profile.events / elapsed, 2),
            "p50_index_latency_seconds": round(profile.percentile(latencies, 0.50), 3),
            "p95_index_latency_seconds": round(profile.percentile(latencies, 0.95), 3),
            "p99_index_latency_seconds": round(profile.percentile(latencies, 0.99), 3),
            # GitHub Actions uses Linux; ru_maxrss is KiB there.
            "max_rss_mib": round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 2),
            "errors": monitor.get_stats()["events_failed"],
        }
        print("KAFKA_LOAD_REPORT=" + json.dumps(report, sort_keys=True), flush=True)
        assert report["errors"] == 0
        profile.check(report)  # Configurable SLOs, no arbitrary runner speed gate.
    finally:
        if timer is not None:
            timer.cancel()
        if monitor is not None:
            monitor.stop()
            monitor.disconnect()
        if producer_thread is not None:
            producer_thread.join(timeout=10)
        producer.close(timeout=10)
        admin.close()
        elastic.close()

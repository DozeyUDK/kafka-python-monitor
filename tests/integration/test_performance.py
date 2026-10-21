"""Opt-in resource smoke benchmark, not a performance guarantee."""
import json
import os
import resource
import threading
import time
import uuid

import pytest

pytestmark = [
    pytest.mark.performance,
    pytest.mark.skipif(os.getenv("KAFKA_INTEGRATION") != "1",
                       reason="Needs disposable integration Kafka+Elasticsearch"),
]


def test_smoke_throughput_and_memory(monkeypatch):
    from kafka import KafkaAdminClient, KafkaProducer
    from kafka.admin import NewTopic
    from src.kafka_monitor import KafkaMonitor

    count = int(os.getenv("KAFKA_PERF_EVENTS", "500"))
    payload_bytes = int(os.getenv("KAFKA_PERF_BYTES", "1024"))
    assert 1 <= count <= 10000
    assert 1 <= payload_bytes <= 1024 * 1024
    broker = os.getenv("KAFKA_TEST_BROKER", "localhost:29092")
    url = os.getenv("ELASTICSEARCH_TEST_URL", "http://localhost:19200")
    assert broker == "localhost:29092" and url == "http://localhost:19200"

    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", broker)
    monkeypatch.setenv("KAFKA_AUTO_OFFSET_RESET", "earliest")
    monkeypatch.setenv("ELASTICSEARCH_ENABLED", "true")
    monkeypatch.setenv("ELASTICSEARCH_HOSTS", url)
    monkeypatch.setenv("MONITOR_INTERVAL_SECONDS", "1")
    monkeypatch.setenv("METRICS_INTERVAL_SECONDS", "60")
    monkeypatch.setenv("KAFKA_BATCH_SIZE", "100")
    topic = "kafka-monitor-perf-" + uuid.uuid4().hex[:12]
    monkeypatch.setenv("KAFKA_TOPICS", topic)
    monkeypatch.setenv("KAFKA_GROUP_ID", topic + "-group")
    admin = KafkaAdminClient(bootstrap_servers=broker)
    producer = KafkaProducer(bootstrap_servers=broker, acks="all")
    monitor = None
    try:
        admin.create_topics([NewTopic(topic, num_partitions=2, replication_factor=1)])
        value = b"x" * payload_bytes
        for i in range(count):
            producer.send(topic, partition=i % 2, value=value)
        producer.flush(timeout=30)
        monitor = KafkaMonitor()
        assert monitor.connect()
        seen = []
        started = time.monotonic()

        def callback(event):
            seen.append(event["offset"])
            if len(seen) == count:
                monitor.stop()

        timer = threading.Timer(120, monitor.stop)
        timer.daemon = True
        timer.start()
        try:
            monitor.monitor(callback=callback)
        finally:
            timer.cancel()
        elapsed = time.monotonic() - started
        assert len(seen) == count
        memory_kib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        print(json.dumps({
            "events": count,
            "payload_bytes": payload_bytes,
            "elapsed_seconds": round(elapsed, 3),
            "events_per_second": round(count / elapsed, 2),
            "max_rss_kib": memory_kib,
        }, sort_keys=True))
    finally:
        if monitor is not None:
            monitor.disconnect()
        producer.close()
        admin.close()

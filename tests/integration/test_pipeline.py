"""End-to-end Kafka -> Elasticsearch checks with disposable Docker services.

Never enable KAFKA_INTEGRATION against production systems.
"""
import os
import threading
import time
import uuid
from contextlib import contextmanager

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("KAFKA_INTEGRATION") != "1",
        reason="Opt-in: set KAFKA_INTEGRATION=1 with disposable integration containers",
    ),
]


@pytest.fixture
def clients(monkeypatch):
    from kafka import KafkaAdminClient, KafkaProducer
    from elasticsearch import Elasticsearch
    from ops.elasticsearch.apply_templates import apply_templates

    broker = os.getenv("KAFKA_TEST_BROKER", "localhost:29092")
    url = os.getenv("ELASTICSEARCH_TEST_URL", "http://localhost:19200")
    if not (broker.endswith(":29092") and url == "http://localhost:19200"):
        pytest.fail("Integration tests intentionally restricted to local disposable services")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", broker)
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    monkeypatch.setenv("ELASTICSEARCH_HOSTS", url)
    monkeypatch.setenv("ELASTICSEARCH_ENABLED", "true")
    monkeypatch.setenv("MONITOR_INTERVAL_SECONDS", "1")
    monkeypatch.setenv("KAFKA_BATCH_SIZE", "100")
    monkeypatch.setenv("KAFKA_AUTO_OFFSET_RESET", "earliest")
    monkeypatch.setenv("METRICS_INTERVAL_SECONDS", "60")
    admin = KafkaAdminClient(bootstrap_servers=broker, request_timeout_ms=10000)
    producer = KafkaProducer(bootstrap_servers=broker, acks="all", retries=3)
    es = Elasticsearch(url, request_timeout=15)
    apply_templates(es)
    yield admin, producer, es
    producer.close()
    admin.close()
    es.close()


@pytest.fixture
def kafka_topic(clients):
    from kafka.admin import NewTopic

    admin, _, _ = clients
    name = "kafka-monitor-ci-" + uuid.uuid4().hex[:12]
    admin.create_topics([NewTopic(name=name, num_partitions=2, replication_factor=1)])
    return name


def send(producer, topic, partition, count, prefix="message"):
    for i in range(count):
        producer.send(
            topic, partition=partition, key=("key-%d" % i).encode(),
            value=("%s-%d" % (prefix, i)).encode(),
        ).get(timeout=15)
    producer.flush(timeout=15)


@contextmanager
def timeout_monitor(monitor, seconds=60):
    timer = threading.Timer(seconds, monitor.stop)
    timer.daemon = True
    timer.start()
    try:
        yield
    finally:
        timer.cancel()


def new_monitor(monkeypatch, topic, group, reset="earliest"):
    from src.kafka_monitor import KafkaMonitor

    monkeypatch.setenv("KAFKA_TOPICS", topic)
    monkeypatch.setenv("KAFKA_GROUP_ID", group)
    monkeypatch.setenv("KAFKA_AUTO_OFFSET_RESET", reset)
    monitor = KafkaMonitor()
    assert monitor.validate_config()
    assert monitor.connect()
    return monitor


def event_count(es, topic):
    es.indices.refresh(index="kafka-events-*", ignore_unavailable=True)
    response = es.count(index="kafka-events-*", query={"term": {"kafka_topic": topic}})
    return response["count"]


def test_real_kafka_es_batch_and_committed_offsets(clients, kafka_topic, monkeypatch):
    from kafka.structs import TopicPartition

    _, producer, es = clients
    group = "monitor-ci-" + uuid.uuid4().hex
    send(producer, kafka_topic, 0, 8)
    send(producer, kafka_topic, 1, 8)

    monitor = new_monitor(monkeypatch, kafka_topic, group)
    seen = []

    def on_record(record):
        seen.append((record["partition"], record["offset"]))
        if len(seen) == 16:
            monitor.stop()

    try:
        with timeout_monitor(monitor):
            monitor.monitor(callback=on_record)
        assert len(seen) == 16
        assert event_count(es, kafka_topic) == 16
        for partition in (0, 1):
            assert monitor.consumer.committed(TopicPartition(kafka_topic, partition)) == 8
    finally:
        monitor.disconnect()


def test_new_group_latest_replays_partial_success_after_restart(clients, kafka_topic, monkeypatch):
    from kafka.structs import TopicPartition

    _, producer, es = clients
    group = "monitor-replay-ci-" + uuid.uuid4().hex
    tp = TopicPartition(kafka_topic, 0)
    first = new_monitor(monkeypatch, kafka_topic, group, reset="latest")
    initial_offset = None
    try:
        # Force an assignment without records: this is the genuine latest-reset
        # case where no committed starting offset exists for a new consumer group.
        for _ in range(12):
            first.consumer.poll(timeout_ms=1000)
            if tp in first.consumer.assignment():
                first._checkpoint_initial_offsets({})
                initial_offset = first.consumer.committed(tp)
                break
        assert initial_offset is not None
        send(producer, kafka_topic, 0, 2, prefix="retry")
        original_export = first.es_exporter.export_events
        fail_once = {"armed": True}

        def injected_failure(events):
            if fail_once["armed"]:
                fail_once["armed"] = False
                original_export(events[:1])  # Real Elasticsearch write succeeded.
                raise ConnectionError("injected partial bulk failure")
            original_export(events)

        first.es_exporter.export_events = injected_failure
        with pytest.raises(ConnectionError, match="injected partial bulk"):
            with timeout_monitor(first):
                first.monitor()
        assert first.consumer.committed(tp) == initial_offset
        assert event_count(es, kafka_topic) == 1
    finally:
        first.disconnect()

    second = new_monitor(monkeypatch, kafka_topic, group, reset="latest")
    seen = []

    def after_restart(record):
        seen.append(record["offset"])
        if len(seen) == 2:
            second.stop()

    try:
        with timeout_monitor(second):
            second.monitor(callback=after_restart)
        assert len(seen) == 2
        assert event_count(es, kafka_topic) == 2
        assert second.consumer.committed(tp) == initial_offset + 2
    finally:
        second.disconnect()


def test_real_elasticsearch_rejects_partial_bulk_then_idempotent_replay(clients, kafka_topic):
    from elasticsearch.helpers import BulkIndexError
    from src.elasticsearch_exporter import ElasticsearchExporter

    _, _, es = clients
    exporter = ElasticsearchExporter(client=es)
    events = [
        {"topic": kafka_topic, "partition": 0, "offset": 41, "key": None,
         "value": "valid", "timestamp": 1735689600000},
        {"topic": kafka_topic, "partition": 0, "offset": 42, "key": None,
         "value": "invalid", "timestamp": 1735689600000},
    ]
    build_action = exporter._event_action

    def invalid_second(event):
        action = build_action(event)
        if event["offset"] == 42:
            action["_source"]["kafka_partition"] = "not-an-integer"
        return action

    exporter._event_action = invalid_second
    with pytest.raises(BulkIndexError):
        exporter.export_events(events)
    assert event_count(es, kafka_topic) == 1
    exporter._event_action = build_action
    exporter.export_events(events)
    assert event_count(es, kafka_topic) == 2
    # Index overwrite with stable _id prevents duplicate documents on retry.
    exporter.export_events(events)
    assert event_count(es, kafka_topic) == 2


def test_retention_policy_and_index_templates_present(clients):
    _, _, es = clients
    assert "kafka-monitor-events-30d" in es.ilm.get_lifecycle(
        name="kafka-monitor-events-30d"
    )
    template = es.indices.get_index_template(name="kafka-monitor-events")
    import json
    settings = template["index_templates"][0]["index_template"]["template"]["settings"]
    assert "kafka-monitor-events-30d" in json.dumps(settings)

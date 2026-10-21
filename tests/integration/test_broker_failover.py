"""Real broker-leader loss with a replicated three-broker KRaft Kafka cluster.

Run ONLY in the isolated GitHub Actions multibroker workflow. The test stops a
container by an allowlisted service name; never run against actual deployments.
"""
import os
from pathlib import Path
import subprocess
import threading
import time
import uuid

import pytest


pytestmark = [
    pytest.mark.multibroker,
    pytest.mark.skipif(
        os.getenv("KAFKA_MULTIBROKER") != "1" or
        os.getenv("GITHUB_ACTIONS") != "true" or
        os.getenv("CI") != "true",
        reason="Only execute in the opt-in disposable GitHub Actions failover job",
    ),
]

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ROOT / "tests" / "integration" / "compose-multibroker.yml"
PROJECT = "kafka-monitor-failover-ci"
BROKERS = "localhost:29191,localhost:29192,localhost:29193"
ES = "http://localhost:19201"
COUNT_PER_PARTITION = 6


def run_compose(action, node):
    assert action in ("stop", "start")
    assert node in ("broker1", "broker2", "broker3")
    args = [
        "docker", "compose", "-p", PROJECT, "-f", str(COMPOSE),
        action,
    ]
    if action == "stop":
        args += ["--timeout", "10"]
    args += [node]
    subprocess.run(args, cwd=ROOT, timeout=75, check=True)


def wait_for_new_leader(survivors, topic, partition, old_leader, timeout=70):
    from kafka import KafkaAdminClient

    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=survivors, request_timeout_ms=4000,
                api_version_auto_timeout_ms=4000,
            )
            try:
                metadata = admin.describe_topics([topic])
                topic_info = next(m for m in metadata if m["topic"] == topic)
                info = next(p for p in topic_info["partitions"] if p["partition"] == partition)
                new_leader = info["leader"]
                if new_leader in {1, 2, 3} - {old_leader}:
                    return new_leader
            finally:
                admin.close()
        except Exception as exc:
            last_error = type(exc).__name__
        time.sleep(2)
    pytest.fail("Partition leader was not re-elected in time: %s" % last_error)


def test_leader_loss_does_not_lose_unacknowledged_kafka_records(monkeypatch):
    from elasticsearch import Elasticsearch
    from kafka import KafkaAdminClient, KafkaProducer
    from kafka.admin import NewTopic
    from kafka.structs import TopicPartition

    assert os.getenv("KAFKA_TEST_BROKER") == BROKERS
    assert os.getenv("ELASTICSEARCH_TEST_URL") == ES
    assert COMPOSE.is_file()

    admin = KafkaAdminClient(bootstrap_servers=BROKERS, request_timeout_ms=8000)
    producer = KafkaProducer(bootstrap_servers=BROKERS, acks="all", retries=5)
    elastic = Elasticsearch(ES, request_timeout=15)
    primary = None
    replacement = None
    primary_thread = None
    stopped_node = None
    callback_started = threading.Event()
    release_callback = threading.Event()
    primary_errors = []
    first_event = []
    try:
        from ops.elasticsearch.apply_templates import apply_templates
        from src.kafka_monitor import KafkaMonitor

        brokers = admin.describe_cluster()["brokers"]
        assert len(brokers) == 3, "This test requires three independent Kafka brokers"

        apply_templates(elastic)
        topic = "monitor-failover-" + uuid.uuid4().hex[:12]
        group = "monitor-failover-group-" + uuid.uuid4().hex[:12]
        admin.create_topics([
            NewTopic(
                name=topic, num_partitions=2, replication_factor=3,
                topic_configs={"min.insync.replicas": "2"},
            )
        ])
        deadline = time.monotonic() + 60
        while True:
            metadata = admin.describe_topics([topic])[0]["partitions"]
            if (len(metadata) == 2 and
                    all(len(p["replicas"]) == 3 and len(p["isr"]) == 3 for p in metadata)):
                break
            if time.monotonic() >= deadline:
                pytest.fail("Replica set not ready: %r" % metadata)
            time.sleep(2)
        for partition in (0, 1):
            for offset in range(COUNT_PER_PARTITION):
                producer.send(
                    topic, partition=partition, value=("event-%d-%d" % (partition, offset)).encode()
                ).get(timeout=20)
        producer.flush(timeout=30)

        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", BROKERS)
        monkeypatch.setenv("KAFKA_TOPICS", topic)
        monkeypatch.setenv("KAFKA_GROUP_ID", group)
        monkeypatch.setenv("KAFKA_AUTO_OFFSET_RESET", "earliest")
        monkeypatch.setenv("KAFKA_BATCH_SIZE", "1")
        monkeypatch.setenv("MONITOR_INTERVAL_SECONDS", "1")
        monkeypatch.setenv("METRICS_INTERVAL_SECONDS", "60")
        monkeypatch.setenv("ELASTICSEARCH_ENABLED", "true")
        monkeypatch.setenv("ELASTICSEARCH_HOSTS", ES)

        primary = KafkaMonitor()
        assert primary.connect()
        original_export = primary.es_exporter.export_events
        exported_once = threading.Event()

        def export_then_fail(events):
            original_export(events)
            exported_once.set()
            raise ConnectionError("intentional crash after Elasticsearch bulk, before commit")

        primary.es_exporter.export_events = export_then_fail

        def on_first(event):
            first_event.append(event)
            callback_started.set()
            if not release_callback.wait(70):
                raise TimeoutError("Broker failure test did not release callback")

        def primary_worker():
            try:
                primary.monitor(callback=on_first)
            except Exception as exc:
                primary_errors.append(exc)

        primary_thread = threading.Thread(target=primary_worker, daemon=True)
        primary_thread.start()
        assert callback_started.wait(30), "No Kafka record arrived in first consumer"
        assert len(first_event) == 1

        first = first_event[0]
        tp = TopicPartition(topic, first["partition"])
        before = admin.list_consumer_group_offsets(group)
        assert before[tp].offset == first["offset"]

        topic_meta = admin.describe_topics([topic])[0]["partitions"]
        leader = next(p["leader"] for p in topic_meta if p["partition"] == tp.partition)
        assert leader in (1, 2, 3)
        stopped_node = "broker%d" % leader
        run_compose("stop", stopped_node)
        survivors = [
            "localhost:%d" % (29190 + i) for i in (1, 2, 3) if i != leader
        ]
        new_leader = wait_for_new_leader(
            survivors, topic, tp.partition, leader,
        )
        assert new_leader != leader
        print("leader_failed=%d new_leader=%d topic=%s" % (leader, new_leader, topic))

        release_callback.set()
        primary_thread.join(timeout=35)
        assert not primary_thread.is_alive()
        assert exported_once.is_set(), "First Elasticsearch document never persisted"
        assert len(primary_errors) == 1
        assert isinstance(primary_errors[0], ConnectionError)
        primary.disconnect()
        primary = None

        elastic.indices.refresh(index="kafka-events-*", ignore_unavailable=True)
        query = {"term": {"kafka_topic": topic}}
        assert elastic.count(index="kafka-events-*", query=query)["count"] == 1

        # A new monitor must finish the backlog while the failed leader remains
        # down. The original record MUST reappear because it was never committed.
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", ",".join(survivors))
        monkeypatch.setenv("KAFKA_BATCH_SIZE", "12")
        replacement = KafkaMonitor()
        assert replacement.connect()
        replayed = set()
        all_events = 2 * COUNT_PER_PARTITION

        def after_restart(event):
            replayed.add((event["topic"], event["partition"], event["offset"]))
            if len(replayed) >= all_events:
                replacement.stop()

        timer = threading.Timer(85, replacement.stop)
        timer.daemon = True
        timer.start()
        try:
            replacement.monitor(callback=after_restart)
        finally:
            timer.cancel()

        assert len(replayed) == all_events, "Recovery did not consume entire backlog"
        assert (first["topic"], first["partition"], first["offset"]) in replayed
        elastic.indices.refresh(index="kafka-events-*", ignore_unavailable=True)
        assert elastic.count(index="kafka-events-*", query=query)["count"] == all_events
        final_admin = KafkaAdminClient(bootstrap_servers=survivors, request_timeout_ms=8000)
        try:
            final = final_admin.list_consumer_group_offsets(group)
            for partition in (0, 1):
                assert final[TopicPartition(topic, partition)].offset == COUNT_PER_PARTITION
        finally:
            final_admin.close()
    finally:
        release_callback.set()
        if primary is not None:
            primary.stop()
        if replacement is not None:
            replacement.stop()
            replacement.disconnect()
        if primary_thread is not None:
            primary_thread.join(timeout=10)
        if primary is not None:
            primary.disconnect()
        if stopped_node is not None:
            run_compose("start", stopped_node)
        producer.close()
        admin.close()
        elastic.close()

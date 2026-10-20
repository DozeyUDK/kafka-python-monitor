"""Real process SIGTERM and Kafka resume integration checks."""
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import uuid

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.getenv("KAFKA_INTEGRATION") != "1",
                       reason="Requires disposable integration services"),
]

ROOT = Path(__file__).resolve().parents[2]


def wait_for_count(es, topic, expected, timeout=75):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        es.indices.refresh(index="kafka-events-*", ignore_unavailable=True)
        count = es.count(index="kafka-events-*", query={"term": {"kafka_topic": topic}})["count"]
        if count >= expected:
            return count
        time.sleep(0.5)
    raise AssertionError("Expected %d indexed documents for %s" % (expected, topic))


def stop_process(proc):
    if proc.poll() is None:
        proc.send_signal(signal.SIGTERM)
    try:
        return proc.wait(timeout=30)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=10)
        raise AssertionError("SIGTERM did not stop the monitor in 30 seconds")


def test_real_process_sigterm_and_restart_reuses_committed_offsets(monkeypatch):
    from elasticsearch import Elasticsearch
    from kafka import KafkaAdminClient, KafkaProducer
    from kafka.admin import NewTopic

    broker = os.getenv("KAFKA_TEST_BROKER", "localhost:29092")
    elastic_url = os.getenv("ELASTICSEARCH_TEST_URL", "http://localhost:19200")
    assert broker == "localhost:29092" and elastic_url == "http://localhost:19200"
    topic = "kafka-monitor-signal-" + uuid.uuid4().hex[:12]
    group = "monitor-signal-" + uuid.uuid4().hex[:12]
    admin = KafkaAdminClient(bootstrap_servers=broker)
    producer = KafkaProducer(bootstrap_servers=broker, acks="all")
    es = Elasticsearch(elastic_url)
    env = dict(os.environ)
    env.update({
        "KAFKA_BOOTSTRAP_SERVERS": broker,
        "KAFKA_TOPICS": topic,
        "KAFKA_GROUP_ID": group,
        "KAFKA_AUTO_OFFSET_RESET": "earliest",
        "KAFKA_BATCH_SIZE": "25",
        "MONITOR_INTERVAL_SECONDS": "1",
        "METRICS_INTERVAL_SECONDS": "60",
        "ELASTICSEARCH_ENABLED": "true",
        "ELASTICSEARCH_HOSTS": elastic_url,
        "HEALTH_CHECK_PORT": "0",
    })
    process = None
    try:
        admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])

        def publish(start, count):
            for i in range(start, start + count):
                producer.send(topic, value=("event-%d" % i).encode()).get(timeout=15)

        publish(0, 8)
        process = subprocess.Popen([sys.executable, "program.py"], cwd=ROOT,
                                   env=env, stdout=subprocess.DEVNULL,
                                   stderr=subprocess.DEVNULL)
        assert wait_for_count(es, topic, 8) == 8
        assert stop_process(process) == 0

        publish(8, 5)
        process = subprocess.Popen([sys.executable, "program.py"], cwd=ROOT,
                                   env=env, stdout=subprocess.DEVNULL,
                                   stderr=subprocess.DEVNULL)
        assert wait_for_count(es, topic, 13) == 13
        assert stop_process(process) == 0
    finally:
        if process is not None and process.poll() is None:
            stop_process(process)
        es.close()
        producer.close()
        admin.close()

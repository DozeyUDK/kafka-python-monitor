"""Regression coverage for explicit offset acknowledgements and readiness."""
import time
from types import SimpleNamespace

import pytest
from kafka.structs import OffsetAndMetadata, TopicPartition

from src.kafka_monitor import KafkaMonitor


@pytest.fixture
def monitor(monkeypatch):
    monkeypatch.setenv("ELASTICSEARCH_ENABLED", "false")
    monkeypatch.setenv("KAFKA_TOPICS", "orders")
    monkeypatch.setenv("KAFKA_POLL_STALE_SECONDS", "120")
    return KafkaMonitor()


def kafka_record(tp, offset):
    return SimpleNamespace(
        topic=tp.topic, partition=tp.partition, offset=offset, key=None,
        value="message", timestamp=1735689600000
    )


class CapturingConsumer:
    def __init__(self):
        self.calls = []
        self.fail = False

    def commit(self, offsets=None):
        if offsets is None:
            raise AssertionError("Implicit consumer position commit is forbidden")
        if self.fail:
            raise ConnectionError("group lost ownership during commit")
        self.calls.append(dict(offsets))


def test_explicit_commit_only_for_partitions_with_acked_records(monitor):
    orders = TopicPartition("orders", 0)
    payments = TopicPartition("payments", 1)
    idle = TopicPartition("orders", 2)
    consumer = CapturingConsumer()
    monitor.consumer = consumer

    poll_batch = {
        orders: [kafka_record(orders, 5), kafka_record(orders, 6)],
        payments: [kafka_record(payments, 100)],
        idle: [],
    }
    monitor._commit_processed_offsets(poll_batch)

    assert consumer.calls == [{
        orders: OffsetAndMetadata(7, "", -1),
        payments: OffsetAndMetadata(101, "", -1)
    }]
    assert idle not in consumer.calls[0]
    assert monitor.get_stats()["last_processed_commit_age_seconds"] is not None


def test_empty_poll_does_not_acknowledge_any_partition(monitor):
    consumer = CapturingConsumer()
    monitor.consumer = consumer
    monitor._commit_processed_offsets({})
    monitor._commit_processed_offsets({TopicPartition("orders", 0): []})
    assert consumer.calls == []
    assert monitor.get_stats()["last_processed_commit_age_seconds"] is None


def test_failed_commit_not_marked_as_acknowledged(monitor):
    consumer = CapturingConsumer()
    consumer.fail = True
    monitor.consumer = consumer
    tp = TopicPartition("orders", 0)
    with pytest.raises(ConnectionError, match="lost ownership"):
        monitor._commit_processed_offsets({tp: [kafka_record(tp, 0)]})
    assert consumer.calls == []
    assert monitor.get_stats()["last_processed_commit_age_seconds"] is None


def test_readiness_requires_initial_successful_poll(monitor):
    monitor.consumer = object()
    monitor.is_running = True
    health = monitor.health_check()
    assert not health["healthy"]
    assert "Kafka consumer has not completed a poll" in health["issues"]


def test_empty_but_successful_poll_is_healthy(monitor):
    monitor.consumer = object()
    monitor.is_running = True
    monitor._last_successful_poll = time.monotonic()
    assert monitor.health_check()["healthy"] is True
    assert monitor.get_stats()["poll_age_seconds"] is not None


def test_stale_poll_fails_readiness_but_not_liveness(monitor):
    monitor.consumer = object()
    monitor.is_running = True
    monitor._last_successful_poll = time.monotonic() - 150
    health = monitor.health_check()
    assert health["healthy"] is False
    assert "Kafka consumer polling stalled" in health["issues"]


def test_reconnect_resets_poll_freshness(monkeypatch, monitor):
    monitor._last_successful_poll = time.monotonic()
    monkeypatch.setattr("src.kafka_monitor.KafkaConsumer", lambda *args, **kwargs: object())
    assert monitor._connect_with_retry()
    assert monitor._last_successful_poll is None


def test_invalid_stale_threshold_rejected(monkeypatch):
    monkeypatch.setenv("ELASTICSEARCH_ENABLED", "false")
    monkeypatch.setenv("KAFKA_POLL_STALE_SECONDS", "0")
    assert not KafkaMonitor().validate_config()

"""Regression tests for new-group offsets and restart replay.

The in-memory consumer models the key broker rule: if no committed offset
exists, a restarting latest group starts at the current log end.
"""
from collections import defaultdict
from types import SimpleNamespace

import pytest
from kafka.structs import OffsetAndMetadata, TopicPartition

from src.kafka_monitor import KafkaMonitor


def kafka_record(tp, offset):
    return SimpleNamespace(
        topic=tp.topic, partition=tp.partition, offset=offset,
        key=b"k", value="value-%d" % offset, timestamp=1735689600000,
    )


class TrackingConsumer:
    def __init__(self, monitor, records, group_offsets=None, fail_checkpoint=False,
                 latest_restart=False, positions=None):
        self.monitor = monitor
        self.records = records
        self.group_offsets = group_offsets if group_offsets is not None else {}
        self.fail_checkpoint = fail_checkpoint
        self.latest_restart = latest_restart
        self.positions = positions or {}
        self.calls = []
        self.poll_number = 0
        self.last_poll = {}

    def assignment(self):
        return set(self.records) | set(self.positions)

    def committed(self, tp):
        return self.group_offsets.get(tp)

    def position(self, tp):
        return self.positions[tp]

    def poll(self, **kwargs):
        self.poll_number += 1
        if self.poll_number > 1:
            self.monitor.stop()
            return {}
        # On restart a group with no saved offset is reset to the new log end.
        self.last_poll = {
            tp: [
                m for m in records
                if m.offset >= self.group_offsets.get(
                    tp, float("inf") if self.latest_restart else -1
                )
            ]
            for tp, records in self.records.items()
        }
        self.calls.append(("poll", None))
        return self.last_poll

    def commit(self, offsets=None):
        if offsets is not None:
            self.calls.append(("checkpoint", dict(offsets)))
            if self.fail_checkpoint:
                raise ConnectionError("checkpoint not acknowledged")
            for tp, value in offsets.items():
                assert isinstance(value, OffsetAndMetadata)
                self.group_offsets[tp] = value.offset
        else:
            self.calls.append(("ack", None))
            for tp, records in self.last_poll.items():
                if records:
                    self.group_offsets[tp] = records[-1].offset + 1

    def close(self):
        pass


class RecordingExporter:
    enabled = True
    client = object()

    def __init__(self, fail_after=None):
        self.fail_after = fail_after
        self.persisted = defaultdict(set)
        self.calls = []

    def export_events(self, events):
        self.calls.append("export")
        for e in events:
            self.persisted[(e["topic"], e["partition"])].add(e["offset"])
            if self.fail_after == e["offset"]:
                raise ConnectionError("partial bulk failure")

    def export_metrics(self, stats):
        pass


def monitor_without_broker(monkeypatch):
    monkeypatch.setenv("ELASTICSEARCH_ENABLED", "false")
    monkeypatch.setenv("KAFKA_AUTO_OFFSET_RESET", "latest")
    return KafkaMonitor()


def test_initial_position_committed_before_first_bulk_and_replayed_after_restart(monkeypatch):
    tp = TopicPartition("orders", 0)
    shared_offsets = {}
    records = {tp: [kafka_record(tp, 10), kafka_record(tp, 11)]}
    exporter = RecordingExporter(fail_after=10)

    first = monitor_without_broker(monkeypatch)
    consumer = TrackingConsumer(first, records, group_offsets=shared_offsets)
    first.consumer, first.es_exporter = consumer, exporter

    with pytest.raises(ConnectionError, match="partial bulk failure"):
        first.monitor()

    assert consumer.calls[0][0] == "poll"
    assert consumer.calls[1] == ("checkpoint", {tp: OffsetAndMetadata(10, "", -1)})
    assert "ack" not in [name for name, _ in consumer.calls]
    assert shared_offsets == {tp: 10}
    assert exporter.persisted[("orders", 0)] == {10}

    # Model exactly the failure reported by the review: latest would skip all
    # records if initial offset hadn't been durably persisted.
    second = monitor_without_broker(monkeypatch)
    second_consumer = TrackingConsumer(
        second, records, group_offsets=shared_offsets, latest_restart=True
    )
    exporter.fail_after = None
    second.consumer, second.es_exporter = second_consumer, exporter
    second.monitor()

    assert exporter.persisted[("orders", 0)] == {10, 11}
    assert shared_offsets == {tp: 12}
    assert [action for action, _ in second_consumer.calls] == ["poll", "ack"]


def test_initial_checkpoint_never_commits_advanced_poll_positions(monkeypatch):
    a = TopicPartition("orders", 0)
    b = TopicPartition("payments", 1)
    records = {a: [kafka_record(a, 7), kafka_record(a, 8)],
               b: [kafka_record(b, 20), kafka_record(b, 21)]}
    monitor = monitor_without_broker(monkeypatch)
    exporter = RecordingExporter(fail_after=8)
    consumer = TrackingConsumer(monitor, records)
    monitor.consumer, monitor.es_exporter = consumer, exporter

    with pytest.raises(ConnectionError, match="partial bulk failure"):
        monitor.monitor()

    assert consumer.calls[1] == (
        "checkpoint", {a: OffsetAndMetadata(7, "", -1), b: OffsetAndMetadata(20, "", -1)}
    )
    assert "ack" not in [name for name, _ in consumer.calls]


def test_only_uncommitted_partitions_are_initially_checkpointed(monkeypatch):
    old = TopicPartition("orders", 0)
    added = TopicPartition("orders", 1)
    offsets = {old: 40}
    records = {old: [kafka_record(old, 40)], added: [kafka_record(added, 90)]}
    monitor = monitor_without_broker(monkeypatch)
    consumer = TrackingConsumer(monitor, records, group_offsets=offsets)
    monitor.consumer, monitor.es_exporter = consumer, RecordingExporter()
    monitor.monitor()

    assert consumer.calls[1] == ("checkpoint", {added: OffsetAndMetadata(90, "", -1)})
    assert consumer.calls[2] == ("ack", None)
    assert offsets == {old: 41, added: 91}


def test_idle_assignment_is_checkpointed_before_other_partition_processed(monkeypatch):
    busy = TopicPartition("events", 0)
    idle = TopicPartition("events", 1)
    records = {busy: [kafka_record(busy, 12)], idle: []}
    monitor = monitor_without_broker(monkeypatch)
    consumer = TrackingConsumer(monitor, records, positions={idle: 300})
    monitor.consumer, monitor.es_exporter = consumer, RecordingExporter(fail_after=12)

    with pytest.raises(ConnectionError):
        monitor.monitor()

    assert consumer.group_offsets == {busy: 12, idle: 300}
    assert "ack" not in [name for name, _ in consumer.calls]


def test_initial_checkpoint_failure_aborts_without_processing(monkeypatch):
    tp = TopicPartition("events", 0)
    monitor = monitor_without_broker(monkeypatch)
    consumer = TrackingConsumer(monitor, {tp: [kafka_record(tp, 1)]},
                                fail_checkpoint=True)
    exporter = RecordingExporter()
    monitor.consumer, monitor.es_exporter = consumer, exporter

    with pytest.raises(ConnectionError, match="checkpoint not acknowledged"):
        monitor.monitor()

    assert not exporter.calls
    assert not consumer.group_offsets
    assert "ack" not in [name for name, _ in consumer.calls]


def test_checkpoint_initial_start_of_idle_group(monkeypatch):
    idle = TopicPartition("events", 1)
    monitor = monitor_without_broker(monkeypatch)
    consumer = TrackingConsumer(monitor, {idle: []}, positions={idle: 22})
    monitor.consumer, monitor.es_exporter = consumer, RecordingExporter()
    monitor.monitor()

    assert consumer.group_offsets[idle] == 22
    assert [kind for kind, _ in consumer.calls] == ["poll", "checkpoint"]

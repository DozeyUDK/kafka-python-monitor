"""Rebalance / acknowledgement recovery at the consumer boundary."""
from types import SimpleNamespace

import pytest
from kafka.structs import OffsetAndMetadata, TopicPartition

from src.kafka_monitor import KafkaMonitor


class RebalanceDuringCommit(Exception):
    pass


class RestartableConsumer:
    def __init__(self, monitor, group_offsets, fail_commit=False):
        self.monitor = monitor
        self.tp = TopicPartition("events", 0)
        self.group_offsets = group_offsets
        self.fail_commit = fail_commit
        self.commits = []
        self.poll_count = 0

    def assignment(self):
        return {self.tp}

    def committed(self, tp):
        return self.group_offsets.get(tp)

    def poll(self, **kwargs):
        self.poll_count += 1
        if self.poll_count > 1:
            self.monitor.stop()
            return {}
        offset = self.group_offsets.get(self.tp, 0)
        return {self.tp: [
            SimpleNamespace(topic="events", partition=0, offset=i, value="payload",
                            key=None, timestamp=1735689600000)
            for i in range(offset, 2)
        ]}

    def commit(self, offsets=None):
        if offsets is None:
            raise AssertionError("Implicit commit forbidden")
        if self.fail_commit and any(entry.offset > 0 for entry in offsets.values()):
            raise RebalanceDuringCommit("rebalance invalidated consumer generation")
        self.commits.append(dict(offsets))
        for tp, value in offsets.items():
            self.group_offsets[tp] = value.offset


class IdempotentExport:
    enabled = True
    client = object()

    def __init__(self):
        self.attempted = 0
        self.documents = set()

    def export_events(self, events):
        for event in events:
            self.attempted += 1
            self.documents.add((event["topic"], event["partition"], event["offset"]))

    def export_metrics(self, stats):
        pass


def new_monitor(monkeypatch, client, group_offsets, fail_commit):
    monkeypatch.setenv("ELASTICSEARCH_ENABLED", "false")
    monkeypatch.setenv("KAFKA_AUTO_OFFSET_RESET", "earliest")
    monitor = KafkaMonitor()
    monitor.consumer = RestartableConsumer(monitor, group_offsets, fail_commit)
    monitor.es_exporter = client
    return monitor


def test_rebalance_during_processed_commit_fails_closed_and_replays(monkeypatch):
    offsets = {TopicPartition("events", 0): 0}
    es = IdempotentExport()
    first = new_monitor(monkeypatch, es, offsets, fail_commit=True)
    with pytest.raises(RebalanceDuringCommit):
        first.monitor()

    assert es.documents == {("events", 0, 0), ("events", 0, 1)}
    assert offsets == {TopicPartition("events", 0): 0}
    assert first._fatal_error == "RebalanceDuringCommit"

    second = new_monitor(monkeypatch, es, offsets, fail_commit=False)
    second.monitor()

    assert es.documents == {("events", 0, 0), ("events", 0, 1)}
    assert es.attempted == 4  # Both records replayed; deterministic IDs avoid duplicates.
    assert offsets == {TopicPartition("events", 0): 2}
    assert second.consumer.commits == [
        {TopicPartition("events", 0): OffsetAndMetadata(2, "", -1)}
    ]

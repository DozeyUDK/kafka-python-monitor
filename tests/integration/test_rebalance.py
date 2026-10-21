"""Rebalance/replay with an observable Kafka coordinator-state barrier.

A single-broker fixture can verify consumer-group reassignment and recovery,
not broker leader/controller failover. The processed-offset commit rejection
is deliberately injected *after* observing the real group rebalance because
whether Kafka itself rejects that exact request is timing-dependent.
"""
import os
import threading
import time
import uuid

import pytest

from test_pipeline import clients, kafka_topic, send, event_count, new_monitor

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("KAFKA_INTEGRATION") != "1",
        reason="Requires disposable Kafka/Elasticsearch services",
    ),
]


def wait_for_coordinator_rebalance(admin, group_id, *, timeout=20):
    """Only return once the broker reports a genuine pending group rebalance."""
    deadline = time.monotonic() + timeout
    states = []
    while time.monotonic() < deadline:
        descriptions = admin.describe_consumer_groups([group_id])
        assert len(descriptions) == 1
        state = descriptions[0].state
        states.append(state)
        if state == "PreparingRebalance":
            return states
        time.sleep(0.2)
    pytest.fail(
        "Kafka never entered PreparingRebalance with the peer joining; "
        "observed states: %s" % states
    )


def test_two_consumers_rebalance_and_recover_without_missing_documents(
    clients, kafka_topic, monkeypatch
):
    from kafka import KafkaConsumer
    from kafka.errors import RebalanceInProgressError
    from kafka.structs import TopicPartition

    broker = os.getenv("KAFKA_TEST_BROKER", "localhost:29092")
    admin, producer, es = clients
    group = "monitor-rebalance-ci-" + uuid.uuid4().hex
    count = 12
    send(producer, kafka_topic, 0, count // 2, prefix="rb0")
    send(producer, kafka_topic, 1, count // 2, prefix="rb1")

    monkeypatch.setenv("KAFKA_BATCH_SIZE", "1")
    monitor = new_monitor(monkeypatch, kafka_topic, group, reset="earliest")
    first_seen = threading.Event()
    continue_processing = threading.Event()
    commit_rejected = threading.Event()
    first_records = []
    primary_failures = []
    peer_errors = []
    peer_joined = threading.Event()
    peer_thread = None
    monitor_closed = False
    competitor = None

    def callback(event):
        first_records.append(event)
        first_seen.set()
        if not continue_processing.wait(40):
            raise TimeoutError("No coordinator rebalance observed")
        monitor.stop()  # Export finishes before the processed-offset commit.

    def primary_worker():
        try:
            monitor.monitor(callback=callback)
        except Exception as exc:
            primary_failures.append(exc)

    thread = threading.Thread(target=primary_worker, daemon=True)
    try:
        thread.start()
        assert first_seen.wait(20), "First consumer never received a Kafka event"
        assert len(first_records) == 1
        first = first_records[0]
        tp = TopicPartition(first["topic"], first["partition"])
        # No processed message has been acknowledged before the callback.
        before = admin.list_consumer_group_offsets(group)
        assert before[tp].offset == first["offset"]

        competitor = KafkaConsumer(
            kafka_topic, bootstrap_servers=broker, group_id=group,
            auto_offset_reset="earliest", enable_auto_commit=False,
            consumer_timeout_ms=20000,
        )

        def peer_worker():
            try:
                for _ in range(40):
                    competitor.poll(timeout_ms=400, max_records=1)
                    if competitor.assignment():
                        peer_joined.set()
                        return
            except Exception as exc:
                peer_errors.append(exc)

        peer_thread = threading.Thread(target=peer_worker, daemon=True)
        peer_thread.start()

        # A deterministic barrier: the broker's coordinator must have begun
        # rebalancing BEFORE callback processing resumes. A sleep or a thread
        # start signal cannot establish this ordering.
        observed_states = wait_for_coordinator_rebalance(admin, group)
        assert "PreparingRebalance" in observed_states

        # Broker-side commit acceptance during PreparingRebalance varies with
        # race timing and group generation. Inject a commit rejection only for
        # the *processed* offset, leaving genuine initial checkpoints intact.
        original_commit = monitor.consumer.commit

        def commit_with_generation_rejection(offsets=None):
            assert offsets is not None
            if any(meta.offset > before[partition].offset
                   for partition, meta in offsets.items() if partition in before):
                commit_rejected.set()
                raise RebalanceInProgressError("injected in-flight processed commit rejection")
            return original_commit(offsets=offsets)

        monitor.consumer.commit = commit_with_generation_rejection
        continue_processing.set()
        thread.join(timeout=20)
        assert not thread.is_alive(), "Primary did not stop on commit failure"
        assert commit_rejected.is_set(), "Processed-offset commit rejection was not exercised"
        assert len(primary_failures) == 1
        assert isinstance(primary_failures[0], RebalanceInProgressError)
        assert monitor._fatal_error == "RebalanceInProgressError"

        # Explicit LeaveGroup before waiting for the peer's assignment;
        # otherwise Kafka may defer group completion for max.poll.interval.
        monitor.disconnect()
        monitor_closed = True
        assert event_count(es, kafka_topic) == 1  # Indexed, not yet committed.
        peer_thread.join(timeout=20)
        assert peer_joined.is_set(), "Peer did not receive partitions after LeaveGroup"
        assert not peer_errors
    finally:
        continue_processing.set()
        monitor.stop()
        thread.join(timeout=5)
        if not monitor_closed:
            monitor.disconnect()
        if competitor is not None:
            if peer_thread is not None:
                peer_thread.join(timeout=5)
            competitor.close()

    # The committed group offset must still point to the exported first
    # record. It has to be replayed, not silently acknowledged.
    persisted = admin.list_consumer_group_offsets(group)
    assert persisted[tp].offset == first["offset"]

    replacement = new_monitor(monkeypatch, kafka_topic, group, reset="earliest")
    monitor_done = threading.Event()
    secondary_failures = []
    replayed = []

    def after_restart(event):
        replayed.append((event["topic"], event["partition"], event["offset"]))

    def replacement_worker():
        try:
            replacement.monitor(callback=after_restart)
        except Exception as exc:
            secondary_failures.append(exc)
        finally:
            monitor_done.set()

    secondary_thread = threading.Thread(target=replacement_worker, daemon=True)
    try:
        secondary_thread.start()
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if event_count(es, kafka_topic) == count:
                break
            time.sleep(0.5)
        assert event_count(es, kafka_topic) == count, "Missing indexed records after replay"
        assert (first["topic"], first["partition"], first["offset"]) in replayed, (
            "Already-exported-but-uncommitted record was not replayed"
        )
        replacement.stop()
        assert monitor_done.wait(10), "Recovery consumer did not stop"
        assert not secondary_failures, "Recovery consumer failed: %r" % secondary_failures
        final = admin.list_consumer_group_offsets(group)
        for partition in (0, 1):
            assert final[TopicPartition(kafka_topic, partition)].offset == count // 2
    finally:
        replacement.stop()
        secondary_thread.join(timeout=10)
        replacement.disconnect()

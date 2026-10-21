"""Exercise a *real* Kafka group rebalance while an Elasticsearch export is pending.

The single-broker CI fixture can validate consumer generation/replay semantics.
It cannot establish resilience to an actual broker leader failure.
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


def test_two_consumers_rebalance_and_recover_without_missing_documents(
    clients, kafka_topic, monkeypatch
):
    from kafka import KafkaConsumer

    broker = os.getenv("KAFKA_TEST_BROKER", "localhost:29092")
    _, producer, es = clients
    group = "monitor-rebalance-ci-" + uuid.uuid4().hex
    count = 12
    send(producer, kafka_topic, 0, count // 2, prefix="rb0")
    send(producer, kafka_topic, 1, count // 2, prefix="rb1")

    monkeypatch.setenv("KAFKA_BATCH_SIZE", "1")
    monitor = new_monitor(monkeypatch, kafka_topic, group, reset="earliest")
    first_seen = threading.Event()
    continue_processing = threading.Event()
    primary_failures = []
    peer_thread = None
    monitor_closed = False
    peer_join_started = threading.Event()
    peer_joined = threading.Event()
    peer_errors = []

    def callback(event):
        first_seen.set()
        if not continue_processing.wait(20):
            raise TimeoutError("Timed out waiting for second consumer to join")
        monitor.stop()  # First record still must be exported before acknowledgement.

    def primary_worker():
        try:
            monitor.monitor(callback=callback)
        except Exception as exc:
            # A generation change can invalidate the in-flight commit.
            primary_failures.append(exc)

    thread = threading.Thread(target=primary_worker, daemon=True)
    competitor = None
    replacement = None
    secondary_thread = None
    try:
        thread.start()
        assert first_seen.wait(20), "First consumer never received a Kafka event"

        competitor = KafkaConsumer(
            kafka_topic, bootstrap_servers=broker, group_id=group,
            auto_offset_reset="earliest", enable_auto_commit=False,
            consumer_timeout_ms=20000,
        )
        # Join concurrently: Kafka may wait for the first member's next poll
        # before finishing a rebalance. Waiting for assignment while first
        # member is held in a callback would deadlock the test.
        def peer_worker():
            peer_join_started.set()
            try:
                for _ in range(40):
                    competitor.poll(timeout_ms=400, max_records=1)
                    if competitor.assignment():
                        peer_joined.set()
                        break
            except Exception as exc:
                peer_errors.append(exc)

        peer_thread = threading.Thread(target=peer_worker, daemon=True)
        peer_thread.start()
        assert peer_join_started.wait(10)
        time.sleep(0.5)
        continue_processing.set()
        thread.join(timeout=20)
        assert not thread.is_alive()
        # LeaveGroup must happen *before* awaiting the peer assignment.
        # Otherwise the original member may block the rebalance for Kafka's
        # default max.poll.interval.ms (300s) even after monitor() stops.
        monitor.disconnect()
        monitor_closed = True
        peer_thread.join(timeout=20)
        assert peer_joined.is_set(), "Second consumer never completed group rebalance"
        assert not peer_errors
    finally:
        continue_processing.set()
        if competitor is not None:
            competitor.close()
        if peer_thread is not None:
            peer_thread.join(timeout=5)
        monitor.stop()
        thread.join(timeout=5)
        if not monitor_closed:
            monitor.disconnect()

    # After all generation members depart, replacement joins the same group.
    # It must index every remaining unacknowledged Kafka offset, including a
    # possible replay of the already-indexed-but-not-committed first record.
    replacement = new_monitor(monkeypatch, kafka_topic, group, reset="earliest")
    monitor_done = threading.Event()
    secondary_failures = []

    def replacement_worker():
        try:
            replacement.monitor()
        except Exception as exc:
            secondary_failures.append(exc)
        finally:
            monitor_done.set()

    secondary_thread = threading.Thread(target=replacement_worker, daemon=True)
    try:
        secondary_thread.start()
        deadline = time.monotonic() + 30
        last_count = 0
        while time.monotonic() < deadline:
            last_count = event_count(es, kafka_topic)
            if last_count == count:
                break
            time.sleep(0.5)
        assert last_count == count, (
            "Rebalance/restart lost indexed Kafka messages: %s/%s" % (last_count, count)
        )
        replacement.stop()
        assert monitor_done.wait(10), "Recovery consumer did not shut down"
        assert not secondary_failures, "Recovery consumer failed: %r" % secondary_failures
    finally:
        replacement.stop()
        secondary_thread.join(timeout=10)
        replacement.disconnect()

"""Unit regression tests; no real Kafka or Elasticsearch required."""
from types import SimpleNamespace

import pytest

from src.elasticsearch_exporter import ElasticsearchExporter
from src.kafka_monitor import KafkaMonitor, Metrics


class FakeElasticsearch:
    def __init__(self, healthy=True):
        self.healthy = healthy
        self.closed = False

    def ping(self):
        return self.healthy

    def close(self):
        self.closed = True


class FakeConsumer:
    def __init__(self, monitor, records, actions):
        self.monitor = monitor
        self.records = records
        self.actions = actions
        self.read_count = 0

    def poll(self, **kwargs):
        assert kwargs["max_records"] == self.monitor.batch_size
        self.read_count += 1
        if self.read_count == 1:
            return {"partition": self.records}
        self.monitor.stop()
        return {}

    def commit(self):
        self.actions.append("commit")

    def close(self):
        self.actions.append("close")


def event(offset=7, topic="foo", partition=0):
    return {
        "topic": topic, "partition": partition, "offset": offset,
        "key": "key", "value": "payload", "timestamp": 1735689600000,
    }


def make_monitor(monkeypatch):
    monkeypatch.setenv("ELASTICSEARCH_ENABLED", "false")
    monkeypatch.setenv("KAFKA_TOPICS", "foo")
    return KafkaMonitor()


def test_config_disables_auto_commit_and_supports_tls(monkeypatch):
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SSL")
    monkeypatch.setenv("KAFKA_SSL_CAFILE", "/ca.pem")
    monitor = make_monitor(monkeypatch)
    config = monitor._get_consumer_config()
    assert config["enable_auto_commit"] is False
    assert config["security_protocol"] == "SSL"
    assert config["ssl_cafile"] == "/ca.pem"
    assert "ssl_context" not in config


def test_bad_security_config_is_rejected(monkeypatch):
    monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    monkeypatch.delenv("KAFKA_SASL_USERNAME", raising=False)
    monkeypatch.delenv("KAFKA_SASL_PASSWORD", raising=False)
    monitor = make_monitor(monkeypatch)
    assert not monitor.validate_config()


def test_metrics_lag_separates_topics_and_partitions():
    stats = Metrics()
    stats.update_lag("orders", 0, 5)
    stats.update_lag("payments", 0, 10)
    stats.update_lag("orders", 1, 3)
    assert stats.get_stats()["lag_by_partition"] == {
        "orders:0": 5, "payments:0": 10, "orders:1": 3,
    }


def test_deterministic_id_and_index_on_replay():
    exporter = object.__new__(ElasticsearchExporter)
    exporter.events_index_prefix = "events"
    exporter.index_date_format = "%Y.%m.%d"
    first = exporter._event_action(event())
    second = exporter._event_action(event())
    assert first["_id"] == second["_id"]
    assert first["_index"] == "events-2025.01.01"
    assert exporter._event_action(event(offset=8))["_id"] != first["_id"]


def test_unknown_timestamp_uses_stable_index():
    exporter = object.__new__(ElasticsearchExporter)
    exporter.events_index_prefix = "events"
    exporter.index_date_format = "%Y.%m.%d"
    payload = event()
    payload["timestamp"] = None
    assert exporter._event_action(payload)["_index"] == "events-1970.01.01"


def test_elasticsearch_unavailable_fails_initialization(monkeypatch):
    monkeypatch.setenv("ELASTICSEARCH_ENABLED", "true")
    with pytest.raises(ConnectionError):
        ElasticsearchExporter(client=FakeElasticsearch(healthy=False))


def test_elasticsearch_bulk_exception_propagates(monkeypatch):
    monkeypatch.setenv("ELASTICSEARCH_ENABLED", "true")
    exporter = ElasticsearchExporter(client=FakeElasticsearch())
    def failed_bulk(*args, **kwargs):
        raise ConnectionError("rejected")
    monkeypatch.setattr("src.elasticsearch_exporter.bulk", failed_bulk)
    with pytest.raises(ConnectionError, match="rejected"):
        exporter.export_events([event()])


def test_successful_bulk_precedes_offset_commit(monkeypatch):
    monitor = make_monitor(monkeypatch)
    actions = []
    messages = [
        SimpleNamespace(topic="foo", partition=0, offset=7,
                        key=b"k", value="hello", timestamp=1735689600000)
    ]
    monitor.consumer = FakeConsumer(monitor, messages, actions)
    class Exporter:
        enabled = True
        client = object()
        def export_events(self, records):
            assert records[0]["value"] == "hello"
            actions.append("export")
        def export_metrics(self, metrics):
            pass
    monitor.es_exporter = Exporter()
    monitor.monitor()
    assert actions == ["export", "commit"]
    assert monitor.metrics.get_stats()["events_processed"] == 1


def test_failed_elasticsearch_bulk_never_commits(monkeypatch):
    monitor = make_monitor(monkeypatch)
    actions = []
    monitor.consumer = FakeConsumer(
        monitor,
        [SimpleNamespace(topic="foo", partition=0, offset=2,
                         key=b"k", value="hello", timestamp=1735689600000)],
        actions,
    )
    class Exporter:
        enabled = True
        client = object()
        def export_events(self, records):
            actions.append("export-failed")
            raise ConnectionError("bulk failure")
    monitor.es_exporter = Exporter()
    with pytest.raises(ConnectionError):
        monitor.monitor()
    assert actions == ["export-failed"]
    assert monitor.is_running is False
    assert monitor.health_check()["healthy"] is False


def test_failed_custom_callback_stops_without_dlq(monkeypatch):
    monitor = make_monitor(monkeypatch)
    def bad_callback(e):
        raise RuntimeError("side effect failure")
    with pytest.raises(RuntimeError, match="side effect failure"):
        monitor._process_batch([event()], callback=bad_callback)
    assert monitor.metrics.get_stats()["events_processed"] == 0


def test_batch_callback_failure_stops_without_ack(monkeypatch):
    monitor = make_monitor(monkeypatch)
    def bad_batch(records):
        raise RuntimeError("batch failure")
    with pytest.raises(RuntimeError, match="batch failure"):
        monitor._process_batch([event()], batch_callback=bad_batch)
    assert monitor.metrics.get_stats()["events_processed"] == 0


def test_dlq_publish_requires_broker_ack(monkeypatch):
    monitor = make_monitor(monkeypatch)
    monitor.dlq_enabled = True
    class FailedFuture:
        def get(self, **kwargs):
            raise TimeoutError("no broker ack")
    class Producer:
        def send(self, *args, **kwargs):
            return FailedFuture()
    monitor.dlq_producer = Producer()
    def bad_callback(e):
        raise RuntimeError("callback failed")
    with pytest.raises(TimeoutError, match="no broker ack"):
        monitor._process_batch([event()], callback=bad_callback)
    assert monitor.metrics.get_stats()["events_dlq"] == 0


def test_elasticsearch_client_configuration_uses_url_scheme_and_api_key(monkeypatch):
    monkeypatch.setenv("ELASTICSEARCH_ENABLED", "true")
    monkeypatch.setenv("ELASTICSEARCH_HOSTS", "one:9200,https://two:9200")
    monkeypatch.setenv("ELASTICSEARCH_USE_SSL", "true")
    monkeypatch.setenv("ELASTICSEARCH_API_KEY", "placeholder")
    supplied = {}
    def make_client(**kwargs):
        supplied.update(kwargs)
        return FakeElasticsearch()
    monkeypatch.setattr("src.elasticsearch_exporter.Elasticsearch", make_client)
    ElasticsearchExporter()
    assert supplied["hosts"] == ["https://one:9200", "https://two:9200"]
    assert supplied["verify_certs"] is True
    assert supplied["api_key"] == "placeholder"
    assert "use_ssl" not in supplied
    assert "http_auth" not in supplied

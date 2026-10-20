"""Small tests for the Prometheus HTTP exposition formatter."""
from src.health_server import HealthHandler


def test_prometheus_metric_labels_are_escaped():
    handler = HealthHandler.__new__(HealthHandler)
    stats = {
        "events_processed": 1,
        "events_by_topic": {'quoted"name': 3},
        "errors_by_type": {"bad": 1},
        "lag_by_partition": {"orders:0": 7, "payments:0": 2},
    }
    result = handler._format_prometheus_metrics(stats)
    assert 'topic="quoted\\"name"' in result
    assert 'kafka_monitor_consumer_lag{topic="orders",partition="0"} 7' in result
    assert 'kafka_monitor_consumer_lag{topic="payments",partition="0"} 2' in result

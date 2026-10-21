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

def test_prometheus_poll_and_commit_age_metrics():
    handler = HealthHandler.__new__(HealthHandler)
    present = handler._format_prometheus_metrics({
        "poll_age_seconds": 4.5,
        "last_processed_commit_age_seconds": 18.25,
    })
    assert "kafka_monitor_poll_age_seconds 4.5" in present
    assert "kafka_monitor_processed_commit_age_seconds 18.25" in present

    missing = handler._format_prometheus_metrics({
        "poll_age_seconds": None,
        "last_processed_commit_age_seconds": None,
    })
    assert "# TYPE kafka_monitor_poll_age_seconds gauge" in missing
    assert "kafka_monitor_poll_age_seconds 0" not in missing
    assert "kafka_monitor_processed_commit_age_seconds 0" not in missing

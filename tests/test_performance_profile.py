"""Fast configuration/budget tests; no Kafka, Elasticsearch or Docker required."""
import pytest

from ops.performance_profile import LoadProfile


def profile(**kwargs):
    return LoadProfile.from_env(kwargs)


def test_profile_defaults_are_bounded_and_have_no_runner_speed_slo():
    p = profile()
    assert (p.events, p.duration_seconds, p.partitions) == (600, 10, 2)
    assert p.min_events_per_second == 0
    assert p.max_p95_seconds == 0


@pytest.mark.parametrize("bad", [
    {"KAFKA_PERF_EVENTS": "21000"},
    {"KAFKA_PERF_EVENTS": "-1"},
    {"KAFKA_PERF_DURATION_SECONDS": "301"},
    {"KAFKA_PERF_PARTITIONS": "5"},
    {"KAFKA_PERF_BYTES": "999999999"},
    {"KAFKA_PERF_BATCH_SIZE": "0"},
    {"KAFKA_PERF_MAX_RSS_MIB": "0"},
    {"KAFKA_PERF_MIN_EPS": "nan"},
    {"KAFKA_PERF_MAX_P95_SECONDS": "inf"},
    {"KAFKA_PERF_MIN_EPS": "-1"},
])
def test_invalid_limits_rejected(bad):
    with pytest.raises(ValueError):
        profile(**bad)


def test_only_exact_disposable_targets_allowed():
    p = profile()
    p.validate_target("localhost:29092", "http://localhost:19200")
    for broker, es in (
        ("prod.example:29092", "http://localhost:19200"),
        ("localhost:29092,prod.example:9092", "http://localhost:19200"),
        ("localhost:29092", "https://production:9200"),
    ):
        with pytest.raises(ValueError, match="disposable"):
            p.validate_target(broker, es)


def test_percentile_is_nearest_rank_with_small_samples():
    assert LoadProfile.percentile([5, 1, 4, 3, 2], 0.95) == 5
    assert LoadProfile.percentile([1, 2, 3, 4, 5], 0.50) == 3
    with pytest.raises(ValueError):
        LoadProfile.percentile([], 0.95)


def test_performance_budget_enforced_only_when_configured():
    p = profile(KAFKA_PERF_EVENTS="600", KAFKA_PERF_MIN_EPS="25",
                KAFKA_PERF_MAX_P95_SECONDS="2", KAFKA_PERF_MAX_RSS_MIB="512")
    good = {"unique_indexed": 600, "events_per_second": 26,
            "p95_index_latency_seconds": 1.9, "max_rss_mib": 180}
    p.check(good)
    for update in (
        {"unique_indexed": 599},
        {"events_per_second": 24},
        {"p95_index_latency_seconds": 2.01},
        {"max_rss_mib": 550},
    ):
        with pytest.raises(AssertionError):
            p.check({**good, **update})

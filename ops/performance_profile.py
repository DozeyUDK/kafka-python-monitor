"""Validated, bounded settings for disposable Kafka/Elasticsearch load checks.

This module does not access networks. Performance SLO thresholds default to
disabled: GitHub-hosted runner timings are not production benchmarks.
"""
from dataclasses import dataclass
import math
import os


@dataclass(frozen=True)
class LoadProfile:
    events: int
    payload_bytes: int
    partitions: int
    duration_seconds: int
    batch_size: int
    max_rss_mib: int
    min_events_per_second: float
    max_p95_seconds: float

    @classmethod
    def from_env(cls, env=None):
        source = os.environ if env is None else env

        def integer(name, default, low, high):
            value = int(source.get(name, default))
            if not low <= value <= high:
                raise ValueError(f"{name} must be between {low} and {high}")
            return value

        def nonnegative_float(name, default):
            value = float(source.get(name, default))
            if not math.isfinite(value) or value < 0:
                raise ValueError(f"{name} must be finite and nonnegative")
            return value

        return cls(
            events=integer("KAFKA_PERF_EVENTS", 600, 100, 20000),
            payload_bytes=integer("KAFKA_PERF_BYTES", 1024, 64, 65536),
            partitions=integer("KAFKA_PERF_PARTITIONS", 2, 1, 4),
            duration_seconds=integer("KAFKA_PERF_DURATION_SECONDS", 10, 3, 300),
            batch_size=integer("KAFKA_PERF_BATCH_SIZE", 100, 1, 500),
            max_rss_mib=integer("KAFKA_PERF_MAX_RSS_MIB", 512, 64, 4096),
            min_events_per_second=nonnegative_float("KAFKA_PERF_MIN_EPS", 0),
            max_p95_seconds=nonnegative_float("KAFKA_PERF_MAX_P95_SECONDS", 0),
        )

    def validate_target(self, broker, elasticsearch):
        # Exact equality prevents accidentally load-testing real infrastructure.
        if broker != "localhost:29092" or elasticsearch != "http://localhost:19200":
            raise ValueError("Load tests are restricted to disposable localhost services")

    @staticmethod
    def percentile(values, percentile):
        if not values:
            raise ValueError("Cannot calculate latency percentile from empty data")
        ordered = sorted(values)
        index = math.ceil(percentile * len(ordered)) - 1
        return ordered[max(0, min(index, len(ordered) - 1))]

    def check(self, report):
        if report["unique_indexed"] != self.events:
            raise AssertionError("Indexed event count does not match published events")
        if report["max_rss_mib"] > self.max_rss_mib:
            raise AssertionError("RSS exceeded KAFKA_PERF_MAX_RSS_MIB")
        if self.min_events_per_second and report["events_per_second"] < self.min_events_per_second:
            raise AssertionError("Measured throughput fell below KAFKA_PERF_MIN_EPS")
        if self.max_p95_seconds and report["p95_index_latency_seconds"] > self.max_p95_seconds:
            raise AssertionError("Indexing latency exceeded KAFKA_PERF_MAX_P95_SECONDS")

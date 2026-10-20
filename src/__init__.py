"""Kafka consumer monitoring with optional Elasticsearch indexing."""

__version__ = "1.0.0"

from .kafka_monitor import KafkaMonitor
from .health_server import start_health_server

__all__ = ["KafkaMonitor", "start_health_server"]

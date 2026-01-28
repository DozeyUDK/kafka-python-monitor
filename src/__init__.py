"""
Kafka Python Monitor - Production-ready Kafka event monitor

A comprehensive Kafka monitoring solution with:
- Multi-topic monitoring
- Retry logic and reconnection handling
- SSL/SASL authentication support
- Batch processing
- Dead Letter Queue (DLQ)
- Elasticsearch integration
- Grafana/Prometheus metrics
- Health checks
"""

__version__ = "1.0.0"

from .kafka_monitor import KafkaMonitor
from .health_server import start_health_server

__all__ = ['KafkaMonitor', 'start_health_server']

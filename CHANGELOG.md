# Changelog

All notable changes to this project will be documented in this file.

## [1.0.0] - 2024-01-15

### Initial Release

### Added
- Initial release
- Multi-topic Kafka monitoring
- Retry logic with exponential backoff
- SSL/SASL authentication support
- Batch processing support
- Dead Letter Queue (DLQ) functionality
- Comprehensive metrics collection
- Health check HTTP endpoints
- Elasticsearch integration for events and metrics
- Prometheus metrics endpoint for Grafana
- Production-ready error handling
- Graceful shutdown support

### Features
- Monitor multiple Kafka topics simultaneously
- Automatic reconnection with configurable retry logic
- Support for SSL and SASL authentication
- Batch event processing for better performance
- Failed event handling via Dead Letter Queue
- Real-time metrics: throughput, lag, success rate, error rates
- HTTP health check endpoints for Kubernetes/Docker
- Elasticsearch export for events and metrics
- Prometheus-compatible metrics for Grafana dashboards

# Kafka Python Monitor

Production-ready Kafka event monitor for Python with comprehensive features including retry logic, metrics, SSL/SASL support, multi-topic monitoring, batch processing, dead letter queue, health checks, Elasticsearch integration, and Grafana support.

## Features

- ✅ **Multi-topic monitoring** - Monitor multiple Kafka topics simultaneously
- ✅ **Retry logic & reconnection** - Automatic retry with exponential backoff
- ✅ **SSL/SASL authentication** - Full support for secure Kafka clusters
- ✅ **Batch processing** - Process events in batches for better performance
- ✅ **Dead Letter Queue (DLQ)** - Automatic handling of failed events
- ✅ **Comprehensive metrics** - Throughput, lag, error rates, and more
- ✅ **Health checks** - HTTP endpoints for monitoring and health checks
- ✅ **Elasticsearch integration** - Export events and metrics to Elasticsearch
- ✅ **Grafana integration** - Prometheus metrics endpoint for Grafana dashboards
- ✅ **Production-ready** - Error handling, logging, graceful shutdown

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure environment variables:
```bash
cp env.example .env
```

3. Edit the `.env` file and set appropriate values (see Configuration section below)

## Usage

### Basic Usage

Run the main program:
```bash
python program.py
```

### Integration with Your Own Code

```python
from src.kafka_monitor import KafkaMonitor

# Create monitor instance
monitor = KafkaMonitor()

# Connect to Kafka
if monitor.connect():
    # Define callback for individual events
    def event_callback(event_data):
        print(f"Received event: {event_data['value']}")
        # Your logic here
    
    # Optional: Define batch callback for batch processing
    def batch_callback(batch):
        print(f"Processing batch of {len(batch)} events")
        # Bulk processing logic here
    
    # Start monitoring
    monitor.monitor(callback=event_callback, batch_callback=batch_callback)
    
    # Stop and close
    monitor.stop()
    monitor.disconnect()
```

### Health Check Endpoints

If `HEALTH_CHECK_PORT` is set (default: 8080), the monitor exposes:

| Endpoint    | Purpose | Use in production |
|------------|---------|--------------------|
| `GET /live`   | Liveness (process up) | Kubernetes **livenessProbe** |
| `GET /ready`  | Readiness (Kafka connected, healthy) | Kubernetes **readinessProbe** |
| `GET /health` | Full health (healthy, issues, stats) | General monitoring |
| `GET /metrics`| Prometheus metrics (default) | Grafana / Prometheus scrape |
| `GET /metrics?format=json` | JSON metrics | Custom dashboards |
| `GET /stats`  | Alias for JSON metrics | Legacy |

Example:
```bash
curl http://localhost:8080/live
curl http://localhost:8080/ready
curl http://localhost:8080/metrics
```

## Configuration

All settings are configured via environment variables in the `.env` file:

### Basic Kafka Configuration

- `KAFKA_BOOTSTRAP_SERVERS` - Kafka server addresses (comma-separated, default: localhost:9092)
- `KAFKA_TOPICS` - Topic names to monitor (comma-separated, default: test-topic)
- `KAFKA_GROUP_ID` - Consumer group ID (default: kafka-monitor-group)
- `KAFKA_AUTO_OFFSET_RESET` - Offset reset policy: 'earliest' or 'latest' (default: latest)
- `KAFKA_CONSUMER_TIMEOUT_MS` - Consumer timeout in ms (default: matches MONITOR_INTERVAL_SECONDS)

### Retry Configuration

- `KAFKA_MAX_RETRIES` - Maximum connection retry attempts (default: 5)
- `KAFKA_RETRY_BACKOFF_MS` - Base backoff time in ms for retries (default: 1000)
- `KAFKA_RECONNECT_INTERVAL_SECONDS` - Interval between reconnection attempts (default: 5)

### Batch Processing

- `KAFKA_BATCH_SIZE` - Number of events to batch before processing (default: 100)
- `KAFKA_BATCH_TIMEOUT_MS` - Maximum time to wait before processing batch (default: 5000)
- `USE_BATCH_PROCESSING` - Enable batch callback processing (default: false)

### SSL/SASL Configuration

- `KAFKA_SECURITY_PROTOCOL` - Security protocol: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL (default: PLAINTEXT)
- `KAFKA_SSL_CAFILE` - Path to CA certificate file (for SSL)
- `KAFKA_SSL_CERTFILE` - Path to client certificate file (for SSL)
- `KAFKA_SSL_KEYFILE` - Path to client key file (for SSL)
- `KAFKA_SASL_MECHANISM` - SASL mechanism: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
- `KAFKA_SASL_USERNAME` - SASL username
- `KAFKA_SASL_PASSWORD` - SASL password

### Dead Letter Queue

- `KAFKA_DLQ_ENABLED` - Enable dead letter queue (default: false)
- `KAFKA_DLQ_TOPIC` - DLQ topic name (default: {topic}-dlq)

### Monitor Configuration

- `LOG_LEVEL` - Logging level: DEBUG, INFO, WARNING, ERROR (default: INFO)
- `MONITOR_INTERVAL_SECONDS` - Poll interval in seconds (default: 5)

### Health Check

- `HEALTH_CHECK_PORT` - HTTP server port for health checks (default: 8080, set to 0 to disable)

### Elasticsearch Configuration

- `ELASTICSEARCH_ENABLED` - Enable Elasticsearch export (default: false)
- `ELASTICSEARCH_HOSTS` - Elasticsearch hosts (comma-separated, default: localhost:9200)
- `ELASTICSEARCH_USE_SSL` - Use SSL for Elasticsearch (default: false)
- `ELASTICSEARCH_VERIFY_CERTS` - Verify SSL certificates (default: true)
- `ELASTICSEARCH_USERNAME` - Elasticsearch username (optional)
- `ELASTICSEARCH_PASSWORD` - Elasticsearch password (optional)
- `ELASTICSEARCH_API_KEY` - Elasticsearch API key (optional, alternative to username/password)
- `ELASTICSEARCH_CA_CERTS` - Path to CA certificate file (optional)
- `ELASTICSEARCH_EVENTS_INDEX` - Events index prefix (default: kafka-events)
- `ELASTICSEARCH_METRICS_INDEX` - Metrics index prefix (default: kafka-metrics)
- `ELASTICSEARCH_INDEX_DATE_FORMAT` - Date format for index suffix (default: %Y.%m.%d)
- `ELASTICSEARCH_BATCH_SIZE` - Batch size for Elasticsearch bulk operations (default: 100)
- `ELASTICSEARCH_BATCH_TIMEOUT_SECONDS` - Timeout for batch flush (default: 10)

### Elasticsearch Configuration

- `ELASTICSEARCH_ENABLED` - Enable Elasticsearch export (default: false)
- `ELASTICSEARCH_HOSTS` - Elasticsearch hosts (comma-separated, default: localhost:9200)
- `ELASTICSEARCH_USE_SSL` - Use SSL for Elasticsearch (default: false)
- `ELASTICSEARCH_VERIFY_CERTS` - Verify SSL certificates (default: true)
- `ELASTICSEARCH_USERNAME` - Elasticsearch username (optional)
- `ELASTICSEARCH_PASSWORD` - Elasticsearch password (optional)
- `ELASTICSEARCH_API_KEY` - Elasticsearch API key (optional, alternative to username/password)
- `ELASTICSEARCH_CA_CERTS` - Path to CA certificate file (optional)
- `ELASTICSEARCH_EVENTS_INDEX` - Events index prefix (default: kafka-events)
- `ELASTICSEARCH_METRICS_INDEX` - Metrics index prefix (default: kafka-metrics)
- `ELASTICSEARCH_INDEX_DATE_FORMAT` - Date format for index suffix (default: %Y.%m.%d)
- `ELASTICSEARCH_BATCH_SIZE` - Batch size for Elasticsearch bulk operations (default: 100)
- `ELASTICSEARCH_BATCH_TIMEOUT_SECONDS` - Timeout for batch flush (default: 10)

## Project Structure

```
kafka_python_monitor/
├── src/
│   ├── __init__.py
│   ├── kafka_monitor.py         # Main Kafka monitor module
│   ├── health_server.py          # Health check HTTP server
│   └── elasticsearch_exporter.py # Elasticsearch exporter
├── program.py              # Example main program
├── requirements.txt        # Python dependencies
├── env.example            # Example configuration
├── .gitignore            # Git ignored files
└── README.md             # Documentation
```

## Metrics

The monitor tracks comprehensive metrics:

- `events_processed` - Total events processed
- `events_failed` - Events that failed processing
- `events_dlq` - Events sent to dead letter queue
- `batches_processed` - Number of batches processed
- `reconnection_count` - Number of reconnections
- `throughput_events_per_second` - Current throughput
- `success_rate` - Percentage of successful events
- `lag_by_partition` - Consumer lag per partition
- `errors_by_type` - Error counts by error type
- `events_by_topic` - Event counts per topic

## Production Deployment

### Production checklist

- **Config validation** – Required env vars (`KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPICS`) are validated at startup; process exits with code 1 if invalid.
- **Graceful shutdown** – SIGTERM/SIGINT is handled: current batch is processed, DLQ producer is flushed, then disconnect. Use in Kubernetes and Docker.
- **No secrets in logs** – Credentials and tokens are never logged.
- **Liveness vs readiness** – Use `/live` for liveness (process up) and `/ready` for readiness (Kafka connected). Prefer these over `/health` for probes.
- **Prometheus** – `/metrics` returns Prometheus format by default for Grafana and Prometheus scraping.
- **Container** – Image runs as non-root user and includes a HEALTHCHECK.

### Docker

Build and run:

```bash
docker build -t kafka-monitor .
docker run -d \
  --name kafka-monitor \
  -p 8080:8080 \
  --env-file .env \
  kafka-monitor
```

Or docker-compose:

```yaml
version: '3.8'
services:
  kafka-monitor:
    build: .
    ports:
      - "8080:8080"
    env_file:
      - .env
    restart: unless-stopped
```

### Kubernetes probes

Use `/live` for liveness and `/ready` for readiness:

```yaml
livenessProbe:
  httpGet:
    path: /live
    port: 8080
  initialDelaySeconds: 15
  periodSeconds: 10

readinessProbe:
  httpGet:
    path: /ready
    port: 8080
  initialDelaySeconds: 5
  periodSeconds: 5
```

## Requirements

- Python 3.7+
- Kafka broker available at configured address
- Libraries from `requirements.txt`

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

MIT License - see [LICENSE](LICENSE) file for details.

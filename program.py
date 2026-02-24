"""
Main program - Kafka Python Monitor entrypoint

Production-ready entrypoint with:
- SIGINT/SIGTERM handling for graceful shutdown (Kubernetes, Docker)
- Health check server with liveness/readiness
- Config validation before start
- Clean disconnect: drain batch, flush DLQ, then exit
"""
import sys
import signal
import threading
import os
import json
import logging
from src.kafka_monitor import KafkaMonitor
from src.health_server import start_health_server

logging.basicConfig(
    level=getattr(logging, os.getenv('LOG_LEVEL', 'INFO').upper(), logging.INFO),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

_shutdown_requested = False


def event_callback(event_data: dict):
    """Callback for each Kafka event. Customize for your use case."""
    logger.debug("Event received: topic=%s partition=%s", event_data.get('topic'), event_data.get('partition'))


def batch_callback(batch: list):
    """Callback for batch processing. Customize for bulk operations."""
    logger.info("Processing batch of %d events", len(batch))


def _signal_handler(sig, frame):
    """Handle SIGINT/SIGTERM for graceful shutdown (no sys.exit in handler)."""
    global _shutdown_requested
    if _shutdown_requested:
        return
    _shutdown_requested = True
    logger.info("Received signal %s, initiating graceful shutdown", sig)
    # Stop monitor loop so it can drain current batch and exit cleanly
    if _monitor_ref:
        _monitor_ref.stop()


_monitor_ref = None


def main():
    """Main entrypoint with production shutdown and exit codes."""
    global _shutdown_requested, _monitor_ref
    monitor = KafkaMonitor()
    _monitor_ref = monitor

    # Validate config before starting (fail fast)
    if not monitor.validate_config():
        logger.error("Invalid configuration. Check required env vars (e.g. KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPICS).")
        sys.exit(1)

    # Health server (liveness/readiness + metrics)
    health_port = int(os.getenv('HEALTH_CHECK_PORT', '8080'))
    health_thread = None
    if health_port > 0:
        health_thread = threading.Thread(
            target=start_health_server,
            args=(monitor, health_port),
            daemon=True
        )
        health_thread.start()

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    if not monitor.connect():
        logger.error("Failed to connect to Kafka. Check configuration and broker availability.")
        sys.exit(1)

    exit_code = 0
    try:
        use_batch = os.getenv('USE_BATCH_PROCESSING', 'false').lower() == 'true'
        if use_batch:
            monitor.monitor(callback=event_callback, batch_callback=batch_callback)
        else:
            monitor.monitor(callback=event_callback)
    except Exception as e:
        logger.exception("Error during monitoring: %s", e)
        exit_code = 1
    finally:
        monitor.disconnect()
        stats = monitor.get_stats()
        logger.info(
            "Shutdown complete. events_processed=%s events_failed=%s success_rate=%s",
            stats.get('events_processed', 0),
            stats.get('events_failed', 0),
            stats.get('success_rate', 100),
        )

    sys.exit(exit_code)


if __name__ == "__main__":
    main()

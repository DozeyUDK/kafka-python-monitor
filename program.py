"""Entrypoint for the Kafka-to-Elasticsearch monitor."""
import logging
import os
import signal
import sys
import threading
from http.server import ThreadingHTTPServer

from src.health_server import HealthHandler
from src.kafka_monitor import KafkaMonitor

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)
_monitor = None


def event_callback(event):
    # Never log message values or credentials by default.
    logger.debug(
        "Event topic=%s partition=%s offset=%s",
        event["topic"], event["partition"], event["offset"],
    )


def batch_callback(batch):
    logger.debug("Received Kafka batch with %d records", len(batch))


def _signal_handler(signum, frame):
    logger.info("Signal %s: requesting graceful stop", signum)
    if _monitor is not None:
        _monitor.stop()


def main():
    global _monitor
    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)
    server = None
    monitor = None
    try:
        monitor = KafkaMonitor()
        _monitor = monitor
        if not monitor.validate_config():
            return 1
        port = int(os.getenv("HEALTH_CHECK_PORT", "8080"))
        if not 0 <= port <= 65535:
            raise ValueError("HEALTH_CHECK_PORT must be between 0 and 65535")
        if port:
            HealthHandler.monitor = monitor
            host = os.getenv("HEALTH_SERVER_HOST", "127.0.0.1")
            server = ThreadingHTTPServer((host, port), HealthHandler)
            threading.Thread(target=server.serve_forever, daemon=True).start()
            logger.info("Health/metrics HTTP listening on %s:%d", host, port)
        if not monitor.connect():
            logger.error("Unable to connect to Kafka")
            return 1
        monitor.monitor(
            callback=event_callback,
            batch_callback=batch_callback
            if os.getenv("USE_BATCH_PROCESSING", "false").lower() == "true"
            else None,
        )
        return 0
    except Exception as exc:
        logger.error("Monitor exited with error (%s)", type(exc).__name__)
        return 1
    finally:
        if monitor is not None:
            try:
                monitor.disconnect()
            except Exception as exc:
                logger.error("Disconnect failed (%s)", type(exc).__name__)
        if server is not None:
            server.shutdown()
            server.server_close()
        _monitor = None


if __name__ == "__main__":
    sys.exit(main())

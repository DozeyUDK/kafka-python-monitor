"""
Main program - Example usage of Kafka Python Monitor

This is an example script demonstrating how to use the Kafka Python Monitor library.
It shows how to:
- Initialize and configure the monitor
- Set up health check server
- Handle events with callbacks
- Use batch processing
- Gracefully shutdown the monitor

For production use, customize the callbacks and configuration according to your needs.
"""
import sys
import signal
import threading
import os
import json
from src.kafka_monitor import KafkaMonitor
from src.health_server import start_health_server
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def event_callback(event_data: dict):
    """
    Example callback function for Kafka events
    
    Args:
        event_data: Dict with event data
    """
    # Here you can add your own event processing logic
    # e.g. database write, alert sending, etc.
    logger.debug(f"Callback received event: {event_data['value']}")


def batch_callback(batch: list):
    """
    Example batch callback function for processing multiple events at once
    
    Args:
        batch: List of event dicts
    """
    logger.info(f"Processing batch of {len(batch)} events")
    # Here you can add batch processing logic
    # e.g. bulk database insert, batch API calls, etc.


def main():
    """Main program function"""
    monitor = KafkaMonitor()
    
    # Start health check server if enabled
    health_port = int(os.getenv('HEALTH_CHECK_PORT', '8080'))
    health_thread = None
    if health_port > 0:
        health_thread = threading.Thread(
            target=start_health_server,
            args=(monitor, health_port),
            daemon=True
        )
        health_thread.start()
    
    # Handle interrupt signal (Ctrl+C)
    def signal_handler(sig, frame):
        logger.info("Received interrupt signal")
        monitor.stop()
        monitor.disconnect()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    
    # Attempt connection
    if not monitor.connect():
        logger.error("Failed to connect to Kafka. Check configuration.")
        sys.exit(1)
    
    try:
        # Start monitoring with callbacks
        # Use batch_callback if batch processing is preferred, otherwise event_callback
        use_batch = os.getenv('USE_BATCH_PROCESSING', 'false').lower() == 'true'
        if use_batch:
            monitor.monitor(callback=event_callback, batch_callback=batch_callback)
        else:
            monitor.monitor(callback=event_callback)
    except Exception as e:
        logger.error(f"Error during monitoring: {e}")
    finally:
        monitor.disconnect()
        # Print final stats
        stats = monitor.get_stats()
        logger.info(f"Final statistics: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()

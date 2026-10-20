"""Kafka consumer with explicit offset acknowledgements and bounded batches."""
import json
import logging
import os
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from threading import Event, Lock

from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

from src.elasticsearch_exporter import ElasticsearchExporter

load_dotenv()
logger = logging.getLogger(__name__)


class Metrics:
    def __init__(self):
        self.events_processed = 0
        self.events_failed = 0
        self.events_dlq = 0
        self.batches_processed = 0
        self.reconnection_count = 0
        self.errors_by_type = defaultdict(int)
        self.events_by_topic = defaultdict(int)
        self.lag_by_partition = {}
        self.last_event_time = None
        self.start_time = time.monotonic()
        self._window = deque(maxlen=100)
        self._lock = Lock()

    def record_event(self, topic, success=True):
        with self._lock:
            self.events_processed += 1
            self.events_by_topic[topic] += 1
            self.last_event_time = datetime.now(timezone.utc).isoformat()
            self._window.append(time.monotonic())
            if not success:
                self.events_failed += 1

    def record_error(self, name):
        with self._lock:
            self.errors_by_type[name] += 1

    def record_dlq(self):
        with self._lock:
            self.events_dlq += 1

    def record_reconnection(self):
        with self._lock:
            self.reconnection_count += 1

    def record_batch(self, count):
        with self._lock:
            self.batches_processed += 1

    def update_lag(self, topic, partition, lag):
        with self._lock:
            self.lag_by_partition["{}:{}".format(topic, partition)] = max(0, lag)

    def get_stats(self):
        with self._lock:
            elapsed = self._window[-1] - self._window[0] if len(self._window) > 1 else 0
            return {
                "events_processed": self.events_processed,
                "events_failed": self.events_failed,
                "events_dlq": self.events_dlq,
                "batches_processed": self.batches_processed,
                "reconnection_count": self.reconnection_count,
                "uptime_seconds": round(time.monotonic() - self.start_time, 2),
                "throughput_events_per_second": round(
                    (len(self._window) - 1) / elapsed, 2
                ) if elapsed > 0 else 0.0,
                "last_event_time": self.last_event_time,
                "success_rate": round(
                    100 * (self.events_processed - self.events_failed) / self.events_processed, 2
                ) if self.events_processed else 100.0,
                "errors_by_type": dict(self.errors_by_type),
                "events_by_topic": dict(self.events_by_topic),
                "lag_by_partition": dict(self.lag_by_partition),
            }


class KafkaMonitor:
    """At-least-once Kafka-to-Elasticsearch ingestion.

    Offset commit follows acknowledged Elasticsearch bulk indexing.
    If any event fails, the process exits without committing that poll batch.
    On restart Elasticsearch replaces replayed events by stable document ID.
    """

    def __init__(self):
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.topics = [
            t.strip() for t in
            os.getenv("KAFKA_TOPICS", os.getenv("KAFKA_TOPIC", "test-topic")).split(",")
            if t.strip()
        ]
        self.group_id = os.getenv("KAFKA_GROUP_ID", "kafka-monitor-group")
        self.auto_offset_reset = os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest")
        self.monitor_interval_ms = int(os.getenv("MONITOR_INTERVAL_SECONDS", "5")) * 1000
        self.consumer_timeout_ms = int(os.getenv("KAFKA_CONSUMER_TIMEOUT_MS", "5000"))
        self.batch_size = int(os.getenv("KAFKA_BATCH_SIZE", "100"))
        self.metrics_interval = int(os.getenv("METRICS_INTERVAL_SECONDS", "30"))
        self.max_retries = int(os.getenv("KAFKA_MAX_RETRIES", "5"))
        self.retry_backoff_ms = int(os.getenv("KAFKA_RETRY_BACKOFF_MS", "1000"))
        self.reconnect_interval = int(os.getenv("KAFKA_RECONNECT_INTERVAL_SECONDS", "5"))
        self.security_protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
        self.ssl_cafile = os.getenv("KAFKA_SSL_CAFILE") or None
        self.ssl_certfile = os.getenv("KAFKA_SSL_CERTFILE") or None
        self.ssl_keyfile = os.getenv("KAFKA_SSL_KEYFILE") or None
        self.sasl_mechanism = os.getenv("KAFKA_SASL_MECHANISM") or None
        self.sasl_username = os.getenv("KAFKA_SASL_USERNAME") or None
        self.sasl_password = os.getenv("KAFKA_SASL_PASSWORD") or None
        self.dlq_enabled = os.getenv("KAFKA_DLQ_ENABLED", "false").lower() == "true"
        self.dlq_topic = os.getenv("KAFKA_DLQ_TOPIC", "kafka-monitor-dlq")
        self.consumer = None
        self.dlq_producer = None
        self.metrics = Metrics()
        self.es_exporter = ElasticsearchExporter()
        self._es_healthy = True
        self.is_running = False
        self._stop_event = Event()
        self._fatal_error = None
        self._last_metrics_time = time.monotonic()

    def validate_config(self):
        if not self.bootstrap_servers.strip() or not self.topics or not self.group_id.strip():
            logger.error("Kafka bootstrap servers, topics and group ID must be non-empty")
            return False
        if self.auto_offset_reset not in ("earliest", "latest"):
            logger.error("KAFKA_AUTO_OFFSET_RESET must be earliest or latest")
            return False
        if self.security_protocol not in ("PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"):
            logger.error("Invalid Kafka security protocol")
            return False
        if "SASL" in self.security_protocol and (
            not self.sasl_username or not self.sasl_password or
            self.sasl_mechanism not in ("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512")
        ):
            logger.error("SASL credentials and supported mechanism are required")
            return False
        if (self.batch_size < 1 or self.monitor_interval_ms < 0 or
            self.metrics_interval < 1 or self.max_retries < 1 or
            self.retry_backoff_ms < 0 or self.reconnect_interval < 0):
            logger.error("Invalid numeric Kafka monitor configuration")
            return False
        if self.dlq_enabled and not self.dlq_topic.strip():
            logger.error("KAFKA_DLQ_TOPIC is required when DLQ is enabled")
            return False
        return True

    def _security_config(self):
        cfg = {"security_protocol": self.security_protocol}
        if "SSL" in self.security_protocol:
            for name in ("ssl_cafile", "ssl_certfile", "ssl_keyfile"):
                value = getattr(self, name)
                if value:
                    cfg[name] = value
        if "SASL" in self.security_protocol:
            cfg.update({
                "sasl_mechanism": self.sasl_mechanism,
                "sasl_plain_username": self.sasl_username,
                "sasl_plain_password": self.sasl_password,
            })
        return cfg

    @staticmethod
    def _safe_deserializer(value):
        if value is None:
            return None
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return "hex:" + value.hex()

    def _get_consumer_config(self):
        return {
            "bootstrap_servers": [s.strip() for s in self.bootstrap_servers.split(",") if s.strip()],
            "group_id": self.group_id,
            "consumer_timeout_ms": self.consumer_timeout_ms,
            "auto_offset_reset": self.auto_offset_reset,
            "enable_auto_commit": False,
            "value_deserializer": self._safe_deserializer,
            **self._security_config(),
        }

    def _connect_with_retry(self):
        for attempt in range(self.max_retries):
            if self._stop_event.is_set():
                return False
            try:
                self.consumer = KafkaConsumer(*self.topics, **self._get_consumer_config())
                logger.info("Connected to Kafka, topics=%s", self.topics)
                return True
            except Exception as exc:
                self.metrics.record_error(type(exc).__name__)
                logger.warning("Kafka connection attempt %d/%d failed: %s",
                               attempt + 1, self.max_retries, type(exc).__name__)
                if attempt + 1 < self.max_retries:
                    delay = min((self.retry_backoff_ms / 1000) * (2 ** attempt), 30)
                    self._stop_event.wait(delay)
        return False

    def connect(self):
        return self.validate_config() and self._connect_with_retry()

    def _send_to_dlq(self, event, exc):
        if not self.dlq_enabled:
            raise exc
        if self.dlq_producer is None:
            self.dlq_producer = KafkaProducer(
                bootstrap_servers=[s.strip() for s in self.bootstrap_servers.split(",") if s.strip()],
                acks="all",
                **self._security_config(),
            )
        payload = json.dumps(
            {"original_event": event, "error_type": type(exc).__name__,
             "timestamp": datetime.now(timezone.utc).isoformat()},
            ensure_ascii=False,
        ).encode("utf-8")
        key = event["key"].encode("utf-8") if event.get("key") else None
        self.dlq_producer.send(self.dlq_topic, value=payload, key=key).get(timeout=30)
        self.metrics.record_dlq()

    def _process_batch(self, batch, callback=None, batch_callback=None):
        if not batch:
            return
        ready_for_export = []
        failed = []
        if batch_callback:
            batch_callback(batch)  # Exception aborts the poll; offsets stay uncommitted.
            ready_for_export = batch
        else:
            for event in batch:
                try:
                    if callback:
                        callback(event)
                    ready_for_export.append(event)
                except Exception as exc:
                    self.metrics.record_error(type(exc).__name__)
                    self._send_to_dlq(event, exc)  # Must be acknowledged before offset commit.
                    failed.append(event)
        # Raises on any partial bulk failure; never ack such a poll to Kafka.
        if self.es_exporter:
            self.es_exporter.export_events(ready_for_export)
        for event in ready_for_export:
            self.metrics.record_event(event["topic"], success=True)
        for event in failed:
            self.metrics.record_event(event["topic"], success=False)
        self.metrics.record_batch(len(batch))

    def _update_lag_metrics(self):
        if not self.consumer:
            return
        try:
            partitions = list(self.consumer.assignment())
            if not partitions:
                return
            ends = self.consumer.end_offsets(partitions)
            for tp in partitions:
                committed = self.consumer.committed(tp)
                if committed is not None and tp in ends:
                    self.metrics.update_lag(tp.topic, tp.partition, ends[tp] - committed)
        except Exception as exc:
            logger.warning("Could not update Kafka lag metrics: %s", type(exc).__name__)
            self.metrics.record_error(type(exc).__name__)

    def _report_metrics(self):
        if time.monotonic() - self._last_metrics_time < self.metrics_interval:
            return
        self._last_metrics_time = time.monotonic()
        self._update_lag_metrics()
        if self.es_exporter.enabled:
            try:
                self._es_healthy = bool(self.es_exporter.client and self.es_exporter.client.ping())
            except Exception:
                self._es_healthy = False
        try:
            self.es_exporter.export_metrics(self.metrics.get_stats())
        except Exception as exc:
            # Metrics are not part of the Kafka event acknowledgement path.
            logger.warning("Elasticsearch metrics export failed: %s", type(exc).__name__)
            self.metrics.record_error(type(exc).__name__)

    def monitor(self, callback=None, batch_callback=None):
        if not self.consumer:
            raise RuntimeError("Call connect() before monitor()")
        self.is_running = True
        self._fatal_error = None
        try:
            while not self._stop_event.is_set():
                try:
                    message_pack = self.consumer.poll(
                        timeout_ms=self.monitor_interval_ms, max_records=self.batch_size
                    )
                except KafkaError as exc:
                    self.metrics.record_error(type(exc).__name__)
                    logger.warning("Kafka poll failure; reconnecting (%s)", type(exc).__name__)
                    try:
                        self.consumer.close()
                    except Exception:
                        logger.warning("Failed to close Kafka consumer after poll failure")
                    self.consumer = None
                    if not self._stop_event.wait(self.reconnect_interval):
                        if not self._connect_with_retry():
                            raise ConnectionError("Kafka reconnect attempts exhausted") from exc
                        self.metrics.record_reconnection()
                    continue

                batch = [
                    {
                        "topic": m.topic,
                        "partition": m.partition,
                        "offset": m.offset,
                        "key": self._safe_deserializer(m.key),
                        "value": m.value,
                        "timestamp": m.timestamp,
                    }
                    for messages in message_pack.values() for m in messages
                ]
                if batch:
                    self._process_batch(batch, callback=callback, batch_callback=batch_callback)
                    # Synchronous commit only after all side effects succeed.
                    self.consumer.commit()
                self._report_metrics()
        except Exception as exc:
            self._fatal_error = type(exc).__name__
            self.metrics.record_error(type(exc).__name__)
            logger.error("Monitor stopped after unacknowledged processing failure: %s", type(exc).__name__)
            raise
        finally:
            self.is_running = False

    def stop(self):
        self._stop_event.set()

    def disconnect(self):
        self.stop()
        if self.consumer is not None:
            try:
                self.consumer.close()
            finally:
                self.consumer = None
        if self.dlq_producer is not None:
            try:
                self.dlq_producer.flush(timeout=10)
                self.dlq_producer.close()
            finally:
                self.dlq_producer = None
        if self.es_exporter is not None:
            self.es_exporter.close()

    def get_stats(self):
        stats = self.metrics.get_stats()
        stats.update({
            "topics": self.topics,
            "group_id": self.group_id,
            "is_running": self.is_running,
            "is_connected": self.consumer is not None,
            "batch_size": self.batch_size,
            "dlq_enabled": self.dlq_enabled,
            "elasticsearch_enabled": self.es_exporter.enabled,
        })
        return stats

    def health_check(self):
        issues = []
        if self.consumer is None or not self.is_running:
            issues.append("Kafka consumer is not running")
        if self._fatal_error:
            issues.append("Fatal error: " + self._fatal_error)
        if self.es_exporter.enabled and (self.es_exporter.client is None or not self._es_healthy):
            issues.append("Elasticsearch exporter unavailable")
        return {"healthy": not issues, "issues": issues, "stats": self.get_stats()}

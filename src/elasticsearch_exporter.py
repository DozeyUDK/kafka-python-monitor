"""Synchronous, bounded Kafka-to-Elasticsearch exporter.

The Kafka consumer owns offset commits. No successful export is acknowledged
until the entire Elasticsearch bulk request succeeds.
"""
import hashlib
import logging
import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

load_dotenv()
logger = logging.getLogger(__name__)


class ElasticsearchExporter:
    def __init__(self, client=None):
        self.enabled = os.getenv("ELASTICSEARCH_ENABLED", "false").lower() == "true"
        self.client = None
        self.events_index_prefix = os.getenv("ELASTICSEARCH_EVENTS_INDEX", "kafka-events")
        self.metrics_index_prefix = os.getenv("ELASTICSEARCH_METRICS_INDEX", "kafka-metrics")
        self.index_date_format = os.getenv("ELASTICSEARCH_INDEX_DATE_FORMAT", "%Y.%m.%d")
        self.batch_size = int(os.getenv("ELASTICSEARCH_BATCH_SIZE", "100"))
        if self.batch_size < 1:
            raise ValueError("ELASTICSEARCH_BATCH_SIZE must be positive")
        if not self.enabled:
            return

        if client is not None:
            self.client = client
        else:
            use_ssl = os.getenv("ELASTICSEARCH_USE_SSL", "false").lower() == "true"
            hosts = []
            for host in os.getenv("ELASTICSEARCH_HOSTS", "localhost:9200").split(","):
                host = host.strip()
                if not host:
                    continue
                if "://" not in host:
                    host = ("https://" if use_ssl else "http://") + host
                hosts.append(host)
            if not hosts:
                raise ValueError("ELASTICSEARCH_HOSTS must contain at least one host")
            kwargs = {
                "hosts": hosts,
                "verify_certs": os.getenv("ELASTICSEARCH_VERIFY_CERTS", "true").lower() == "true",
                "request_timeout": 30,
            }
            ca_certs = os.getenv("ELASTICSEARCH_CA_CERTS")
            if ca_certs:
                kwargs["ca_certs"] = ca_certs
            api_key = os.getenv("ELASTICSEARCH_API_KEY")
            username = os.getenv("ELASTICSEARCH_USERNAME")
            password = os.getenv("ELASTICSEARCH_PASSWORD")
            if api_key:
                kwargs["api_key"] = api_key
            elif username and password:
                kwargs["basic_auth"] = (username, password)
            self.client = Elasticsearch(**kwargs)

        if not self.client.ping():
            raise ConnectionError("Elasticsearch ping failed; refusing to consume Kafka")
        logger.info("Elasticsearch connection established")

    def _index_name(self, prefix, timestamp=None):
        stamp = timestamp or datetime.now(timezone.utc)
        return "{}-{}".format(prefix, stamp.strftime(self.index_date_format))

    def _event_action(self, event):
        # Stable ID and index across retries, even when the calendar day changes.
        identity = "{}\0{}\0{}".format(
            event["topic"], event["partition"], event["offset"]
        )
        document_id = hashlib.sha256(identity.encode("utf-8")).hexdigest()
        timestamp_ms = event.get("timestamp")
        valid_timestamp = timestamp_ms is not None and timestamp_ms >= 0
        event_time = (
            datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
            if valid_timestamp else datetime(1970, 1, 1, tzinfo=timezone.utc)
        )
        # Unknown Kafka timestamp uses a stable fallback date, not today's date.
        return {
            "_op_type": "index",
            "_index": self._index_name(self.events_index_prefix, event_time),
            "_id": document_id,
            "_source": {
                "@timestamp": event_time.isoformat(),
                "kafka_topic": event["topic"],
                "kafka_partition": event["partition"],
                "kafka_offset": event["offset"],
                "kafka_timestamp": timestamp_ms,
                "kafka_key": event.get("key"),
                "kafka_value": event.get("value"),
            },
        }

    def export_events(self, events):
        """Write all events synchronously. Raise on *any* rejected bulk item."""
        if not self.enabled:
            return
        if self.client is None:
            raise RuntimeError("Elasticsearch exporter is unavailable")
        if not events:
            return
        # The consumer polls bounded batches. This inner chunking also bounds
        # the size of individual Elasticsearch bulk requests.
        for start in range(0, len(events), self.batch_size):
            actions = [self._event_action(e) for e in events[start:start + self.batch_size]]
            bulk(
                self.client,
                actions,
                raise_on_error=True,
                raise_on_exception=True,
                max_retries=3,
                initial_backoff=1,
                max_backoff=8,
                retry_on_status=(429, 502, 503, 504),
                request_timeout=30,
            )

    def export_event(self, event_data, metadata=None):
        """Compatibility method; executes a confirmed single-document write."""
        self.export_events([event_data])

    def export_metrics(self, metrics):
        """Best-effort operational metrics. Never part of the event commit path."""
        if not self.enabled or self.client is None:
            return
        # Dynamic topic/error names are kept as data, not arbitrary field names.
        document = {
            "@timestamp": datetime.now(timezone.utc).isoformat(),
            "events_processed": metrics.get("events_processed", 0),
            "events_failed": metrics.get("events_failed", 0),
            "reconnection_count": metrics.get("reconnection_count", 0),
            "throughput_events_per_second": metrics.get("throughput_events_per_second", 0),
            "success_rate": metrics.get("success_rate", 100.0),
        }
        self.client.index(index=self._index_name(self.metrics_index_prefix), document=document)

    def flush_all(self):
        """No asynchronous buffer: writes are confirmed before returning."""
        pass

    def close(self):
        if self.client is not None:
            self.client.close()
            self.client = None

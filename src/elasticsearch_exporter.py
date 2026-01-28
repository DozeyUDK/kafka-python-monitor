"""
Elasticsearch exporter for Kafka Python Monitor

Exports Kafka events and metrics to Elasticsearch for:
- Event indexing and search
- Metrics aggregation and analysis
- Integration with Kibana dashboards
"""
import os
import json
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


class ElasticsearchExporter:
    """Export events and metrics to Elasticsearch"""
    
    def __init__(self):
        """Initialize Elasticsearch exporter"""
        self.enabled = os.getenv('ELASTICSEARCH_ENABLED', 'false').lower() == 'true'
        
        if not self.enabled:
            logger.info("Elasticsearch exporter disabled")
            return
        
        # Elasticsearch configuration
        self.hosts = os.getenv('ELASTICSEARCH_HOSTS', 'localhost:9200').split(',')
        self.use_ssl = os.getenv('ELASTICSEARCH_USE_SSL', 'false').lower() == 'true'
        self.verify_certs = os.getenv('ELASTICSEARCH_VERIFY_CERTS', 'true').lower() == 'true'
        self.username = os.getenv('ELASTICSEARCH_USERNAME', None)
        self.password = os.getenv('ELASTICSEARCH_PASSWORD', None)
        self.api_key = os.getenv('ELASTICSEARCH_API_KEY', None)
        self.ca_certs = os.getenv('ELASTICSEARCH_CA_CERTS', None)
        
        # Index configuration
        self.events_index_prefix = os.getenv('ELASTICSEARCH_EVENTS_INDEX', 'kafka-events')
        self.metrics_index_prefix = os.getenv('ELASTICSEARCH_METRICS_INDEX', 'kafka-metrics')
        self.index_date_format = os.getenv('ELASTICSEARCH_INDEX_DATE_FORMAT', '%Y.%m.%d')
        
        # Batch configuration
        self.batch_size = int(os.getenv('ELASTICSEARCH_BATCH_SIZE', '100'))
        self.batch_timeout = int(os.getenv('ELASTICSEARCH_BATCH_TIMEOUT_SECONDS', '10'))
        
        self.client: Optional[Elasticsearch] = None
        self._event_buffer: List[Dict[str, Any]] = []
        self._metrics_buffer: List[Dict[str, Any]] = []
        self._last_flush = datetime.now()
        
        self._connect()
    
    def _connect(self):
        """Connect to Elasticsearch"""
        if not self.enabled:
            return
        
        try:
            config = {
                'hosts': self.hosts,
                'use_ssl': self.use_ssl,
                'verify_certs': self.verify_certs,
            }
            
            if self.ca_certs:
                config['ca_certs'] = self.ca_certs
            
            if self.api_key:
                config['api_key'] = self.api_key
            elif self.username and self.password:
                config['http_auth'] = (self.username, self.password)
            
            self.client = Elasticsearch(**config)
            
            # Test connection
            if self.client.ping():
                logger.info(f"Connected to Elasticsearch at {self.hosts}")
            else:
                logger.error("Failed to ping Elasticsearch")
                self.enabled = False
        except Exception as e:
            logger.error(f"Failed to connect to Elasticsearch: {e}")
            self.enabled = False
    
    def _get_index_name(self, prefix: str) -> str:
        """Get index name with date suffix"""
        date_str = datetime.now().strftime(self.index_date_format)
        return f"{prefix}-{date_str}"
    
    def export_event(self, event_data: Dict[str, Any], metadata: Optional[Dict[str, Any]] = None):
        """
        Export event to Elasticsearch
        
        Args:
            event_data: Event data dictionary
            metadata: Optional metadata (error info, processing time, etc.)
        """
        if not self.enabled or not self.client:
            return
        
        try:
            doc = {
                '@timestamp': datetime.now().isoformat(),
                'event': event_data,
                'kafka_topic': event_data.get('topic'),
                'kafka_partition': event_data.get('partition'),
                'kafka_offset': event_data.get('offset'),
                'kafka_key': event_data.get('key'),
                'kafka_value': event_data.get('value'),
                'kafka_timestamp': event_data.get('timestamp'),
            }
            
            if metadata:
                doc['metadata'] = metadata
            
            self._event_buffer.append({
                '_index': self._get_index_name(self.events_index_prefix),
                '_source': doc
            })
            
            # Flush if buffer is full or timeout reached
            if len(self._event_buffer) >= self.batch_size:
                self._flush_events()
            elif (datetime.now() - self._last_flush).total_seconds() >= self.batch_timeout:
                self._flush_events()
                
        except Exception as e:
            logger.error(f"Failed to buffer event for Elasticsearch: {e}")
    
    def export_metrics(self, metrics: Dict[str, Any]):
        """
        Export metrics to Elasticsearch
        
        Args:
            metrics: Metrics dictionary from get_stats()
        """
        if not self.enabled or not self.client:
            return
        
        try:
            doc = {
                '@timestamp': datetime.now().isoformat(),
                'metrics': metrics,
                'uptime_seconds': metrics.get('uptime_seconds', 0),
                'events_processed': metrics.get('events_processed', 0),
                'events_failed': metrics.get('events_failed', 0),
                'throughput_events_per_second': metrics.get('throughput_events_per_second', 0),
                'success_rate': metrics.get('success_rate', 100.0),
                'reconnection_count': metrics.get('reconnection_count', 0),
            }
            
            # Add topic-specific metrics
            if 'events_by_topic' in metrics:
                for topic, count in metrics['events_by_topic'].items():
                    doc[f'topic_{topic}_events'] = count
            
            # Add error metrics
            if 'errors_by_type' in metrics:
                for error_type, count in metrics['errors_by_type'].items():
                    doc[f'error_{error_type}'] = count
            
            self._metrics_buffer.append({
                '_index': self._get_index_name(self.metrics_index_prefix),
                '_source': doc
            })
            
            # Flush metrics immediately (they're less frequent)
            if len(self._metrics_buffer) >= 10:
                self._flush_metrics()
                
        except Exception as e:
            logger.error(f"Failed to buffer metrics for Elasticsearch: {e}")
    
    def _flush_events(self):
        """Flush event buffer to Elasticsearch"""
        if not self._event_buffer or not self.client:
            return
        
        try:
            bulk(self.client, self._event_buffer)
            logger.debug(f"Flushed {len(self._event_buffer)} events to Elasticsearch")
            self._event_buffer = []
            self._last_flush = datetime.now()
        except Exception as e:
            logger.error(f"Failed to flush events to Elasticsearch: {e}")
            self._event_buffer = []  # Clear buffer on error to prevent memory issues
    
    def _flush_metrics(self):
        """Flush metrics buffer to Elasticsearch"""
        if not self._metrics_buffer or not self.client:
            return
        
        try:
            bulk(self.client, self._metrics_buffer)
            logger.debug(f"Flushed {len(self._metrics_buffer)} metrics to Elasticsearch")
            self._metrics_buffer = []
        except Exception as e:
            logger.error(f"Failed to flush metrics to Elasticsearch: {e}")
            self._metrics_buffer = []
    
    def flush_all(self):
        """Flush all buffers to Elasticsearch"""
        self._flush_events()
        self._flush_metrics()
    
    def close(self):
        """Close Elasticsearch connection and flush buffers"""
        if self.enabled:
            self.flush_all()
            if self.client:
                self.client.close()

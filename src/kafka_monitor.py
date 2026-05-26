"""
Kafka Python Monitor - Production-ready Kafka event monitor

A comprehensive Kafka monitoring solution with:
- Multi-topic monitoring
- Retry logic and reconnection handling
- SSL/SASL authentication support
- Batch processing
- Dead Letter Queue (DLQ)
- Elasticsearch integration
- Comprehensive metrics collection
- Production-ready error handling
"""
import os
import logging
import time
import json
from typing import Optional, Callable, List, Dict, Any
from datetime import datetime
from collections import defaultdict, deque
from threading import Lock
from kafka import KafkaConsumer
from kafka.errors import KafkaError, KafkaTimeoutError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Logging configuration
logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'INFO'),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class Metrics:
    """Metrics collector for monitoring performance"""
    
    def __init__(self):
        self.events_processed = 0
        self.events_failed = 0
        self.events_dlq = 0
        self.batches_processed = 0
        self.reconnection_count = 0
        self.last_event_time: Optional[datetime] = None
        self.start_time = datetime.now()
        self.errors_by_type = defaultdict(int)
        self.events_by_topic = defaultdict(int)
        self.lag_by_partition = defaultdict(int)
        self._lock = Lock()
        self._throughput_window = deque(maxlen=100)  # Last 100 events for throughput calc
    
    def record_event(self, topic: str, success: bool = True):
        """Record a processed event"""
        with self._lock:
            self.events_processed += 1
            self.events_by_topic[topic] += 1
            self.last_event_time = datetime.now()
            self._throughput_window.append(time.time())
            if not success:
                self.events_failed += 1
    
    def record_dlq(self):
        """Record event sent to dead letter queue"""
        with self._lock:
            self.events_dlq += 1
    
    def record_error(self, error_type: str):
        """Record an error"""
        with self._lock:
            self.errors_by_type[error_type] += 1
    
    def record_reconnection(self):
        """Record a reconnection"""
        with self._lock:
            self.reconnection_count += 1
    
    def record_batch(self, batch_size: int):
        """Record a batch processing"""
        with self._lock:
            self.batches_processed += 1
    
    def update_lag(self, partition: int, lag: int):
        """Update consumer lag for partition"""
        with self._lock:
            self.lag_by_partition[partition] = lag
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current statistics"""
        with self._lock:
            uptime = (datetime.now() - self.start_time).total_seconds()
            throughput = 0
            if len(self._throughput_window) > 1:
                time_span = self._throughput_window[-1] - self._throughput_window[0]
                if time_span > 0:
                    throughput = len(self._throughput_window) / time_span
            
            return {
                'events_processed': self.events_processed,
                'events_failed': self.events_failed,
                'events_dlq': self.events_dlq,
                'batches_processed': self.batches_processed,
                'reconnection_count': self.reconnection_count,
                'uptime_seconds': uptime,
                'throughput_events_per_second': round(throughput, 2),
                'last_event_time': self.last_event_time.isoformat() if self.last_event_time else None,
                'errors_by_type': dict(self.errors_by_type),
                'events_by_topic': dict(self.events_by_topic),
                'lag_by_partition': dict(self.lag_by_partition),
                'success_rate': round(
                    (self.events_processed - self.events_failed) / self.events_processed * 100, 2
                ) if self.events_processed > 0 else 100.0
            }


class KafkaMonitor:
    """Production-ready Kafka event monitor with comprehensive features"""
    
    def __init__(self):
        """Initialize monitor with configuration from environment variables"""
        # Basic configuration
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        topics_str = os.getenv('KAFKA_TOPICS', os.getenv('KAFKA_TOPIC', 'test-topic'))
        self.topics = [t.strip() for t in topics_str.split(',')]
        self.group_id = os.getenv('KAFKA_GROUP_ID', 'kafka-monitor-group')
        self.monitor_interval_ms = int(os.getenv('MONITOR_INTERVAL_SECONDS', '5')) * 1000
        self.consumer_timeout_ms = int(os.getenv('KAFKA_CONSUMER_TIMEOUT_MS', str(self.monitor_interval_ms)))
        self.auto_offset_reset = os.getenv('KAFKA_AUTO_OFFSET_RESET', 'latest')
        
        # Retry configuration
        self.max_retries = int(os.getenv('KAFKA_MAX_RETRIES', '5'))
        self.retry_backoff_ms = int(os.getenv('KAFKA_RETRY_BACKOFF_MS', '1000'))
        self.reconnect_interval = int(os.getenv('KAFKA_RECONNECT_INTERVAL_SECONDS', '5'))
        
        # Batch processing
        self.batch_size = int(os.getenv('KAFKA_BATCH_SIZE', '100'))
        self.batch_timeout_ms = int(os.getenv('KAFKA_BATCH_TIMEOUT_MS', '5000'))
        
        # SSL/SASL configuration
        self.security_protocol = os.getenv('KAFKA_SECURITY_PROTOCOL', 'PLAINTEXT')  # PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL
        self.ssl_cafile = os.getenv('KAFKA_SSL_CAFILE', None)
        self.ssl_certfile = os.getenv('KAFKA_SSL_CERTFILE', None)
        self.ssl_keyfile = os.getenv('KAFKA_SSL_KEYFILE', None)
        self.sasl_mechanism = os.getenv('KAFKA_SASL_MECHANISM', None)  # PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
        self.sasl_username = os.getenv('KAFKA_SASL_USERNAME', None)
        self.sasl_password = os.getenv('KAFKA_SASL_PASSWORD', None)
        
        # Dead letter queue
        self.dlq_enabled = os.getenv('KAFKA_DLQ_ENABLED', 'false').lower() == 'true'
        self.dlq_topic = os.getenv('KAFKA_DLQ_TOPIC', f'{self.topics[0]}-dlq' if self.topics else 'dlq')
        
        self.consumer: Optional[KafkaConsumer] = None
        self.dlq_producer: Optional[Any] = None  # Will be KafkaProducer if DLQ enabled
        self.is_running = False
        self.metrics = Metrics()
        self._lock = Lock()
        
        # Elasticsearch exporter (optional)
        self.es_exporter = None
        try:
            from src.elasticsearch_exporter import ElasticsearchExporter
            self.es_exporter = ElasticsearchExporter()
        except ImportError:
            logger.warning("Elasticsearch exporter not available (elasticsearch package not installed)")
        except Exception as e:
            logger.warning(f"Failed to initialize Elasticsearch exporter: {e}")
        
        # Log config without secrets (production-safe)
        logger.info(
            "KafkaMonitor initialized: bootstrap_servers=%s topics=%s group_id=%s security_protocol=%s batch_size=%s dlq_enabled=%s",
            self.bootstrap_servers,
            self.topics,
            self.group_id,
            self.security_protocol,
            self.batch_size,
            self.dlq_enabled,
        )

    def validate_config(self) -> bool:
        """Validate required configuration. Returns False if invalid (fail fast)."""
        if not self.bootstrap_servers or self.bootstrap_servers.strip() == '':
            logger.error("KAFKA_BOOTSTRAP_SERVERS is required and must be non-empty")
            return False
        if not self.topics or all(not t.strip() for t in self.topics):
            logger.error("KAFKA_TOPICS is required and must contain at least one topic")
            return False
        if 'SASL' in self.security_protocol and (not self.sasl_username or not self.sasl_password):
            logger.error("KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD are required when using SASL")
            return False
        return True
    
    def _get_consumer_config(self) -> Dict[str, Any]:
        """Build consumer configuration with SSL/SASL support"""
        config = {
            'bootstrap_servers': self.bootstrap_servers.split(','),
            'group_id': self.group_id,
            'consumer_timeout_ms': self.consumer_timeout_ms,
            'auto_offset_reset': self.auto_offset_reset,
            'enable_auto_commit': True,
            'value_deserializer': self._safe_deserializer,
        }
        
        # SSL configuration
        if 'SSL' in self.security_protocol:
            ssl_config = {}
            if self.ssl_cafile:
                ssl_config['ca_location'] = self.ssl_cafile
            if self.ssl_certfile:
                ssl_config['cert_location'] = self.ssl_certfile
            if self.ssl_keyfile:
                ssl_config['key_location'] = self.ssl_keyfile
            if ssl_config:
                config['security_protocol'] = self.security_protocol
                config['ssl_context'] = ssl_config
        
        # SASL configuration
        if 'SASL' in self.security_protocol:
            if not self.sasl_mechanism or not self.sasl_username or not self.sasl_password:
                logger.warning("SASL configured but credentials missing")
            else:
                config['security_protocol'] = self.security_protocol
                config['sasl_mechanism'] = self.sasl_mechanism
                config['sasl_plain_username'] = self.sasl_username
                config['sasl_plain_password'] = self.sasl_password
        
        return config
    
    def _safe_deserializer(self, m: bytes) -> str:
        """Safe deserializer with error handling"""
        try:
            return m.decode('utf-8')
        except UnicodeDecodeError:
            logger.warning("Failed to decode message as UTF-8, using hex representation")
            return m.hex()
    
    def _connect_with_retry(self) -> bool:
        """Connect to Kafka with retry logic"""
        for attempt in range(self.max_retries):
            try:
                config = self._get_consumer_config()
                self.consumer = KafkaConsumer(
                    *self.topics,
                    **config
                )
                logger.info("Connection to Kafka established successfully")
                return True
            except KafkaError as e:
                self.metrics.record_error(type(e).__name__)
                if attempt < self.max_retries - 1:
                    wait_time = self.retry_backoff_ms * (2 ** attempt) / 1000
                    logger.warning(f"Connection attempt {attempt + 1} failed: {e}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to connect after {self.max_retries} attempts: {e}")
                    return False
            except Exception as e:
                logger.error(f"Unexpected error during connection: {e}")
                self.metrics.record_error(type(e).__name__)
                return False
        return False
    
    def connect(self) -> bool:
        """Establish connection to Kafka"""
        return self._connect_with_retry()
    
    def disconnect(self):
        """Close connection to Kafka"""
        with self._lock:
            if self.consumer:
                try:
                    self.consumer.close()
                    logger.info("Connection to Kafka closed")
                except Exception as e:
                    logger.error(f"Error closing connection: {e}")
                finally:
                    self.consumer = None
            
            if self.dlq_producer:
                try:
                    self.dlq_producer.flush(timeout=10)
                    self.dlq_producer.close()
                except Exception as e:
                    logger.error(f"Error closing DLQ producer: {e}")
                finally:
                    self.dlq_producer = None
            
            # Close Elasticsearch exporter
            if self.es_exporter:
                try:
                    self.es_exporter.close()
                except Exception as e:
                    logger.error(f"Error closing Elasticsearch exporter: {e}")
    
    def _send_to_dlq(self, event_data: Dict[str, Any], error: str):
        """Send failed event to dead letter queue"""
        if not self.dlq_enabled:
            return
        
        try:
            from kafka import KafkaProducer
            
            if not self.dlq_producer:
                config = {'bootstrap_servers': self.bootstrap_servers.split(',')}
                if 'SSL' in self.security_protocol:
                    config['security_protocol'] = self.security_protocol
                if 'SASL' in self.security_protocol and self.sasl_username:
                    config['security_protocol'] = self.security_protocol
                    config['sasl_mechanism'] = self.sasl_mechanism
                    config['sasl_plain_username'] = self.sasl_username
                    config['sasl_plain_password'] = self.sasl_password
                
                self.dlq_producer = KafkaProducer(**config)
            
            dlq_message = {
                'original_event': event_data,
                'error': error,
                'timestamp': datetime.now().isoformat()
            }
            
            self.dlq_producer.send(
                self.dlq_topic,
                value=json.dumps(dlq_message).encode('utf-8'),
                key=event_data.get('key', '').encode('utf-8') if event_data.get('key') else None
            )
            self.metrics.record_dlq()
            logger.info(f"Event sent to DLQ: {self.dlq_topic}")
        except Exception as e:
            logger.error(f"Failed to send event to DLQ: {e}")
    
    def _process_batch(
        self,
        batch: List[Dict[str, Any]],
        callback: Optional[Callable] = None,
        batch_callback: Optional[Callable] = None,
    ):
        """Process a batch of events. Use batch_callback for the whole batch, else callback per event."""
        if not batch:
            return
        
        try:
            if batch_callback:
                # Batch callback: pass whole list
                batch_callback(batch)
                self.metrics.record_batch(len(batch))
                for event in batch:
                    self.metrics.record_event(event['topic'], success=True)
                    if self.es_exporter:
                        self.es_exporter.export_event(event, {'batch_processed': True})
                return
            
            # Process individually with event callback
            for event_data in batch:
                try:
                    if callback:
                        callback(event_data)
                    self.metrics.record_event(event_data['topic'], success=True)
                    # Export to Elasticsearch if enabled
                    if self.es_exporter:
                        self.es_exporter.export_event(event_data, {'success': True})
                except Exception as e:
                    error_msg = str(e)
                    logger.error(f"Error processing event: {error_msg}")
                    self.metrics.record_event(event_data['topic'], success=False)
                    self.metrics.record_error(type(e).__name__)
                    # Export failed event to Elasticsearch
                    if self.es_exporter:
                        self.es_exporter.export_event(event_data, {'success': False, 'error': error_msg})
                    self._send_to_dlq(event_data, error_msg)
        except Exception as e:
            logger.error(f"Error processing batch: {e}")
            self.metrics.record_error(type(e).__name__)
    
    def _update_lag_metrics(self):
        """Update consumer lag metrics"""
        if not self.consumer:
            return
        
        try:
            partitions = list(self.consumer.assignment())
            if not partitions:
                return
            
            # Get end offsets (high water marks)
            end_offsets = self.consumer.end_offsets(partitions)
            
            for partition in partitions:
                try:
                    committed = self.consumer.committed(partition)
                    end_offset = end_offsets.get(partition)
                    
                    if committed is not None and end_offset is not None:
                        lag = end_offset - committed
                        self.metrics.update_lag(partition.partition, lag)
                except Exception as e:
                    logger.debug(f"Could not update lag for partition {partition}: {e}")
        except Exception as e:
            logger.debug(f"Could not update lag metrics: {e}")
    
    def monitor(self, callback: Optional[Callable] = None, batch_callback: Optional[Callable] = None):
        """
        Monitor Kafka events with production-ready features
        
        Args:
            callback: Optional callback function called for each event
                     Accepts dict with keys: 'topic', 'partition', 'offset', 'key', 'value', 'timestamp'
            batch_callback: Optional callback function for batch processing
                          Accepts list of event dicts
        """
        self.is_running = True
        logger.info(f"Started monitoring topics: {self.topics}")
        
        batch = []
        batch_start_time = time.time()
        
        while self.is_running:
            try:
                # Reconnect if needed
                if not self.consumer:
                    logger.info("Reconnecting to Kafka...")
                    if not self._connect_with_retry():
                        logger.warning(f"Reconnection failed. Retrying in {self.reconnect_interval}s...")
                        time.sleep(self.reconnect_interval)
                        continue
                    self.metrics.record_reconnection()
                
                # Poll for messages
                message_pack = self.consumer.poll(timeout_ms=self.monitor_interval_ms)
                
                if not message_pack:
                    # Process batch if timeout reached
                    if batch and (time.time() - batch_start_time) * 1000 >= self.batch_timeout_ms:
                        self._process_batch(batch, callback=callback, batch_callback=batch_callback)
                        batch = []
                        batch_start_time = time.time()
                    continue
                
                # Process messages
                for topic_partition, messages in message_pack.items():
                    for message in messages:
                        event_data = {
                            'topic': message.topic,
                            'partition': message.partition,
                            'offset': message.offset,
                            'key': message.key.decode('utf-8') if message.key else None,
                            'value': message.value,
                            'timestamp': message.timestamp
                        }
                        
                        # Add to batch
                        batch.append(event_data)
                        
                        # Process batch if size reached
                        if len(batch) >= self.batch_size:
                            self._process_batch(batch, callback=callback, batch_callback=batch_callback)
                            batch = []
                            batch_start_time = time.time()
                
                # Update lag metrics and export to Elasticsearch periodically
                if self.metrics.events_processed % 100 == 0:
                    self._update_lag_metrics()
                    # Export metrics to Elasticsearch
                    if self.es_exporter:
                        stats = self.metrics.get_stats()
                        self.es_exporter.export_metrics(stats)
                
            except KeyboardInterrupt:
                logger.info("Monitoring interrupted (Ctrl+C)")
                break
            except (KafkaError, KafkaTimeoutError) as e:
                logger.error(f"Kafka error during monitoring: {e}")
                self.metrics.record_error(type(e).__name__)
                self.consumer = None  # Force reconnection
                time.sleep(self.reconnect_interval)
            except Exception as e:
                logger.error(f"Unexpected error during monitoring: {e}")
                self.metrics.record_error(type(e).__name__)
                time.sleep(1)
        
        # Process remaining batch
        if batch:
            self._process_batch(batch, callback=callback, batch_callback=batch_callback)
        
        # Flush Elasticsearch buffers
        if self.es_exporter:
            self.es_exporter.flush_all()
        
        self.is_running = False
        stats = self.metrics.get_stats()
        logger.info(f"Monitoring ended. Processed {stats['events_processed']} events")
    
    def stop(self):
        """Stop monitoring"""
        self.is_running = False
        logger.info("Stopping monitor...")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get monitor statistics"""
        stats = self.metrics.get_stats()
        stats.update({
            'bootstrap_servers': self.bootstrap_servers,
            'topics': self.topics,
            'group_id': self.group_id,
            'is_running': self.is_running,
            'is_connected': self.consumer is not None,
            'batch_size': self.batch_size,
            'dlq_enabled': self.dlq_enabled
        })
        return stats
    
    def health_check(self) -> Dict[str, Any]:
        """Perform health check"""
        is_healthy = True
        issues = []
        
        if not self.consumer:
            is_healthy = False
            issues.append("Not connected to Kafka")
        
        stats = self.metrics.get_stats()
        if stats['events_processed'] > 0:
            if stats['success_rate'] < 95:
                is_healthy = False
                issues.append(f"Low success rate: {stats['success_rate']}%")
        
        if stats['reconnection_count'] > 10:
            is_healthy = False
            issues.append(f"High reconnection count: {stats['reconnection_count']}")
        
        return {
            'healthy': is_healthy,
            'issues': issues,
            'stats': stats
        }

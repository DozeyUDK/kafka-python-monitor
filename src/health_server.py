"""
Health check and metrics HTTP server for Kafka Python Monitor

Provides HTTP endpoints for:
- Health checks (for Kubernetes/Docker)
- JSON metrics endpoint
- Prometheus metrics endpoint (for Grafana integration)
"""
import os
import json
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Optional
from src.kafka_monitor import KafkaMonitor
import logging

logger = logging.getLogger(__name__)


class HealthHandler(BaseHTTPRequestHandler):
    """HTTP handler for health checks and metrics"""
    
    monitor: Optional[KafkaMonitor] = None
    
    def do_GET(self):
        """Handle GET requests"""
        if self.path == '/health':
            self._handle_health()
        elif self.path == '/metrics' or self.path.startswith('/metrics?'):
            # Check if Prometheus format requested
            if 'format=prometheus' in self.path or 'Accept' in self.headers and 'text/plain' in self.headers.get('Accept', ''):
                self._handle_prometheus_metrics()
            else:
                self._handle_metrics()
        elif self.path == '/stats':
            self._handle_stats()
        elif self.path == '/prometheus':
            self._handle_prometheus_metrics()
        else:
            self._send_response(404, {'error': 'Not found'})
    
    def _handle_health(self):
        """Handle health check endpoint"""
        if not self.monitor:
            self._send_response(503, {'healthy': False, 'error': 'Monitor not initialized'})
            return
        
        health = self.monitor.health_check()
        status_code = 200 if health['healthy'] else 503
        self._send_response(status_code, health)
    
    def _handle_metrics(self):
        """Handle metrics endpoint (JSON format)"""
        if not self.monitor:
            self._send_response(503, {'error': 'Monitor not initialized'})
            return
        
        stats = self.monitor.get_stats()
        self._send_response(200, stats)
    
    def _handle_prometheus_metrics(self):
        """Handle Prometheus metrics endpoint for Grafana"""
        if not self.monitor:
            self._send_response(503, 'Monitor not initialized')
            return
        
        stats = self.monitor.get_stats()
        prometheus_output = self._format_prometheus_metrics(stats)
        
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain; version=0.0.4')
        self.end_headers()
        self.wfile.write(prometheus_output.encode('utf-8'))
    
    def _format_prometheus_metrics(self, stats: dict) -> str:
        """Format metrics in Prometheus format"""
        lines = []
        
        # Basic metrics
        lines.append(f'# HELP kafka_monitor_events_processed Total events processed')
        lines.append(f'# TYPE kafka_monitor_events_processed counter')
        lines.append(f'kafka_monitor_events_processed {stats.get("events_processed", 0)}')
        
        lines.append(f'# HELP kafka_monitor_events_failed Total events failed')
        lines.append(f'# TYPE kafka_monitor_events_failed counter')
        lines.append(f'kafka_monitor_events_failed {stats.get("events_failed", 0)}')
        
        lines.append(f'# HELP kafka_monitor_events_dlq Total events sent to DLQ')
        lines.append(f'# TYPE kafka_monitor_events_dlq counter')
        lines.append(f'kafka_monitor_events_dlq {stats.get("events_dlq", 0)}')
        
        lines.append(f'# HELP kafka_monitor_batches_processed Total batches processed')
        lines.append(f'# TYPE kafka_monitor_batches_processed counter')
        lines.append(f'kafka_monitor_batches_processed {stats.get("batches_processed", 0)}')
        
        lines.append(f'# HELP kafka_monitor_reconnection_count Total reconnections')
        lines.append(f'# TYPE kafka_monitor_reconnection_count counter')
        lines.append(f'kafka_monitor_reconnection_count {stats.get("reconnection_count", 0)}')
        
        lines.append(f'# HELP kafka_monitor_throughput_events_per_second Current throughput')
        lines.append(f'# TYPE kafka_monitor_throughput_events_per_second gauge')
        lines.append(f'kafka_monitor_throughput_events_per_second {stats.get("throughput_events_per_second", 0)}')
        
        lines.append(f'# HELP kafka_monitor_success_rate Success rate percentage')
        lines.append(f'# TYPE kafka_monitor_success_rate gauge')
        lines.append(f'kafka_monitor_success_rate {stats.get("success_rate", 100.0)}')
        
        lines.append(f'# HELP kafka_monitor_uptime_seconds Uptime in seconds')
        lines.append(f'# TYPE kafka_monitor_uptime_seconds gauge')
        lines.append(f'kafka_monitor_uptime_seconds {stats.get("uptime_seconds", 0)}')
        
        lines.append(f'# HELP kafka_monitor_is_running Whether monitor is running')
        lines.append(f'# TYPE kafka_monitor_is_running gauge')
        lines.append(f'kafka_monitor_is_running {1 if stats.get("is_running", False) else 0}')
        
        lines.append(f'# HELP kafka_monitor_is_connected Whether connected to Kafka')
        lines.append(f'# TYPE kafka_monitor_is_connected gauge')
        lines.append(f'kafka_monitor_is_connected {1 if stats.get("is_connected", False) else 0}')
        
        # Topic-specific metrics
        if 'events_by_topic' in stats:
            lines.append(f'# HELP kafka_monitor_events_by_topic Events processed per topic')
            lines.append(f'# TYPE kafka_monitor_events_by_topic counter')
            for topic, count in stats['events_by_topic'].items():
                lines.append(f'kafka_monitor_events_by_topic{{topic="{topic}"}} {count}')
        
        # Error metrics
        if 'errors_by_type' in stats:
            lines.append(f'# HELP kafka_monitor_errors_by_type Errors by error type')
            lines.append(f'# TYPE kafka_monitor_errors_by_type counter')
            for error_type, count in stats['errors_by_type'].items():
                # Sanitize error type name for Prometheus
                error_type_safe = error_type.replace('-', '_').replace('.', '_').replace(' ', '_')
                lines.append(f'kafka_monitor_errors_by_type{{error_type="{error_type_safe}"}} {count}')
        
        # Lag metrics
        if 'lag_by_partition' in stats:
            lines.append(f'# HELP kafka_monitor_consumer_lag Consumer lag per partition')
            lines.append(f'# TYPE kafka_monitor_consumer_lag gauge')
            for partition, lag in stats['lag_by_partition'].items():
                lines.append(f'kafka_monitor_consumer_lag{{partition="{partition}"}} {lag}')
        
        return '\n'.join(lines) + '\n'
    
    def _handle_stats(self):
        """Handle stats endpoint (alias for metrics)"""
        self._handle_metrics()
    
    def _send_response(self, status_code: int, data):
        """Send JSON response"""
        self.send_response(status_code)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        if isinstance(data, dict):
            self.wfile.write(json.dumps(data, indent=2).encode('utf-8'))
        else:
            self.wfile.write(str(data).encode('utf-8'))
    
    def log_message(self, format, *args):
        """Override to use our logger"""
        logger.info(f"{self.address_string()} - {format % args}")


def start_health_server(monitor: KafkaMonitor, port: int = 8080):
    """Start health check HTTP server"""
    HealthHandler.monitor = monitor
    server = HTTPServer(('0.0.0.0', port), HealthHandler)
    logger.info(f"Health check server started on port {port}")
    logger.info(f"  - Health: http://localhost:{port}/health")
    logger.info(f"  - Metrics: http://localhost:{port}/metrics")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Health check server stopped")
        server.shutdown()

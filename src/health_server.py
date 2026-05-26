"""
Health check and metrics HTTP server for Kafka Python Monitor

Provides HTTP endpoints for:
- Health checks (for Kubernetes/Docker)
- JSON metrics endpoint
- Prometheus metrics endpoint (for Grafana integration)
- Basic authentication (optional)
- CORS support (configurable)
"""
import os
import base64
import json
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Optional
from src.kafka_monitor import KafkaMonitor
import logging

logger = logging.getLogger(__name__)


class HealthHandler(BaseHTTPRequestHandler):
    """HTTP handler for health checks and metrics"""
    
    monitor: Optional[KafkaMonitor] = None
    
    # Security configuration (class-level)
    auth_enabled = False
    auth_username = ""
    auth_password = ""
    cors_enabled = False
    cors_allowed_origins = "*"
    
    def _check_auth(self) -> bool:
        """Check Basic Auth credentials. Returns True if allowed."""
        if not self.auth_enabled:
            return True
        
        auth_header = self.headers.get('Authorization', '')
        if not auth_header.startswith('Basic '):
            return False
        
        try:
            encoded = auth_header[6:]
            decoded = base64.b64decode(encoded).decode('utf-8')
            username, password = decoded.split(':', 1)
            return username == self.auth_username and password == self.auth_password
        except Exception:
            return False
    
    def _send_auth_required(self):
        """Send 401 Unauthorized with WWW-Authenticate header"""
        self.send_response(401)
        self.send_header('WWW-Authenticate', 'Basic realm="Kafka Monitor"')
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(b'{"error": "Authentication required"}')
    
    def _add_security_headers(self):
        """Add security headers to response"""
        self.send_header('X-Content-Type-Options', 'nosniff')
        self.send_header('X-Frame-Options', 'DENY')
        self.send_header('X-XSS-Protection', '1; mode=block')
        self.send_header('Strict-Transport-Security', 'max-age=31536000; includeSubDomains')
        
        if self.cors_enabled:
            self.send_header('Access-Control-Allow-Origin', self.cors_allowed_origins)
            self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
            self.send_header('Access-Control-Allow-Headers', 'Authorization, Content-Type')
    
    def do_OPTIONS(self):
        """Handle CORS preflight requests"""
        self.send_response(204)
        self._add_security_headers()
        self.end_headers()
    
    def do_GET(self):
        """Handle GET requests"""
        # Check authentication
        if not self._check_auth():
            self._send_auth_required()
            return
        
        path = self.path.split('?')[0]
        if path == '/health':
            self._handle_health()
        elif path == '/live':
            self._handle_live()
        elif path == '/ready':
            self._handle_ready()
        elif path == '/metrics':
            # Prometheus format by default (Grafana/K8s); ?format=json for JSON
            if 'format=json' in (self.path or ''):
                self._handle_metrics()
            else:
                self._handle_prometheus_metrics()
        elif path == '/stats':
            self._handle_stats()
        elif path == '/prometheus':
            self._handle_prometheus_metrics()
        else:
            self._send_response(404, {'error': 'Not found'})
    
    def _handle_live(self):
        """Liveness: process is running (always 200). For Kubernetes livenessProbe."""
        self.send_response(200)
        self._add_security_headers()
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(b'{"alive": true}\n')

    def _handle_ready(self):
        """Readiness: ready to consume (Kafka connected). For Kubernetes readinessProbe."""
        if not self.monitor:
            self._send_response(503, {'ready': False, 'error': 'Monitor not initialized'})
            return
        health = self.monitor.health_check()
        status_code = 200 if health['healthy'] else 503
        self._send_response(status_code, {'ready': health['healthy'], 'issues': health.get('issues', [])})

    def _handle_health(self):
        """Full health check (healthy, issues, stats). Use /ready for K8s readiness."""
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
        self._add_security_headers()
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
                # Ensure partition is string (partition can be int key from defaultdict)
                partition_str = str(int(partition)) if isinstance(partition, int) else str(partition)
                lines.append(f'kafka_monitor_consumer_lag{{partition="{partition_str}"}} {lag}')
        
        return '\n'.join(lines) + '\n'
    
    def _handle_stats(self):
        """Handle stats endpoint (alias for metrics)"""
        self._handle_metrics()
    
    def _send_response(self, status_code: int, data):
        """Send JSON response with security headers"""
        self.send_response(status_code)
        self._add_security_headers()
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
    """Start health check HTTP server with optional authentication and CORS"""
    HealthHandler.monitor = monitor
    
    # Configure authentication
    HealthHandler.auth_enabled = os.getenv('HEALTH_AUTH_ENABLED', 'false').lower() == 'true'
    if HealthHandler.auth_enabled:
        HealthHandler.auth_username = os.getenv('HEALTH_AUTH_USERNAME', '')
        HealthHandler.auth_password = os.getenv('HEALTH_AUTH_PASSWORD', '')
        if not HealthHandler.auth_username or not HealthHandler.auth_password:
            logger.warning("HEALTH_AUTH_ENABLED=true but credentials not set. Auth DISABLED.")
            HealthHandler.auth_enabled = False
        else:
            logger.info("Basic authentication ENABLED for health server")
    
    # Configure CORS
    HealthHandler.cors_enabled = os.getenv('HEALTH_CORS_ENABLED', 'false').lower() == 'true'
    if HealthHandler.cors_enabled:
        HealthHandler.cors_allowed_origins = os.getenv('HEALTH_CORS_ORIGINS', '*')
        logger.info(f"CORS ENABLED for origins: {HealthHandler.cors_allowed_origins}")
    
    host = os.getenv('HEALTH_SERVER_HOST', '0.0.0.0')
    server = HTTPServer((host, port), HealthHandler)
    logger.info(f"Health check server started on {host}:{port}")
    logger.info(f"  - Health: http://localhost:{port}/health")
    logger.info(f"  - Metrics: http://localhost:{port}/metrics")
    logger.info(f"  - Auth: {'ENABLED' if HealthHandler.auth_enabled else 'DISABLED'}")
    logger.info(f"  - CORS: {'ENABLED' if HealthHandler.cors_enabled else 'DISABLED'}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Health check server stopped")
        server.shutdown()

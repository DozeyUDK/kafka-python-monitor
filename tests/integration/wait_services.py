"""Wait for disposable Kafka and Elasticsearch CI services (never production)."""
import os
import sys
import time

from elasticsearch import Elasticsearch
from kafka import KafkaAdminClient

BROKER = os.getenv("KAFKA_TEST_BROKER", "localhost:29092")
ES_URL = os.getenv("ELASTICSEARCH_TEST_URL", "http://localhost:19200")
EXPECTED_BROKERS = int(os.getenv("KAFKA_EXPECTED_BROKERS", "1"))

def main():
    deadline = time.monotonic() + 150
    kafka_ready = False
    elastic_ready = False
    while time.monotonic() < deadline:
        if not kafka_ready:
            try:
                admin = KafkaAdminClient(bootstrap_servers=BROKER, request_timeout_ms=5000)
                brokers = admin.describe_cluster()["brokers"]
                kafka_ready = len(brokers) >= EXPECTED_BROKERS
                admin.close()
            except Exception:
                pass
        if not elastic_ready:
            try:
                client = Elasticsearch(ES_URL, request_timeout=3)
                elastic_ready = client.ping()
                client.close()
            except Exception:
                pass
        if kafka_ready and elastic_ready:
            print("Kafka and Elasticsearch are ready")
            return 0
        time.sleep(3)
    print("Timed out: kafka_ready=%s elasticsearch_ready=%s" % (kafka_ready, elastic_ready), file=sys.stderr)
    return 1

if __name__ == "__main__":
    sys.exit(main())

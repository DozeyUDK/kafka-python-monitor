"""Explicit opt-in Elasticsearch index/retention bootstrap.

Read-only by default. Run with --apply to change the target cluster.
Requires indices:admin/index_template/put and cluster:admin/ilm/put privileges.
"""
import argparse
import json
import os
from pathlib import Path

from elasticsearch import Elasticsearch

HERE = Path(__file__).resolve().parent
RESOURCES = (
    ("ilm", "kafka-monitor-events-30d", "ilm-events.json"),
    ("ilm", "kafka-monitor-metrics-7d", "ilm-metrics.json"),
    ("template", "kafka-monitor-events", "events-template.json"),
    ("template", "kafka-monitor-metrics", "metrics-template.json"),
)


def load_resources():
    return [(kind, name, json.loads((HERE / filename).read_text(encoding="utf-8")))
            for kind, name, filename in RESOURCES]


def apply_templates(client):
    """Apply versioned templates/policies to an explicitly selected cluster."""
    for kind, name, payload in load_resources():
        if kind == "ilm":
            client.ilm.put_lifecycle(name=name, policy=payload["policy"])
        else:
            client.indices.put_index_template(name=name, **payload)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Actually modify the target cluster")
    args = parser.parse_args()
    resources = load_resources()
    for kind, name, payload in resources:
        print("%s %s: %s" % (kind, name, json.dumps(payload, sort_keys=True)))
    if not args.apply:
        print("Dry run. Nothing applied. Use --apply to update an intentionally selected cluster.")
        return 0

    urls = [s.strip() for s in os.environ.get("ELASTICSEARCH_HOSTS", "").split(",") if s.strip()]
    if not urls or any(not url.startswith(("https://", "http://")) for url in urls):
        parser.error("Set explicit ELASTICSEARCH_HOSTS=http(s)://host:port")
    if any(url.startswith("http://") and not url.startswith(
        ("http://localhost:", "http://127.0.0.1:", "http://[::1]:")
    ) for url in urls):
        parser.error("Refusing non-local HTTP Elasticsearch URL; use HTTPS")
    kwargs = {"hosts": urls, "verify_certs": True, "request_timeout": 30}
    if os.environ.get("ELASTICSEARCH_CA_CERTS"):
        kwargs["ca_certs"] = os.environ["ELASTICSEARCH_CA_CERTS"]
    if os.environ.get("ELASTICSEARCH_API_KEY"):
        kwargs["api_key"] = os.environ["ELASTICSEARCH_API_KEY"]
    elif os.environ.get("ELASTICSEARCH_USERNAME") and os.environ.get("ELASTICSEARCH_PASSWORD"):
        kwargs["basic_auth"] = (
            os.environ["ELASTICSEARCH_USERNAME"], os.environ["ELASTICSEARCH_PASSWORD"]
        )
    with Elasticsearch(**kwargs) as client:
        if not client.ping():
            raise ConnectionError("Unable to reach target Elasticsearch cluster")
        apply_templates(client)
    print("Elasticsearch templates and retention policies applied.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

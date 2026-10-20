"""Offline tests for index lifecycle templates and safe deployment defaults."""
import json
from pathlib import Path

from ops.elasticsearch.apply_templates import load_resources


ROOT = Path(__file__).resolve().parents[1]


def test_elasticsearch_templates_match_exporter_indexes():
    resources = load_resources()
    assert len(resources) == 4
    policy_names = {name for kind, name, _ in resources if kind == "ilm"}
    assert policy_names == {"kafka-monitor-events-30d", "kafka-monitor-metrics-7d"}
    for kind, name, doc in resources:
        if kind == "ilm":
            assert doc["policy"]["phases"]["delete"]["actions"] == {"delete": {}}
        else:
            assert doc["template"]["settings"]["index.lifecycle.name"] in policy_names
            assert doc["template"]["settings"]["number_of_shards"] == 1
            assert doc["index_patterns"] == [
                "kafka-events-*" if "events" in name else "kafka-metrics-*"
            ]


def test_templates_do_not_provision_unbounded_dynamic_fields():
    for kind, name, doc in load_resources():
        if kind == "template":
            assert doc["template"]["mappings"]["dynamic"] is False
            assert doc["template"]["settings"]["index.mapping.total_fields.limit"] <= 200


def test_deploy_configuration_does_not_expose_http_publicly():
    text = (ROOT / "deploy" / "compose.example.yml").read_text(encoding="utf-8")
    assert '"127.0.0.1:8080:8080"' in text
    for safety in ("read_only: true", "no-new-privileges:true", "cap_drop:", "mem_limit:"):
        assert safety in text

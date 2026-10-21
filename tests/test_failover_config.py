"""Offline checks that the opt-in failover CI cannot target production."""
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]


def test_failover_compose_has_three_unique_brokers_and_loopback_ports():
    compose = (ROOT / "tests/integration/compose-multibroker.yml").read_text()
    assert "NEVER deploy to production" in compose
    assert compose.count("image: apache/kafka:3.9.1") == 3
    assert set(re.findall(r'KAFKA_NODE_ID: "(\d+)"', compose)) == {"1", "2", "3"}
    assert compose.count("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: \"3\"") == 3
    assert compose.count("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: \"2\"") == 3
    assert set(re.findall(r'"127\.0\.0\.1:(2919[123]):9092"', compose)) == {
        "29191", "29192", "29193"
    }
    assert '"127.0.0.1:19201:9200"' in compose


def test_failover_test_guards_docker_actions_and_enforces_ci():
    src = (ROOT / "tests/integration/test_broker_failover.py").read_text()
    for expected in (
        'os.getenv("GITHUB_ACTIONS") != "true"',
        'os.getenv("CI") != "true"',
        'os.getenv("KAFKA_MULTIBROKER") != "1"',
        '"stop", "start"',
        '"broker1", "broker2", "broker3"',
        "wait_for_new_leader(",
        "run_compose(\"stop\", stopped_node)",
        "run_compose(\"start\", stopped_node)",
    ):
        assert expected in src


def test_multi_broker_job_does_not_run_in_single_broker_workflow():
    workflow = (ROOT / ".github/workflows/multibroker.yml").read_text()
    assert "kafka-monitor-failover-ci" in workflow
    assert 'KAFKA_MULTIBROKER: "1"' in workflow
    assert "-m multibroker" in workflow
    assert "down -v --remove-orphans" in workflow
    single = (ROOT / ".github/workflows/integration.yml").read_text()
    assert "-m integration tests/integration" in single

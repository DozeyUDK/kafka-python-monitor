# Contributing

## Development

    git clone https://github.com/DozeyUDK/kafka-python-monitor.git
    cd kafka-python-monitor
    python -m venv .venv
    . .venv/bin/activate
    pip install -r requirements.txt
    pip install pytest
    cp env.example .env

Use fake clients for unit tests. Real broker and Elasticsearch integration tests must run against disposable test infrastructure, not production clusters.

    python -m pytest -q
    python -m compileall -q program.py src

Before opening a pull request, update tests and document any change to Kafka commit behavior, Elasticsearch bulk handling or delivery guarantees. Do not log Kafka payloads or include credentials and secrets in code or fixtures.

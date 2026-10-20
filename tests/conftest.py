"""Allow unit tests to run without installing service dependencies globally.

Real dependency imports are used when installed. These minimal stubs only
enable isolated unit tests; integration testing requires the actual packages.
"""
import importlib.util
import sys
import types
import warnings

if importlib.util.find_spec("dotenv") is None:
    dotenv = types.ModuleType("dotenv")
    dotenv.load_dotenv = lambda *a, **kw: None
    sys.modules["dotenv"] = dotenv
    warnings.warn("Unit tests using dotenv stub; install requirements for integration tests.")

if importlib.util.find_spec("kafka") is None:
    kafka = types.ModuleType("kafka")
    kafka.KafkaConsumer = type("KafkaConsumerStub", (), {})
    kafka.KafkaProducer = type("KafkaProducerStub", (), {})
    errors = types.ModuleType("kafka.errors")
    errors.KafkaError = type("KafkaError", (Exception,), {})
    kafka.errors = errors
    sys.modules["kafka"] = kafka
    sys.modules["kafka.errors"] = errors
    warnings.warn("Unit tests using Kafka stub; no broker connection tested.")

if importlib.util.find_spec("elasticsearch") is None:
    elasticsearch = types.ModuleType("elasticsearch")
    elasticsearch.Elasticsearch = type("ElasticsearchStub", (), {})
    helpers = types.ModuleType("elasticsearch.helpers")
    def no_bulk(*args, **kwargs):
        raise AssertionError("Unit test requires bulk to be mocked explicitly")
    helpers.bulk = no_bulk
    elasticsearch.helpers = helpers
    sys.modules["elasticsearch"] = elasticsearch
    sys.modules["elasticsearch.helpers"] = helpers
    warnings.warn("Unit tests using Elasticsearch stub; no cluster connection tested.")

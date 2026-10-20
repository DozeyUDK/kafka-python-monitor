"""
Setup script for kafka-python-monitor
"""
from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="kafka-python-monitor",
    version="1.0.0",
    author="",
    description="Lightweight Kafka consumer with optional Elasticsearch export and Prometheus metrics",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/DozeyUDK/kafka-python-monitor",
    packages=find_packages(),
    py_modules=["program"],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Monitoring",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    python_requires=">=3.10",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "kafka-monitor=program:main",
        ],
    },
)

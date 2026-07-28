"""Common pytest setup for the retail sales unit tests."""

import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession


SRC_PATH = Path(__file__).resolve().parents[1] / "src"

if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))


@pytest.fixture(scope="session")
def spark():
    """Provide one Spark session for the complete test session."""

    session = SparkSession.getActiveSession()

    if session is None:
        session = (
            SparkSession.builder
            .appName("retail-sales-tests")
            .master("local[2]")
            .getOrCreate()
        )

    session.sparkContext.setLogLevel("ERROR")

    return session

"""Common pytest setup."""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Provide one Spark session for all tests."""

    session = SparkSession.getActiveSession()

    if session is None:
        session = (
            SparkSession.builder
            .appName("retail-sales-tests")
            .getOrCreate()
        )

    session.sparkContext.setLogLevel("ERROR")

    return session

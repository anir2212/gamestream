"""Local Spark DataFrame checks; external Iceberg/Kafka are tested separately."""

import os
import sys
from datetime import UTC, datetime, timedelta

import pytest
from pyspark.sql import SparkSession

from spark.common import deduplicate_silver, session_counts


@pytest.fixture(scope="module")
def spark():
    previous = os.environ.get("PYSPARK_PYTHON")
    os.environ["PYSPARK_PYTHON"] = sys.executable
    try:
        session = (
            SparkSession.builder.master("local[1]").appName("gamestream-tests")
            .config("spark.ui.enabled", "false").getOrCreate()
        )
        try:
            yield session
        finally:
            session.stop()
    finally:
        if previous is None:
            os.environ.pop("PYSPARK_PYTHON", None)
        else:
            os.environ["PYSPARK_PYTHON"] = previous


def test_static_deduplication_by_event_id(spark):
    rows = [("e1", "u1"), ("e1", "u1"), ("e2", "u2")]
    frame = spark.createDataFrame(rows, ["event_id", "user_id"])
    assert deduplicate_silver(frame).count() == 2


def test_thirty_minute_inactivity_sessions(spark):
    start = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [
        ("e1", "u1", "g1", start),
        ("e2", "u1", "g1", start + timedelta(minutes=20)),
        ("e3", "u1", "g1", start + timedelta(minutes=60)),
    ]
    frame = spark.createDataFrame(rows, ["event_id", "user_id", "game_id", "event_ts"])
    counts = sorted(row["event_count"] for row in session_counts(frame).collect())
    assert counts == [1, 2]

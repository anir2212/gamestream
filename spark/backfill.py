"""Replay a bounded slice of bronze into silver using target-side idempotent merge."""

from __future__ import annotations

import sys
from datetime import date, timedelta

from spark.common import parse_bronze, spark_session
from spark.stream_silver import merge_batch


def main(start: str, end: str) -> None:
    first, last = date.fromisoformat(start), date.fromisoformat(end)
    if first > last or (last - first).days > 31:
        raise ValueError("Backfill range must be ascending and at most 31 days")
    spark = spark_session("gamestream-backfill")
    # Read one extra Kafka day: an event on `last` can arrive just after midnight.
    ingest_last = last + timedelta(days=1)
    bronze = spark.table("lake.bronze.events").where(
        f"CAST(kafka_ts AS DATE) BETWEEN DATE '{first}' AND DATE '{ingest_last}'"
    )
    parsed = parse_bronze(bronze, "http://registry:8080/apis/ccompat/v7")
    filtered = parsed.where(f"CAST(event_ts AS DATE) BETWEEN DATE '{first}' AND DATE '{last}'")
    merge_batch(filtered, 0)
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("Usage: backfill.py START_DATE END_DATE")
    main(sys.argv[1], sys.argv[2])

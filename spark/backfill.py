"""Replay a bounded slice of bronze into silver using target-side idempotent merge."""

from __future__ import annotations

import sys
from datetime import date

from spark.common import parse_bronze, spark_session
from spark.stream_silver import merge_batch


def main(start: str, end: str) -> None:
    first, last = date.fromisoformat(start), date.fromisoformat(end)
    if first > last or (last - first).days >= 31:
        raise ValueError("Backfill range must be ascending and at most 31 days")
    spark = spark_session("gamestream-backfill")
    try:
        # Bronze is partitioned by Kafka arrival day, not event day. A late event
        # can arrive after the requested range; pruning arrival days would lose it.
        bronze = spark.table("lake.bronze.events")
        parsed = parse_bronze(bronze, "http://registry:8080/apis/ccompat/v7")
        filtered = parsed.where(
            f"CAST(event_ts AS DATE) BETWEEN DATE '{first}' AND DATE '{last}'"
        )
        merge_batch(filtered, 0)
    finally:
        spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("Usage: backfill.py START_DATE END_DATE")
    main(sys.argv[1], sys.argv[2])

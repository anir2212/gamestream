"""Check the bounded Kafka-to-ClickHouse stage on a published fixture.

The Kafka source must already have consumed the fixture. This checks the serving
tables, not Kafka delivery acknowledgements or Iceberg/Trino parity.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def query(sql: str) -> dict:
    endpoint = os.getenv("GAMESTREAM_CLICKHOUSE_URL", "http://127.0.0.1:18123")
    params = urlencode({"user": "gamestream", "password": "local-demo-only"})
    request = Request(f"{endpoint}/?{params}", data=f"{sql} FORMAT JSONEachRow".encode())
    with urlopen(request, timeout=30) as response:
        rows = [json.loads(line) for line in response if line.strip()]
    if len(rows) != 1:
        raise AssertionError(f"Expected one result row, got {len(rows)}")
    return rows[0]


def verify(expected_unique: int | None = None) -> dict:
    events = query("""
        SELECT count() AS event_rows, uniqExact(event_id) AS unique_event_ids,
               countIf(schema_version = 1) AS schema_v1_rows,
               countIf(schema_version = 2) AS schema_v2_rows,
               countIf(schema_version = 1 AND device_type IS NOT NULL) AS v1_with_device,
               countIf(event_type = 'purchase') AS purchase_rows
        FROM gamestream.events FINAL
    """)
    minutes = query("""
        SELECT count() AS minute_game_buckets,
               sum(unique_events) AS unique_events,
               sum(unique_purchases) AS unique_purchases
        FROM (
            SELECT minute, game_id, uniqMerge(event_ids) AS unique_events,
                   uniqIfMerge(purchase_ids) AS unique_purchases
            FROM gamestream.minute_aggregates GROUP BY minute, game_id
        )
    """)
    if events["event_rows"] == 0:
        raise AssertionError("ClickHouse has no events")
    if events["event_rows"] != events["unique_event_ids"]:
        raise AssertionError("FINAL still has duplicate event IDs")
    if expected_unique is not None and events["unique_event_ids"] != expected_unique:
        raise AssertionError(f"Expected {expected_unique} IDs, got {events['unique_event_ids']}")
    if not events["schema_v1_rows"] or not events["schema_v2_rows"]:
        raise AssertionError("Expected both Avro schema versions")
    if events["v1_with_device"]:
        raise AssertionError("V1 rows unexpectedly have a device_type")
    if minutes["unique_events"] != events["unique_event_ids"]:
        raise AssertionError("Minute aggregates disagree on distinct event IDs")
    if minutes["unique_purchases"] != events["purchase_rows"]:
        raise AssertionError("Minute aggregates disagree on purchase IDs")
    return {
        "observed_at_utc": datetime.now(UTC).isoformat(),
        "status": "passed",
        "expected_unique": expected_unique,
        "events_final": events,
        "minute_aggregates": minutes,
        "scope": "one local ClickHouse serving stage; no Iceberg parity or fault-tolerance claim",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-unique", type=int)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = verify(args.expected_unique)
    rendered = json.dumps(result, indent=2) + "\n"
    if args.output:
        args.output.write_text(rendered)
    print(rendered, end="")


if __name__ == "__main__":
    main()

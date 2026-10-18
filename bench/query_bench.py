"""Measure five comparable analytical reads from Trino and ClickHouse.

The two stores are fed independently, so compare latency only after checking counts
and ingestion freshness. This script prints a reproducible JSON report; no numbers
are baked into the documentation.
"""

from __future__ import annotations

import ast
import hashlib
import json
import os
import statistics
import time
from urllib.parse import urlencode
from urllib.request import Request, urlopen

QUERIES = {
    "row_count": (
        "SELECT count(*) FROM lake.silver.events",
        "SELECT count(*) FROM gamestream.events FINAL",
    ),
    "purchase_count": (
        "SELECT count(*) FROM lake.silver.events WHERE event_type='purchase'",
        "SELECT count(*) FROM gamestream.events FINAL WHERE event_type='purchase'",
    ),
    "active_users": (
        "SELECT count(DISTINCT user_id) FROM lake.silver.events",
        "SELECT uniqExact(user_id) FROM gamestream.events FINAL",
    ),
    "revenue": (
        "SELECT sum(amount_cents) FROM lake.silver.events WHERE event_type='purchase'",
        "SELECT sum(amount_cents) FROM gamestream.events FINAL WHERE event_type='purchase'",
    ),
    "top_game": (
        "SELECT game_id,count(*) AS n FROM lake.silver.events GROUP BY 1 "
        "ORDER BY n DESC, game_id LIMIT 1",
        "SELECT game_id,count(*) AS n FROM gamestream.events FINAL "
        "GROUP BY 1 ORDER BY n DESC, game_id LIMIT 1",
    ),
}


def clickhouse_query(sql: str) -> str:
    endpoint = os.getenv("GAMESTREAM_CLICKHOUSE_URL", "http://127.0.0.1:18123")
    params = urlencode({"user": "gamestream", "password": "local-demo-only"})
    with urlopen(Request(f"{endpoint}/?{params}", data=sql.encode()), timeout=30) as response:
        return response.read().decode().strip()


def trino_query(sql: str) -> str:
    import trino

    connection = trino.dbapi.connect(
        host=os.getenv("GAMESTREAM_TRINO_HOST", "127.0.0.1"),
        port=int(os.getenv("GAMESTREAM_TRINO_PORT", "18082")),
        user="gamestream-bench", catalog="lake",
    )
    try:
        cursor = connection.cursor()
        cursor.execute(sql)
        return repr(cursor.fetchall())
    finally:
        connection.close()


def measure(fn, sql: str, repeats: int = 6) -> dict:
    samples = []
    result = None
    for i in range(repeats):
        started = time.perf_counter()
        result = fn(sql)
        elapsed = (time.perf_counter() - started) * 1000
        if i:  # the first query warms caches and connections
            samples.append(elapsed)
    return {
        "result": result,
        "samples_ms": samples,
        "p50_ms": statistics.median(samples),
        "p95_ms": sorted(samples)[int(0.95 * (len(samples) - 1))],
    }


def validate_parity() -> dict:
    """Refuse a latency comparison if the sinks cover different event IDs."""
    trino_rows = ast.literal_eval(trino_query(
        "SELECT event_id FROM lake.silver.events ORDER BY event_id"
    ))
    trino_ids = [row[0] for row in trino_rows]
    clickhouse_raw = clickhouse_query(
        "SELECT event_id FROM gamestream.events FINAL ORDER BY event_id FORMAT JSONEachRow"
    )
    clickhouse_ids = [json.loads(line)["event_id"] for line in clickhouse_raw.splitlines()]
    if len(trino_ids) != len(set(trino_ids)) or len(clickhouse_ids) != len(set(clickhouse_ids)):
        raise ValueError("At least one sink still contains duplicate event IDs")
    if trino_ids != clickhouse_ids:
        raise ValueError(
            "The sinks contain different event IDs; refusing a latency comparison "
            f"(Trino={len(trino_ids)}, ClickHouse={len(clickhouse_ids)})."
        )
    digest = hashlib.sha256("\n".join(trino_ids).encode()).hexdigest()
    return {"event_ids": len(trino_ids), "sha256": digest}


def normalize_result(value: str, *, trino_result: bool) -> tuple[str, ...]:
    if trino_result:
        rows = ast.literal_eval(value)
        if len(rows) != 1:
            raise ValueError(f"Expected one Trino result row, got {len(rows)}")
        return tuple("NULL" if item is None else str(item) for item in rows[0])
    lines = value.splitlines()
    if len(lines) != 1:
        raise ValueError(f"Expected one ClickHouse result row, got {len(lines)}")
    return tuple("NULL" if item == r"\N" else item for item in lines[0].split("\t"))


def main() -> None:
    result = {"parity": validate_parity(), "queries": {}}
    for name, (trino_sql, clickhouse_sql) in QUERIES.items():
        measurements = {
            "trino": measure(trino_query, trino_sql),
            "clickhouse": measure(clickhouse_query, clickhouse_sql),
        }
        if normalize_result(measurements["trino"]["result"], trino_result=True) != normalize_result(
            measurements["clickhouse"]["result"], trino_result=False
        ):
            raise ValueError(f"{name} returned different values; refusing a latency report")
        result["queries"][name] = measurements
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    try:
        main()
    except ValueError as error:
        raise SystemExit(str(error)) from error

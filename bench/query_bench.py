"""Measure five comparable analytical reads from Trino and ClickHouse.

The two stores are fed independently, so compare latency only after checking counts
and ingestion freshness. This script prints a reproducible JSON report; no numbers
are baked into the documentation.
"""

from __future__ import annotations

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
        "SELECT game_id,count(*) AS n FROM lake.silver.events GROUP BY 1 ORDER BY n DESC LIMIT 1",
        "SELECT game_id,count(*) AS n FROM gamestream.events FINAL "
        "GROUP BY 1 ORDER BY n DESC LIMIT 1",
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


def main() -> None:
    result = {}
    for name, (trino_sql, clickhouse_sql) in QUERIES.items():
        result[name] = {
            "trino": measure(trino_query, trino_sql),
            "clickhouse": measure(clickhouse_query, clickhouse_sql),
        }
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

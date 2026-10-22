"""Measure five comparable analytical reads from Trino and ClickHouse.

The two stores are fed independently. Every query is scoped to one versioned fixture's
event-ID prefix, and the script refuses to time anything until:

1. both sinks hold exactly the manifest's event IDs (count and sorted-ID SHA-256);
2. each query returns the same answer from both engines and from the manifest.

Each timed run must also return the checked answer. On any mismatch it exits with an
error and writes no report. `--preflight` instead checks that no rows with the fixture
prefix exist yet, before the fixture is published. No numbers are baked into the docs.
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import platform
import re
import shutil
import statistics
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from gamestream_producer.fixture import event_id_digest, load_manifest

TRINO_EVENTS = "lake.silver.events"
CLICKHOUSE_EVENTS = "gamestream.events FINAL"
QUERIES = {
    "row_count": (
        "SELECT count(*) FROM lake.silver.events WHERE {scope}",
        "SELECT count(*) FROM gamestream.events FINAL WHERE {scope}",
    ),
    "purchase_count": (
        "SELECT count(*) FROM lake.silver.events WHERE {scope} AND event_type='purchase'",
        "SELECT count(*) FROM gamestream.events FINAL WHERE {scope} AND event_type='purchase'",
    ),
    "active_users": (
        "SELECT count(DISTINCT user_id) FROM lake.silver.events WHERE {scope}",
        "SELECT uniqExact(user_id) FROM gamestream.events FINAL WHERE {scope}",
    ),
    "revenue": (
        "SELECT sum(amount_cents) FROM lake.silver.events "
        "WHERE {scope} AND event_type='purchase'",
        "SELECT sum(amount_cents) FROM gamestream.events FINAL "
        "WHERE {scope} AND event_type='purchase'",
    ),
    "top_game": (
        "SELECT game_id,count(*) AS n FROM lake.silver.events WHERE {scope} GROUP BY 1 "
        "ORDER BY n DESC, game_id LIMIT 1",
        "SELECT game_id,count(*) AS n FROM gamestream.events FINAL WHERE {scope} "
        "GROUP BY 1 ORDER BY n DESC, game_id LIMIT 1",
    ),
}
MIN_TIMED_RUNS = 3


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


def scope_clause(manifest: dict) -> str:
    """SQL filter for the fixture's IDs; the prefix is validated before interpolation."""
    prefix = manifest["event_id_prefix"]
    if not re.fullmatch(r"e-\d+-", prefix):
        raise ValueError(f"Unsafe or malformed event_id_prefix {prefix!r}")
    return f"event_id LIKE '{prefix}%'"


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


def _sink_ids(manifest: dict) -> tuple[list[str], list[str]]:
    scope = scope_clause(manifest)
    trino_rows = ast.literal_eval(trino_query(
        f"SELECT event_id FROM {TRINO_EVENTS} WHERE {scope} ORDER BY event_id"
    ))
    clickhouse_raw = clickhouse_query(
        f"SELECT event_id FROM {CLICKHOUSE_EVENTS} WHERE {scope} "
        "ORDER BY event_id FORMAT JSONEachRow"
    )
    return ([row[0] for row in trino_rows],
            [json.loads(line)["event_id"] for line in clickhouse_raw.splitlines()])


def preflight(manifest: dict) -> dict:
    """Before publishing, require that neither sink already holds the fixture prefix."""
    trino_ids, clickhouse_ids = _sink_ids(manifest)
    if trino_ids or clickhouse_ids:
        raise ValueError(
            f"Prefix {manifest['event_id_prefix']} is already present "
            f"(Trino={len(trino_ids)}, ClickHouse={len(clickhouse_ids)}); "
            "choose an unused seed and a new fixture version."
        )
    return {"status": "clear", "fixture_version": manifest["fixture_version"],
            "event_id_prefix": manifest["event_id_prefix"]}


def validate_parity(manifest: dict) -> dict:
    """Refuse a latency comparison unless both sinks hold exactly the manifest IDs."""
    trino_ids, clickhouse_ids = _sink_ids(manifest)
    if len(trino_ids) != len(set(trino_ids)) or len(clickhouse_ids) != len(set(clickhouse_ids)):
        raise ValueError("At least one sink still contains duplicate event IDs")
    if trino_ids != clickhouse_ids:
        raise ValueError(
            "The sinks contain different event IDs; refusing a latency comparison "
            f"(Trino={len(trino_ids)}, ClickHouse={len(clickhouse_ids)}, "
            f"only in Trino={len(set(trino_ids) - set(clickhouse_ids))}, "
            f"only in ClickHouse={len(set(clickhouse_ids) - set(trino_ids))})."
        )
    digest = event_id_digest(trino_ids)
    if len(trino_ids) != manifest["unique_event_ids"] or digest != manifest["event_ids_sha256"]:
        raise ValueError(
            "The sinks agree with each other but not with the fixture manifest; refusing "
            f"(sinks={len(trino_ids)}, manifest={manifest['unique_event_ids']})."
        )
    return {"event_ids": len(trino_ids), "sha256": digest, "matches_manifest": True}


def check_answers(manifest: dict) -> dict[str, list[str]]:
    """Run each query once, untimed, and require agreement before any timing starts."""
    scope = scope_clause(manifest)
    expected = manifest["expected_answers"]
    if set(expected) != set(QUERIES):
        raise ValueError("Manifest expected_answers do not cover the benchmark queries")
    answers = {}
    for name, (trino_sql, clickhouse_sql) in QUERIES.items():
        trino_answer = normalize_result(trino_query(trino_sql.format(scope=scope)),
                                        trino_result=True)
        clickhouse_answer = normalize_result(clickhouse_query(clickhouse_sql.format(scope=scope)),
                                             trino_result=False)
        if trino_answer != clickhouse_answer:
            raise ValueError(
                f"{name} returned different values (Trino={trino_answer}, "
                f"ClickHouse={clickhouse_answer}); refusing a latency report"
            )
        if list(trino_answer) != expected[name]:
            raise ValueError(
                f"{name} disagrees with the manifest (sinks={trino_answer}, "
                f"manifest={expected[name]}); refusing a latency report"
            )
        answers[name] = list(trino_answer)
    return answers


def measure(fn, sql: str, expected: tuple[str, ...], *, trino_result: bool,
            warmups: int = 1, repeats: int = 5) -> dict:
    """Time `repeats` runs after `warmups`; every run must return the checked answer."""
    samples = []
    for i in range(warmups + repeats):
        started = time.perf_counter()
        result = fn(sql)
        elapsed = (time.perf_counter() - started) * 1000
        if normalize_result(result, trino_result=trino_result) != expected:
            raise ValueError(f"Answer changed during timing run {i + 1}; refusing a report")
        if i >= warmups:  # warmups fill caches and connections
            samples.append(round(elapsed, 3))
    return {
        "samples_ms": samples,
        "p50_ms": statistics.median(samples),
        "p95_ms": sorted(samples)[int(0.95 * (len(samples) - 1))],
    }


def environment(docker_memory_bytes: int | None) -> dict:
    return {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "python": platform.python_version(),
        "logical_cpus": os.cpu_count(),
        "host_free_bytes": shutil.disk_usage("/").free,
        "docker_memory_bytes": docker_memory_bytes,
    }


def run(manifest: dict, *, warmups: int = 1, repeats: int = 5,
        docker_memory_bytes: int | None = None) -> dict:
    if repeats < MIN_TIMED_RUNS or warmups < 0:
        raise ValueError(f"Use at least {MIN_TIMED_RUNS} timed runs and nonnegative warmups")
    parity = validate_parity(manifest)
    answers = check_answers(manifest)
    scope = scope_clause(manifest)
    queries = {}
    for name, (trino_sql, clickhouse_sql) in QUERIES.items():
        expected = tuple(answers[name])
        queries[name] = {
            "answer": answers[name],
            "trino": measure(trino_query, trino_sql.format(scope=scope), expected,
                             trino_result=True, warmups=warmups, repeats=repeats),
            "clickhouse": measure(clickhouse_query, clickhouse_sql.format(scope=scope),
                                  expected, trino_result=False, warmups=warmups,
                                  repeats=repeats),
        }
    return {
        "status": "passed",
        "observed_at_utc": datetime.now(UTC).isoformat(),
        "fixture": {key: manifest[key] for key in (
            "fixture_version", "event_id_prefix", "unique_event_ids", "event_ids_sha256"
        )} | {"seed": manifest["generator"]["seed"]},
        "environment": environment(docker_memory_bytes),
        "method": {
            "scope": scope,
            "warmup_runs_per_engine_query": warmups,
            "timed_runs_per_engine_query": repeats,
            "trino_client": "Python DB-API, new connection per query",
            "clickhouse_client": "HTTP POST per query",
            "p95_rule": "sorted samples at index int(0.95 * (n - 1))",
        },
        "parity": parity,
        "queries": queries,
        "scope_note": "One local run over one synthetic fixture; not a general speed claim.",
    }


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--manifest", type=Path, required=True,
                        help="manifest.json from gamestream-fixture")
    parser.add_argument("--preflight", action="store_true",
                        help="Only check that the fixture prefix is absent from both sinks")
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--docker-memory-bytes", type=int,
                        help="Record Docker VM memory from `docker info` in the report")
    parser.add_argument("--output", type=Path, help="Also write the passing report here")
    args = parser.parse_args(argv)
    manifest = load_manifest(args.manifest)
    if args.preflight:
        report = preflight(manifest)
    else:
        report = run(manifest, warmups=args.warmups, repeats=args.repeats,
                     docker_memory_bytes=args.docker_memory_bytes)
    text = json.dumps(report, indent=2)
    if args.output:
        args.output.write_text(text + "\n")
    print(text)


if __name__ == "__main__":
    try:
        main(sys.argv[1:])
    except ValueError as error:
        raise SystemExit(str(error)) from error

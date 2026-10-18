"""The benchmark must not compare latency over different data."""

import json

import pytest

from bench import query_bench


def test_parity_rejects_different_event_sets(monkeypatch):
    monkeypatch.setattr(query_bench, "trino_query", lambda _sql: "[('a',), ('b',)]")
    monkeypatch.setattr(
        query_bench, "clickhouse_query", lambda _sql: json.dumps({"event_id": "a"})
    )
    with pytest.raises(ValueError, match="different event IDs"):
        query_bench.validate_parity()


def test_parity_accepts_matching_ids(monkeypatch):
    monkeypatch.setattr(query_bench, "trino_query", lambda _sql: "[('a',), ('b',)]")
    monkeypatch.setattr(
        query_bench,
        "clickhouse_query",
        lambda _sql: '\n'.join(json.dumps({"event_id": value}) for value in ("a", "b")),
    )
    result = query_bench.validate_parity()
    assert result["event_ids"] == 2
    assert len(result["sha256"]) == 64


def test_normalize_scalar_and_top_game():
    assert query_bench.normalize_result("[(34,)]", trino_result=True) == ("34",)
    assert query_bench.normalize_result("34", trino_result=False) == ("34",)
    assert query_bench.normalize_result("[('game_1', 42)]", trino_result=True) == (
        "game_1", "42"
    )
    assert query_bench.normalize_result("game_1\t42", trino_result=False) == (
        "game_1", "42"
    )

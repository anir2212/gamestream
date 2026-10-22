"""The benchmark must not compare latency over different data or answers."""

import json
from datetime import UTC, datetime

import pytest
from gamestream_producer import fixture

from bench import query_bench

START = datetime(2026, 2, 1, tzinfo=UTC)


@pytest.fixture(scope="module")
def built():
    rows, manifest = fixture.build_fixture("parity-v1", count=200, seed=11, start=START)
    return rows, manifest


def install_sinks(monkeypatch, manifest, trino_rows, clickhouse_rows, *,
                  trino_answers=None, clickhouse_answers=None, calls=None):
    """Fake both engines by answering the exact SQL the benchmark sends."""
    scope = query_bench.scope_clause(manifest)
    prefix = manifest["event_id_prefix"]

    def responses(rows, answers, trino):
        scoped = [row for row in rows if row["event_id"].startswith(prefix)]
        ids = sorted({row["event_id"] for row in scoped})
        answers = answers or (fixture.expected_answers(scoped) if ids else {})
        if trino:
            table = {f"SELECT event_id FROM {query_bench.TRINO_EVENTS} WHERE {scope} "
                     "ORDER BY event_id": repr([(value,) for value in ids])}
        else:
            table = {f"SELECT event_id FROM {query_bench.CLICKHOUSE_EVENTS} WHERE {scope} "
                     "ORDER BY event_id FORMAT JSONEachRow":
                     "\n".join(json.dumps({"event_id": value}) for value in ids)}
        for name, sql in query_bench.QUERIES.items():
            answer = answers.get(name, ["0"])
            key = sql[0 if trino else 1].format(scope=scope)
            table[key] = repr([tuple(answer)]) if trino else "\t".join(answer)
        return table

    trino_table = responses(trino_rows, trino_answers, True)
    clickhouse_table = responses(clickhouse_rows, clickhouse_answers, False)

    def lookup(table, engine):
        def query(sql):
            if calls is not None:
                calls.append((engine, sql))
            return table[sql]
        return query

    monkeypatch.setattr(query_bench, "trino_query", lookup(trino_table, "trino"))
    monkeypatch.setattr(query_bench, "clickhouse_query", lookup(clickhouse_table, "clickhouse"))
    return trino_table, clickhouse_table


def forbid_timing(monkeypatch):
    def fail(*_args, **_kwargs):
        raise AssertionError("timing started before checks passed")
    monkeypatch.setattr(query_bench, "measure", fail)


def test_parity_rejects_different_event_sets(monkeypatch, built):
    rows, manifest = built
    install_sinks(monkeypatch, manifest, rows, rows[:-5])
    forbid_timing(monkeypatch)
    with pytest.raises(ValueError, match="different event IDs"):
        query_bench.run(manifest)


def test_parity_rejects_sinks_that_agree_but_miss_manifest_ids(monkeypatch, built):
    rows, manifest = built
    short = [row for row in rows if row["event_id"] != rows[0]["event_id"]]
    install_sinks(monkeypatch, manifest, short, short)
    forbid_timing(monkeypatch)
    with pytest.raises(ValueError, match="not with the fixture manifest"):
        query_bench.run(manifest)


def test_parity_accepts_exact_manifest_ids(monkeypatch, built):
    rows, manifest = built
    install_sinks(monkeypatch, manifest, rows, rows)
    result = query_bench.validate_parity(manifest)
    assert result == {"event_ids": 200, "sha256": manifest["event_ids_sha256"],
                      "matches_manifest": True}


def test_other_prefixes_in_the_sinks_are_ignored(monkeypatch, built):
    rows, manifest = built
    older, _ = fixture.build_fixture("old", count=50, seed=7, start=START)
    install_sinks(monkeypatch, manifest, rows + older, rows)
    assert query_bench.validate_parity(manifest)["event_ids"] == 200


def test_engine_answer_mismatch_refuses_before_timing(monkeypatch, built):
    rows, manifest = built
    wrong = fixture.expected_answers(rows) | {"revenue": ["1"]}
    install_sinks(monkeypatch, manifest, rows, rows, clickhouse_answers=wrong)
    forbid_timing(monkeypatch)
    with pytest.raises(ValueError, match="revenue returned different values"):
        query_bench.run(manifest)


def test_engines_agreeing_against_the_manifest_are_refused(monkeypatch, built):
    rows, manifest = built
    wrong = fixture.expected_answers(rows) | {"top_game": ["g-99999", "1"]}
    install_sinks(monkeypatch, manifest, rows, rows, trino_answers=wrong,
                  clickhouse_answers=wrong)
    forbid_timing(monkeypatch)
    with pytest.raises(ValueError, match="top_game disagrees with the manifest"):
        query_bench.run(manifest)


def test_answer_changing_during_timing_is_refused(monkeypatch, built):
    rows, manifest = built
    install_sinks(monkeypatch, manifest, rows, rows)
    answers = iter(["[(200,)]", "[(200,)]", "[(199,)]"])
    with pytest.raises(ValueError, match="Answer changed during timing run 3"):
        query_bench.measure(lambda _sql: next(answers), "q", ("200",), trino_result=True,
                            warmups=1, repeats=3)


def test_passing_run_records_environment_method_and_samples(monkeypatch, built, tmp_path):
    rows, manifest = built
    calls = []
    install_sinks(monkeypatch, manifest, rows, rows, calls=calls)
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    output = tmp_path / "report.json"
    query_bench.main(["--manifest", str(manifest_path), "--repeats", "3",
                      "--docker-memory-bytes", "12528242688", "--output", str(output)])
    report = json.loads(output.read_text())
    assert report["status"] == "passed"
    assert report["fixture"]["event_ids_sha256"] == manifest["event_ids_sha256"]
    assert report["environment"]["docker_memory_bytes"] == 12528242688
    assert report["environment"]["logical_cpus"]
    assert report["method"]["timed_runs_per_engine_query"] == 3
    assert report["method"]["warmup_runs_per_engine_query"] == 1
    for name, result in report["queries"].items():
        assert result["answer"] == manifest["expected_answers"][name]
        assert len(result["trino"]["samples_ms"]) == len(result["clickhouse"]["samples_ms"]) == 3
    assert all(f"LIKE '{manifest['event_id_prefix']}%'" in sql for _engine, sql in calls)
    # Parity and answer checks come before the first repeated timing query.
    first_query = query_bench.QUERIES["row_count"][0].format(
        scope=query_bench.scope_clause(manifest))
    trino_calls = [sql for engine, sql in calls if engine == "trino"]
    assert trino_calls[0].startswith("SELECT event_id")
    assert trino_calls.count(first_query) == 1 + 1 + 3


def test_refused_run_writes_no_report(monkeypatch, built, tmp_path):
    rows, manifest = built
    install_sinks(monkeypatch, manifest, rows, rows[:10])
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    output = tmp_path / "report.json"
    with pytest.raises(ValueError):
        query_bench.main(["--manifest", str(manifest_path), "--output", str(output)])
    assert not output.exists()


def test_too_few_timed_runs_are_rejected(monkeypatch, built):
    rows, manifest = built
    install_sinks(monkeypatch, manifest, rows, rows)
    with pytest.raises(ValueError, match="at least 3 timed runs"):
        query_bench.run(manifest, repeats=2)


def test_preflight_requires_an_unused_prefix(monkeypatch, built):
    rows, manifest = built
    install_sinks(monkeypatch, manifest, [], [])
    assert query_bench.preflight(manifest)["status"] == "clear"
    install_sinks(monkeypatch, manifest, [], rows[:3])
    with pytest.raises(ValueError, match="already present"):
        query_bench.preflight(manifest)


def test_scope_rejects_unsafe_prefix(built):
    _rows, manifest = built
    with pytest.raises(ValueError, match="Unsafe"):
        query_bench.scope_clause(manifest | {"event_id_prefix": "e-1-' OR '1'='1"})


def test_normalize_scalar_and_top_game():
    assert query_bench.normalize_result("[(34,)]", trino_result=True) == ("34",)
    assert query_bench.normalize_result("34", trino_result=False) == ("34",)
    assert query_bench.normalize_result("[('game_1', 42)]", trino_result=True) == (
        "game_1", "42"
    )
    assert query_bench.normalize_result("game_1\t42", trino_result=False) == (
        "game_1", "42"
    )
    assert query_bench.normalize_result("[(None,)]", trino_result=True) == ("NULL",)
    assert query_bench.normalize_result(r"\N", trino_result=False) == ("NULL",)

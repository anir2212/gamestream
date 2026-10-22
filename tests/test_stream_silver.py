"""The opt-in restart hook must fail only after the silver MERGE has run."""

from __future__ import annotations

import pytest

from spark import stream_silver


class FakeRdd:
    def __init__(self, empty):
        self.empty = empty

    def isEmpty(self):
        return self.empty


class FakeSession:
    def __init__(self):
        self.statements = []

    def sql(self, statement):
        self.statements.append(statement)


class FakeBatch:
    def __init__(self, empty=False):
        self.rdd = FakeRdd(empty)
        self.sparkSession = FakeSession()
        self.view = None

    def dropDuplicates(self, columns):
        assert columns == ["event_id"]
        return self

    def createOrReplaceTempView(self, name):
        self.view = name


def test_merge_runs_without_failure_by_default(monkeypatch):
    monkeypatch.delenv(stream_silver.FAIL_AFTER_MERGE_ENV, raising=False)
    batch = FakeBatch()
    stream_silver.merge_batch(batch, 4)
    assert batch.view == "gamestream_silver_batch"
    assert "MERGE INTO lake.silver.events" in batch.sparkSession.statements[0]


def test_injected_failure_happens_after_the_merge(monkeypatch):
    monkeypatch.setenv(stream_silver.FAIL_AFTER_MERGE_ENV, "1")
    batch = FakeBatch()
    with pytest.raises(stream_silver.InjectedRestartFailure, match="batch 7"):
        stream_silver.merge_batch(batch, 7)
    assert len(batch.sparkSession.statements) == 1


def test_empty_batch_does_not_trigger_the_hook(monkeypatch):
    monkeypatch.setenv(stream_silver.FAIL_AFTER_MERGE_ENV, "1")
    batch = FakeBatch(empty=True)
    stream_silver.merge_batch(batch, 1)
    assert batch.sparkSession.statements == []


def test_other_values_do_not_trigger_the_hook(monkeypatch):
    monkeypatch.setenv(stream_silver.FAIL_AFTER_MERGE_ENV, "0")
    stream_silver.merge_batch(FakeBatch(), 2)

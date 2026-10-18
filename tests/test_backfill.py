"""Guard the date and arrival-time boundaries of the operator backfill."""

from __future__ import annotations

import pytest

from spark import backfill


class FakeFrame:
    def __init__(self):
        self.filters = []

    def where(self, expression):
        self.filters.append(expression)
        return self


class FakeSpark:
    def __init__(self):
        self.source = None
        self.stopped = False
        self.bronze = FakeFrame()

    def table(self, name):
        self.source = name
        return self.bronze

    def stop(self):
        self.stopped = True


def test_backfill_selects_event_day_after_reading_all_arrival_days(monkeypatch):
    spark = FakeSpark()
    parsed = FakeFrame()
    received = []
    monkeypatch.setattr(backfill, "spark_session", lambda _: spark)
    monkeypatch.setattr(backfill, "parse_bronze", lambda bronze, registry: (
        received.append((bronze, registry)) or parsed
    ))
    monkeypatch.setattr(backfill, "merge_batch", lambda frame, batch: (
        received.append((frame, batch))
    ))

    backfill.main("2026-01-01", "2026-01-31")

    assert spark.source == "lake.bronze.events"
    assert spark.bronze.filters == []
    assert parsed.filters == [
        "CAST(event_ts AS DATE) BETWEEN DATE '2026-01-01' AND DATE '2026-01-31'"
    ]
    assert received == [(spark.bronze, "http://registry:8080/apis/ccompat/v7"),
                        (parsed, 0)]
    assert spark.stopped


@pytest.mark.parametrize("first,last", [
    ("2026-01-01", "2026-02-01"),
    ("2026-01-02", "2026-01-01"),
])
def test_backfill_rejects_more_than_31_inclusive_days(monkeypatch, first, last):
    monkeypatch.setattr(backfill, "spark_session", lambda _: pytest.fail(
        "invalid range must fail before starting Spark"
    ))
    with pytest.raises(ValueError, match="at most 31 days"):
        backfill.main(first, last)


def test_backfill_stops_spark_after_merge_error(monkeypatch):
    spark = FakeSpark()
    monkeypatch.setattr(backfill, "spark_session", lambda _: spark)
    monkeypatch.setattr(backfill, "parse_bronze", lambda *args: FakeFrame())

    def fail_merge(*args):
        raise RuntimeError("merge failed")

    monkeypatch.setattr(backfill, "merge_batch", fail_merge)
    with pytest.raises(RuntimeError, match="merge failed"):
        backfill.main("2026-01-01", "2026-01-01")
    assert spark.stopped

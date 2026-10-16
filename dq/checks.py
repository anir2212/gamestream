"""Fail closed on data quality errors before publishing marts."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Metrics:
    rows: int
    missing_ids: int
    duplicate_ids: int
    invalid_types: int
    future_events: int


def validate(metrics: Metrics, *, minimum_rows: int = 1) -> None:
    failures = []
    if metrics.rows < minimum_rows:
        failures.append(f"rows {metrics.rows} < {minimum_rows}")
    for name in ("missing_ids", "duplicate_ids", "invalid_types", "future_events"):
        value = getattr(metrics, name)
        if value:
            failures.append(f"{name}={value}")
    if failures:
        raise ValueError("Data quality failed: " + ", ".join(failures))

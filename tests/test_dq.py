import pytest

from dq.checks import Metrics, validate


def test_valid_metrics_pass():
    validate(Metrics(100, 0, 0, 0, 0))


@pytest.mark.parametrize(
    "field", ["rows", "missing_ids", "duplicate_ids", "invalid_types", "future_events"]
)
def test_broken_input_stops_pipeline(field):
    values = dict(rows=100, missing_ids=0, duplicate_ids=0, invalid_types=0, future_events=0)
    values[field] = 0 if field == "rows" else 1
    with pytest.raises(ValueError, match="Data quality failed"):
        validate(Metrics(**values))

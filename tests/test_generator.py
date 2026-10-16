import json
from collections import Counter
from io import BytesIO
from pathlib import Path

from fastavro import parse_schema, schemaless_reader, schemaless_writer
from gamestream_producer.generator import generate_events

SCHEMAS = Path(__file__).resolve().parents[1] / "producer" / "schemas"


def test_exact_duplicate_late_and_v2_counts():
    rows = generate_events(1000)
    counts = Counter(row["event_id"] for row in rows)
    assert len(rows) == 1020
    assert len(counts) == 1000
    assert sum(count == 2 for count in counts.values()) == 20
    distinct = {row["event_id"]: row for row in rows}.values()
    assert sum(row["ingest_ts"] - row["event_ts"] == 30 * 60 * 1000
               for row in distinct) == 10
    assert sum(row["schema_version"] == 2 for row in distinct) == 500
    assert all("device_type" in row for row in distinct if row["schema_version"] == 2)
    assert all("device_type" not in row for row in distinct if row["schema_version"] == 1)


def test_reproducible_and_timezone_guard():
    assert generate_events(100, seed=4) == generate_events(100, seed=4)


def test_v2_reader_decodes_v1_with_default():
    v1 = parse_schema(json.loads((SCHEMAS / "event_v1.avsc").read_text()))
    v2 = parse_schema(json.loads((SCHEMAS / "event_v2.avsc").read_text()))
    row = next(row for row in generate_events(100) if row["schema_version"] == 1)
    buffer = BytesIO()
    schemaless_writer(buffer, v1, row)
    buffer.seek(0)
    decoded = schemaless_reader(buffer, v1, v2)
    assert decoded["event_id"] == row["event_id"]
    assert decoded["device_type"] is None

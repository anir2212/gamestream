import json
from io import BytesIO
from pathlib import Path

import pytest
from fastavro import parse_schema, schemaless_writer
from gamestream_producer.generator import generate_events

from spark.avro_decode import decode_frame

SCHEMAS = Path(__file__).resolve().parents[1] / "producer" / "schemas"


@pytest.mark.parametrize("version", [1, 2])
def test_schema_id_selects_writer_schema_and_normalizes_v1(version):
    schema_text = (SCHEMAS / f"event_v{version}.avsc").read_text()
    schema = parse_schema(json.loads(schema_text))
    row = next(row for row in generate_events(100) if row["schema_version"] == version)
    buffer = BytesIO()
    schemaless_writer(buffer, schema, row)
    frame = bytes([0]) + version.to_bytes(4, "big") + buffer.getvalue()
    decoded = decode_frame(frame, lambda schema_id: schema_text if schema_id == version else "")
    assert decoded["event_id"] == row["event_id"]
    assert decoded["device_type"] == row.get("device_type")
    assert decoded["schema_id"] == version
    assert int(decoded["event_ts"].timestamp() * 1000) == row["event_ts"]


def test_rejects_missing_confluent_header():
    with pytest.raises(ValueError, match="framing"):
        decode_frame(b"hello", lambda _: "")

"""Decode Confluent-framed Avro with the writer schema ID on every record."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime, timezone
from io import BytesIO

from fastavro import parse_schema, schemaless_reader


def decode_frame(frame: bytes, schema_lookup: Callable[[int], str]) -> dict:
    if len(frame) < 6 or frame[0] != 0:
        raise ValueError("Expected five-byte Confluent Avro framing")
    schema_id = int.from_bytes(frame[1:5], "big")
    writer_schema = parse_schema(json.loads(schema_lookup(schema_id)))
    row = schemaless_reader(BytesIO(frame[5:]), writer_schema)
    for name in ("event_ts", "ingest_ts"):
        value = row[name]
        if isinstance(value, datetime):
            row[name] = value.astimezone(timezone.utc)
        else:
            row[name] = datetime.fromtimestamp(value / 1000, tz=timezone.utc)
    row.setdefault("device_type", None)
    row["schema_id"] = schema_id
    return row


def registry_decoder(registry_url: str):
    """Return worker-local decoder; schema lookup is cached by Confluent's client."""
    from confluent_kafka.schema_registry import SchemaRegistryClient

    client = SchemaRegistryClient({"url": registry_url})

    def lookup(schema_id: int) -> str:
        return client.get_schema(schema_id).schema_str

    return lambda frame: decode_frame(frame, lookup)

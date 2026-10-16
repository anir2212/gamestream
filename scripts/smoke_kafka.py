"""Publish then read 1,020 delivered Avro frames on a fresh consumer group."""

from __future__ import annotations

import json
import time
import uuid
from collections import Counter

from confluent_kafka import Consumer
from gamestream_producer.generator import generate_events
from gamestream_producer.kafka import publish


def main() -> None:
    rows = generate_events(1000)
    consumer = Consumer({
        "bootstrap.servers": "127.0.0.1:19092",
        "group.id": f"gamestream-smoke-{uuid.uuid4()}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    })
    consumer.subscribe(["game_events"])
    try:
        # Wait for partition assignment before publishing to avoid missing records.
        deadline = time.monotonic() + 30
        while not consumer.assignment() and time.monotonic() < deadline:
            consumer.poll(0.5)
        if not consumer.assignment():
            raise RuntimeError("Consumer was not assigned partitions")
        produced = publish(rows, brokers="127.0.0.1:19092",
                           registry_url="http://127.0.0.1:18081/apis/ccompat/v7", rate=5000)
        seen = []
        deadline = time.monotonic() + 45
        while len(seen) < len(rows) and time.monotonic() < deadline:
            message = consumer.poll(0.5)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(str(message.error()))
            payload = message.value()
            if not payload or payload[0] != 0:
                raise AssertionError("Expected Confluent Avro wire framing")
            seen.append((message.key().decode(), int.from_bytes(payload[1:5], "big")))
        if len(seen) != len(rows):
            raise AssertionError(f"Consumed {len(seen)} of {len(rows)} records")
        counts = Counter(key for key, _ in seen)
        if not counts or len({schema for _, schema in seen}) != 2:
            raise AssertionError("Expected keyed events and both Avro schema versions")
        print(json.dumps({"produced": produced, "consumed": len(seen),
                          "schema_ids": sorted({schema for _, schema in seen})}))
    finally:
        consumer.close()


if __name__ == "__main__":
    main()

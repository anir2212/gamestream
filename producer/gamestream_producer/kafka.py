"""Schema Registry backed, idempotent Kafka delivery."""

from __future__ import annotations

import json
import time
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

SCHEMA_DIR = Path(__file__).resolve().parents[1] / "schemas"


def publish(
    rows: list[dict], *, brokers: str, registry_url: str, topic: str = "game_events",
    rate: int = 1000,
) -> dict[str, int | float]:
    """Publish in ingress order; user_id is the Kafka key for per-user ordering.

    Idempotence prevents a retry by this producer from duplicating a record. The
    intentionally duplicated fixture events remain duplicates and are handled in silver.
    """
    if not 1 <= rate <= 50_000:
        raise ValueError("rate must be between 1 and 50000 events/s")
    client = SchemaRegistryClient({"url": registry_url})
    client.set_compatibility(subject_name=f"{topic}-value", level="BACKWARD")
    serializers = {
        version: AvroSerializer(client, (SCHEMA_DIR / f"event_v{version}.avsc").read_text())
        for version in (1, 2)
    }
    producer = Producer({
        "bootstrap.servers": brokers,
        "enable.idempotence": True,
        "acks": "all",
        "max.in.flight.requests.per.connection": 5,
        "retries": 2_147_483_647,
        "compression.type": "zstd",
        "linger.ms": 20,
    })
    errors: list[str] = []

    def delivered(error, message):
        if error is not None:
            errors.append(str(error))

    started = time.perf_counter()
    for index, row in enumerate(rows):
        target = started + index / rate
        delay = target - time.perf_counter()
        if delay > 0:
            time.sleep(delay)
        context = SerializationContext(topic, MessageField.VALUE)
        payload = serializers[row["schema_version"]](row, context)
        producer.produce(topic, key=row["user_id"].encode(), value=payload,
                         on_delivery=delivered)
        producer.poll(0)
    outstanding = producer.flush(30)
    if outstanding or errors:
        raise RuntimeError(json.dumps({"undelivered": outstanding, "errors": errors[:5]}))
    duration = time.perf_counter() - started
    return {"delivered": len(rows), "seconds": round(duration, 3),
            "events_per_second": round(len(rows) / duration, 1) if duration else 0.0}

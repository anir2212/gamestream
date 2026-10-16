"""Three-broker KRaft/ISR experiment on this project's isolated Compose stack."""

from __future__ import annotations

import json
import subprocess
import time
import uuid
from pathlib import Path

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ["docker", "compose", "-p", "gamestream-full", "-f",
           str(ROOT / "compose" / "compose.yaml")]
BROKERS = "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092"
TOPIC = "availability_probe"


def compose(*args: str) -> None:
    subprocess.run([*COMPOSE, *args], check=True, capture_output=True, text=True)


def send(prefix: str, count: int) -> dict:
    producer = Producer({
        "bootstrap.servers": BROKERS,
        "acks": "all",
        "enable.idempotence": True,
        "delivery.timeout.ms": 8_000,
        "request.timeout.ms": 5_000,
        "retries": 1_000,
        "log_level": 3,
    })
    delivered = []
    errors = []

    def callback(error, message):
        if error:
            errors.append(str(error))
        else:
            delivered.append(message.key().decode())

    started = time.perf_counter()
    for index in range(count):
        key = f"{prefix}-{index:04d}"
        producer.produce(TOPIC, key=key.encode(), value=key.encode(), on_delivery=callback)
        producer.poll(0)
    outstanding = producer.flush(15)
    return {
        "attempted": count, "acknowledged": len(delivered), "keys": delivered,
        "errors": errors[:3], "outstanding": outstanding,
        "seconds": round(time.perf_counter() - started, 3),
    }


def read_keys(expected: int) -> list[str]:
    consumer = Consumer({
        "bootstrap.servers": BROKERS,
        "group.id": f"gamestream-fault-{uuid.uuid4()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "log_level": 3,
    })
    consumer.subscribe([TOPIC])
    keys = []
    deadline = time.monotonic() + 45
    try:
        while len(keys) < expected and time.monotonic() < deadline:
            message = consumer.poll(0.5)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(str(message.error()))
            keys.append(message.key().decode())
    finally:
        consumer.close()
    return keys


def main() -> None:
    admin = AdminClient({"bootstrap.servers": BROKERS})
    future = admin.create_topics([NewTopic(
        TOPIC, num_partitions=6, replication_factor=3,
        config={"min.insync.replicas": "2"},
    )])[TOPIC]
    try:
        future.result(30)
    except Exception as exc:
        if "TOPIC_ALREADY_EXISTS" not in str(exc):
            raise
    baseline = send("healthy", 100)
    try:
        compose("stop", "broker3")
        time.sleep(5)
        one_down = send("one-down", 100)
        compose("stop", "broker2")
        time.sleep(3)
        two_down = send("two-down", 10)
    finally:
        compose("start", "broker2", "broker3")
    time.sleep(8)
    acknowledged = set(baseline["keys"] + one_down["keys"] + two_down["keys"])
    consumed = read_keys(len(acknowledged))
    result = {
        "scope": "isolated three-node combined KRaft broker/controller cluster",
        "topic": TOPIC, "replication_factor": 3, "min_insync_replicas": 2,
        "healthy": baseline, "one_broker_stopped": one_down,
        "two_brokers_stopped": two_down,
        "acknowledged_unique": len(acknowledged),
        "consumed_unique": len(set(consumed)),
        "missing_acknowledged_keys": sorted(acknowledged - set(consumed)),
    }
    (ROOT / "bench" / "kafka_fault_results.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({key: value for key, value in result.items()
                      if key not in {"healthy", "one_broker_stopped", "two_brokers_stopped"}},
                     indent=2))
    print(json.dumps({name: {k: v for k, v in result[name].items() if k != "keys"}
                      for name in ("healthy", "one_broker_stopped", "two_brokers_stopped")},
                     indent=2))
    if result["missing_acknowledged_keys"]:
        raise SystemExit("Data loss: an acknowledged record was not read back")


if __name__ == "__main__":
    main()

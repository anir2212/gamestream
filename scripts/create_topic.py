"""Create the local events topic with profile-specific replication and ISR."""

from __future__ import annotations

import argparse

from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic, ResourceType


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--brokers", default="127.0.0.1:19092")
    parser.add_argument("--topic", default="game_events")
    parser.add_argument("--replication", type=int, default=1)
    parser.add_argument("--min-isr", type=int, default=1)
    parser.add_argument("--partitions", type=int, default=6)
    args = parser.parse_args()
    if args.replication < args.min_isr or args.min_isr < 1 or args.partitions < 1:
        raise SystemExit("Require replication >= min-isr >= 1 and partitions >= 1")
    admin = AdminClient({"bootstrap.servers": args.brokers})
    future = admin.create_topics([NewTopic(
        args.topic, num_partitions=args.partitions, replication_factor=args.replication,
        config={"min.insync.replicas": str(args.min_isr), "retention.ms": "86400000"},
    )])[args.topic]
    try:
        future.result(30)
    except Exception as exc:
        if "TOPIC_ALREADY_EXISTS" not in str(exc):
            raise
        metadata = admin.list_topics(topic=args.topic, timeout=10).topics[args.topic]
        if metadata.error or len(metadata.partitions) != args.partitions:
            raise RuntimeError("Existing topic has incompatible partition count") from exc
        resource = ConfigResource(ResourceType.TOPIC, args.topic)
        config = admin.describe_configs([resource])[resource].result(10)
        actual_isr = int(config["min.insync.replicas"].value)
        actual_replication = len(next(iter(metadata.partitions.values())).replicas)
        if actual_isr != args.min_isr or actual_replication != args.replication:
            raise RuntimeError(
                f"Existing topic has replication={actual_replication}, min-isr={actual_isr}"
            ) from exc
    print(f"topic={args.topic} partitions={args.partitions} replication={args.replication}")


if __name__ == "__main__":
    main()

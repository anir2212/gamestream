"""Command-line entry point for synthetic generation and Kafka publication."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from gamestream_producer.generator import generate_events
from gamestream_producer.kafka import publish


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic game-store events")
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--rate", type=int, default=1000)
    parser.add_argument("--topic", default="game_events")
    parser.add_argument("--brokers", default=os.getenv("GAMESTREAM_BROKERS", "localhost:19092"))
    parser.add_argument("--registry", default=os.getenv(
        "GAMESTREAM_REGISTRY", "http://localhost:18081/apis/ccompat/v7"
    ))
    parser.add_argument("--jsonl", type=Path, help="Write a fixture file instead of Kafka delivery")
    args = parser.parse_args()
    rows = generate_events(args.count, seed=args.seed)
    if args.jsonl:
        args.jsonl.parent.mkdir(parents=True, exist_ok=True)
        with args.jsonl.open("w") as output:
            for row in rows:
                output.write(json.dumps(row, sort_keys=True) + "\n")
        print(json.dumps({"generated": len(rows), "path": str(args.jsonl)}))
    else:
        print(json.dumps(publish(rows, brokers=args.brokers, registry_url=args.registry,
                                 topic=args.topic, rate=args.rate)))


if __name__ == "__main__":
    main()

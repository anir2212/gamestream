"""Command-line entry point for synthetic generation and Kafka publication."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from gamestream_producer.fixture import load_fixture
from gamestream_producer.generator import generate_events
from gamestream_producer.kafka import publish


def main(argv: list[str] | None = None) -> None:
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
    parser.add_argument("--fixture", type=Path, help=(
        "Publish a verified gamestream-fixture directory; ignores --count and --seed"
    ))
    args = parser.parse_args(argv)
    if args.fixture and args.jsonl:
        parser.error("--fixture publishes an existing export; it cannot be combined with --jsonl")
    manifest = None
    if args.fixture:
        rows, manifest = load_fixture(args.fixture)
    else:
        rows = generate_events(args.count, seed=args.seed)
    if args.jsonl:
        args.jsonl.parent.mkdir(parents=True, exist_ok=True)
        with args.jsonl.open("w") as output:
            for row in rows:
                output.write(json.dumps(row, sort_keys=True) + "\n")
        print(json.dumps({"generated": len(rows), "path": str(args.jsonl)}))
    else:
        result = publish(rows, brokers=args.brokers, registry_url=args.registry,
                         topic=args.topic, rate=args.rate)
        if manifest:
            result |= {"fixture_version": manifest["fixture_version"],
                       "event_ids_sha256": manifest["event_ids_sha256"]}
        print(json.dumps(result))


if __name__ == "__main__":
    main()

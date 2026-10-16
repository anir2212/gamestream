"""Measure only local generation throughput; Kafka is a separate benchmark."""

from __future__ import annotations

import json
import platform
import time
from pathlib import Path

from gamestream_producer.generator import generate_events


def main() -> None:
    count = 50_000
    start = time.perf_counter()
    rows = generate_events(count)
    seconds = time.perf_counter() - start
    result = {
        "workload": "synthetic generation in memory, no Kafka or serialization",
        "unique_events": count,
        "delivered_rows_with_intentional_duplicates": len(rows),
        "seconds": round(seconds, 4),
        "rows_per_second": round(len(rows) / seconds, 1),
        "machine": platform.machine(),
        "platform": platform.platform(),
        "python": platform.python_version(),
    }
    path = Path(__file__).with_name("generator_results.json")
    path.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

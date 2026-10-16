"""Query silver in Trino and raise on null IDs, duplicates, bad types, future events."""

from __future__ import annotations

import os

from dq.checks import Metrics, validate

QUERY = """
SELECT count(*) AS rows,
  count_if(event_id IS NULL OR user_id IS NULL OR game_id IS NULL) AS missing_ids,
  count(*) - count(DISTINCT event_id) AS duplicate_ids,
  count_if(event_type IS NULL OR event_type NOT IN ('session_start','page_view','search',
    'add_to_wishlist','purchase','play_start','play_end','review_posted')) AS invalid_types,
  count_if(event_ts > current_timestamp + INTERVAL '1' DAY) AS future_events
FROM lake.silver.events
"""


def main() -> None:
    import trino

    connection = trino.dbapi.connect(
        host=os.getenv("GAMESTREAM_TRINO_HOST", "trino"),
        port=int(os.getenv("GAMESTREAM_TRINO_PORT", "8080")),
        user="gamestream-dq",
        catalog="lake",
    )
    try:
        cursor = connection.cursor()
        cursor.execute(QUERY)
        row = cursor.fetchone()
        if row is None:
            raise RuntimeError("Trino returned no quality metrics")
        metrics = Metrics(*(int(value or 0) for value in row))
        validate(metrics)
        print(metrics)
    finally:
        connection.close()


if __name__ == "__main__":
    main()

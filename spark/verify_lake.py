"""Read back bronze and silver row counts from the shared Iceberg catalog."""

from __future__ import annotations

import json

from spark.common import spark_session


def main() -> None:
    spark = spark_session("gamestream-verify-lake")
    try:
        bronze = spark.sql("SELECT COUNT(*) AS rows FROM lake.bronze.events").first()[0]
        silver = spark.sql("""
            SELECT COUNT(*) AS rows, COUNT(DISTINCT event_id) AS distinct_ids
            FROM lake.silver.events
        """).first()
        result = {"bronze_rows": bronze, "silver_rows": silver[0],
                  "silver_distinct_event_ids": silver[1]}
        if bronze < 1 or silver[0] < 1 or silver[0] != silver[1]:
            raise RuntimeError(f"Iceberg row verification failed: {result}")
        print(json.dumps(result, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

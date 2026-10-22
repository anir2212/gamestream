"""Parse/evolve Avro and perform event-ID deduplication into Iceberg silver."""

from __future__ import annotations

import os

from spark.common import create_tables, deduplicate_silver, parse_bronze, spark_session

# Restart-test hook only: when set to "1", the first nonempty batch commits its Iceberg
# MERGE and then fails before Spark records the checkpoint commit. Restart without it;
# the same batch must replay and add no rows.
FAIL_AFTER_MERGE_ENV = "GAMESTREAM_SILVER_FAIL_AFTER_MERGE"


class InjectedRestartFailure(RuntimeError):
    """Raised deliberately after a committed MERGE for the post-commit restart test."""


def merge_batch(batch, batch_id: int) -> None:
    """MERGE makes a replayed checkpoint epoch idempotent at the target table."""
    if batch.rdd.isEmpty():
        return
    spark = batch.sparkSession
    deduped = batch.dropDuplicates(["event_id"])
    deduped.createOrReplaceTempView("gamestream_silver_batch")
    spark.sql("""
        MERGE INTO lake.silver.events target
        USING gamestream_silver_batch source
        ON target.event_id = source.event_id
        WHEN NOT MATCHED THEN INSERT *
    """)
    if os.getenv(FAIL_AFTER_MERGE_ENV) == "1":
        raise InjectedRestartFailure(
            f"Injected failure after the MERGE for batch {batch_id}; "
            f"restart the silver stream without {FAIL_AFTER_MERGE_ENV}"
        )


def main() -> None:
    spark = spark_session("gamestream-silver")
    create_tables(spark)
    bronze = spark.readStream.format("iceberg").load("lake.bronze.events")
    parsed = parse_bronze(bronze, os.getenv(
        "GAMESTREAM_REGISTRY", "http://registry:8080/apis/ccompat/v7"
    ))
    silver = deduplicate_silver(parsed)
    query = (
        silver.writeStream.foreachBatch(merge_batch).outputMode("append")
        .option("checkpointLocation", os.getenv("GAMESTREAM_SILVER_CHECKPOINT",
                                             "s3a://warehouse/checkpoints/silver"))
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()

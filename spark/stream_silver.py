"""Parse/evolve Avro and perform event-ID deduplication into Iceberg silver."""

from __future__ import annotations

import os

from spark.common import create_tables, deduplicate_silver, parse_bronze, spark_session


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

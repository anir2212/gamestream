"""Append Kafka's raw Avro frames and broker metadata into Iceberg bronze."""

from __future__ import annotations

import os

from spark.common import create_tables, spark_session


def main() -> None:
    spark = spark_session("gamestream-bronze")
    create_tables(spark)
    brokers = os.getenv("GAMESTREAM_BROKERS", "broker1:9092")
    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("subscribe", "game_events")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .load()
    )
    bronze = stream.selectExpr(
        "CAST(key AS STRING) AS kafka_key", "value AS payload", "timestamp AS kafka_ts",
        "topic", "partition", "offset",
    )
    query = (
        bronze.writeStream.format("iceberg").outputMode("append")
        .option("checkpointLocation", os.getenv("GAMESTREAM_BRONZE_CHECKPOINT",
                                             "s3a://warehouse/checkpoints/bronze"))
        .toTable("lake.bronze.events")
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()

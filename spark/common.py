"""Shared Spark schemas, catalog configuration, and parsing transforms."""

from __future__ import annotations

import os


def event_schema():
    from pyspark.sql.types import (
        BooleanType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType([
        StructField("event_id", StringType()),
        StructField("user_id", StringType()),
        StructField("game_id", StringType()),
        StructField("event_type", StringType()),
        StructField("event_ts", TimestampType()),
        StructField("ingest_ts", TimestampType()),
        StructField("schema_version", IntegerType()),
        StructField("genre", StringType()),
        StructField("amount_cents", IntegerType()),
        StructField("review_positive", BooleanType()),
        StructField("device_type", StringType()),
        StructField("schema_id", IntegerType()),
    ])


def spark_session(name: str):
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName(name)
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "hive")
        .config("spark.sql.catalog.lake.uri", os.getenv("GAMESTREAM_HIVE_URI", "thrift://metastore:9083"))
        .config("spark.sql.catalog.lake.warehouse", os.getenv("GAMESTREAM_WAREHOUSE", "s3a://warehouse/"))
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("GAMESTREAM_S3_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("GAMESTREAM_S3_KEY", "gamestream"))
        .config("spark.hadoop.fs.s3a.secret.key",
                os.getenv("GAMESTREAM_S3_SECRET", "local-demo-only"))
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def create_tables(spark) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.bronze")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.silver")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lake.bronze.events (
          kafka_key STRING, payload BINARY, kafka_ts TIMESTAMP,
          topic STRING, partition INT, offset BIGINT
        ) USING iceberg PARTITIONED BY (days(kafka_ts))
    """)
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lake.silver.events (
          event_id STRING, user_id STRING, game_id STRING, event_type STRING,
          event_ts TIMESTAMP, ingest_ts TIMESTAMP, schema_version INT,
          genre STRING, amount_cents INT, review_positive BOOLEAN,
          device_type STRING, schema_id INT
        ) USING iceberg PARTITIONED BY (days(event_ts))
    """)


def parse_bronze(bronze, registry_url: str):
    """Resolve each writer schema ID; a v2-only `from_avro` would misread v1 bytes."""
    from pyspark.sql import functions as F

    schema = event_schema()
    # Each Python worker keeps its registry client; serialization is per record.
    decoder = None

    def decode(value):
        nonlocal decoder
        if decoder is None:
            from spark.avro_decode import registry_decoder
            decoder = registry_decoder(registry_url)
        return decoder(value) if value else None

    parsed = bronze.select(F.udf(decode, schema)(F.col("payload")).alias("event"))
    return parsed.select("event.*").where(
        F.col("event_id").isNotNull() & F.col("user_id").isNotNull()
        & F.col("event_ts").isNotNull()
    )


def deduplicate_silver(events):
    """Bound streaming ID state to a 45-minute event-time watermark."""
    if events.isStreaming:
        return events.withWatermark("event_ts", "45 minutes").dropDuplicatesWithinWatermark(
            ["event_id"]
        )
    return events.dropDuplicates(["event_id"])


def session_counts(events):
    """Thirty-minute inactivity sessions per user and game."""
    from pyspark.sql import functions as F

    source = events.withWatermark("event_ts", "45 minutes") if events.isStreaming else events
    return source.groupBy("user_id", "game_id", F.session_window(
        "event_ts", "30 minutes"
    ).alias("session")).agg(F.count("event_id").alias("event_count"))

# GameStream local core

A reproducible synthetic game event producer and SQLite bronze/silver/gold pipeline. It generates 2% duplicate deliveries, occasional events 30 minutes late, and a v2 `device_type` field. Silver uses stable event IDs for idempotent deduplication. Gold queries calculate daily active users and a unique-user purchase funnel.

```sh
python3 gamestream.py demo --n 1000
python3 gamestream.py report
python3 -m unittest -v
```

The local database is `gamestream.sqlite`. Event data is wholly synthetic. Kafka, Spark, Iceberg, Hive, Trino, ClickHouse, dbt, Airflow, Superset, distributed failure experiments and public Steam data are not implemented. The local pipeline demonstrates the ingestion and data-quality rules before those infrastructure ports.

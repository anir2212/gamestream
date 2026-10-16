# GameStream

A game-event streaming and lakehouse lab.

A game-event pipeline with an Avro producer, Kafka, Spark transforms, Iceberg table code, dbt models, and analytics schemas. The small Kafka/registry profile is the verified local path.

## Why this project

A game team cannot answer retention or purchase-funnel questions reliably if event schemas drift and retries duplicate records. This lab follows an event from producer to analytics tables and tests the failure boundaries along the way.

## Run locally

You need Docker Desktop, Python 3.12, uv; Java for Spark tests.

```sh
uv sync --extra test --extra spark --extra analytics
uv run --extra test --extra spark pytest -q
make up-small
make topic-small
make smoke
make down-small
```

The smoke run round-tripped 1,020 Avro events. The full lakehouse is currently blocked at Spark-to-Iceberg table creation: the Iceberg Hive client and Hive 4.0.1 metastore disagree on a Thrift method. `make demo` is not a passing end-to-end path yet. See [data model](docs/DATA_MODEL.md) and [failure experiment notes](docs/DISTRIBUTED_SYSTEMS.md).

## Repository map

`spark/` contains the bronze/silver transforms; `dbt/` and `clickhouse/` define analytics outputs; `bench/` records generator and broker experiments.

## Scope

Events are generated. There is no completed Iceberg sink, full-stack benchmark, or production-scale result; [results](RESULTS.md) separates measured work from planned work.

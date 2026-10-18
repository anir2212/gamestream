# GameStream

GameStream is a local pipeline for game telemetry. A producer writes versioned Avro events to Kafka; Spark keeps the raw frames in an Iceberg bronze table and decodes, validates, and deduplicates them into silver. The goal is to make retention and funnel queries possible even when client schemas evolve or events are delivered twice.

I built this as a fault-aware lab rather than a production service. The events are generated, and each measured stage is recorded in [RESULTS.md](RESULTS.md).

## What has run

- A one-broker smoke round-tripped 1,020 Avro deliveries using both schema versions.
- A separate three-broker test recovered all 200 acknowledged keys after broker stops. It is one small fault experiment, not a durability guarantee.
- A staged run used PostgreSQL for Iceberg catalog metadata, MinIO for table data, and a persistent PostgreSQL store for Apicurio schemas. A catalog smoke wrote and read one row. Later table readback found **2,142 bronze deliveries** and **1,003 distinct silver event IDs**. Most events in the final publish were too old for the silver watermark, so these counts are not throughput figures.

An earlier attempt hit a Hive 4.0.1 / Iceberg 1.10.0 Thrift incompatibility. The JDBC catalog removed that blocker for Spark. Trino, ClickHouse, Airflow, Superset, dbt build, and a silver restart after a committed checkpoint still need live verification.

## Run a bounded local check

You need Docker Desktop, Python 3.12, `uv`, and enough disk space for the Kafka and Spark images. Install the Python dependencies and run the local tests first:

```sh
uv sync --extra test --extra spark --extra analytics
make test
make lint
```

For the small Kafka/registry smoke, use `make up-small`, `make topic-small`, `make smoke`, then `make down-small`. For the staged lakehouse path:

```sh
make up-core
make lakehouse-init
make catalog-smoke
make bronze
make silver
uv run gamestream-produce --count 1000 --brokers 127.0.0.1:19092 --registry http://127.0.0.1:18081/apis/ccompat/v7
make verify-lake
make down
```

The streams start in the background; wait for their first commits before running `make verify-lake`. Keep the Kafka and registry volumes together when resuming a retained topic. The larger `make up` profile defines analytics and UI services but is not a verified one-command demo yet.

## Read the implementation

The [data model](docs/DATA_MODEL.md) describes bronze, silver, and proposed marts. [Distributed-systems notes](docs/DISTRIBUTED_SYSTEMS.md) cover broker failure, replay, watermarks, and remaining fault tests. [The staged result record](bench/lakehouse_stage_results.json) has the exact catalog and row-count observations. Spark jobs live in `spark/`; the producer and schemas are in `producer/`; `dbt/`, `clickhouse/`, and `airflow/` contain the downstream work awaiting full-stack checks.

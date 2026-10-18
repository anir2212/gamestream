# GameStream

GameStream is a local pipeline for game telemetry. A producer writes versioned Avro events to Kafka; Spark keeps the raw frames in an Iceberg bronze table and decodes, validates, and deduplicates them into silver. The goal is to make retention and funnel queries possible even when client schemas evolve or events are delivered twice.

I built this as a fault-aware lab rather than a production service. The events are generated, and each measured stage is recorded in [RESULTS.md](RESULTS.md).

## What has run

- A one-broker smoke round-tripped 1,020 Avro deliveries using both schema versions.
- A separate three-broker test recovered all 200 acknowledged keys after broker stops. It is one small fault experiment, not a durability guarantee.
- A staged run used PostgreSQL for Iceberg catalog metadata, MinIO for table data, and a persistent PostgreSQL store for Apicurio schemas. A catalog smoke wrote and read one row. Later table readback found **2,142 bronze deliveries** and **1,003 distinct silver event IDs**. Most events in the final publish were too old for the silver watermark, so these counts are not throughput figures.
- Trino independently read those same Iceberg counts. A live dbt build created eight models and passed 23 data tests; the separate silver quality gate found no invalid rows among 1,003 records.
- A manual January 1 backfill recovered 97 watermarked-out event IDs into silver, raising it to 1,100 unique rows. Replaying the same date added no rows. Trino DQ and all 31 dbt build nodes passed again after the change.

An earlier attempt hit a Hive 4.0.1 / Iceberg 1.10.0 Thrift incompatibility. The JDBC catalog removed that blocker for Spark and Trino. ClickHouse, Airflow, Superset, and a silver restart after a committed checkpoint still need live verification.

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

The streams start in the background; wait for their first commits before running `make verify-lake`. The 18 October run reused a cached Spark image with `--no-build`, so it needed the newer verification script copied into that container; `make up-core` rebuilds the image for a fresh run. Keep the Kafka and registry volumes together when resuming a retained topic. The larger `make up` profile defines analytics and UI services but is not a verified one-command demo yet.

After the core stage has committed data, the analytical stage can run with only the catalog database, MinIO, and Trino:

```sh
docker compose -p gamestream-full -f compose/compose.yaml --profile full --profile lakehouse up -d --no-build metastore-db minio trino
cd dbt && ../.venv/bin/dbt build --profiles-dir . --no-partial-parse --threads 1
cd .. && GAMESTREAM_TRINO_HOST=127.0.0.1 GAMESTREAM_TRINO_PORT=18082 .venv/bin/python -m dq.run_checks
make down
```

The 18 October run stopped Kafka and Spark before dbt to leave memory for Trino. Keep the existing named volumes; this sequence assumes the earlier core stage committed bronze and silver data.

The manual backfill is limited to 31 inclusive event dates. It scans bronze across arrival dates before selecting decoded event dates, so a record that arrived much later is still eligible for recovery. That choice is safer for this small lab but would require an indexing or lateness policy at larger scale. Local tests cover the date bound, selection logic, and Spark cleanup after failure. The one-day Iceberg backfill and same-date replay passed on saved synthetic data; the Airflow DAG and streaming restart after a successful commit remain open. Stop the silver writer before running the manual backfill against the same table.

## Read the implementation

The [data model](docs/DATA_MODEL.md) describes bronze, silver, and gold marts. [Distributed-systems notes](docs/DISTRIBUTED_SYSTEMS.md) cover broker failure, replay, watermarks, and remaining fault tests. The [Spark stage record](bench/lakehouse_stage_results.json), [Trino/dbt stage record](bench/trino_dbt_stage_results.json), and [manual backfill record](bench/backfill_stage_results.json) contain the measured counts and limits. Spark jobs live in `spark/`; the producer and schemas are in `producer/`. The `clickhouse/` and `airflow/` paths still await live full-stack checks.

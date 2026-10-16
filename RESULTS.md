# GameStream measured results

`bench/generator_bench.py` records in-memory synthetic generation only. It does not measure Kafka delivery, Spark ingestion, Trino queries, ClickHouse queries, or fault tolerance. Those result sections remain open until the relevant scripts have been run.

| Machine | Unique events | Rows including intentional duplicates | Wall time | Rows/s |
|---|---:|---:|---:|---:|
| macOS 15.3.1, arm64, Python 3.12.13 | 50,000 | 51,000 | 0.1709 s | 298,463.6 |

Source: `bench/generator_results.json`. The benchmark allocates all rows in memory and includes sorting. It excludes Avro encoding, broker acknowledgements, consumer work, and disk persistence.

## Small-profile Kafka/Avro smoke

On one local Kafka 3.9.1 broker with Apicurio Registry 3.3.3, `scripts/create_topic.py` created six partitions with replication 1 and minimum ISR 1. `scripts/smoke_kafka.py` published 1,020 deliveries for 1,000 unique generated events, then consumed **all 1,020** on a fresh group. Registry-assigned schema IDs were `1` and `2` for v1/v2. Producer wall time was **1.272 s**, or **802.0 delivered rows/s**, despite a requested rate of 5,000/s; first-time schema registration and broker startup are included. This single run is a correctness smoke, **not** a steady-state throughput benchmark or a three-broker durability result. The broker's first advertised listener used `localhost` and logged transient IPv6 connection refusals; it fell back to IPv4 and completed. The listener is now configured as `127.0.0.1` for subsequent runs.

## Three-broker ISR fault experiment

`bench/kafka_faults.py` created a separate six-partition topic with replication factor 3 and `min.insync.replicas=2` on three combined KRaft broker/controller containers. It sent 100 records healthy, stopped broker 3 and sent 100 more, stopped broker 2 and attempted 10 more, then restarted both and read the topic from the beginning.

| State | Attempts | Acknowledged | Send duration | Delivery errors |
|---|---:|---:|---:|---:|
| Three brokers | 100 | 100 | 1.051 s | 0 |
| One broker stopped | 100 | 100 | 1.108 s | 0 |
| Two brokers stopped | 10 | 0 | 8.101 s | 10 timeouts |

After recovery, **200 distinct acknowledged keys** were consumed; **0 acknowledged keys were missing**. Source: `bench/kafka_fault_results.json`. This is one local run on a tiny topic, not a general zero-data-loss proof. In combined mode, stopping two nodes also loses the KRaft controller majority, so this does not isolate ISR from quorum effects. The failed writes timed out locally; none was acknowledged.

## Spark transform checks

Local Spark 3.5.7 DataFrame tests passed for `event_id` deduplication and 30-minute inactivity session grouping. Avro frame tests passed for decoding v1 and v2 by embedded schema ID and defaulting `device_type` in v1. No Iceberg sink or stream-restart result has yet been recorded.

On 2026-10-15, the local Spark suite initially failed after the sandbox socket allowance because PySpark chose system Python 3.13 for its worker while the driver used the project Python 3.12. The module fixture now sets `PYSPARK_PYTHON` to the active interpreter for the test and restores the prior setting afterward. With local loopback access, **14/14** project tests passed, including both Spark transforms; the scoped Ruff check passed. This verifies local static DataFrame logic only.

## 2026-10-16 staged lakehouse run

Docker reported 12,528,242,688 bytes (11.67 GiB) of VM RAM. Its sparse `Docker.raw` file had a 460 GiB logical ceiling, but the host had only about 35 GiB free at the beginning. The staged run stopped new image pulls when free space reached about 17–18 GiB. No full-stack run or Iceberg row count is claimed.

The three Kafka brokers, Apicurio Registry, MinIO, PostgreSQL-backed Hive Metastore, Spark master, and Spark worker started. A six-partition topic was created with replication factor three and minimum ISR two. The live smoke published and consumed **1,020/1,020** Avro deliveries, with schema IDs 1 and 2. The producer reported 0.76 s and 1,342.2 deliveries/s for this one startup-sensitive run. This is a round-trip correctness check, not a steady-state benchmark. The MinIO client created the `warehouse` bucket, Hive initialized its PostgreSQL schema, and the Spark worker registered with the master.

The Spark image initially failed to build because the official Spark 3.5.7 image uses Python 3.8, while `fastavro==1.13.1` and `datetime.UTC` need a newer interpreter. A Python 3.8-compatible `fastavro==1.9.7`, `timezone.utc`, and upgraded pip resolved image build and dependency installation. The first bronze submission also found that Spark's default Ivy cache was `/nonexistent/.ivy2`; the explicit `/tmp/ivy` setting resolved Maven package loading. Iceberg, Kafka, and Hadoop S3A JARs downloaded, and the Spark driver reached `create_tables`.

The first table-creation attempt failed because Hive Metastore lacked S3A on its classpath. Linking the S3A JARs already bundled in the Hive image and adding the MinIO Hadoop configuration advanced the attempt to the next boundary. The second attempt failed at Iceberg's `CREATE TABLE IF NOT EXISTS lake.bronze.events`: the Hive 4.0.1 server rejected the Iceberg 1.10.0 Hive client's Thrift call with `Invalid method name: 'get_table'`. This is a measured catalog compatibility failure. Bronze and silver contain **no verified Iceberg rows**, and Trino, ClickHouse, Airflow, Superset, dbt build, stream restart, and query benchmarks remain unverified. P03 containers were stopped with `docker compose down`; named volumes and images were preserved.

## Model and quality checks

`dbt parse --profiles-dir . --no-partial-parse` registered the Trino adapter and found eight models, 23 data tests, and one source after adding accepted-value, composite-grain, and count-order checks on 2026-10-15. Parsing had no model errors. `dbt compile` attempted a Trino connection and could not proceed because the full stack is not running; no SQL execution or dbt build result is claimed. The pure quality gate passed six local tests, including five deliberately invalid metric variants. The full Airflow DAG import, DQ query, and dashboard remain unverified.

## Resource boundary

At initial implementation the host had about 3.9 GiB free disk and Docker was configured with 8.32 billion bytes (about 7.75 GiB) of memory. On 2026-10-15, `df -h` showed about 45 GiB free on the host. After the user changed Docker Desktop settings, `docker info` reported 12,528,242,688 bytes (about 11.67 GiB) available to the VM. On 2026-10-16, the staged Docker run consumed roughly 17–18 GiB of host free space, including image pulls, build cache, and volumes. The `Docker.raw` sparse file reported a 460 GiB logical size, so host free space was the practical limit. Query latency, Iceberg compaction, time travel, dashboard refresh, and Spark stream restart are **unmeasured**.

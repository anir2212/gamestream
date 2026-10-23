# GameStream measured results

## 2026-10-23 dashboard date filters

Only the services the dashboards read were started: MinIO, the metastore PostgreSQL that holds the Iceberg JDBC catalog, Trino 483, ClickHouse 25.8 and Superset 4.1.2, on the named volumes kept from 22 October. Kafka, Spark, Airflow and dbt did not run, and no data changed. Compact record: [`bench/dashboard_filters_2026-10-23.json`](bench/dashboard_filters_2026-10-23.json).

- **Data dates.** ClickHouse's minute table has data in 623 minutes on 1 January, 646 on 1 February and 177 on 3 February 2026. The Trino gold marts have the same three days (retention has two cohorts, 1 January and 1 February).
- **What changed.** `scripts/superset_dashboards.py` now gives each dashboard a native **Event date** time-range filter scoped to all its charts, with the default `2026-01-01T00:00:00 : 2026-02-04T00:00:00` (Superset's end is exclusive). Every chart has a `TEMPORAL_RANGE` filter on its date column for the dashboard filter to replace. Line charts are resampled to their grain (`1T` or `1D`) with null filling, so empty periods break the line instead of being joined, and point markers show isolated periods. The leaderboard dataset now keeps `day`, so the filter applies before the per-game sum. Existing charts, datasets and dashboards are updated in place: after repeated runs Superset still held 7 charts, 7 datasets, 2 dashboards and 2 databases.
- **Command.** `uv run python scripts/superset_dashboards.py --compare-range '2026-02-01T00:00:00 : 2026-02-02T00:00:00' --out superset/exports/gamestream_dashboards_2026-10-23.zip`. Every chart query returned data at the default range. Rows with a value under the default range against 1 February only:

  | Chart | Default range | 1 February only |
  |---|---:|---:|
  | Events / active users / purchase rate per minute (each) | 1,446 minutes | 646 minutes |
  | Purchase funnel by day | 3 days | 1 day (157 viewers, 30 wishlisters, 1 purchaser) |
  | D1 and D7 retention | 2 cohorts | 1 cohort |
  | Game leaderboard by purchases | 20 games | 20 games, different order and counts |
  | DAU, WAU and MAU | 3 days | 1 day (DAU 198) |

  After resampling, the minute charts return 48,939 rows over the default range (1,425 for 1 February) and DAU/WAU/MAU returns 34 rows, of which 3 hold data. The rest are the nulls that leave the gaps blank. Checked directly in Trino, the leaderboard's source holds 87 purchases over all days and 44 on 1 February. Its top game, g-00039, leads in both ranges because all 5 of its purchases are on 1 February; the rest of the list changes.
- **Real UI check.** `scripts/superset_browser_check.mjs` logged in through Superset's login page in headless Chrome 156, opened each dashboard, and recorded every chart-data request the page made. At the default, all 7 requests carried the default range and returned the counts above; 3/3 and 4/4 charts drew, with no error alerts. With the 1 February range in the dashboard URL (Superset's encoding of a changed filter), all 7 requests carried that range and returned 1,425 resampled minute rows, 1 funnel row, 1 cohort, 20 games and 1 DAU row. Screenshots: [live operations](superset/exports/live_operations_2026-10-23.png), [live operations, 1 February](superset/exports/live_operations_feb01_2026-10-23.png), [product analytics](superset/exports/product_analytics_2026-10-23.png), [product analytics, 1 February](superset/exports/product_analytics_feb01_2026-10-23.png).
- **Export.** [`superset/exports/gamestream_dashboards_2026-10-23.zip`](superset/exports/gamestream_dashboards_2026-10-23.zip) (32,741 bytes, 19 files) includes the filter configuration. Superset masked the ClickHouse password (`XXXXXXXXXX`), and the archive has no plain password.
- **Checks.** `uv run --extra test --extra spark pytest -q` passed 57 tests (5 new for the filter, resampling, leaderboard and summary logic), and Ruff passed. Stopped with `make down` (no `-v`; 10 named volumes kept). The images pulled for this run were removed afterwards.
- **Limits.** The data is three synthetic days, so this shows the filter wiring, not product insight. The default range is fixed to the fixtures' dates rather than read from the data. Over the full range each minute chart is a thin band per day, so the minutes are read by narrowing the filter. The filtered view was opened through the URL rather than by clicking the date picker. The ZIP was not re-imported into a fresh Superset. Retention remains 0 because no fixture user returns.

## 2026-10-22 full local stack: parity benchmark, restart test, Airflow and Superset

All services ran on the M3 Pro (18 GiB; Docker 11.67 GiB) with the named volumes from earlier runs. Compact record: [`bench/live_checklist_2026-10-22.json`](bench/live_checklist_2026-10-22.json).

- **Stale bronze checkpoint.** Bronze first failed with `OffsetOutOfRangeException`. Its checkpoint pointed at Kafka offsets the topic's 24-hour retention had already deleted, and `failOnDataLoss=true` stopped it instead of skipping. The topic was empty, and its 3,162 total offsets equal the 2,142 deliveries bronze had read plus the 1,020 published on 19 October for the separate ClickHouse stage. Those 1,020 therefore never reached bronze. Bronze was restarted with a new checkpoint path through `GAMESTREAM_BRONZE_CHECKPOINT`, so nothing old was re-read, and the old checkpoint was kept.
- **Same events in both stores.** `parity-v1` regenerated byte-identical to its manifest. The preflight found the prefix in neither store, and 1,020 deliveries were published in 1.0 s. ClickHouse held the 1,000 IDs within 10 s and Trino silver within 30 s, with ClickHouse consumer lag 0.
- **Benchmark (passed its parity gate)** ([report](bench/query_bench_parity-v1_2026-10-22.json)). One warm-up and 5 timed runs per engine and query, median in ms:

  | Query | Trino | ClickHouse |
  |---|---:|---:|
  | Row count | 96.3 | 4.5 |
  | Purchase count | 66.6 | 3.1 |
  | Active users | 68.5 | 2.8 |
  | Revenue | 56.6 | 2.9 |
  | Top game | 82.6 | 3.6 |

  Each engine returned the manifest's answer on every run (1,000 rows, 44 purchases, 198 users, revenue 68,956, top game g-00011 with 40). Trino times include opening a new DB-API connection per query; ClickHouse uses one HTTP request per query. With 5 samples the script's "p95" is the fourth-smallest value. This is one run over 1,000 synthetic events, not a general speed comparison.
- **Restart test.** A new `restart-v1` fixture (200 IDs, 204 deliveries) was published while silver was stopped. With `GAMESTREAM_SILVER_FAIL_AFTER_MERGE=1`, silver raised `InjectedRestartFailure` after the MERGE of batch 9 (37 input rows), leaving 2,137 rows and 2,137 distinct IDs. Restarted normally, it replayed batch 9 (37 input rows, no new rows) and then batch 10 (167). The result was 200 of 200 fixture IDs and 2,300 rows / 2,300 distinct in silver. The checkpoint path and the 200 shuffle partitions were unchanged.
- **Airflow.** The first two `gamestream_daily_marts` runs passed the quality gate, but `dbt build` failed three times with "Read-only file system", because dbt writes `logs/` and `target/` into the read-only project mount. The DAG now passes `--log-path /tmp/dbt-logs --target-path /tmp/dbt-target`. The next run succeeded: quality gate on 2,300 rows (0 missing IDs, 0 duplicates, 0 invalid types, 0 future events), then `dbt build` PASS=31 (7 tables, 1 view, 23 tests) in 4.3 s. The DAG was paused afterwards.
- **Superset.** `scripts/superset_dashboards.py` created the two dashboards from `superset/DASHBOARDS.md`:
  - ClickHouse live operations: 3 line charts, 1,446 minutes each.
  - Trino product analytics: funnel (3 days), retention (2 cohorts), leaderboard (20 games) and DAU/WAU/MAU (3 days).

  It checked that every chart query returned rows, then exported [the dashboard ZIP](superset/exports/gamestream_dashboards_2026-10-22.zip), with Superset masking the passwords. Headless Chrome 155 rendered 3/3 and 4/4 charts with no error alerts ([live operations](superset/exports/live_operations_2026-10-22.png), [product analytics](superset/exports/product_analytics_2026-10-22.png)). The data covers three synthetic days, so at this point the line charts joined across the gap (fixed on 23 October; see above), and retention is 0 because no fixture user returns.
- **Stopped** with `make down` (no `-v`; 10 named volumes kept) and Docker Desktop stopped. Tests: 52 passed (3 new for the dashboard builder), and Ruff passed.

## 2026-10-20 parity fixture and benchmark gate (local tooling only)

No Docker, Kafka, Spark streaming, Iceberg, Trino, ClickHouse, Airflow or Superset service ran. Host free disk was below the handoff's safe margin for the full profile, and other local work was sharing the machine. This entry records tooling that makes the next live run checkable. It is not a pipeline or latency result.

- **Versioned fixture.** `gamestream-fixture --version parity-v1 --seed 20261020 --count 1000 --start 2026-02-01T00:00:00Z` wrote 1,020 deliveries: 1,000 unique IDs, 20 intentional duplicates and 10 late events, with the ID prefix `e-20261020-`. The tracked [`data/fixtures/parity-v1/manifest.json`](data/fixtures/parity-v1/manifest.json) records the generator version and inputs, the SHA-256 of the sorted unique IDs (`e53ae7eb…6653e`), the events file hash and the expected answers to the five benchmark queries. An independent re-export on 20 October was byte-identical. The events file is regenerated rather than tracked.
- **Benchmark gate.** `bench/query_bench.py --manifest …` scopes every query to the fixture's ID prefix. It refuses to time unless both sinks hold exactly the manifest's IDs (count and hash) and both engines return the manifest's answer for each query. It requires at least 3 timed runs, refuses a report if an answer changes during timing, records the environment, and writes nothing on failure. `--preflight` checks that the prefix is still unused before publishing. `gamestream-produce --fixture DIR` publishes exactly the verified fixture rows.
- **Restart hook.** With `GAMESTREAM_SILVER_FAIL_AFTER_MERGE=1`, the silver stream fails right after its first nonempty `MERGE` commits and before Spark records the batch. A restart without the variable should replay that batch and add no rows. Unit tests check that the failure follows the MERGE and that empty batches and other values do not trigger it. The live restart had **not** been run at this point (it passed on 22 October; see above).
- **Checks.** `uv run --extra test --extra spark pytest -q` passed **49 tests**, up from 21. Ruff passed, and `dbt parse` built 8 models, 23 data tests and 1 source without running SQL ([`bench/fixture_tooling_checks.json`](bench/fixture_tooling_checks.json)).

## 2026-10-19 staged ClickHouse serving check

With 15–20 GiB of host disk free during this stage, three saved Kafka brokers, the persistent Apicurio registry and ClickHouse 25.8 ran together. A fresh `scripts/smoke_kafka.py` call acknowledged **1,020/1,020** Confluent-framed Avro deliveries and read all 1,020 back with schema IDs 1 and 2. The installed `confluent-kafka` 2.16 serializer's default subject-association lookup stalled against Apicurio's empty 404 response; the producer now explicitly uses the project's existing topic subject naming. The first smoke attempt was interrupted before it published; only the successful repeat is counted.

ClickHouse initially rejected v1 records because `device_type` is absent from their writer schema. The Kafka-engine table now sets `input_format_avro_allow_missing_fields = 1`, so that Nullable column becomes null for v1. Placing the setting on the materialized view did not fix the reader; recreating the Kafka table with the setting did. The corrected consumer group reached **zero lag on all six partitions**. `events FINAL` held **1,000 rows and 1,000 distinct IDs**: 500 v1, 500 v2, zero v1 rows with a device value, and 34 purchases. The minute aggregate table held **986 minute/game buckets** whose merged states counted **1,000 distinct events and 34 distinct purchases**. `bench/verify_clickhouse.py --expected-unique 1000` passed and saved [`bench/clickhouse_stage_results.json`](bench/clickhouse_stage_results.json).

The 1,020 deliveries minus 1,000 final IDs match the fixture's 20 intentional duplicates; a pre-merge physical row count was not preserved. Kafka acknowledgement and consumer offsets are the evidence for delivery count. The ClickHouse consumer started after old topic records already existed and consumed this fresh fixture. Its 1,000 IDs are **not** the same measured set as the 1,100-row Iceberg silver table after backfill. With cached Trino and Iceberg services also running, `bench/query_bench.py` refused the comparison before timing with `Trino=1100, ClickHouse=1000`. It now checks event-ID sets and query results. No cross-engine latency, Airflow run, Superset dashboard, full-stack start, or failure-recovery guarantee is claimed from this stage.

## 2026-10-18 manual backfill and mart refresh

With the saved **2,142-row bronze** table and **1,003-row silver** table, the current `spark/backfill.py` was submitted manually for **2026-01-01** using a local Spark driver. It scanned the bronze table across arrival dates, decoded the Avro frames through the persisted registry, selected the requested event date, and merged by `event_id` into Iceberg silver. The first run added **97 rows**, bringing silver to **1,100**. Repeating the same date added **zero** rows; the second MERGE still committed a metadata-only Iceberg snapshot. Spark readback then showed **2,142 bronze / 1,100 silver / 1,100 distinct IDs**.

After stopping Kafka and Spark to free memory, Trino's silver quality gate reported **1,100 rows** with zero missing IDs, duplicate IDs, invalid types, or future events. A fresh dbt build passed **31/31 nodes**: eight models and 23 data tests. Trino read back the seven gold tables: active users **1**, game dimensions **40**, popularity **40**, funnel **1**, retention **1**, revenue **24**, and review sentiment **29** rows. The compact record is [`bench/backfill_stage_results.json`](bench/backfill_stage_results.json).

This verifies one manual backfill, same-date target row idempotence, DQ and mart refresh on a saved synthetic fixture. It does **not** verify the Airflow backfill DAG, a streaming-query kill/restart after an Iceberg commit, ClickHouse serving, or performance at scale. The cached Spark image lacked the current verifier; the current backfill and verifier scripts were copied into that test container. A fresh image still needs a rebuild. No new images were pulled in this stage; free host disk reached approximately **11 GiB** while Docker was running.

## 2026-10-18 Trino, dbt, and silver quality stage

With 11.67 GiB of Docker VM RAM and approximately 17 GiB host disk free before the core stack started, the saved Spark/Iceberg tables read back **2,142 bronze rows** and **1,003 silver rows with 1,003 distinct event IDs**. The cached Spark image lacked the newer `spark/verify_lake.py`; the script was copied into the running container for this read-only check. The image still needs rebuilding before `make verify-lake` works in a fresh container.

Trino 483 initially rejected `node.environment=gamestream-local`; the mounted setting is now `gamestream_local`. After restarting, Trino's Iceberg JDBC catalog showed the bronze and silver schemas, and Trino independently returned the same **2,142 / 1,003 / 1,003** counts. This verifies cross-engine access to these committed tables, not ongoing stream freshness.

With Kafka and Spark stopped to release memory, `dbt build --profiles-dir . --no-partial-parse --threads 1` against Trino completed **31/31 nodes**: one staging view, seven gold tables, and 23 data tests. Trino read back 1 active-user row, 40 game dimensions, 40 popularity rows, 1 funnel row, 1 retention row, 23 revenue rows, and 27 review rows. The live `dq.run_checks` gate returned 1,003 rows and zero missing IDs, duplicate IDs, invalid event types, or future events. The compact record is [`bench/trino_dbt_stage_results.json`](bench/trino_dbt_stage_results.json).

This is one staged run on saved synthetic data. Host free disk reached about **9.3 GiB** after pulling Trino, so ClickHouse, Airflow, and Superset were not started. No live Airflow schedule, ClickHouse ingestion, dashboard, full `make demo` run, query latency comparison, or production fault-tolerance claim follows from this check.

## Backfill boundary regression, 18 October 2026

The manual backfill now treats its limit as **31 inclusive event dates**. A fake-Spark regression test verifies that the operator scans all bronze arrival dates before filtering decoded `event_ts`, so a very late event is not excluded solely because its Kafka arrival date is after the requested range. It also verifies that Spark stops after a merge failure. At this earlier checkpoint, no live backfill had run; the later manual Iceberg result is above. Scanning all bronze partitions can be expensive at larger scale; a safe arrival-date cutoff would require a proven maximum lateness policy.

`bench/generator_bench.py` records in-memory synthetic generation only. It does not measure Kafka delivery, Spark ingestion, Trino queries, ClickHouse queries, or fault tolerance. Separate staged sections below record Kafka and Spark checks; the later Trino/dbt result is above. ClickHouse and the complete dashboard stack were still open at that point; both ran on 22 October.

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

Local Spark 3.5.7 DataFrame tests passed for `event_id` deduplication and 30-minute inactivity session grouping. Avro frame tests passed for decoding v1 and v2 by embedded schema ID and defaulting `device_type` in v1. This paragraph describes the earlier isolated transform checks; a later live Iceberg sink result is below.

On 2026-10-15, the local Spark suite initially failed after the sandbox socket allowance because PySpark chose system Python 3.13 for its worker while the driver used the project Python 3.12. The module fixture now sets `PYSPARK_PYTHON` to the active interpreter for the test and restores the prior setting afterward. With local loopback access, **14/14** project tests passed, including both Spark transforms; the scoped Ruff check passed. This verifies local static DataFrame logic only.

## 2026-10-16 staged lakehouse run

Docker reported 12,528,242,688 bytes (11.67 GiB) of VM RAM. Its sparse `Docker.raw` file had a 460 GiB logical ceiling, but the host had only about 35 GiB free at the beginning. The staged run stopped new image pulls when free space reached about 17–18 GiB. No full-stack run or Iceberg row count is claimed.

The three Kafka brokers, Apicurio Registry, MinIO, PostgreSQL-backed Hive Metastore, Spark master, and Spark worker started. A six-partition topic was created with replication factor three and minimum ISR two. The live smoke published and consumed **1,020/1,020** Avro deliveries, with schema IDs 1 and 2. The producer reported 0.76 s and 1,342.2 deliveries/s for this one startup-sensitive run. This is a round-trip correctness check, not a steady-state benchmark. The MinIO client created the `warehouse` bucket, Hive initialized its PostgreSQL schema, and the Spark worker registered with the master.

The Spark image initially failed to build because the official Spark 3.5.7 image uses Python 3.8, while `fastavro==1.13.1` and `datetime.UTC` need a newer interpreter. A Python 3.8-compatible `fastavro==1.9.7`, `timezone.utc`, and upgraded pip resolved image build and dependency installation. The first bronze submission also found that Spark's default Ivy cache was `/nonexistent/.ivy2`; the explicit `/tmp/ivy` setting resolved Maven package loading. Iceberg, Kafka, and Hadoop S3A JARs downloaded, and the Spark driver reached `create_tables`.

The first table-creation attempt failed because Hive Metastore lacked S3A on its classpath. Linking the S3A JARs already bundled in the Hive image and adding the MinIO Hadoop configuration advanced the attempt to the next boundary. The second attempt failed at Iceberg's `CREATE TABLE IF NOT EXISTS lake.bronze.events`: the Hive 4.0.1 server rejected the Iceberg 1.10.0 Hive client's Thrift call with `Invalid method name: 'get_table'`. This is a measured catalog compatibility failure. At that checkpoint, bronze and silver contained **no verified Iceberg rows**. The replacement JDBC catalog and later row counts are recorded below. P03 containers were stopped with `docker compose down`; named volumes and images were preserved.

## 2026-10-16 JDBC catalog and stream stage

Spark 3.5.7 and Iceberg 1.10.0 were configured to use a V1 Iceberg JDBC catalog in the existing PostgreSQL 16 service, with data files in MinIO. The isolated `make catalog-smoke` created an Iceberg table, wrote and read **one row**, and dropped its table/namespace. The first attempt stopped before SQL because the running Spark image predated the new smoke script; rebuilding that one image fixed the packaging mistake. The following results use the rebuilt image. The matching Trino catalog configuration has passed static inspection only; Trino has not read this table.

The three-broker RF3/minimum-ISR2 topic, registry, MinIO, PostgreSQL, Spark master, and one two-core worker ran without Trino, ClickHouse, Airflow, or Superset. The bronze stream resumed from its MinIO checkpoint after one controlled stop and committed **2,040** Kafka deliveries to `lake.bronze.events`. At first the silver stream could not schedule because bronze claimed both worker cores; limiting each stream to one Spark core let them run together. Silver decoded the saved Avro frames, committed a microbatch, and `make verify-lake` read **2,040 bronze rows**, **1,000 silver rows**, and **1,000 distinct silver event IDs**. These are original synthetic fixtures, including intentional duplicate deliveries.

Silver initially failed to decode an older schema ID after Apicurio restarted with its default in-memory storage while Kafka retained its topic. The Compose registry now uses a separate PostgreSQL database/volume. Before replaying, a sample of 100 existing Kafka frames was independently decoded with the checked-in writer schemas: 50 ID-1 frames matched v1 exactly and 50 ID-2 frames matched v2 exactly. Both schemas were registered in that order and their parsed contents matched. After restarting the new registry container, both IDs and schema contents survived. A new seed published **102 acknowledged deliveries** (100 IDs and two intentional duplicates); bronze advanced to **2,142** rows. Silver advanced to **1,003** rows with **1,003 distinct IDs**. Most newly published rows carried January 1 timestamps after the earlier full-day batch had advanced the 45-minute watermark; their exclusion is expected late-data behavior, not proof that all 100 new IDs were ingested. This is a boundary to account for when replaying old synthetic days.

The compact record is [`bench/lakehouse_stage_results.json`](bench/lakehouse_stage_results.json). `make verify-lake` reads committed Iceberg tables and refuses an empty or duplicate-ID silver table. These checks establish one local catalog write/read, bronze checkpoint continuation, silver deduplication, and registry restart persistence. They do not establish silver recovery after a successful checkpoint, Trino/dbt interoperability, complete end-to-end scheduling, query latency, or production fault tolerance. The tiny stateful batch used the worker's default 200 shuffle partitions; reducing that setting needs a new compatible checkpoint plan before changing this saved stream. Host free space fell from about 20 GiB to 10 GiB at the low point and recovered to about 15 GiB after `docker compose down` and Docker Desktop shutdown; named volumes remain. The exact share attributable to Spark state, Docker layers, logs, or macOS swap was not isolated. No further image pulls were attempted under that boundary.

## Model and quality checks

`dbt parse --profiles-dir . --no-partial-parse` registered the Trino adapter and found eight models, 23 data tests, and one source after adding accepted-value, composite-grain, and count-order checks on 2026-10-15. Parsing had no model errors. At that checkpoint, `dbt compile` could not connect because Trino was not running. The pure quality gate passed six local tests, including five deliberately invalid metric variants. The later live dbt build and DQ query are recorded above. The Airflow DAG run and the dashboards were verified on 22 October, and the dashboard date filter on 23 October (both above).

## Resource boundary

At initial implementation the host had about 3.9 GiB free disk and Docker was configured with 8.32 billion bytes (about 7.75 GiB) of memory. On 2026-10-15, `df -h` showed about 45 GiB free on the host. After the user changed Docker Desktop settings, `docker info` reported 12,528,242,688 bytes (about 11.67 GiB) available to the VM. On 2026-10-16, the first staged Docker run consumed roughly 17–18 GiB of host free space, including image pulls, build cache, and volumes. The later JDBC/stream stage ran with about 20 GiB free at the start, 10 GiB at its low point, and 15 GiB after shutdown. The 2026-10-18 Trino/dbt stage reached about 9.3 GiB free and therefore did not expand the stack further. The `Docker.raw` sparse file reported a 460 GiB logical size, so host free space was the practical limit. Iceberg compaction, time travel and dashboard refresh timing are **unmeasured**. Query latency (the parity-gated benchmark) and the silver post-commit restart (the replay added 0 rows; 200 of 200 IDs) were measured on 22 October; see above.

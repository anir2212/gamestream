# GameStream distributed-systems experiments

## What was actually run

The three-broker KRaft experiment is scripted in `bench/kafka_faults.py`; exact one-run counts and timings are in `RESULTS.md` and machine-readable `bench/kafka_fault_results.json`. The topic has six partitions, replication factor 3, and minimum ISR 2. The producer sets `acks=all` and `enable.idempotence=true`.

When broker 3 stopped, the two remaining replicas still satisfied minimum ISR and all 100 attempted records received acknowledgements. When broker 2 also stopped, 10 attempted records timed out without acknowledgement. The test restarted both brokers and consumed all 200 acknowledged keys. This demonstrates the configured availability boundary for **this local run**. It does not prove that all possible failures or power-loss timings are loss-free. A timeout means the producer does not know whether an unacknowledged write reached a replica; an application must retry with its own event ID and deduplicate downstream.

The three nodes run brokers and controllers together to fit the local lab. Stopping two also removes the controller majority, so the experiment cannot attribute the blocked writes solely to ISR. A production KRaft cluster should use separate controller nodes and test broker loss independently. Kafka's partition leader can move during a one-node failure, causing transient retriable errors even when the final batch is fully acknowledged.

## Delivery semantics by hop

| Hop | Expected behavior | Duplicate or loss boundary |
|---|---|---|
| Generator → producer | `event_id` is stable; 2% of generated records are deliberately delivered twice. | Producer idempotence prevents duplicates caused by retries in one producer session. It cannot remove the intentional duplicate record or guarantee exactly once across an application restart. |
| Producer → Kafka | `acks=all`, replication 3, minimum ISR 2 in the full profile. | Acknowledged writes should survive one replica loss in the tested scenario. Timed-out writes have ambiguous outcome and require application-level retry. |
| Kafka → bronze | Structured Streaming records raw key, payload, topic, partition, offset, and broker timestamp in Iceberg. | The checkpoint tracks offsets. Replaying a batch after a partial commit can create raw duplicates unless the Iceberg sink and checkpoint transaction boundary handles it; verify by kill/restart before claiming exactly once. The checkpoint is only as good as Kafka retention: on 22 October, after more than a day idle, the saved offsets had been deleted (24-hour retention) and `failOnDataLoss=true` stopped bronze instead of skipping silently. Recovery used a new checkpoint path on the then-empty topic. |
| Bronze → silver | Decode by embedded Avro writer schema ID, watermark event time by 45 minutes, deduplicate `event_id`, and `MERGE` by `event_id`. The opt-in `GAMESTREAM_SILVER_FAIL_AFTER_MERGE=1` hook fails a batch after its MERGE commits, for the planned restart test. | Watermark bounds state and permits duplicates after expiry. A target-side merge makes replay idempotent for retained IDs. On 22 October a live restart test crashed silver after a MERGE and before its checkpoint commit. The replayed batch added no rows: silver ended with 200 of 200 fixture IDs and no duplicates. |
| Silver → marts | dbt recomputes tables from silver. | A full refresh replaces a mart; incremental models would need unique keys, late-data windows, and backfill tests. |
| Kafka → ClickHouse | Kafka engine and materialized view will ingest live data. | `ReplacingMergeTree` replacement is eventual and queries without `FINAL` or explicit latest-version aggregation can see duplicates. |

## Partitioning choice

The producer's Kafka key is `user_id`, which keeps one user's event order in a stable partition assignment. It helps sessionisation but can create a hot partition for a bot or very active account. Adding partitions changes the key-to-partition mapping, so order across the change is not guaranteed. Iceberg bronze and silver partition by event day for pruning. Day partitions are appropriate for queries by date; too many tiny files from frequent microbatches require compaction.

## Planned measurements with no fabricated results

- `bench/partition_parallelism.py`: partition count versus consumer-group worker count; report p50/p95 and sample size under a fixed payload and run length.
- `bench/iceberg_compaction.py`: file-count and query latency before/after `rewrite_data_files`, with cache warmup specified.
- `bench/iceberg_pruning.py`: equivalent filtered and unfiltered queries with scan bytes and plan output.
- `bench/iceberg_time_travel.py`: capture a snapshot ID, make a deliberate bad write in a disposable table, and query `FOR VERSION AS OF` the earlier snapshot.

Do not claim these Iceberg results until the full local stack has been run and the scripts produce result files.

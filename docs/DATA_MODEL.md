# GameStream data model

## Event contract

Every generated action has a stable `event_id`, `user_id`, `game_id`, `event_type`, `event_ts`, `ingest_ts`, `schema_version`, `genre`, and optional purchase/review fields. Version 2 adds nullable `device_type` with an Avro default. Times are UTC milliseconds on the wire. `event_ts` is user action time; `ingest_ts` is delivery time. A deliberately late event has the former 30 minutes earlier than the latter. A logical duplicate repeats the entire record and ID.

`user_id` is the Kafka key. It keeps a user's records in one partition while the topic partition count is unchanged, but creates hot-key risk. The six-partition topic is replicated three times in the full profile with minimum ISR two. Kafka offsets are transport positions, not business identifiers.

## Storage layers

| Layer | Columns and key | Storage | Purpose |
|---|---|---|---|
| Bronze | Kafka key, binary payload, broker timestamp, topic, partition, offset | Iceberg by Kafka day | Retain the original frame and source position for audit and replay. |
| Silver | Decoded event contract plus `schema_id`; `event_id` is the logical key | Iceberg by event day | Validate and deduplicate within a 45-minute streaming watermark; `MERGE` guards target replay. |
| Gold | Date/game/cohort aggregates | dbt tables in Iceberg via Trino | Reproducible product analytics, rebuilt from silver. |
| Live | Decoded event rows; eventually replaced by `event_id` | ClickHouse `ReplacingMergeTree` by event month | Low-latency reads and materialized minute aggregate states. |

Bronze uses `kafka_ts` as its partition transform because raw Avro has not been decoded yet. Silver uses `event_ts`, which supports date-filtered analytical queries but produces small files under frequent microbatches. The authored maintenance DAG is intended to rewrite files and expire snapshots; it has not been exercised in the full stack. Never remove recent snapshots before a recovery window is agreed.

## Gold model grain and formulas

| Model | Grain | Definition |
|---|---|---|
| `dim_games` | one game | Latest observed genre by `event_ts`. |
| `active_users` | one activity day | Distinct users active on that day, in trailing seven days, and in trailing 30 days. |
| `retention_cohorts` | first activity date | Distinct first-day users who return one or seven calendar days later, divided by cohort size. |
| `purchase_funnel` | one day | Users with a page view, then same-day wishlist after the first view, then same-day purchase after the first wishlist. |
| `revenue_by_game` | day/game/genre | Purchase count and integer-cent sum; no floating currency arithmetic. |
| `game_popularity` | day/game | Views, starts, purchases, and unique users. |
| `review_sentiment` | day/game | Positive and negative counts from synthetic labels; it is not an NLP model. |

The synthetic generator chooses event types independently, so the funnel is a demonstration of temporal SQL rather than a calibrated conversion estimate. Gold models are full table rebuilds; incremental production use would require unique keys, a late-arrival window, and backfill tests.

## Serving semantics and checks

ClickHouse `ReplacingMergeTree` replacement is eventual. `FINAL` makes raw reads deduplicated but can cost more. Minute aggregates use `uniqState(event_id)` for duplicate-resistant counts. Before comparing ClickHouse and Trino query times, compare row counts and freshness because the stores have independent ingestion paths.

The DQ gate queries silver for null identifiers, duplicate IDs, unknown event types, and events more than a day in the future. A failure raises, so Airflow does not run the downstream dbt task. dbt also tests silver and mart keys, nulls, and game relationships. These checks catch defined faults; they do not prove all source records arrived.

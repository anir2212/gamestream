CREATE DATABASE IF NOT EXISTS gamestream;

CREATE TABLE IF NOT EXISTS gamestream.events (
  event_id String,
  user_id String,
  game_id String,
  event_type String,
  event_ts DateTime64(3, 'UTC'),
  ingest_ts DateTime64(3, 'UTC'),
  schema_version UInt8,
  genre String,
  amount_cents Nullable(Int32),
  review_positive Nullable(Bool),
  device_type Nullable(String)
) ENGINE = ReplacingMergeTree(ingest_ts)
PARTITION BY toYYYYMM(event_ts)
ORDER BY (event_id);

CREATE TABLE IF NOT EXISTS gamestream.events_kafka (
  event_id String,
  user_id String,
  game_id String,
  event_type String,
  event_ts DateTime64(3, 'UTC'),
  ingest_ts DateTime64(3, 'UTC'),
  schema_version UInt8,
  genre String,
  amount_cents Nullable(Int32),
  review_positive Nullable(Bool),
  device_type Nullable(String)
) ENGINE = Kafka SETTINGS
  kafka_broker_list = 'broker1:9092,broker2:9092,broker3:9092',
  kafka_topic_list = 'game_events',
  kafka_group_name = 'gamestream-clickhouse',
  kafka_format = 'AvroConfluent',
  format_avro_schema_registry_url = 'http://registry:8080/apis/ccompat/v7',
  kafka_num_consumers = 3;

CREATE MATERIALIZED VIEW IF NOT EXISTS gamestream.events_mv
TO gamestream.events AS
SELECT * FROM gamestream.events_kafka;

CREATE TABLE IF NOT EXISTS gamestream.minute_aggregates (
  minute DateTime('UTC'),
  game_id String,
  event_ids AggregateFunction(uniq, String),
  purchase_ids AggregateFunction(uniqIf, String, UInt8),
  active_users AggregateFunction(uniq, String)
) ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMMDD(minute)
ORDER BY (minute, game_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS gamestream.minute_mv
TO gamestream.minute_aggregates AS
SELECT
  toStartOfMinute(event_ts) AS minute,
  game_id,
  uniqState(event_id) AS event_ids,
  uniqIfState(event_id, event_type = 'purchase') AS purchase_ids,
  uniqState(user_id) AS active_users
FROM gamestream.events
GROUP BY minute, game_id;

# Superset dashboard specification

This is the intended local dashboard layout. The Superset image includes Trino and ClickHouse SQLAlchemy drivers. The actual dashboards have not yet been created/exported because the full stack cannot run within the current disk and Docker memory limit. Do not show mock screenshots as a running dashboard.

Connect the two databases after first local startup:

- ClickHouse: `clickhousedb://gamestream:local-demo-only@clickhouse:8123/gamestream`
- Trino: `trino://gamestream@trino:8080/lake/gold`

## Live operations (ClickHouse)

1. **Events/minute:** `SELECT minute, uniqMerge(event_ids) AS events FROM gamestream.minute_aggregates GROUP BY minute ORDER BY minute`.
2. **Active users/minute:** `SELECT minute, uniqMerge(active_users) AS users FROM gamestream.minute_aggregates GROUP BY minute ORDER BY minute`.
3. **Purchase rate/minute:** `SELECT minute, uniqIfMerge(purchase_ids)/nullIf(uniqMerge(event_ids),0) AS rate FROM gamestream.minute_aggregates GROUP BY minute ORDER BY minute`.

The minute state table is grouped by `(minute, game_id)`. Merge the aggregate states across games to avoid double-counting a user who visits two games. `uniq` is approximate; use `uniqExact` on raw events if an exact distinct count is required.

## Product analytics (Trino)

1. **Funnel:** `SELECT * FROM lake.gold.purchase_funnel ORDER BY day`.
2. **Retention:** `SELECT cohort_date, d1_rate, d7_rate FROM lake.gold.retention_cohorts ORDER BY cohort_date`.
3. **Game leaderboard:** `SELECT game_id, sum(purchases) AS purchases FROM lake.gold.game_popularity GROUP BY 1 ORDER BY 2 DESC LIMIT 20`.
4. **DAU/WAU/MAU:** `SELECT * FROM lake.gold.active_users ORDER BY day`.

Filter all charts to the synthetic event dates (the generator defaults to January 2026), then export the native dashboard ZIP from a live Superset instance and commit it. Until then the acceptance criterion for exported dashboards and screenshots is open.

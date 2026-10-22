# Superset dashboard specification

This is the local dashboard layout. The Superset image includes Trino and ClickHouse SQLAlchemy drivers. On 22 October both dashboards were created from this specification by `scripts/superset_dashboards.py` against a live Superset 4.1.2. The script also exported them to `exports/gamestream_dashboards_2026-10-22.zip`; the two PNG files beside it are headless-Chrome screenshots. Recreate them with `uv run python scripts/superset_dashboards.py --out superset/exports/<name>.zip` while the stack is running. The script reuses existing objects and refuses to export if any chart query fails or returns no rows. To restore the saved version instead, import the ZIP in Superset (Dashboards → Import) and re-enter the database passwords, which the export masks.

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

The data behind the saved export covers three synthetic days (1 January, 1 February and 3 February 2026). Charts are not yet filtered to those dates, so the line charts draw straight across the empty weeks, and D1/D7 retention is 0 because no fixture user returns the next day. Adding a date filter is a presentation change still open.

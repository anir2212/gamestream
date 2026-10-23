# Superset dashboard specification

This is the local dashboard layout. The Superset image includes Trino and ClickHouse SQLAlchemy drivers. `scripts/superset_dashboards.py` builds both dashboards from this specification through Superset's REST API. On 22 October it first created them against a live Superset 4.1.2 (`exports/gamestream_dashboards_2026-10-22.zip` and its two screenshots). On 23 October it added the date filter described below and exported `exports/gamestream_dashboards_2026-10-23.zip`; the four `*_2026-10-23.png` files beside it are headless-Chrome screenshots of each dashboard at the default range and restricted to 1 February. Recreate them with `uv run python scripts/superset_dashboards.py --out superset/exports/<name>.zip` while the stack is running. The script updates objects that already exist instead of duplicating them, and refuses to export if any chart query fails or returns no data. `node scripts/superset_browser_check.mjs <dashboard-id> <out.png> <out.json> [time-range]` opens a dashboard as the local admin in headless Chrome and records the date range and row count of every chart query the page sent. To restore a saved version instead, import the ZIP in Superset (Dashboards → Import) and re-enter the database passwords, which the export masks.

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
3. **Game leaderboard:** dataset `SELECT day, game_id, purchases FROM lake.gold.game_popularity`; the chart sums `purchases` per `game_id` and keeps the top 20. The sum lives in the chart rather than the SQL so that the date filter applies to `day` first.
4. **DAU/WAU/MAU:** `SELECT * FROM lake.gold.active_users ORDER BY day`.

## Date filter and empty periods

Each dashboard has one native **Event date** filter (a Superset time-range filter, id `NATIVE_FILTER-gamestream-date-<dashboard id>`) scoped to all of its charts. Every chart carries a `TEMPORAL_RANGE` filter on its date column (`minute`, `day` or `cohort_date`), and the dashboard filter's range replaces that filter's value. The default is `2026-01-01T00:00:00 : 2026-02-04T00:00:00`. The synthetic data covers 1 January, 1 February and 3 February 2026, and Superset treats the end of a range as exclusive. Pass `--time-range` to the builder to choose another default.

Line charts are resampled to their own grain (`1T` for the minute charts, `1D` for DAU/WAU/MAU) with the null-filling method (`asfreq`). A minute or day without data becomes a null, and the chart breaks the line there instead of drawing a straight line across empty weeks. Point markers are on, so a period with no neighbours still shows as a dot. Tables are unaffected by resampling. Over the full default range each minute chart is a thin band per day; narrow the filter (for example to `2026-02-01T00:00:00 : 2026-02-02T00:00:00`) to read the minutes.

D1/D7 retention is 0 because no fixture user returns the next day or week. That comes from the data, not the dashboard.

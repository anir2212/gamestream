"""Create the two Superset dashboards in superset/DASHBOARDS.md and export them.

Run against a live local Superset (compose service `superset`, port 18088):

    uv run python scripts/superset_dashboards.py --out superset/exports/gamestream_dashboards.zip

It logs in with the local demo admin, creates the ClickHouse and Trino database
connections, one virtual dataset per chart (the SQL from DASHBOARDS.md), the
charts and two dashboards, then asks Superset to run every chart's query and
refuses to export if any query fails or returns no rows. Each dashboard has a
native date-range filter that every chart responds to; its default covers the
synthetic data's real dates (``--time-range``). Line charts are resampled to
their own grain with empty periods left as nulls, so a gap between days is drawn
as a gap rather than a straight line. Objects whose names already exist are
updated in place, so a second run does not duplicate them.
"""

from __future__ import annotations

import argparse
import http.cookiejar
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import NamedTuple

BASE = "http://127.0.0.1:18088"
DATABASES = {
    "GameStream ClickHouse": "clickhousedb://gamestream:local-demo-only@clickhouse:8123/gamestream",
    "GameStream Trino gold": "trino://gamestream@trino:8080/lake/gold",
}
# The saved fixtures cover 2026-01-01, 2026-02-01 and 2026-02-03; Superset's end is exclusive.
DEFAULT_TIME_RANGE = "2026-01-01T00:00:00 : 2026-02-04T00:00:00"
NO_FILTER = "No filter"
LINE, TABLE = "echarts_timeseries_line", "table"
GRAIN = {"minute": ("PT1M", "1T"), "day": ("P1D", "1D")}  # time grain, resample rule


class Chart(NamedTuple):
    name: str
    database: str
    viz: str
    time_column: str  # the dashboard date filter applies here; also the line chart's x-axis
    columns: list[str]  # table columns, or the measured columns of a line/aggregated table
    sql: str
    groupby: tuple[str, ...] = ()  # aggregated table only: group by these, SUM the columns


DASHBOARDS = {
    "GameStream live operations (ClickHouse)": [
        Chart("Events per minute", "GameStream ClickHouse", LINE, "minute", ["events"],
              "SELECT minute, uniqMerge(event_ids) AS events FROM gamestream.minute_aggregates "
              "GROUP BY minute ORDER BY minute"),
        Chart("Active users per minute", "GameStream ClickHouse", LINE, "minute", ["users"],
              "SELECT minute, uniqMerge(active_users) AS users FROM gamestream.minute_aggregates "
              "GROUP BY minute ORDER BY minute"),
        Chart("Purchase rate per minute", "GameStream ClickHouse", LINE, "minute", ["rate"],
              "SELECT minute, uniqIfMerge(purchase_ids) / nullIf(uniqMerge(event_ids), 0) AS rate "
              "FROM gamestream.minute_aggregates GROUP BY minute ORDER BY minute"),
    ],
    "GameStream product analytics (Trino)": [
        Chart("Purchase funnel by day", "GameStream Trino gold", TABLE, "day",
              ["day", "viewers", "wishlisters", "purchasers"],
              "SELECT * FROM lake.gold.purchase_funnel ORDER BY day"),
        Chart("D1 and D7 retention", "GameStream Trino gold", TABLE, "cohort_date",
              ["cohort_date", "d1_rate", "d7_rate"],
              "SELECT cohort_date, d1_rate, d7_rate FROM lake.gold.retention_cohorts "
              "ORDER BY cohort_date"),
        # Kept at day grain so the date filter can apply before the per-game sum.
        Chart("Game leaderboard by purchases", "GameStream Trino gold", TABLE, "day",
              ["purchases"],
              "SELECT day, game_id, purchases FROM lake.gold.game_popularity",
              groupby=("game_id",)),
        Chart("DAU, WAU and MAU", "GameStream Trino gold", LINE, "day", ["dau", "wau", "mau"],
              "SELECT * FROM lake.gold.active_users ORDER BY day"),
    ],
}


class Client:
    def __init__(self, base: str):
        self.base = base
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
        self.headers = {"Content-Type": "application/json", "Referer": base}

    def call(self, method: str, path: str, body=None, raw: bool = False):
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(self.base + path, data=data, method=method,
                                         headers=self.headers)
        try:
            with self.opener.open(request, timeout=120) as response:
                payload = response.read()
        except urllib.error.HTTPError as error:
            raise RuntimeError(
                f"{method} {path} -> {error.code}: {error.read()[:400]!r}") from None
        return payload if raw else json.loads(payload or b"{}")

    def login(self, user: str, password: str) -> None:
        token = self.call("POST", "/api/v1/security/login", {
            "username": user, "password": password, "provider": "db", "refresh": True,
        })["access_token"]
        self.headers["Authorization"] = f"Bearer {token}"
        self.headers["X-CSRFToken"] = self.call("GET", "/api/v1/security/csrf_token/")["result"]

    def find(self, resource: str, column: str, value: str) -> int | None:
        query = f"(filters:!((col:{column},opr:eq,value:'{value}')))"
        rows = self.call("GET", f"/api/v1/{resource}/?q={urllib.request.quote(query)}")["result"]
        return rows[0]["id"] if rows else None


def dataset_name(chart: Chart) -> str:
    return f"gs_{chart.name.lower().replace(' ', '_').replace(',', '')}"


def metric(column: str, aggregate: str = "MAX") -> dict:
    return {"expressionType": "SQL", "sqlExpression": f"{aggregate}({column})", "label": column}


def chart_metrics(chart: Chart) -> list[dict]:
    # Line datasets already hold one row per minute or day, so MAX returns that value.
    return [metric(column, "SUM" if chart.groupby else "MAX") for column in chart.columns]


def time_filter(column: str, time_range: str = NO_FILTER) -> dict:
    """The adhoc filter whose comparator a dashboard time-range filter replaces."""
    return {"clause": "WHERE", "subject": column, "operator": "TEMPORAL_RANGE",
            "comparator": time_range, "expressionType": "SIMPLE"}


def chart_params(chart: Chart, dataset_id: int) -> dict:
    params = {"datasource": f"{dataset_id}__table", "viz_type": chart.viz, "row_limit": 10000,
              "adhoc_filters": [time_filter(chart.time_column)]}
    if chart.viz == LINE:
        grain, rule = GRAIN["minute" if chart.time_column == "minute" else "day"]
        params.update({
            "x_axis": chart.time_column, "time_grain_sqla": grain,
            "metrics": chart_metrics(chart), "groupby": [],
            # Fill missing periods with nulls; ECharts breaks the line at a null.
            "resample_rule": rule, "resample_method": "asfreq",
            # A period with empty neighbours has no line to draw, so show every point.
            "markerEnabled": True, "markerSize": 6 if grain == "P1D" else 2,
        })
    elif chart.groupby:
        params.update({"query_mode": "aggregate", "groupby": list(chart.groupby),
                       "metrics": chart_metrics(chart), "row_limit": 20,
                       "timeseries_limit_metric": chart_metrics(chart)[0],
                       "order_desc": True})
    else:
        params.update({"query_mode": "raw", "all_columns": chart.columns,
                       "order_by_cols": [json.dumps([chart.time_column, True])]})
    return params


def chart_query(chart: Chart, dataset_id: int, time_range: str = NO_FILTER) -> dict:
    """The query the dashboard sends for this chart under the given date range.

    As in the Superset UI, the date range travels as ``time_range``; the server copies it
    into the chart's TEMPORAL_RANGE filter."""
    filters = [{"col": chart.time_column, "op": "TEMPORAL_RANGE", "val": NO_FILTER}]
    if chart.viz == LINE:
        grain, rule = GRAIN["minute" if chart.time_column == "minute" else "day"]
        axis = {"columnType": "BASE_AXIS", "sqlExpression": chart.time_column,
                "label": chart.time_column, "expressionType": "SQL", "timeGrain": grain}
        labels = list(chart.columns)
        query = {"columns": [axis], "metrics": chart_metrics(chart),
                 "orderby": [[chart_metrics(chart)[0], False]], "row_limit": 10000,
                 "post_processing": [
                     {"operation": "pivot", "options": {
                         "index": [chart.time_column], "columns": [],
                         "aggregates": {label: {"operator": "mean"} for label in labels},
                         "drop_missing_columns": False}},
                     {"operation": "resample", "options": {
                         "method": "asfreq", "rule": rule, "fill_value": None}},
                     {"operation": "flatten"},
                 ]}
    elif chart.groupby:
        query = {"columns": list(chart.groupby), "metrics": chart_metrics(chart),
                 "orderby": [[chart_metrics(chart)[0], False]], "row_limit": 20}
    else:
        query = {"columns": chart.columns, "metrics": [],
                 "orderby": [[chart.time_column, True]], "row_limit": 10000}
    query.update({"filters": filters, "time_range": time_range})
    return {"datasource": {"id": dataset_id, "type": "table"}, "force": True,
            "queries": [query], "result_format": "json", "result_type": "full"}


def date_filter(dashboard_id: int, chart_ids: list[int], time_range: str) -> dict:
    """One native time-range filter scoped to every chart on the dashboard."""
    return {
        "id": f"NATIVE_FILTER-gamestream-date-{dashboard_id}", "name": "Event date",
        "filterType": "filter_time", "type": "NATIVE_FILTER", "targets": [{}],
        "controlValues": {"enableEmptyFilter": False},
        "defaultDataMask": {"extraFormData": {"time_range": time_range},
                            "filterState": {"value": time_range}, "ownState": {}},
        "cascadeParentIds": [], "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
        "chartsInScope": chart_ids, "tabsInScope": [],
        "description": "Event date range; the end date is exclusive.",
    }


def dashboard_metadata(dashboard_id: int, chart_ids: list[int], time_range: str) -> dict:
    return {"native_filter_configuration": [date_filter(dashboard_id, chart_ids, time_range)],
            "chart_configuration": {}, "global_chart_configuration": {
                "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
                "chartsInScope": chart_ids},
            "cross_filters_enabled": False, "refresh_frequency": 0,
            "timed_refresh_immune_slices": [], "expanded_slices": {}, "color_scheme": "",
            "label_colors": {}, "shared_label_colors": {}}


def layout(title: str, charts: list[tuple[int, str]]) -> dict:
    position = {"DASHBOARD_VERSION_KEY": "v2",
                "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
                "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [],
                            "parents": ["ROOT_ID"]},
                "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": title}}}
    for row_index in range(0, len(charts), 2):
        row_id = f"ROW-{row_index // 2 + 1}"
        position["GRID_ID"]["children"].append(row_id)
        position[row_id] = {"type": "ROW", "id": row_id, "children": [],
                            "parents": ["ROOT_ID", "GRID_ID"],
                            "meta": {"background": "BACKGROUND_TRANSPARENT"}}
        for chart_id, name in charts[row_index:row_index + 2]:
            key = f"CHART-{chart_id}"
            position[row_id]["children"].append(key)
            position[key] = {"type": "CHART", "id": key, "children": [],
                             "parents": ["ROOT_ID", "GRID_ID", row_id],
                             "meta": {"chartId": chart_id, "width": 6, "height": 50,
                                      "sliceName": name}}
    return position


def summarize(result: dict, chart: Chart) -> dict:
    """Rows returned, rows with a value (resampling adds empty periods as null rows), and
    the first three rows with a value, so two date ranges can be compared."""
    data = result.get("data", [])
    filled = [row for row in data if row.get(chart.columns[0]) is not None]
    return {"rows": result.get("rowcount", len(data)), "rows_with_value": len(filled),
            "first_rows": filled[:3]}


def upsert_dataset(client: Client, database_id: int, chart: Chart) -> int:
    name = dataset_name(chart)
    dataset_id = client.find("dataset", "table_name", name)
    if dataset_id is None:
        return client.call("POST", "/api/v1/dataset/", {"database": database_id,
                                                         "table_name": name,
                                                         "sql": chart.sql})["id"]
    if client.call("GET", f"/api/v1/dataset/{dataset_id}")["result"]["sql"] != chart.sql:
        client.call("PUT", f"/api/v1/dataset/{dataset_id}", {"sql": chart.sql})
        client.call("PUT", f"/api/v1/dataset/{dataset_id}/refresh")
    return dataset_id


def upsert_chart(client: Client, chart: Chart, dataset_id: int) -> int:
    body = {"slice_name": chart.name, "viz_type": chart.viz, "datasource_id": dataset_id,
            "datasource_type": "table", "params": json.dumps(chart_params(chart, dataset_id)),
            "query_context": json.dumps(chart_query(chart, dataset_id))}
    chart_id = client.find("chart", "slice_name", chart.name)
    if chart_id is None:
        return client.call("POST", "/api/v1/chart/", body)["id"]
    client.call("PUT", f"/api/v1/chart/{chart_id}", body)
    return chart_id


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--base", default=BASE)
    parser.add_argument("--user", default="admin")
    parser.add_argument("--password", default="local-demo-only")
    parser.add_argument("--time-range", default=DEFAULT_TIME_RANGE,
                        help="Default of each dashboard's date filter (Superset syntax)")
    parser.add_argument("--compare-range",
                        help="Also report every chart's rows under this range (not saved)")
    parser.add_argument("--out", type=Path, required=True, help="Path for the exported ZIP")
    args = parser.parse_args()
    client = Client(args.base)
    client.login(args.user, args.password)

    database_ids = {}
    for name, uri in DATABASES.items():
        database_ids[name] = client.find("database", "database_name", name) or client.call(
            "POST", "/api/v1/database/", {"database_name": name, "sqlalchemy_uri": uri,
                                          "expose_in_sqllab": True})["id"]

    report = {"time_range": args.time_range, "compare_range": args.compare_range,
              "dashboards": {}, "charts": {}}
    dashboard_ids = []
    for title, specs in DASHBOARDS.items():
        charts = []
        for chart in specs:
            dataset_id = upsert_dataset(client, database_ids[chart.database], chart)
            chart_id = upsert_chart(client, chart, dataset_id)
            query = chart_query(chart, dataset_id, args.time_range)
            result = client.call("POST", "/api/v1/chart/data", query)["result"][0]
            counts = summarize(result, chart)
            if result.get("error") or not counts["rows_with_value"]:
                raise RuntimeError(f"chart {chart.name!r} returned no data: {result.get('error')}")
            report["charts"][chart.name] = {"id": chart_id, "dataset": dataset_name(chart),
                                            "viz_type": chart.viz, **counts}
            if args.compare_range:
                query = chart_query(chart, dataset_id, args.compare_range)
                result = client.call("POST", "/api/v1/chart/data", query)["result"][0]
                report["charts"][chart.name]["compare"] = summarize(result, chart)
            charts.append((chart_id, chart.name))
        chart_ids = [chart_id for chart_id, _ in charts]
        dashboard_id = client.find("dashboard", "dashboard_title", title)
        body = {"dashboard_title": title, "published": True,
                "position_json": json.dumps(layout(title, charts))}
        if not dashboard_id:
            dashboard_id = client.call("POST", "/api/v1/dashboard/", body)["id"]
        body["json_metadata"] = json.dumps(
            dashboard_metadata(dashboard_id, chart_ids, args.time_range))
        client.call("PUT", f"/api/v1/dashboard/{dashboard_id}", body)
        for chart_id in chart_ids:
            client.call("PUT", f"/api/v1/chart/{chart_id}", {"dashboards": [dashboard_id]})
        report["dashboards"][title] = {"id": dashboard_id, "charts": len(charts),
                                       "date_filter": date_filter(dashboard_id, chart_ids,
                                                                  args.time_range)["id"]}
        dashboard_ids.append(dashboard_id)

    ids = ",".join(str(item) for item in dashboard_ids)
    archive = client.call("GET", f"/api/v1/dashboard/export/?q=!({ids})", raw=True)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_bytes(archive)
    report["export"] = {"file": str(args.out), "bytes": len(archive)}
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())

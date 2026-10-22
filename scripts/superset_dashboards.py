"""Create the two Superset dashboards in superset/DASHBOARDS.md and export them.

Run against a live local Superset (compose service `superset`, port 18088):

    uv run python scripts/superset_dashboards.py --out superset/exports/gamestream_dashboards.zip

It logs in with the local demo admin, creates the ClickHouse and Trino database
connections, one virtual dataset per chart (the SQL from DASHBOARDS.md), the
charts and two dashboards, then asks Superset to run every chart's query and
refuses to export if any query fails or returns no rows. Objects whose names
already exist are reused, so a second run does not duplicate them.
"""

from __future__ import annotations

import argparse
import http.cookiejar
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path

BASE = "http://127.0.0.1:18088"
DATABASES = {
    "GameStream ClickHouse": "clickhousedb://gamestream:local-demo-only@clickhouse:8123/gamestream",
    "GameStream Trino gold": "trino://gamestream@trino:8080/lake/gold",
}
LINE, TABLE = "echarts_timeseries_line", "table"
DASHBOARDS = {
    "GameStream live operations (ClickHouse)": [
        ("Events per minute", "GameStream ClickHouse", LINE, "minute", ["events"],
         "SELECT minute, uniqMerge(event_ids) AS events FROM gamestream.minute_aggregates "
         "GROUP BY minute ORDER BY minute"),
        ("Active users per minute", "GameStream ClickHouse", LINE, "minute", ["users"],
         "SELECT minute, uniqMerge(active_users) AS users FROM gamestream.minute_aggregates "
         "GROUP BY minute ORDER BY minute"),
        ("Purchase rate per minute", "GameStream ClickHouse", LINE, "minute", ["rate"],
         "SELECT minute, uniqIfMerge(purchase_ids) / nullIf(uniqMerge(event_ids), 0) AS rate "
         "FROM gamestream.minute_aggregates GROUP BY minute ORDER BY minute"),
    ],
    "GameStream product analytics (Trino)": [
        ("Purchase funnel by day", "GameStream Trino gold", TABLE, None,
         ["day", "viewers", "wishlisters", "purchasers"],
         "SELECT * FROM lake.gold.purchase_funnel ORDER BY day"),
        ("D1 and D7 retention", "GameStream Trino gold", TABLE, None,
         ["cohort_date", "d1_rate", "d7_rate"],
         "SELECT cohort_date, d1_rate, d7_rate FROM lake.gold.retention_cohorts "
         "ORDER BY cohort_date"),
        ("Game leaderboard by purchases", "GameStream Trino gold", TABLE, None,
         ["game_id", "purchases"],
         "SELECT game_id, sum(purchases) AS purchases FROM lake.gold.game_popularity "
         "GROUP BY 1 ORDER BY 2 DESC LIMIT 20"),
        ("DAU, WAU and MAU", "GameStream Trino gold", LINE, "day", ["dau", "wau", "mau"],
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


def metric(column: str) -> dict:
    return {"expressionType": "SQL", "sqlExpression": f"MAX({column})", "label": column}


def chart_params(viz: str, dataset_id: int, axis: str | None, columns: list[str]) -> dict:
    params = {"datasource": f"{dataset_id}__table", "viz_type": viz, "row_limit": 10000,
              "adhoc_filters": []}
    if viz == LINE:
        params.update({"x_axis": axis, "time_grain_sqla": "PT1M" if axis == "minute" else "P1D",
                       "metrics": [metric(column) for column in columns], "groupby": []})
    else:
        params.update({"query_mode": "raw", "all_columns": columns, "order_by_cols": []})
    return params


def chart_query(viz: str, dataset_id: int, axis: str | None, columns: list[str]) -> dict:
    if viz == LINE:
        query = {"columns": [axis], "metrics": [metric(column) for column in columns],
                 "orderby": [[metric(columns[0]), False]], "row_limit": 10000}
        query["groupby"] = [axis]
    else:
        query = {"columns": columns, "metrics": [], "row_limit": 10000}
    return {"datasource": {"id": dataset_id, "type": "table"}, "force": True,
            "queries": [query], "result_format": "json", "result_type": "full"}


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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--base", default=BASE)
    parser.add_argument("--user", default="admin")
    parser.add_argument("--password", default="local-demo-only")
    parser.add_argument("--out", type=Path, required=True, help="Path for the exported ZIP")
    args = parser.parse_args()
    client = Client(args.base)
    client.login(args.user, args.password)

    database_ids = {}
    for name, uri in DATABASES.items():
        database_ids[name] = client.find("database", "database_name", name) or client.call(
            "POST", "/api/v1/database/", {"database_name": name, "sqlalchemy_uri": uri,
                                          "expose_in_sqllab": True})["id"]

    report = {"dashboards": {}, "charts": {}}
    dashboard_ids = []
    for title, specs in DASHBOARDS.items():
        charts = []
        for name, database, viz, axis, columns, sql in specs:
            dataset_name = f"gs_{name.lower().replace(' ', '_').replace(',', '')}"
            dataset_id = client.find("dataset", "table_name", dataset_name) or client.call(
                "POST", "/api/v1/dataset/", {"database": database_ids[database],
                                             "table_name": dataset_name, "sql": sql})["id"]
            params = chart_params(viz, dataset_id, axis, columns)
            query = chart_query(viz, dataset_id, axis, columns)
            chart_id = client.find("chart", "slice_name", name) or client.call(
                "POST", "/api/v1/chart/", {"slice_name": name, "viz_type": viz,
                                           "datasource_id": dataset_id, "datasource_type": "table",
                                           "params": json.dumps(params),
                                           "query_context": json.dumps(query)})["id"]
            result = client.call("POST", "/api/v1/chart/data", query)["result"][0]
            rows = result.get("rowcount", len(result.get("data", [])))
            if result.get("error") or not rows:
                raise RuntimeError(f"chart {name!r} returned no data: {result.get('error')}")
            report["charts"][name] = {"id": chart_id, "dataset": dataset_name, "rows": rows,
                                      "viz_type": viz}
            charts.append((chart_id, name))
        dashboard_id = client.find("dashboard", "dashboard_title", title)
        body = {"dashboard_title": title, "published": True,
                "position_json": json.dumps(layout(title, charts))}
        if dashboard_id:
            client.call("PUT", f"/api/v1/dashboard/{dashboard_id}", body)
        else:
            dashboard_id = client.call("POST", "/api/v1/dashboard/", body)["id"]
        for chart_id, _ in charts:
            client.call("PUT", f"/api/v1/chart/{chart_id}", {"dashboards": [dashboard_id]})
        report["dashboards"][title] = {"id": dashboard_id, "charts": len(charts)}
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

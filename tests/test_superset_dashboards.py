"""The Superset dashboard builder's layout and chart definitions (no server needed)."""

import json

from scripts.superset_dashboards import DASHBOARDS, LINE, TABLE, chart_params, chart_query, layout


def test_layout_places_every_chart_once_in_rows_of_two():
    position = layout("Demo", [(1, "a"), (2, "b"), (3, "c")])
    charts = [key for key, value in position.items()
              if isinstance(value, dict) and value.get("type") == "CHART"]
    assert sorted(charts) == ["CHART-1", "CHART-2", "CHART-3"]
    assert position["GRID_ID"]["children"] == ["ROW-1", "ROW-2"]
    assert position["ROW-1"]["children"] == ["CHART-1", "CHART-2"]
    assert position["CHART-3"]["meta"] == {"chartId": 3, "width": 6, "height": 50, "sliceName": "c"}
    json.dumps(position)


def test_line_and_table_charts_build_matching_params_and_queries():
    line = chart_params(LINE, 7, "minute", ["events"])
    assert line["x_axis"] == "minute" and line["time_grain_sqla"] == "PT1M"
    assert line["metrics"][0]["sqlExpression"] == "MAX(events)"
    assert chart_query(LINE, 7, "minute", ["events"])["queries"][0]["columns"] == ["minute"]
    table = chart_params(TABLE, 8, None, ["day", "viewers"])
    assert table["query_mode"] == "raw" and table["all_columns"] == ["day", "viewers"]
    assert chart_query(TABLE, 8, None, ["day", "viewers"])["queries"][0]["metrics"] == []


def test_dashboards_match_the_specification():
    assert {title: len(specs) for title, specs in DASHBOARDS.items()} == {
        "GameStream live operations (ClickHouse)": 3, "GameStream product analytics (Trino)": 4}
    for specs in DASHBOARDS.values():
        for _name, _database, viz, axis, _columns, sql in specs:
            assert sql.lstrip().upper().startswith("SELECT")
            assert (axis is not None) == (viz == LINE)

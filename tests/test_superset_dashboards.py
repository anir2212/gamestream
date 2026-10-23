"""The Superset dashboard builder's layout, chart and filter definitions (no server needed)."""

import json

from scripts.superset_dashboards import (
    DASHBOARDS,
    DEFAULT_TIME_RANGE,
    LINE,
    NO_FILTER,
    TABLE,
    Chart,
    chart_params,
    chart_query,
    dashboard_metadata,
    date_filter,
    layout,
    summarize,
)

EVENTS = Chart("Events", "db", LINE, "minute", ["events"], "SELECT 1")
FUNNEL = Chart("Funnel", "db", TABLE, "day", ["day", "viewers"], "SELECT 1")
LEADERS = Chart("Leaders", "db", TABLE, "day", ["purchases"], "SELECT 1", groupby=("game_id",))
DAU = Chart("DAU", "db", LINE, "day", ["dau", "wau"], "SELECT 1")


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
    line = chart_params(EVENTS, 7)
    assert line["x_axis"] == "minute" and line["time_grain_sqla"] == "PT1M"
    assert line["metrics"][0]["sqlExpression"] == "MAX(events)"
    assert chart_query(EVENTS, 7)["queries"][0]["columns"][0]["sqlExpression"] == "minute"
    table = chart_params(FUNNEL, 8)
    assert table["query_mode"] == "raw" and table["all_columns"] == ["day", "viewers"]
    assert chart_query(FUNNEL, 8)["queries"][0]["metrics"] == []


def test_every_chart_carries_a_temporal_filter_the_dashboard_can_override():
    for chart in (EVENTS, FUNNEL, LEADERS, DAU):
        (adhoc,) = chart_params(chart, 1)["adhoc_filters"]
        assert adhoc["operator"] == "TEMPORAL_RANGE" and adhoc["subject"] == chart.time_column
        assert adhoc["comparator"] == NO_FILTER
        query = chart_query(chart, 1, "2026-02-01T00:00:00 : 2026-02-02T00:00:00")["queries"][0]
        assert query["time_range"] == "2026-02-01T00:00:00 : 2026-02-02T00:00:00"
        assert query["filters"] == [
            {"col": chart.time_column, "op": "TEMPORAL_RANGE", "val": NO_FILTER}]
    assert chart_query(FUNNEL, 1)["queries"][0]["time_range"] == NO_FILTER


def test_line_charts_resample_empty_periods_to_nulls_instead_of_joining_them():
    for chart, grain, rule in ((EVENTS, "PT1M", "1T"), (DAU, "P1D", "1D")):
        params = chart_params(chart, 1)
        assert params["time_grain_sqla"] == grain
        assert (params["resample_rule"], params["resample_method"]) == (rule, "asfreq")
        assert params["markerEnabled"] is True
        steps = chart_query(chart, 1)["queries"][0]["post_processing"]
        assert [step["operation"] for step in steps] == ["pivot", "resample", "flatten"]
        assert steps[1]["options"] == {"method": "asfreq", "rule": rule, "fill_value": None}
        assert set(steps[0]["options"]["aggregates"]) == set(chart.columns)


def test_aggregated_leaderboard_sums_per_game_after_the_date_filter():
    params = chart_params(LEADERS, 3)
    assert params["query_mode"] == "aggregate" and params["groupby"] == ["game_id"]
    assert params["metrics"][0]["sqlExpression"] == "SUM(purchases)"
    query = chart_query(LEADERS, 3)["queries"][0]
    assert query["columns"] == ["game_id"] and query["row_limit"] == 20
    assert query["orderby"] == [[query["metrics"][0], False]]


def test_dashboard_date_filter_targets_every_chart_with_the_default_range():
    metadata = dashboard_metadata(2, [4, 5, 6, 7], DEFAULT_TIME_RANGE)
    (native,) = metadata["native_filter_configuration"]
    assert native == date_filter(2, [4, 5, 6, 7], DEFAULT_TIME_RANGE)
    assert native["id"] == "NATIVE_FILTER-gamestream-date-2"
    assert native["filterType"] == "filter_time" and native["chartsInScope"] == [4, 5, 6, 7]
    assert native["defaultDataMask"]["extraFormData"] == {"time_range": DEFAULT_TIME_RANGE}
    assert native["defaultDataMask"]["filterState"] == {"value": DEFAULT_TIME_RANGE}
    assert DEFAULT_TIME_RANGE == "2026-01-01T00:00:00 : 2026-02-04T00:00:00"
    json.dumps(metadata)


def test_summary_counts_only_rows_with_values():
    result = {"rowcount": 4, "data": [{"day": 1, "dau": 3}, {"day": 2, "dau": None},
                                      {"day": 3, "dau": None}, {"day": 4, "dau": 5}]}
    summary = summarize(result, Chart("DAU", "db", LINE, "day", ["dau"], "SELECT 1"))
    assert summary == {"rows": 4, "rows_with_value": 2,
                       "first_rows": [{"day": 1, "dau": 3}, {"day": 4, "dau": 5}]}


def test_dashboards_match_the_specification():
    assert {title: len(specs) for title, specs in DASHBOARDS.items()} == {
        "GameStream live operations (ClickHouse)": 3, "GameStream product analytics (Trino)": 4}
    for specs in DASHBOARDS.values():
        for chart in specs:
            assert chart.sql.lstrip().upper().startswith("SELECT")
            assert chart.time_column in chart.sql or chart.sql.startswith("SELECT *")
            assert not chart.groupby or chart.viz == TABLE

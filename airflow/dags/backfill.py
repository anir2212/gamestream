"""Manually trigger a bounded event-date backfill, followed by DQ and marts."""

from __future__ import annotations

import subprocess
from datetime import UTC, date, datetime

from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from airflow import DAG


def run_backfill(**context):
    params = context["params"]
    start = date.fromisoformat(params["start_date"])
    end = date.fromisoformat(params["end_date"])
    if start > end or (end - start).days > 31:
        raise ValueError("Backfill requires an ascending range of at most 31 days")
    subprocess.run(
        [
            "spark-submit", "--master", "spark://spark-master:7077",
            "--packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.7",
            "/opt/gamestream/spark/backfill.py", start.isoformat(), end.isoformat(),
        ],
        cwd="/opt/gamestream",
        check=True,
    )


def run_quality():
    subprocess.run(["python", "-m", "dq.run_checks"], cwd="/opt/gamestream", check=True)


def rebuild_marts():
    subprocess.run(
        ["dbt", "build", "--profiles-dir", ".", "--target", "local"],
        cwd="/opt/gamestream/dbt",
        check=True,
    )


with DAG(
    dag_id="gamestream_backfill",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    schedule=None,
    catchup=False,
    params={
        "start_date": Param("2026-01-01", type="string", format="date"),
        "end_date": Param("2026-01-01", type="string", format="date"),
    },
    tags=["gamestream"],
) as dag:
    load = PythonOperator(task_id="backfill_silver", python_callable=run_backfill)
    quality = PythonOperator(task_id="quality_gate", python_callable=run_quality)
    marts = PythonOperator(task_id="rebuild_marts", python_callable=rebuild_marts)
    load >> quality >> marts

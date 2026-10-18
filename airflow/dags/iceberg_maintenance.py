"""Scheduled Iceberg compaction, snapshot expiry, and orphan-file cleanup."""

from __future__ import annotations

from datetime import UTC, datetime

from airflow.operators.bash import BashOperator

from airflow import DAG

SPARK = "cd /opt/gamestream && spark-submit --master spark://spark-master:7077 "
PACKAGES = (
    "--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.7 "
)

with DAG(
    dag_id="gamestream_iceberg_maintenance",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    schedule="0 4 * * 0",
    catchup=False,
    tags=["gamestream"],
) as dag:
    compact = BashOperator(
        task_id="compact_and_expire",
        bash_command=SPARK + PACKAGES + "spark/batch_compact.py",
    )

"""Quality-gated daily dbt refresh. Airflow only schedules; SQL remains in dbt."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from airflow.operators.bash import BashOperator

from airflow import DAG

with DAG(
    dag_id="gamestream_daily_marts",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    schedule="0 3 * * *",
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["gamestream"],
) as dag:
    quality = BashOperator(
        task_id="silver_quality_gate",
        bash_command="cd /opt/gamestream && python -m dq.run_checks",
    )
    build = BashOperator(
        task_id="dbt_build",
        # The dbt project is mounted read-only; logs and compiled files go to /tmp.
        bash_command=(
            "cd /opt/gamestream/dbt && dbt build --profiles-dir . --target local"
            " --log-path /tmp/dbt-logs --target-path /tmp/dbt-target"
        ),
    )
    quality >> build

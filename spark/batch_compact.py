"""Compact tiny Iceberg files and expire old snapshots after retention."""

from __future__ import annotations

from spark.common import spark_session


def main() -> None:
    spark = spark_session("gamestream-maintenance")
    for table in ("bronze.events", "silver.events"):
        spark.sql(f"CALL lake.system.rewrite_data_files(table => '{table}')")
        spark.sql(f"CALL lake.system.expire_snapshots(table => '{table}', retain_last => 7)")
        spark.sql(f"CALL lake.system.remove_orphan_files(table => '{table}')")
    spark.stop()


if __name__ == "__main__":
    main()

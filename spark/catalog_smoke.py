"""Check that Spark can create, write, and read an Iceberg table in MinIO."""

from __future__ import annotations

from spark.common import spark_session


def main() -> None:
    spark = spark_session("gamestream-catalog-smoke")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.catalog_smoke")
    try:
        spark.sql("DROP TABLE IF EXISTS lake.catalog_smoke.probe")
        spark.sql("CREATE TABLE lake.catalog_smoke.probe (id INT) USING iceberg")
        spark.sql("INSERT INTO lake.catalog_smoke.probe VALUES (1)")
        count = spark.sql("SELECT COUNT(*) FROM lake.catalog_smoke.probe").first()[0]
        if count != 1:
            raise RuntimeError(f"expected one Iceberg probe row, got {count}")
        print("Iceberg JDBC catalog and MinIO write/read passed: 1 row")
    finally:
        spark.sql("DROP TABLE IF EXISTS lake.catalog_smoke.probe")
        spark.sql("DROP NAMESPACE IF EXISTS lake.catalog_smoke")
        spark.stop()


if __name__ == "__main__":
    main()

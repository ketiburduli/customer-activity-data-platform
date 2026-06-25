"""
loader.py

Purpose
-------
Load transformed datasets into ClickHouse.

Notes
-----
Business-level deduplication is performed before loading.
ReplacingMergeTree provides an additional storage-level
safeguard against accidental duplicate inserts during reruns.
"""

from pyspark.sql import DataFrame


def write_to_clickhouse(df: DataFrame, table_name: str) -> None:
    (
        df.write
        .format("jdbc")
        .option("url", "jdbc:clickhouse://localhost:8123/warehouse")
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("dbtable", table_name)
        .option("user", "dpe")
        .option("password", "dpe")
        .mode("append")
        .save()
    )
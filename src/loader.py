import clickhouse_connect
from pyspark.sql import DataFrame

from config import (
    CLICKHOUSE_HOST,
    CLICKHOUSE_PORT,
    CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_DATABASE,
)


def write_to_clickhouse(df: DataFrame, table_name: str) -> None:
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )

    rows = [tuple(row) for row in df.collect()]
    columns = df.columns

    if rows:
        client.insert(table_name, rows, column_names=columns)
"""
reader.py

Purpose
-------
Read raw newline-delimited JSON (NDJSON) files into Spark DataFrames.

Money fields are explicitly read as strings so exact Decimal(38,18)
values are not parsed as floating-point numbers before validation.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType


PAM_CUSTOMERS_SCHEMA = StructType([
    StructField("pam_customer_id", StringType(), True),
    StructField("tenant", StringType(), True),
    StructField("full_name", StringType(), True),
    StructField("email_sha256", StringType(), True),
    StructField("country", StringType(), True),
    StructField("registered_at", StringType(), True),
])

APP_CUSTOMERS_SCHEMA = StructType([
    StructField("app_customer_id", StringType(), True),
    StructField("username", StringType(), True),
    StructField("email_sha256", StringType(), True),
    StructField("created_at", StringType(), True),
])

ACTIVITY_EVENTS_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("app_customer_id", StringType(), True),
    StructField("tenant", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("net_result_usdt", StringType(), True),
    StructField("platform_fee_usdt", StringType(), True),
])

CUSTOMER_SESSIONS_SCHEMA = StructType([
    StructField("session_id", StringType(), True),
    StructField("pam_customer_id", StringType(), True),
    StructField("login_at", StringType(), True),
    StructField("logout_at", StringType(), True),
    StructField("device", StringType(), True),
    StructField("ip_region", StringType(), True),
])


def read_ndjson(spark: SparkSession, path: str, schema: StructType) -> DataFrame:
    return spark.read.schema(schema).json(str(path))
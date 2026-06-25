"""
validator.py

Purpose
-------
Validate incoming datasets before loading the warehouse.

Business Rules
--------------
- Required fields must be present.
- Monetary values must be valid Decimal(38,18).
- platform_fee_usdt cannot be negative.
- Duplicate activity events are removed using the natural key
  (event_id, app_customer_id).

Invalid records are excluded from downstream processing and
reported through application logging.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


DECIMAL_38_18 = DecimalType(38, 18)


def validate_activity_events(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    df = (
        df
        .withColumn("net_result_decimal", F.col("net_result_usdt").cast(DECIMAL_38_18))
        .withColumn("platform_fee_decimal", F.col("platform_fee_usdt").cast(DECIMAL_38_18))
    )

    invalid_condition = (
        F.col("event_id").isNull()
        | F.col("app_customer_id").isNull()
        | F.col("tenant").isNull()
        | F.col("event_time").isNull()
        | F.col("net_result_decimal").isNull()
        | F.col("platform_fee_decimal").isNull()
        | (F.col("platform_fee_decimal") < F.lit(0).cast(DECIMAL_38_18))
    )

    invalid_df = df.filter(invalid_condition)

    valid_df = (
        df
        .filter(~invalid_condition)
        .drop("net_result_usdt", "platform_fee_usdt")
        .withColumnRenamed("net_result_decimal", "net_result_usdt")
        .withColumnRenamed("platform_fee_decimal", "platform_fee_usdt")
        .dropDuplicates(["event_id", "app_customer_id"])
    )

    return valid_df, invalid_df


def validate_sessions(df: DataFrame) -> DataFrame:
    return (
        df
        .filter(
            F.col("session_id").isNotNull()
            & F.col("pam_customer_id").isNotNull()
            & F.col("login_at").isNotNull()
            & F.col("logout_at").isNotNull()
        )
        .dropDuplicates(["session_id"])
    )
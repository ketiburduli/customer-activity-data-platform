"""
validator.py
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

DECIMAL_38_18 = DecimalType(38, 18)


def validate_activity_events(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    prepared_df = (
        df
        .withColumn("net_result_decimal", F.col("net_result_usdt").cast(DECIMAL_38_18))
        .withColumn("platform_fee_decimal", F.col("platform_fee_usdt").cast(DECIMAL_38_18))
        .withColumn(
            "rejection_reason",
            F.when(F.col("event_id").isNull(), "missing_event_id")
            .when(F.col("app_customer_id").isNull(), "missing_app_customer_id")
            .when(F.col("tenant").isNull(), "missing_tenant")
            .when(F.col("event_time").isNull(), "missing_event_time")
            .when(F.col("net_result_decimal").isNull(), "invalid_net_result_usdt")
            .when(F.col("platform_fee_decimal").isNull(), "invalid_platform_fee_usdt")
            .when(F.col("platform_fee_decimal") < F.lit(0).cast(DECIMAL_38_18), "negative_platform_fee_usdt")
        )
    )

    invalid_df = prepared_df.filter(F.col("rejection_reason").isNotNull())

    valid_df = (
        prepared_df
        .filter(F.col("rejection_reason").isNull())
        .drop("rejection_reason", "net_result_usdt", "platform_fee_usdt")
        .withColumnRenamed("net_result_decimal", "net_result_usdt")
        .withColumnRenamed("platform_fee_decimal", "platform_fee_usdt")
        .dropDuplicates(["event_id", "app_customer_id"])
    )

    return valid_df, invalid_df


def validate_sessions(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    prepared_df = (
        df
        .withColumn("login_at_ts", F.to_timestamp("login_at"))
        .withColumn("logout_at_ts", F.to_timestamp("logout_at"))
        .withColumn(
            "rejection_reason",
            F.when(F.col("session_id").isNull(), "missing_session_id")
            .when(F.col("pam_customer_id").isNull(), "missing_pam_customer_id")
            .when(F.col("login_at_ts").isNull(), "invalid_login_at")
            .when(F.col("logout_at_ts").isNull(), "invalid_logout_at")
            .when(F.col("logout_at_ts") < F.col("login_at_ts"), "logout_before_login")
        )
    )

    invalid_df = prepared_df.filter(F.col("rejection_reason").isNotNull())

    valid_df = (
        prepared_df
        .filter(F.col("rejection_reason").isNull())
        .drop("rejection_reason", "login_at_ts", "logout_at_ts")
        .dropDuplicates(["session_id"])
    )

    return valid_df, invalid_df
"""
Customer Activity Data Platform pipeline.

Flow
----
Read raw NDJSON files
Validate and deduplicate records
Resolve canonical customer identity
Build warehouse contract tables
Load into ClickHouse
"""

import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config import (
    PAM_CUSTOMERS_PATH,
    APP_CUSTOMERS_PATH,
    ACTIVITY_EVENTS_PATH,
    CUSTOMER_SESSIONS_PATH,
)
from reader import read_ndjson
from validator import validate_activity_events, validate_sessions
from identity import build_dim_customer, build_customer_bridge
from loader import write_to_clickhouse


logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)


def build_fact_activity(valid_activity_df, bridge_df):
    app_bridge = bridge_df.filter(F.col("source_system") == "APP")

    return (
        valid_activity_df.alias("a")
        .join(
            app_bridge.alias("b"),
            F.col("a.app_customer_id") == F.col("b.source_customer_id"),
            "left",
        )
        .select(
            F.col("a.event_id"),
            F.col("a.app_customer_id"),
            F.col("b.canonical_customer_id"),
            F.col("a.tenant"),
            F.col("a.event_type"),
            F.to_timestamp("a.event_time").alias("event_time"),
            F.to_date(F.to_timestamp("a.event_time")).alias("event_date"),
            F.col("a.net_result_usdt"),
            F.col("a.platform_fee_usdt"),
        )
    )


def build_fact_sessions(valid_sessions_df, bridge_df):
    pam_bridge = bridge_df.filter(F.col("source_system") == "PAM")

    return (
        valid_sessions_df.alias("s")
        .join(
            pam_bridge.alias("b"),
            F.col("s.pam_customer_id") == F.col("b.source_customer_id"),
            "left",
        )
        .select(
            F.col("s.session_id"),
            F.col("s.pam_customer_id"),
            F.col("b.canonical_customer_id"),
            F.col("b.tenant"),
            F.to_timestamp("s.login_at").alias("login_at"),
            F.to_timestamp("s.logout_at").alias("logout_at"),
            F.to_date(F.to_timestamp("s.login_at")).alias("session_date"),
            F.col("s.device"),
            F.col("s.ip_region"),
        )
    )


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("customer-activity-data-platform")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "com.clickhouse:clickhouse-jdbc:0.6.4,com.clickhouse:clickhouse-http-client:0.6.4",
        )
        .getOrCreate()
    )

    pam_df = read_ndjson(spark, PAM_CUSTOMERS_PATH)
    app_df = read_ndjson(spark, APP_CUSTOMERS_PATH)
    activity_df = read_ndjson(spark, ACTIVITY_EVENTS_PATH)
    sessions_df = read_ndjson(spark, CUSTOMER_SESSIONS_PATH)

    valid_activity_df, invalid_activity_df = validate_activity_events(activity_df)
    valid_sessions_df = validate_sessions(sessions_df)

    invalid_count = invalid_activity_df.count()
    if invalid_count:
        log.warning("Rejected %s invalid activity records", invalid_count)
        invalid_activity_df.show(truncate=False)

    dim_customer_df = build_dim_customer(pam_df, app_df)
    bridge_df = build_customer_bridge(pam_df, app_df)

    fact_activity_df = build_fact_activity(valid_activity_df, bridge_df)
    fact_sessions_df = build_fact_sessions(valid_sessions_df, bridge_df)

    write_to_clickhouse(dim_customer_df, "dim_customer")
    write_to_clickhouse(bridge_df, "bridge_customer_identity")
    write_to_clickhouse(fact_activity_df, "fact_activity_event")
    write_to_clickhouse(fact_sessions_df, "fact_customer_session")

    log.info("Pipeline finished successfully.")

    spark.stop()


if __name__ == "__main__":
    main()
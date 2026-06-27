"""
Customer Activity Data Platform pipeline.
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
from reader import (
    read_ndjson,
    PAM_CUSTOMERS_SCHEMA,
    APP_CUSTOMERS_SCHEMA,
    ACTIVITY_EVENTS_SCHEMA,
    CUSTOMER_SESSIONS_SCHEMA,
)
from validator import validate_activity_events, validate_sessions
from identity import build_dim_customer, build_customer_bridge
from loader import write_to_clickhouse


logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)


def build_fact_activity(valid_activity_df, bridge_df):
    app_bridge = bridge_df.filter(F.col("source_system") == "APP")

    enriched_df = (
        valid_activity_df.alias("a")
        .join(
            app_bridge.alias("b"),
            (F.col("a.app_customer_id") == F.col("b.source_customer_id"))
            & (F.col("a.tenant") == F.col("b.tenant")),
            "left",
        )
        .withColumn("event_time_ts", F.to_timestamp("a.event_time"))
    )

    return enriched_df.select(
        F.col("a.event_id"),
        F.col("a.app_customer_id"),
        F.col("b.canonical_customer_id"),
        F.col("a.tenant"),
        F.col("a.event_type"),
        F.col("event_time_ts").alias("event_time"),
        F.to_date("event_time_ts").alias("event_date"),
        F.col("a.net_result_usdt"),
        F.col("a.platform_fee_usdt"),
    )


def build_fact_sessions(valid_sessions_df, bridge_df):
    pam_bridge = bridge_df.filter(F.col("source_system") == "PAM")

    enriched_df = (
        valid_sessions_df.alias("s")
        .join(
            pam_bridge.alias("b"),
            F.col("s.pam_customer_id") == F.col("b.source_customer_id"),
            "left",
        )
        .withColumn("login_at_ts", F.to_timestamp("s.login_at"))
        .withColumn("logout_at_ts", F.to_timestamp("s.logout_at"))
    )

    return enriched_df.select(
        F.col("s.session_id"),
        F.col("s.pam_customer_id"),
        F.col("b.canonical_customer_id"),
        F.col("b.tenant").alias("tenant"),
        F.col("login_at_ts").alias("login_at"),
        F.col("logout_at_ts").alias("logout_at"),
        F.to_date("login_at_ts").alias("session_date"),
        F.col("s.device"),
        F.col("s.ip_region"),
    )


def log_rejections(df, label: str) -> None:
    df = df.cache()
    rejected_count = df.count()

    if rejected_count:
        log.warning("Rejected %s invalid %s records", rejected_count, label)
        df.show(truncate=False)

    df.unpersist()


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("customer-activity-data-platform")
        .master("local[*]")
        .getOrCreate()
    )

    pam_df = read_ndjson(spark, PAM_CUSTOMERS_PATH, PAM_CUSTOMERS_SCHEMA)
    app_df = read_ndjson(spark, APP_CUSTOMERS_PATH, APP_CUSTOMERS_SCHEMA)
    activity_df = read_ndjson(spark, ACTIVITY_EVENTS_PATH, ACTIVITY_EVENTS_SCHEMA)
    sessions_df = read_ndjson(spark, CUSTOMER_SESSIONS_PATH, CUSTOMER_SESSIONS_SCHEMA)

    app_tenant_df = (
        activity_df
        .select("app_customer_id", "tenant")
        .dropDuplicates(["app_customer_id", "tenant"])
    )

    app_df = app_df.join(app_tenant_df, on="app_customer_id", how="left")

    valid_activity_df, invalid_activity_df = validate_activity_events(activity_df)
    valid_sessions_df, invalid_sessions_df = validate_sessions(sessions_df)

    log_rejections(invalid_activity_df, "activity")
    log_rejections(invalid_sessions_df, "session")

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
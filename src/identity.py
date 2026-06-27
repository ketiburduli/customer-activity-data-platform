"""
identity.py

Purpose
-------
Resolve customer identities across multiple source systems.

Business Rules
--------------
- email_sha256 is the trusted cross-system join key.
- tenant is part of canonical identity to support multi-tenancy.
- One real customer receives one deterministic canonical_customer_id per tenant.
- Multiple source identifiers may map to the same canonical customer.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def canonical_customer_id_expr(tenant_col, email_col):
    """
    Build a deterministic canonical customer id using Spark native functions.

    This avoids Python UDF serialization overhead and remains stable across reruns.
    """
    return F.sha2(F.concat_ws(":", tenant_col, email_col), 256)


def build_dim_customer(pam_df: DataFrame, app_df: DataFrame) -> DataFrame:
    joined_df = (
        pam_df.alias("pam")
        .join(app_df.alias("app"), on="email_sha256", how="full")
        .select(
            F.coalesce(F.col("pam.tenant"), F.col("app.tenant")).alias("tenant"),
            F.col("email_sha256"),
            F.col("pam.full_name").alias("full_name"),
            F.col("app.username").alias("username"),
            F.col("pam.country").alias("country"),
            F.to_timestamp("pam.registered_at").alias("registered_at"),
            F.to_timestamp(
                F.coalesce(F.col("pam.registered_at"), F.col("app.created_at"))
            ).alias("first_seen_at"),
        )
        .withColumn(
            "canonical_customer_id",
            canonical_customer_id_expr(F.col("tenant"), F.col("email_sha256")),
        )
    )

    window = Window.partitionBy("canonical_customer_id").orderBy(
        F.col("registered_at").asc_nulls_last(),
        F.col("first_seen_at").asc_nulls_last(),
        F.col("email_sha256").asc(),
    )

    return (
        joined_df
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )


def build_customer_bridge(pam_df: DataFrame, app_df: DataFrame) -> DataFrame:
    pam_bridge = (
        pam_df
        .withColumn(
            "canonical_customer_id",
            canonical_customer_id_expr(F.col("tenant"), F.col("email_sha256")),
        )
        .select(
            "canonical_customer_id",
            "tenant",
            F.lit("PAM").alias("source_system"),
            F.col("pam_customer_id").alias("source_customer_id"),
            "email_sha256",
        )
    )

    app_bridge = (
        app_df
        .withColumn(
            "canonical_customer_id",
            canonical_customer_id_expr(F.col("tenant"), F.col("email_sha256")),
        )
        .select(
            "canonical_customer_id",
            "tenant",
            F.lit("APP").alias("source_system"),
            F.col("app_customer_id").alias("source_customer_id"),
            "email_sha256",
        )
    )

    return pam_bridge.unionByName(app_bridge).dropDuplicates(
        ["tenant", "source_system", "source_customer_id"]
    )
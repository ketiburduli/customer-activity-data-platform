"""
identity.py

Purpose
-------
Resolve customer identities across multiple source systems.

Business Rules
--------------
- email_sha256 is the only trusted cross-system join key.
- One real customer receives one deterministic canonical_customer_id.
- Multiple source identifiers may map to the same canonical customer.
"""
"""
Resolve customer identities across PAM and activity-platform source systems.
"""

import uuid

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

CANONICAL_NAMESPACE = uuid.UUID("12345678-1234-5678-1234-567812345678")


@F.udf(returnType=StringType())
def canonical_customer_id(tenant: str, email_sha256: str) -> str:
    return str(uuid.uuid5(CANONICAL_NAMESPACE, f"{tenant}:{email_sha256}"))


def build_dim_customer(pam_df: DataFrame, app_df: DataFrame) -> DataFrame:
    return (
        pam_df.alias("pam")
        .join(app_df.alias("app"), on="email_sha256", how="full")
        .withColumn("tenant", F.coalesce(F.col("pam.tenant"), F.lit("tenant-01")))
        .withColumn("canonical_customer_id", canonical_customer_id(F.col("tenant"), F.col("email_sha256")))
        .select(
            "canonical_customer_id",
            "tenant",
            "email_sha256",
            F.col("pam.full_name").alias("full_name"),
            F.col("app.username").alias("username"),
            F.col("pam.country").alias("country"),
            F.to_timestamp("pam.registered_at").alias("registered_at"),
            F.to_timestamp(F.coalesce(F.col("pam.registered_at"), F.col("app.created_at"))).alias("first_seen_at"),
        )
        .dropDuplicates(["canonical_customer_id"])
    )


def build_customer_bridge(pam_df: DataFrame, app_df: DataFrame) -> DataFrame:
    pam_bridge = (
        pam_df
        .withColumn("canonical_customer_id", canonical_customer_id(F.col("tenant"), F.col("email_sha256")))
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
        .withColumn("tenant", F.lit("tenant-01"))
        .withColumn("canonical_customer_id", canonical_customer_id(F.col("tenant"), F.col("email_sha256")))
        .select(
            "canonical_customer_id",
            "tenant",
            F.lit("APP").alias("source_system"),
            F.col("app_customer_id").alias("source_customer_id"),
            "email_sha256",
        )
    )

    return pam_bridge.unionByName(app_bridge).dropDuplicates(["tenant", "source_system", "source_customer_id"])
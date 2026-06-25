"""
reader.py

Purpose
-------
Read raw newline-delimited JSON (NDJSON) files into Spark DataFrames.

Responsibilities
----------------
- Load all source datasets.
- Preserve the raw schema for downstream validation.
"""
from pyspark.sql import DataFrame, SparkSession


def read_ndjson(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.json(path)
## Pipeline Architecture

```text
Raw NDJSON files
      |
      v
Spark reader
      |
      v
Validation + deduplication
      |
      v
Identity resolution
      |
      v
Warehouse contract
  - dim_customer
  - bridge_customer_identity
  - fact_activity_event
  - fact_customer_session
      |
      v
ClickHouse
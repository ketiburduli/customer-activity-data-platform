# Design Note

## Modelling Approach

The warehouse follows a star-schema inspired design.

The source data originates from two operational systems:

- PAM customer system
- Activity Platform

Since these systems use different customer identifiers, the warehouse separates customer identity resolution from analytical facts.

The warehouse consists of four analytical tables:

- `dim_customer`
- `bridge_customer_identity`
- `fact_activity_event`
- `fact_customer_session`

`dim_customer` stores one canonical customer record.

`bridge_customer_identity` maps multiple source-specific customer identifiers to a single canonical customer.

The fact tables store validated customer activity events and customer sessions.

This separation keeps analytical queries simple while preserving full traceability back to the operational systems.

---

## Table Grain

### `dim_customer`

**Grain:** One row per canonical customer.

### `bridge_customer_identity`

**Grain:** One row per source-system customer identifier.

Multiple operational identifiers may reference the same canonical customer.

### `fact_activity_event`

**Grain:** One validated activity event.

Natural business key:

- `event_id`
- `app_customer_id`

### `fact_customer_session`

**Grain:** One validated customer session.

---

## Identity Resolution

Customer identities are reconciled using the trusted business key:

`email_sha256`

A deterministic `canonical_customer_id` is generated from:

- tenant
- email hash

Spark native hashing functions are used instead of Python UDFs because they:

- preserve Catalyst optimization
- avoid Python serialization overhead
- execute efficiently on Spark executors
- produce deterministic values across reruns

The bridge table preserves traceability between operational systems and the canonical warehouse identity.

The pipeline intentionally avoids guessing identity relationships when the trusted business key is missing or inconsistent.

---

## Data Validation

Before loading data into the warehouse, the pipeline validates:

- required fields
- timestamp values
- decimal values
- negative platform fees
- duplicate activity events
- session consistency (`logout_at >= login_at`)

Duplicate activity events are identified using the natural business key:

- `event_id`
- `app_customer_id`

Rejected records are enriched with a deterministic `rejection_reason`, logged for auditing, and excluded from downstream processing.

In a production environment, rejected records would be persisted to a dedicated quarantine table for investigation and replay.

---

## Idempotency

The pipeline is safe to rerun.

Protection is implemented at two levels.

### Spark

Incoming business records are deduplicated before loading using their natural business keys.

### ClickHouse

Warehouse tables use `ReplacingMergeTree`, providing an additional safeguard against accidental duplicate inserts during repeated executions.

Analytical queries use the `FINAL` modifier to guarantee consistent results even before background merges complete.

---

## Late and Out-of-Order Events

The pipeline does not depend on the physical ordering of input files.

Activity events are treated as immutable records and deduplicated using their natural business key.

As a result, replayed, late-arriving, or out-of-order events produce the same logical warehouse state after processing.

Ordering would only become significant for stateful computations such as sessionization or last-write-wins semantics.

---

## Multi-Tenant Onboarding

Every warehouse table contains a `tenant` column.

The pipeline is designed to onboard additional tenants without bespoke application code.

Tenant-specific configuration, such as:

- input locations
- schema mappings
- validation parameters
- tenant metadata

would be externalized into configuration files or manifests rather than implemented in code.

Adding a new tenant would therefore require only a new configuration entry while reusing the same ingestion, validation, identity resolution, and warehouse logic.

Since `canonical_customer_id` is derived from `tenant + email_sha256`, identical email hashes belonging to different tenants cannot collide.

---

## Streaming Architecture

Although this implementation processes bounded NDJSON files, the warehouse model and business rules are compatible with a streaming architecture.

In production, activity events would be consumed from Kafka using Spark Structured Streaming with a sub-minute micro-batch trigger.

The same validation, identity resolution, and deduplication logic would be applied before writing to ClickHouse.

Natural-key deduplication together with watermarking would provide bounded state while remaining resilient to replayed and late-arriving events.

This approach preserves the same warehouse contract while reducing end-to-end latency to well below one minute.

---

## Main Risk

The largest architectural risk is identity quality.

The solution assumes that `email_sha256` is the only trusted cross-system identifier.

If source systems contain missing or inconsistent email hashes, customers may be incorrectly split or merged.

In production I would introduce:

- identity quality monitoring
- duplicate email detection
- missing email reporting
- manual review workflows for ambiguous matches

When identity conflicts are detected, the pipeline would preserve the original source identities and flag the records for manual review instead of automatically merging customers based on uncertain information.

---

## Production Considerations

The supplied dataset is intentionally small.

To keep the implementation simple, records are inserted into ClickHouse from the Spark driver.

The warehouse design remains compatible with distributed loading strategies without changing the warehouse model.

For production-scale workloads, I would replace the loading stage with:

- Spark ClickHouse Connector
- distributed `foreachPartition()` writes
- batched JDBC inserts

Additional production improvements would include:

- incremental processing
- Airflow orchestration
- unit and integration tests
- CI/CD pipeline
- monitoring and alerting

---

## Assumptions

The implementation assumes that:

- `email_sha256` is the only trusted cross-system identity key.
- Activity events are immutable.
- Source files may be replayed.
- Reprocessing the same input must not change the logical warehouse state.
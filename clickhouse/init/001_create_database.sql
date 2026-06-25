-- Runs once on first ClickHouse start (docker-entrypoint-initdb.d).
-- Provides an empty `warehouse` database for you to define your own schema in.
-- The schema itself (the contract) is YOUR deliverable — see Part A. We do not
-- pre-create any tables.

CREATE DATABASE IF NOT EXISTS warehouse;


CREATE TABLE IF NOT EXISTS warehouse.dim_customer
(
    canonical_customer_id UUID,
    tenant LowCardinality(String),
    email_sha256 String,
    full_name Nullable(String),
    username Nullable(String),
    country Nullable(String),
    registered_at Nullable(DateTime64(3, 'UTC')),
    first_seen_at DateTime64(3, 'UTC'),
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3)

    -- Grain: one row per canonical real-world customer.
    -- canonical_customer_id is deterministic from tenant + email_sha256.
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (tenant, canonical_customer_id);

CREATE TABLE IF NOT EXISTS warehouse.bridge_customer_identity
(
    canonical_customer_id UUID,
    tenant LowCardinality(String),
    source_system LowCardinality(String), -- PAM or APP
    source_customer_id String,
    email_sha256 String,
    linked_at DateTime64(3, 'UTC') DEFAULT now64(3)

    -- Grain: one row per source-system customer identifier.
    -- Example: two PAM ids can point to the same canonical_customer_id.
)
ENGINE = ReplacingMergeTree(linked_at)
ORDER BY (tenant, source_system, source_customer_id);

CREATE TABLE IF NOT EXISTS warehouse.fact_activity_event
(
    event_id String,
    app_customer_id String,
    canonical_customer_id UUID,
    tenant LowCardinality(String),
    event_type LowCardinality(String),
    event_time DateTime64(3, 'UTC'),
    event_date Date,
    net_result_usdt Decimal(38, 18),
    platform_fee_usdt Decimal(38, 18),
    loaded_at DateTime64(3, 'UTC') DEFAULT now64(3)

    -- Grain: one row per valid activity event.
    -- Natural key: event_id + app_customer_id.
)
ENGINE = ReplacingMergeTree(loaded_at)
PARTITION BY toYYYYMM(event_date)
ORDER BY (tenant, event_date, canonical_customer_id, event_id, app_customer_id);

CREATE TABLE IF NOT EXISTS warehouse.fact_customer_session
(
    session_id String,
    pam_customer_id String,
    canonical_customer_id UUID,
    tenant LowCardinality(String),
    login_at DateTime64(3, 'UTC'),
    logout_at DateTime64(3, 'UTC'),
    session_date Date,
    device LowCardinality(String),
    ip_region LowCardinality(String),
    loaded_at DateTime64(3, 'UTC') DEFAULT now64(3)

    -- Grain: one row per valid customer session.
    -- Natural key: session_id.
)
ENGINE = ReplacingMergeTree(loaded_at)
PARTITION BY toYYYYMM(session_date)
ORDER BY (tenant, session_date, canonical_customer_id, session_id);

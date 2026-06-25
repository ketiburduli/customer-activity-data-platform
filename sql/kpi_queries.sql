-- KPI 1: Total platform fees per tenant, in USDT, for the sample period.
-- FINAL makes the query rerun-safe when ReplacingMergeTree has not merged parts yet.
SELECT
    tenant,
    sum(platform_fee_usdt) AS total_platform_fee_usdt
FROM warehouse.fact_activity_event FINAL
GROUP BY tenant
ORDER BY tenant;

-- KPI 2: Events and net result per canonical customer, top 10 by event count.
SELECT
    canonical_customer_id,
    count() AS event_count,
    sum(net_result_usdt) AS net_result_usdt
FROM warehouse.fact_activity_event FINAL
GROUP BY canonical_customer_id
ORDER BY event_count DESC, net_result_usdt DESC
LIMIT 10;

-- KPI 3: Daily active customers per day across the sample period.
SELECT
    event_date,
    countDistinct(canonical_customer_id) AS daily_active_customers
FROM warehouse.fact_activity_event FINAL
GROUP BY event_date
ORDER BY event_date;
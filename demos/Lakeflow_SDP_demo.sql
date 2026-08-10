-- This step is not needed since it happened in the notebook
-- -- ============ BRONZE (streaming ingest from your owned copy) ============
-- CREATE OR REFRESH STREAMING TABLE bronze_booking_updates
-- COMMENT "Raw CDC feed from wanderbricks booking updates"
-- AS SELECT * FROM STREAM ibm_dbx_sandbox.maxm_bronze.booking_updates;

-- ============ SILVER (CDC collapsed to current state) ============
CREATE OR REFRESH STREAMING TABLE silver_bookings;

APPLY CHANGES INTO silver_bookings 
FROM STREAM (ibm_dbx_sandbox.maxm_bronze.booking_updates)
KEYS (booking_id)
SEQUENCE BY updated_at
STORED AS SCD TYPE 1;

-- ============ GOLD (aggregate for metric view + Genie) ============
CREATE OR REFRESH MATERIALIZED VIEW gold_booking_metrics
COMMENT "Reveneu and cancellation metrics by property and month"
AS SELECT
    property_id,
    date_trunc('MONTH', check_in) AS month,
    count(*) as bookings,
    sum(total_amount) as gross_revenue,
    sum(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) as cancellations,
    round(100.0 * sum(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) / count(*), 2) as cancellation_rate
FROM silver_bookings
GROUP BY property_id, date_trunc('MONTH', check_in)
ORDER BY property_id, month;
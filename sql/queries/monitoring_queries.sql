/* =========================================================
   MONITORING QUERY 1: DATA FRESHNESS CHECK
   Objective:
   - Check latest record timestamps across pipeline layers
   - Measure freshness lag between staging, production, warehouse
   ========================================================= */

-- Staging freshness
SELECT
    'staging' AS layer,
    MAX(loaded_at) AS latest_record_time,
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(loaded_at))) / 3600 AS hours_since_last_update
FROM staging.transactions;

-- Production freshness
SELECT
    'production' AS layer,
    MAX(created_at) AS latest_record_time,
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(created_at))) / 3600 AS hours_since_last_update
FROM production.transactions;

-- Warehouse freshness
SELECT
    'warehouse' AS layer,
    MAX(created_at) AS latest_record_time,
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(created_at))) / 3600 AS hours_since_last_update
FROM warehouse.fact_sales;



/* =========================================================
   MONITORING QUERY 2: DATA VOLUME TREND & ANOMALY DETECTION
   Objective:
   - Detect spikes or drops in daily transaction volume
   - Use statistical thresholds (mean + 3 * stddev)
   ========================================================= */

WITH daily_counts AS (
    SELECT
        DATE(created_at) AS txn_date,
        COUNT(*) AS transaction_count
    FROM warehouse.fact_sales
    WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY DATE(created_at)
),
stats AS (
    SELECT
        AVG(transaction_count) AS avg_count,
        STDDEV(transaction_count) AS stddev_count
    FROM daily_counts
)
SELECT
    d.txn_date,
    d.transaction_count,
    s.avg_count,
    s.stddev_count,
    CASE
        WHEN d.transaction_count > s.avg_count + (3 * s.stddev_count) THEN 'SPIKE'
        WHEN d.transaction_count < s.avg_count - (3 * s.stddev_count) THEN 'DROP'
        ELSE 'NORMAL'
    END AS anomaly_type
FROM daily_counts d
CROSS JOIN stats s
ORDER BY d.txn_date DESC;



/* =========================================================
   MONITORING QUERY 3: DATA QUALITY CHECKS
   Objective:
   - Detect orphan records
   - Detect null violations in mandatory fields
   ========================================================= */

-- Orphan customer keys in fact_sales
SELECT
    COUNT(*) AS orphan_customer_records
FROM warehouse.fact_sales f
LEFT JOIN warehouse.dim_customers c
    ON f.customer_key = c.customer_key
WHERE c.customer_key IS NULL;

-- Orphan product keys in fact_sales
SELECT
    COUNT(*) AS orphan_product_records
FROM warehouse.fact_sales f
LEFT JOIN warehouse.dim_products p
    ON f.product_key = p.product_key
WHERE p.product_key IS NULL;

-- Null violations in fact table
SELECT
    COUNT(*) AS null_violations
FROM warehouse.fact_sales
WHERE customer_key IS NULL
   OR product_key IS NULL
   OR date_key IS NULL
   OR quantity <= 0
   OR total_amount <= 0;



/* =========================================================
   MONITORING QUERY 4: PIPELINE EXECUTION HISTORY
   Objective:
   - Track pipeline success / failure trends
   - Measure execution duration patterns
   ========================================================= */

SELECT
    pipeline_execution_id,
    start_time,
    end_time,
    total_duration_seconds,
    status
FROM pipeline_execution_logs
ORDER BY start_time DESC
LIMIT 20;



/* =========================================================
   MONITORING QUERY 5: DATABASE HEALTH & STATISTICS
   Objective:
   - Monitor table sizes and growth
   - Detect abnormal growth patterns
   ========================================================= */

-- Row counts per warehouse table
SELECT
    relname AS table_name,
    n_live_tup AS estimated_row_count
FROM pg_stat_user_tables
WHERE schemaname = 'warehouse'
ORDER BY estimated_row_count DESC;

-- Database size
SELECT
    pg_database.datname AS database_name,
    pg_size_pretty(pg_database_size(pg_database.datname)) AS database_size
FROM pg_database
WHERE datname = current_database();


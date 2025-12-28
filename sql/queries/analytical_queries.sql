-- ============================================================
-- Phase 4.1 : Analytical Queries (Warehouse Schema)
-- Purpose:
--   Business Intelligence queries built on star schema
--   Uses fact_sales with dimension tables
-- ============================================================

-- ------------------------------------------------------------
-- Query 1: Top 10 Products by Revenue
-- ------------------------------------------------------------
SELECT
    dp.product_name,
    dp.category,
    SUM(fs.total_amount) AS total_revenue,
    SUM(fs.quantity) AS units_sold
FROM warehouse.fact_sales fs
JOIN warehouse.dim_products dp
    ON fs.product_key = dp.product_key
WHERE dp.is_current = TRUE
GROUP BY dp.product_name, dp.category
ORDER BY total_revenue DESC
LIMIT 10;

-- ------------------------------------------------------------
-- Query 2: Monthly Revenue Trend
-- ------------------------------------------------------------
SELECT
    dd.year,
    dd.month,
    dd.month_name,
    SUM(fs.total_amount) AS monthly_revenue
FROM warehouse.fact_sales fs
JOIN warehouse.dim_date dd
    ON fs.date_key = dd.date_key
GROUP BY dd.year, dd.month, dd.month_name
ORDER BY dd.year, dd.month;

-- ------------------------------------------------------------
-- Query 3: Customer Segmentation by Age Group
-- ------------------------------------------------------------
SELECT
    dc.age_group,
    COUNT(DISTINCT fs.transaction_id) AS total_transactions,
    SUM(fs.total_amount) AS total_spent
FROM warehouse.fact_sales fs
JOIN warehouse.dim_customers dc
    ON fs.customer_key = dc.customer_key
WHERE dc.is_current = TRUE
GROUP BY dc.age_group
ORDER BY total_spent DESC;

-- ------------------------------------------------------------
-- Query 4: Category-wise Performance
-- ------------------------------------------------------------
SELECT
    dp.category,
    SUM(fs.total_amount) AS category_revenue,
    SUM(fs.quantity) AS total_units
FROM warehouse.fact_sales fs
JOIN warehouse.dim_products dp
    ON fs.product_key = dp.product_key
WHERE dp.is_current = TRUE
GROUP BY dp.category
ORDER BY category_revenue DESC;

-- ------------------------------------------------------------
-- Query 5: Payment Method Distribution
-- ------------------------------------------------------------
SELECT
    t.payment_method,
    COUNT(DISTINCT t.transaction_id) AS transaction_count
FROM production.transactions t
GROUP BY t.payment_method
ORDER BY transaction_count DESC;

-- ------------------------------------------------------------
-- Query 6: Geographic Sales Analysis (State Level)
-- ------------------------------------------------------------
SELECT
    dc.state,
    SUM(fs.total_amount) AS total_revenue,
    COUNT(DISTINCT fs.transaction_id) AS total_transactions
FROM warehouse.fact_sales fs
JOIN warehouse.dim_customers dc
    ON fs.customer_key = dc.customer_key
WHERE dc.is_current = TRUE
GROUP BY dc.state
ORDER BY total_revenue DESC;

-- ------------------------------------------------------------
-- Query 7: Customer Lifetime Value (CLV)
-- ------------------------------------------------------------
SELECT
    dc.customer_id,
    dc.first_name,
    dc.last_name,
    SUM(fs.total_amount) AS lifetime_value,
    COUNT(DISTINCT fs.transaction_id) AS total_transactions
FROM warehouse.fact_sales fs
JOIN warehouse.dim_customers dc
    ON fs.customer_key = dc.customer_key
WHERE dc.is_current = TRUE
GROUP BY dc.customer_id, dc.first_name, dc.last_name
ORDER BY lifetime_value DESC;

-- ------------------------------------------------------------
-- Query 8: Product Profitability
-- ------------------------------------------------------------
SELECT
    dp.product_name,
    SUM(
        fs.total_amount -
        (fs.quantity * dp.cost)
    ) AS total_profit
FROM warehouse.fact_sales fs
JOIN warehouse.dim_products dp
    ON fs.product_key = dp.product_key
WHERE dp.is_current = TRUE
GROUP BY dp.product_name
ORDER BY total_profit DESC;

-- ------------------------------------------------------------
-- Query 9: Day of Week Sales Pattern
-- ------------------------------------------------------------
SELECT
    dd.day_name,
    COUNT(DISTINCT fs.transaction_id) AS transaction_count,
    SUM(fs.total_amount) AS total_revenue
FROM warehouse.fact_sales fs
JOIN warehouse.dim_date dd
    ON fs.date_key = dd.date_key
GROUP BY dd.day_name, dd.day_of_week
ORDER BY dd.day_of_week;

-- ------------------------------------------------------------
-- Query 10: Discount Impact Analysis
-- ------------------------------------------------------------
SELECT
    CASE
        WHEN fs.discount_amount > 0 THEN 'Discounted'
        ELSE 'Non-Discounted'
    END AS discount_flag,
    SUM(fs.total_amount) AS total_revenue,
    COUNT(DISTINCT fs.transaction_id) AS total_transactions
FROM warehouse.fact_sales fs
GROUP BY discount_flag;

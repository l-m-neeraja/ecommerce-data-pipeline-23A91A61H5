-- ===============================
-- COMPLETENESS CHECKS
-- ===============================

-- Null checks on mandatory fields
SELECT 'customers.email' AS field, COUNT(*) AS null_count
FROM production.customers
WHERE email IS NULL;

SELECT 'products.price' AS field, COUNT(*) AS null_count
FROM production.products
WHERE price IS NULL;

-- Transactions without items
SELECT COUNT(DISTINCT t.transaction_id) AS transactions_without_items
FROM production.transactions t
LEFT JOIN production.transaction_items ti
ON t.transaction_id = ti.transaction_id
WHERE ti.transaction_id IS NULL;


-- ===============================
-- UNIQUENESS CHECKS
-- ===============================

-- Duplicate customer emails
SELECT email, COUNT(*) AS cnt
FROM production.customers
GROUP BY email
HAVING COUNT(*) > 1;

-- Duplicate transactions (same customer, date, amount)
SELECT customer_id, transaction_date, total_amount, COUNT(*) AS cnt
FROM production.transactions
GROUP BY customer_id, transaction_date, total_amount
HAVING COUNT(*) > 1;


-- ===============================
-- VALIDITY / RANGE CHECKS
-- ===============================

-- Invalid price or cost
SELECT COUNT(*) AS invalid_price
FROM production.products
WHERE price <= 0 OR cost < 0 OR cost >= price;

-- Invalid discounts
SELECT COUNT(*) AS invalid_discount
FROM production.transaction_items
WHERE discount_percentage < 0 OR discount_percentage > 100;

-- Invalid quantities
SELECT COUNT(*) AS invalid_quantity
FROM production.transaction_items
WHERE quantity <= 0;


-- ===============================
-- CONSISTENCY CHECKS
-- ===============================

-- Line total mismatch
SELECT COUNT(*) AS line_total_mismatch
FROM production.transaction_items
WHERE ABS(
    line_total - (quantity * unit_price * (1 - discount_percentage/100))
) > 0.01;

-- Transaction total mismatch
SELECT COUNT(*) AS transaction_total_mismatch
FROM production.transactions t
JOIN production.transaction_items ti
ON t.transaction_id = ti.transaction_id
GROUP BY t.transaction_id, t.total_amount
HAVING ABS(t.total_amount - SUM(ti.line_total)) > 0.01;


-- ===============================
-- REFERENTIAL INTEGRITY CHECKS
-- ===============================

-- Orphan transactions (customer missing)
SELECT COUNT(*) AS orphan_transactions
FROM production.transactions t
LEFT JOIN production.customers c
ON t.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- Orphan transaction items (transaction missing)
SELECT COUNT(*) AS orphan_items_transaction
FROM production.transaction_items ti
LEFT JOIN production.transactions t
ON ti.transaction_id = t.transaction_id
WHERE t.transaction_id IS NULL;

-- Orphan transaction items (product missing)
SELECT COUNT(*) AS orphan_items_product
FROM production.transaction_items ti
LEFT JOIN production.products p
ON ti.product_id = p.product_id
WHERE p.product_id IS NULL;


-- ===============================
-- BUSINESS RULE / ACCURACY CHECKS
-- ===============================

-- Future transactions
SELECT COUNT(*) AS future_transactions
FROM production.transactions
WHERE transaction_date > CURRENT_DATE;

-- Customer registered after transaction
SELECT COUNT(*) AS invalid_customer_dates
FROM production.transactions t
JOIN production.customers c
ON t.customer_id = c.customer_id
WHERE c.registration_date > t.transaction_date;

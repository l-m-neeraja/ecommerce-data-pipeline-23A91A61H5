-- NULL checks
SELECT COUNT(*) FROM staging.customers WHERE email IS NULL;

-- Duplicate email check
SELECT email, COUNT(*) 
FROM staging.customers 
GROUP BY email 
HAVING COUNT(*) > 1;

-- Orphan transactions
SELECT COUNT(*) 
FROM staging.transactions t
LEFT JOIN staging.customers c 
ON t.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- Line total consistency check
SELECT COUNT(*) 
FROM staging.transaction_items
WHERE ABS(line_total - (quantity * unit_price * (1 - discount_percentage/100))) > 0.01;

-- ============================================================
-- procedure: load_customer_summary
-- Source: Oracle Data Warehouse → AWS Redshift migration
-- Originally a PL/SQL stored procedure in a prior enterprise data-engineering project (2021-2022)
-- ============================================================

-- Step 1: Build staging aggregate from raw transaction data
CREATE TABLE staging.customer_txn AS
SELECT customer_id, SUM(amount) as total_amount, COUNT(*) as txn_count
FROM raw.transactions
WHERE txn_date >= TRUNC(SYSDATE) - 30
GROUP BY customer_id;

-- Step 2: Enrich with customer dimension data
CREATE TABLE mart.customer_summary AS
SELECT c.customer_id, c.name, c.segment, t.total_amount, t.txn_count
FROM staging.customer_txn t
JOIN raw.customers c ON t.customer_id = c.customer_id
WHERE t.total_amount > 100;

-- Step 3: Populate high-value customer segment
INSERT INTO mart.high_value_customers
SELECT customer_id, name, total_amount
FROM mart.customer_summary
WHERE total_amount > 10000;

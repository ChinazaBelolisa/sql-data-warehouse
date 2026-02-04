/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs quality checks to validate the integrity, consistency, 
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage Notes:
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

USE datawarehouse;

-- ====================================================================
-- Checking 'gold__dim_customers'
-- ====================================================================
-- Check for Uniqueness of Customer Key
-- Expectation: No results
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold__dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;


-- ====================================================================
-- Checking 'gold__dim_products'
-- ====================================================================
-- Check for Uniqueness of Product Key
-- Expectation: No results
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold__dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;


-- ====================================================================
-- Checking 'gold__fact_sales'
-- ====================================================================
-- Check model connectivity: every fact row should match a customer + product
-- Expectation: No results
SELECT 
    f.*
FROM gold__fact_sales f
LEFT JOIN gold__dim_customers c
    ON c.customer_key = f.customer_key
LEFT JOIN gold__dim_products p
    ON p.product_key = f.product_key
WHERE p.product_key IS NULL
   OR c.customer_key IS NULL;

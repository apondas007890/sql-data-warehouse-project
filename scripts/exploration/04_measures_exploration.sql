/*
===============================================================================
File Name: measures_exploration.sql
===============================================================================

Purpose:
    - To calculate key business performance metrics from Gold Layer tables.
    - To provide quick insights into sales, customers, and product performance.
    - Acts as a KPI dashboard in SQL form.

Business Metrics Covered:
    - Revenue (Total Sales)
    - Sales Volume (Quantity)
    - Pricing Behavior (Average Price)
    - Order Activity (Orders, Customers)
    - Product & Customer Coverage

SQL Functions Used:
    - SUM(), COUNT(), AVG(), DISTINCT
===============================================================================
*/

-- ============================================================================
-- Total Revenue (Sales)
-- ============================================================================
SELECT SUM(sales_amount) AS total_sales
FROM gold.fact_sales;


-- ============================================================================
-- Total Quantity Sold
-- ============================================================================
SELECT SUM(quantity) AS total_quantity
FROM gold.fact_sales;


-- ============================================================================
-- Average Selling Price
-- ============================================================================
SELECT AVG(price) AS avg_price
FROM gold.fact_sales;


-- ============================================================================
-- Total Orders (all rows)
-- ============================================================================
SELECT COUNT(order_number) AS total_orders
FROM gold.fact_sales;


-- ============================================================================
-- Total Unique Orders (real business metric)
-- ============================================================================
SELECT COUNT(DISTINCT order_number) AS total_unique_orders
FROM gold.fact_sales;

-- ============================================================================
-- Total Customers in Master Data
-- ============================================================================
SELECT COUNT(customer_key) AS total_customers
FROM gold.dim_customers;


-- ============================================================================
-- Active Customers (customers who placed orders)
-- ============================================================================
SELECT COUNT(DISTINCT customer_key) AS active_customers
FROM gold.fact_sales;


-- ============================================================================
-- Inactive Customers (never ordered)
-- ============================================================================
SELECT 
    COUNT(*) AS inactive_customers
FROM gold.dim_customers
WHERE customer_key NOT IN (
    SELECT DISTINCT customer_key
    FROM gold.fact_sales
);


-- ============================================================================
-- Total Products
-- ============================================================================
SELECT COUNT(product_name) AS total_products
FROM gold.dim_products;


-- ============================================================================
-- Products Actually Sold
-- ============================================================================
SELECT COUNT(DISTINCT product_key) AS sold_products
FROM gold.fact_sales;


-- Generate a Report that shows all key metrics of the business
SELECT 'Total Sales' AS measure_name, SUM(sales_amount) AS measure_value FROM gold.fact_sales
UNION ALL
SELECT 'Total Quantity', SUM(quantity) FROM gold.fact_sales
UNION ALL
SELECT 'Average Price', AVG(price) FROM gold.fact_sales
UNION ALL
SELECT 'Total Orders', COUNT(DISTINCT order_number) FROM gold.fact_sales
UNION ALL
SELECT 'Total Products', COUNT(DISTINCT product_name) FROM gold.dim_products
UNION ALL
SELECT 'Total Customers', COUNT(customer_key) FROM gold.dim_customers;
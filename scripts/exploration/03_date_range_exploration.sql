/*
===============================================================================
File Name: date_range_exploration.sql
===============================================================================

Purpose:
    - To analyze the temporal coverage of the dataset.
    - To understand how much historical data is available.
    - To explore customer age distribution from birthdate.

Focus Areas:
    1. Sales date range (fact_sales)
    2. Customer age range (dim_customers)

SQL Functions Used:
    - MIN(), MAX()
    - DATEDIFF()
===============================================================================
*/

-- ============================================================================
-- Sales Date Range Analysis
-- ============================================================================
SELECT 
    MIN(order_date) AS first_order_date,
    MAX(order_date) AS last_order_date,

    DATEDIFF(YEAR, MIN(order_date), MAX(order_date)) AS order_range_years,
    DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS order_range_months,
    DATEDIFF(DAY, MIN(order_date), MAX(order_date)) AS order_range_days
FROM gold.fact_sales;


-- ============================================================================
-- Customer Age Analysis (based on birthdate)
-- ============================================================================
SELECT
    MIN(birthdate) AS oldest_birthdate,
    MAX(birthdate) AS youngest_birthdate,

    -- Age approximation (EDA level)
    DATEDIFF(YEAR, MIN(birthdate), GETDATE()) AS oldest_age,
    DATEDIFF(YEAR, MAX(birthdate), GETDATE()) AS youngest_age
FROM gold.dim_customers;


-- ============================================================================
-- Average Customer Age Insight
-- ============================================================================
SELECT 
    AVG(DATEDIFF(YEAR, birthdate, GETDATE())) AS avg_customer_age
FROM gold.dim_customers;
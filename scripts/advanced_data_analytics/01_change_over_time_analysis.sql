/*
===============================================================================
File Name: change_over_time_analysis.sql
===============================================================================

Purpose:
    - To analyze business performance trends over time.
    - To measure growth, decline, and seasonal patterns.
    - To support time-series and trend-based analysis.

Business Questions Answered:
    - How do sales change over time?
    - Which months generate the highest revenue?
    - How does customer activity vary across periods?
    - Are there seasonal sales patterns?

SQL Functions Used:
    - Date Functions:
        YEAR(), MONTH(), DATETRUNC(), FORMAT()
    - Aggregate Functions:
        SUM(), COUNT()
===============================================================================
*/


-- ============================================================================
-- Analyze Monthly Sales Performance
-- ============================================================================
SELECT 
    MONTH(order_date) AS order_month,
    SUM(sales_amount) AS total_sales,
    COUNT(DISTINCT customer_key) AS total_customers,
    SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY MONTH(order_date)
ORDER BY MONTH(order_date);


-- ============================================================================
-- Analyze Yearly Sales Performance
-- ============================================================================
SELECT 
    YEAR(order_date) AS order_year,
    SUM(sales_amount) AS total_sales,
    COUNT(DISTINCT customer_key) AS total_customers,
    SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date)
ORDER BY YEAR(order_date);


-- ============================================================================
-- Analyze Sales Performance by Year and Month
-- ============================================================================
SELECT 
    YEAR(order_date) AS order_year,
    MONTH(order_date) AS order_month,
    SUM(sales_amount) AS total_sales,
    COUNT(DISTINCT customer_key) AS total_customers,
    SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY 
    YEAR(order_date),
    MONTH(order_date)
ORDER BY 
    YEAR(order_date),
    MONTH(order_date);


-- ============================================================================
-- Monthly Trend Analysis Using DATETRUNC()
-- ============================================================================
SELECT
    DATETRUNC(MONTH, order_date) AS order_month,
    SUM(sales_amount) AS total_sales,
    COUNT(DISTINCT customer_key) AS total_customers,
    SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATETRUNC(MONTH, order_date)
ORDER BY DATETRUNC(MONTH, order_date);


-- ============================================================================
-- Monthly Reporting View Using FORMAT()
-- ============================================================================
SELECT
    FORMAT(order_date, 'yyyy-MMM') AS order_period,
    SUM(sales_amount) AS total_sales,
    COUNT(DISTINCT customer_key) AS total_customers,
    SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY FORMAT(order_date, 'yyyy-MMM')
ORDER BY FORMAT(order_date, 'yyyy-MMM');
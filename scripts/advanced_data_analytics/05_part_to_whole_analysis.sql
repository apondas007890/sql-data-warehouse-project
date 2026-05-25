/*
===============================================================================
Part-to-Whole Analysis
===============================================================================

Purpose:
    - To understand how each category contributes to the total business value.
    - To measure proportional impact of each segment (part vs whole).
    - To identify dominant and weak contributors in overall performance.

Business Use Cases:
    - Revenue contribution by category
    - Market share analysis
    - Product or regional contribution comparison
    - Business dependency analysis

Key SQL Concepts Used:
    - SUM() aggregation
    - Window Function: SUM() OVER()
    - Percentage calculations
    - CTE (Common Table Expression)

Core Idea:
    Part % of Whole = (Category Value / Total Value) × 100
===============================================================================
*/


-- ============================================================================
-- Analyze how each product category contributes to total sales
-- ============================================================================
WITH category_sales AS
(
    SELECT
        p.category,
        -- Total sales per category
        SUM(f.sales_amount) AS total_sales
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p
           ON p.product_key = f.product_key
    GROUP BY p.category
)

-- Part-to-Whole Calculation
SELECT
    category,
    total_sales,
    SUM(total_sales) OVER () AS overall_sales,
    CONCAT(ROUND((CAST(total_sales AS FLOAT) / SUM(total_sales) OVER ()) * 100, 2), '%') AS percentage_contribution
FROM category_sales
ORDER BY total_sales DESC;


-- ============================================================================
-- Revenue contribution by country
-- ============================================================================
WITH country_sales AS
(
    SELECT
        c.country,
        SUM(f.sales_amount) AS total_sales
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_customers c
           ON c.customer_key = f.customer_key
    GROUP BY c.country
)

SELECT
    country,
    total_sales,
    SUM(total_sales) OVER () AS overall_sales,
    CONCAT(ROUND((CAST(total_sales AS FLOAT) / SUM(total_sales) OVER ()) * 100,2), '%') AS contribution_percentage
FROM country_sales
ORDER BY total_sales DESC;
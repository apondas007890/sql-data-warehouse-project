/*
===============================================================================
Cumulative Analysis
===============================================================================

Purpose:
    - To calculate cumulative business metrics over time.
    - To analyze long-term growth trends and performance progression.
    - To measure running totals and moving averages.

Business Questions Answered:
    - How does revenue accumulate over time?
    - Is the business growing consistently?
    - What are the long-term sales trends?
    - How does average pricing change over time?

SQL Functions Used:
    - Window Functions:
        SUM() OVER()
        AVG() OVER()

    - Date Functions:
        DATETRUNC()

===============================================================================
*/


-- ============================================================================
-- Calculate Monthly Sales and Running Total Over Time
-- ============================================================================
SELECT
    order_date,
    total_sales,
    -- Cumulative sales progression over time
    SUM(total_sales) OVER (ORDER BY order_date) AS running_total_sales
FROM
(
    SELECT 
        DATETRUNC(MONTH, order_date) AS order_date,
        SUM(sales_amount) AS total_sales
    FROM gold.fact_sales
    WHERE order_date IS NOT NULL
    GROUP BY DATETRUNC(MONTH, order_date)
) AS monthly_sales;


-- ============================================================================
-- Calculate Yearly Sales and Running Total Over Time
-- ============================================================================
SELECT
    order_date,
    total_sales,
    -- Running yearly sales total
    SUM(total_sales) OVER (ORDER BY order_date) AS running_total_sales
FROM
(
    SELECT 
        DATETRUNC(YEAR, order_date) AS order_date,
        SUM(sales_amount) AS total_sales
    FROM gold.fact_sales
    WHERE order_date IS NOT NULL
    GROUP BY DATETRUNC(YEAR, order_date)
) AS yearly_sales;


-- ============================================================================
-- Calculate Running Total Sales and Moving Average Price
-- ============================================================================
SELECT
    order_date,
    total_sales,
    -- Cumulative sales growth over time
    SUM(total_sales) OVER (ORDER BY order_date) AS running_total_sales,
    -- Moving average of product pricing
    AVG(avg_price) OVER (ORDER BY order_date) AS moving_average_price
FROM
(
    SELECT 
        DATETRUNC(YEAR, order_date) AS order_date,
        SUM(sales_amount) AS total_sales,
        AVG(price) AS avg_price
    FROM gold.fact_sales
    WHERE order_date IS NOT NULL
    GROUP BY DATETRUNC(YEAR, order_date)
) AS yearly_performance;


-- ============================================================================
-- Running Total of Orders Over Time
-- ============================================================================
SELECT
    order_date,
    total_orders,
    SUM(total_orders) OVER (ORDER BY order_date) AS running_total_orders
FROM
(
    SELECT
        DATETRUNC(MONTH, order_date) AS order_date,
        COUNT(DISTINCT order_number) AS total_orders
    FROM gold.fact_sales
    WHERE order_date IS NOT NULL
    GROUP BY DATETRUNC(MONTH, order_date)
) AS monthly_orders;


-- ============================================================================
-- Running Sales Total by Product Category
-- ============================================================================
SELECT
    order_date,
    category,
    total_sales,
    SUM(total_sales) OVER (PARTITION BY category ORDER BY order_date) AS running_category_sales
FROM
(
    SELECT
        DATETRUNC(YEAR, f.order_date) AS order_date,
        p.category,
        SUM(f.sales_amount) AS total_sales
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p
           ON p.product_key = f.product_key
    WHERE f.order_date IS NOT NULL
    GROUP BY
        DATETRUNC(YEAR, f.order_date),
        p.category
) AS category_sales;
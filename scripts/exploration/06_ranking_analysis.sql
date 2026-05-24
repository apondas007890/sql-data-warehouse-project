/*
===============================================================================
File Name: ranking_analysis.sql
===============================================================================

Purpose:
    - To rank products, customers, and categories based on business performance.
    - To identify top-performing and low-performing entities.
    - To support business decision-making through comparative analysis.

Business Questions Answered:
    - Which products generate the highest revenue?
    - Which products perform poorly?
    - Who are the top revenue-generating customers?
    - Which customers place the fewest orders?
    - Which categories dominate sales performance?

SQL Functions Used:
    - Aggregate Functions: SUM(), COUNT()
    - Window Functions: RANK(), DENSE_RANK(), ROW_NUMBER()
    - TOP, GROUP BY, ORDER BY
===============================================================================
*/


-- ============================================================================
-- Top 5 Revenue-Generating Products (Simple Ranking)
-- ============================================================================
SELECT TOP 5
    p.product_name,
    SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
       ON p.product_key = f.product_key
GROUP BY p.product_name
ORDER BY total_revenue DESC;


-- ============================================================================
-- Top 5 Revenue-Generating Products Using Window Function Ranking
-- ============================================================================
SELECT *
FROM (
    SELECT
        p.product_name,
        SUM(f.sales_amount) AS total_revenue,

        RANK() OVER (
            ORDER BY SUM(f.sales_amount) DESC
        ) AS product_rank

    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p
           ON p.product_key = f.product_key
    GROUP BY p.product_name
) AS ranked_products
WHERE product_rank <= 5;


-- ============================================================================
-- Top 5 Products by Quantity Sold
-- ============================================================================
SELECT TOP 5
    p.product_name,
    SUM(f.quantity) AS total_quantity_sold
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
       ON p.product_key = f.product_key
GROUP BY p.product_name
ORDER BY total_quantity_sold DESC;


-- ============================================================================
-- Bottom 5 Products by Revenue
-- ============================================================================
SELECT TOP 5
    p.product_name,
    SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
       ON p.product_key = f.product_key
GROUP BY p.product_name
ORDER BY total_revenue;


-- ============================================================================
-- Bottom 5 Products by Quantity Sold
-- ============================================================================
SELECT TOP 5
    p.product_name,
    SUM(f.quantity) AS total_quantity_sold
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
       ON p.product_key = f.product_key
GROUP BY p.product_name
ORDER BY total_quantity_sold;


-- ============================================================================
-- Top 10 Customers by Revenue
-- ============================================================================
SELECT TOP 10
    c.customer_key,
    c.first_name,
    c.last_name,
    SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
       ON c.customer_key = f.customer_key
GROUP BY 
    c.customer_key,
    c.first_name,
    c.last_name
ORDER BY total_revenue DESC;


-- ============================================================================
-- Top 10 Customers by Number of Orders
-- ============================================================================
SELECT TOP 10
    c.customer_key,
    c.first_name,
    c.last_name,
    COUNT(DISTINCT f.order_number) AS total_orders
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
       ON c.customer_key = f.customer_key
GROUP BY
    c.customer_key,
    c.first_name,
    c.last_name
ORDER BY total_orders DESC;


-- ============================================================================
-- Bottom 3 Customers by Number of Orders
-- ============================================================================
SELECT TOP 3
    c.customer_key,
    c.first_name,
    c.last_name,
    COUNT(DISTINCT f.order_number) AS total_orders
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
       ON c.customer_key = f.customer_key
GROUP BY 
    c.customer_key,
    c.first_name,
    c.last_name
ORDER BY total_orders;


-- ============================================================================
-- Top Product Categories by Revenue
-- ============================================================================
SELECT
    p.category,
    SUM(f.sales_amount) AS total_revenue,

    RANK() OVER (
        ORDER BY SUM(f.sales_amount) DESC
    ) AS category_rank

FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
       ON p.product_key = f.product_key
GROUP BY p.category
ORDER BY category_rank;


-- ============================================================================
-- Top Product Categories by Quantity Sold
-- ============================================================================
SELECT
    p.category,
    SUM(f.quantity) AS total_quantity_sold
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
       ON p.product_key = f.product_key
GROUP BY p.category
ORDER BY total_quantity_sold DESC;
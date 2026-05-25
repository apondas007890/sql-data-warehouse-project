/*
===============================================================================
Segmentation Analysis
===============================================================================

Purpose:
    - To divide data into meaningful business groups for targeted analysis.
    - To support customer targeting, product positioning, and strategic decisions.
    - To convert raw data into actionable business segments.

Business Use Cases:
    - Customer segmentation (VIP, Regular, New)
    - Product segmentation (price/cost tiers)
    - Market segmentation (region-based grouping)
    - Behavioral segmentation (spending patterns)

Key SQL Concepts Used:
    - CASE WHEN (conditional grouping logic)
    - GROUP BY (aggregation by segment)
    - CTE (Common Table Expressions)
    - Aggregate Functions (SUM(), COUNT(), MIN(), MAX())

Core Idea:
    Segments = CASE(Logical Rules on Measures + Dimensions)
===============================================================================
*/


-- ============================================================================
-- Segment products based on cost ranges
-- ============================================================================
WITH product_segments AS
(
    SELECT
        product_key,
        product_name,
        cost,
        -- Business-driven cost segmentation
        CASE
            WHEN cost < 100 THEN 'Below 100'
            WHEN cost BETWEEN 100 AND 500 THEN '100-500'
            WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
            ELSE 'Above 1000'
        END AS cost_segment
    FROM gold.dim_products
)

-- Count products in each cost segment
SELECT
    cost_segment,
    COUNT(product_key) AS total_products
FROM product_segments
GROUP BY cost_segment
ORDER BY total_products DESC;


-- ============================================================================
-- Segment customers based on spending behavior and lifecycle
-- ============================================================================
WITH customer_spending AS
(
    SELECT
        c.customer_key,
        -- Total lifetime spending
        SUM(f.sales_amount) AS total_spending,
        -- First and last purchase dates
        MIN(f.order_date) AS first_order_date,
        MAX(f.order_date) AS last_order_date,
        -- Customer lifecycle in months
        DATEDIFF(MONTH, MIN(f.order_date), MAX(f.order_date)) AS lifespan_months
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_customers c
    ON f.customer_key = c.customer_key
    GROUP BY c.customer_key
),
segmented_customers AS
(
    SELECT
        customer_key,
        -- Business segmentation logic
        CASE
            WHEN lifespan_months >= 12 AND total_spending > 5000 THEN 'VIP'
            WHEN lifespan_months >= 12 AND total_spending <= 5000 THEN 'Regular'
            ELSE 'New'
        END AS customer_segment
    FROM customer_spending
)

-- Final Segment Distribution
SELECT
    customer_segment,
    COUNT(customer_key) AS total_customers
FROM segmented_customers
GROUP BY customer_segment
ORDER BY total_customers DESC;


-- ============================================================================
-- Segment products based on revenue contribution
-- ============================================================================
WITH product_revenue AS
(
    SELECT
        p.product_name,
        SUM(f.sales_amount) AS total_revenue
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p
           ON p.product_key = f.product_key
    GROUP BY p.product_name
)

SELECT
    product_name,
    total_revenue,

    CASE
        WHEN total_revenue >= 100000 THEN 'High Value'
        WHEN total_revenue BETWEEN 50000 AND 99999 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS revenue_segment

FROM product_revenue
ORDER BY total_revenue DESC;


-- ============================================================================
-- Customer segmentation based on total revenue contribution
-- ============================================================================
WITH customer_revenue AS
(
    SELECT
        c.customer_key,
        SUM(f.sales_amount) AS total_spending
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_customers c
           ON c.customer_key = f.customer_key
    GROUP BY c.customer_key
)

SELECT
    customer_key,
    total_spending,
    CASE
        WHEN total_spending >= 10000 THEN 'Premium'
        WHEN total_spending BETWEEN 5000 AND 9999 THEN 'Standard'
        ELSE 'Basic'
    END AS value_segment
FROM customer_revenue
ORDER BY total_spending DESC;
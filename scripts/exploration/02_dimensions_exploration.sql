/*
===============================================================================
File Name: gold_dimension_exploration.sql
===============================================================================

Purpose:
    This script is used for exploratory data analysis (EDA) on Gold Layer
    dimension tables in the Data Warehouse.

    It helps understand business-level attributes such as:
        - Customer geography
        - Product hierarchy (category, subcategory)
        - Data uniqueness and structure validation

Gold Layer Context:
    The Gold Layer contains business-ready, analytics-friendly tables:
        - gold.dim_customers
        - gold.dim_products
        - gold.fact_sales

Usage:
    - Used for validation and exploratory analysis before reporting
    - Helps analysts understand data distribution and uniqueness

===============================================================================
*/

-- ============================================================================
-- Explore unique customer countries
-- ============================================================================
SELECT DISTINCT 
    country 
FROM gold.dim_customers
ORDER BY country;


-- ============================================================================
-- Explore product hierarchy (Category → Subcategory → Product)
-- ============================================================================
SELECT DISTINCT 
    category,
    subcategory,
    product_name
FROM gold.dim_products
ORDER BY category, subcategory, product_name;


-- ============================================================================
-- Full view of product dimension (quick inspection)
-- ============================================================================
SELECT *
FROM gold.dim_products;
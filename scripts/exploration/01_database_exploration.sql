/*
===============================================================================
File Name: database_exploration.sql
===============================================================================
Purpose:
    - Explore all objects available in the database.
    - Inspect table structures and column-level metadata.
    - Understand schemas before performing analysis or transformations.

Description:
    This script queries the INFORMATION_SCHEMA views to:
        1. List all tables available in the database.
        2. Explore column details for key dimension and fact tables.

Tables Explored:
    - INFORMATION_SCHEMA.TABLES
    - INFORMATION_SCHEMA.COLUMNS

Target Tables:
    - dim_customers
    - dim_products
    - fact_sales
===============================================================================
*/

-- ============================================================================
-- Retrieve all tables and views available in the database
-- ============================================================================
SELECT 
    TABLE_CATALOG,
    TABLE_SCHEMA,
    TABLE_NAME,
    TABLE_TYPE
FROM INFORMATION_SCHEMA.TABLES;


-- ============================================================================
-- Retrieve column details for the dim_customers table
-- ============================================================================
SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    CHARACTER_MAXIMUM_LENGTH
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'dim_customers';


-- ============================================================================
-- Retrieve column details for the dim_products table
-- ============================================================================
SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    CHARACTER_MAXIMUM_LENGTH
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'dim_products';


-- ============================================================================
-- Retrieve column details for the fact_sales table
-- ============================================================================
SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    CHARACTER_MAXIMUM_LENGTH
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'fact_sales';
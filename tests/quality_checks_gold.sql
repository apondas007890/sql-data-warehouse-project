/*
===============================================================================
						Quality Checks - Gold Layer
===============================================================================
Script Purpose:
    This script performs validation and quality checks on the Gold Layer 
    tables to ensure the data model is reliable for analytics and reporting.

    The checks include:
        - Detecting duplicate records after joins during dimension creation.
        - Verifying surrogate key uniqueness in dimension tables.
        - Validating standardized values (e.g., gender).
        - Ensuring referential integrity between fact and dimension tables.

Usage Notes:
    - These checks should be executed after the Gold Layer views/tables 
      are created.
    - If any query returns results where none are expected, investigate 
      and resolve the issue in the transformation logic.

===============================================================================
*/

-- =============================================================================
--						Test Dimension: gold.dim_customers
-- =============================================================================

-- Check if duplicate customers appear after joining CRM and ERP sources
-- Expectation: No duplicate records per cst_id
SELECT 
    cst_id,
    COUNT(*) AS duplicate_count
FROM (
    SELECT 
        ci.cst_id,
        ci.cst_key,
        ci.cst_firstname,
        ci.cst_lastname,
        ci.cst_marital_status,
        ci.cst_gndr,
        ci.cst_create_date,
        ca.bdate,
        ca.gen,
        la.cntry
    FROM silver.crm_cust_info ci
    LEFT JOIN silver.erp_cust_az12 ca
        ON ci.cst_key = ca.cid
    LEFT JOIN silver.erp_loc_a101 la
        ON ci.cst_key = la.cid
) t
GROUP BY cst_id
HAVING COUNT(*) > 1;


-- Validate gender standardization logic
-- CRM gender is treated as the master source
-- If CRM gender is 'n/a', use ERP gender value
SELECT DISTINCT
    ci.cst_gndr,
    ca.gen,
    CASE 
        WHEN ci.cst_gndr <> 'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, 'n/a')
    END AS standardized_gender
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid
ORDER BY 1,2;


-- Validate the dim_customers view
SELECT *
FROM gold.dim_customers;


-- Check distinct gender values in the final dimension
SELECT DISTINCT gender
FROM gold.dim_customers;



-- =============================================================================
--						Test Dimension: gold.dim_products
-- =============================================================================

-- Check if product_key remains unique after joining product and category tables
-- Expectation: No duplicates
SELECT 
    prd_key,
    COUNT(*) AS duplicate_count
FROM (
    SELECT
        pn.prd_id,
        pn.prd_key,
        pn.prd_nm,
        pn.cat_id,
        pc.cat,
        pc.subcat,
        pc.maintenance,
        pn.prd_cost,
        pn.prd_line,
        pn.prd_start_dt
    FROM silver.crm_prd_info pn
    LEFT JOIN silver.erp_px_cat_g1v2 pc
        ON pn.cat_id = pc.id
    WHERE pn.prd_end_dt IS NULL  -- Exclude historical product records
) t
GROUP BY prd_key
HAVING COUNT(*) > 1;


-- Validate the dim_products view
SELECT *
FROM gold.dim_products;



-- =============================================================================
--						Test Fact Table: gold.fact_sales
-- =============================================================================

-- Validate the fact table structure
SELECT *
FROM gold.fact_sales;



-- =============================================================================
-- Referential Integrity Check
-- =============================================================================
-- Ensure every fact record is linked to valid dimension records
-- Expectation: No NULL dimension keys

SELECT *
FROM gold.fact_sales fc
LEFT JOIN gold.dim_customers dc
    ON fc.customer_key = dc.customer_key
LEFT JOIN gold.dim_products dp
    ON fc.product_key = dp.product_key
WHERE dc.customer_key IS NULL 
   OR dp.product_key IS NULL;
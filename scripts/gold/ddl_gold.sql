/*
===============================================================================
				Gold Layer - Dimension and Fact Views
===============================================================================

Script Purpose:
    This script creates the analytical data model in the Gold Layer of the 
    Data Warehouse. The Gold Layer contains business-ready data structures 
    designed for reporting and analytics.

    The script creates:
        1. Customer Dimension   (gold.dim_customers)
        2. Product Dimension    (gold.dim_products)
        3. Sales Fact Table     (gold.fact_sales)

Design Principles:
    - Dimensions contain descriptive attributes about business entities.
    - Fact tables store measurable business events.
    - Surrogate keys are generated using ROW_NUMBER().
    - Source data is integrated from the Silver Layer.

Usage Notes:
    - Run this script after the Silver Layer is fully prepared.
    - Views are recreated each time to reflect updated transformations.

===============================================================================
*/


-- =============================================================================
--						Create Dimension: gold.dim_customers
-- =============================================================================
-- This dimension provides a unified customer view by integrating
-- CRM and ERP customer data sources.

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS

SELECT 
    ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS customer_key, -- Surrogate key

    ci.cst_id        AS customer_id,
    ci.cst_key       AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname  AS last_name,

    la.cntry         AS country,
    ci.cst_marital_status AS marital_status,

    -- Gender standardization logic
    -- CRM system is treated as the master source for gender
    -- If CRM gender is unavailable ('n/a'), ERP gender is used
    CASE 
        WHEN ci.cst_gndr <> 'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, 'n/a')
    END AS gender,

    ca.bdate         AS birthdate,
    ci.cst_create_date AS create_date

FROM silver.crm_cust_info ci

-- ERP system containing additional customer attributes
LEFT JOIN silver.erp_cust_az12 ca
       ON ci.cst_key = ca.cid

-- ERP system containing customer location data
LEFT JOIN silver.erp_loc_a101 la
       ON ci.cst_key = la.cid;

GO



-- =============================================================================
--						Create Dimension: gold.dim_products
-- =============================================================================
-- This dimension provides product information enriched with category data.

IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS

SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- Surrogate key

    pn.prd_id       AS product_id,
    pn.prd_key      AS product_number,
    pn.prd_nm       AS product_name,
    pn.cat_id       AS category_id,

    pc.cat          AS category,
    pc.subcat       AS subcategory,
    pc.maintenance  AS maintenance,

    pn.prd_cost     AS cost,
    pn.prd_line     AS product_line,
    pn.prd_start_dt AS start_date

FROM silver.crm_prd_info pn

-- Join with ERP product category reference table
LEFT JOIN silver.erp_px_cat_g1v2 pc
       ON pn.cat_id = pc.id

-- Only active products are included in the dimension
WHERE pn.prd_end_dt IS NULL;

GO



-- =============================================================================
--						Create Fact Table: gold.fact_sales
-- =============================================================================
-- This fact view stores transactional sales data and connects
-- to the customer and product dimensions via surrogate keys.

IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS

SELECT
    sd.sls_ord_num   AS order_number,

    -- Foreign keys linking to dimension tables
    pr.product_key   AS product_key,
    cu.customer_key  AS customer_key,

    -- Date attributes
    sd.sls_order_dt  AS order_date,
    sd.sls_ship_dt   AS shipping_date,
    sd.sls_due_dt    AS due_date,

    -- Measures
    sd.sls_sales     AS sales_amount,
    sd.sls_quantity  AS quantity,
    sd.sls_price     AS price

FROM silver.crm_sales_details sd

-- Link sales records to product dimension
LEFT JOIN gold.dim_products pr
       ON sd.sls_prd_key = pr.product_number

-- Link sales records to customer dimension
LEFT JOIN gold.dim_customers cu
       ON sd.sls_cust_id = cu.customer_id;

GO
/*
	===============================================================================
	Data Quality Validation Script
	Bronze & Silver Layers
	===============================================================================

	Script Purpose:
		This script performs data quality validation checks on the Bronze and
		Silver layers of the Data Warehouse. These checks help ensure that the
		ingested and transformed data meets basic quality standards before it is
		used for downstream analytics or reporting.

	Quality Checks Included:
		1. Primary Key Validation
			- Detect NULL values in primary keys.
			- Detect duplicate records.

		2. String Field Validation
			- Detect leading or trailing spaces.

		3. Data Standardization
			- Identify inconsistent categorical values.

		4. Date Validation
			- Detect invalid or improperly formatted dates.
			- Detect incorrect date sequences.

		5. Referential Integrity
			- Ensure foreign keys reference valid records.

		6. Business Logic Validation
			- Verify calculations and numeric consistency.

	Usage Notes:
		- Execute this script during the data loading process for the Bronze and Silver layers, and after the Silver layer is completed.
		- Queries should return **no results** if data quality is correct.
		- Any returned records indicate potential data quality issues that
		  require investigation.

	===============================================================================
*/

-- =============================================================================
--						Checking 'bronze.crm_cust_info'
-- =============================================================================


-- Check 1: Detect NULL or Duplicate Primary Keys
-- Expectation: No results
SELECT 
    cst_id,
    COUNT(*) 
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;


-- Check 2: Detect Leading or Trailing Spaces in String Fields
-- Expectation: No results
SELECT cst_key
FROM bronze.crm_cust_info 
WHERE cst_key <> TRIM(cst_key);

SELECT cst_firstname
FROM bronze.crm_cust_info 
WHERE cst_firstname <> TRIM(cst_firstname);

SELECT cst_lastname
FROM bronze.crm_cust_info 
WHERE cst_lastname <> TRIM(cst_lastname);

SELECT cst_marital_status
FROM bronze.crm_cust_info 
WHERE cst_marital_status <> TRIM(cst_marital_status);

SELECT cst_gndr
FROM bronze.crm_cust_info 
WHERE cst_gndr <> TRIM(cst_gndr);


-- Check 3: Data Standardization & Consistency
-- Purpose: Identify inconsistent categorical values
SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info;

SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;


-- Check 4: Date Validation
-- Detect invalid or improperly formatted dates
SELECT *
FROM (
    SELECT cst_create_date,
           ISDATE(CAST(cst_create_date AS VARCHAR)) AS CheckDate
    FROM bronze.crm_cust_info
) t
WHERE CheckDate <> 1;


-- ====================================================================
--					Checking 'silver.crm_cust_info'
-- ====================================================================


-- Check 1: Detect NULL or Duplicate Primary Keys
-- Expectation: No results
SELECT 
    cst_id,
    COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;


-- Check 2: Detect Leading or Trailing Spaces in String Fields
-- Expectation: No results
SELECT cst_firstname
FROM silver.crm_cust_info 
WHERE cst_firstname <> TRIM(cst_firstname);

SELECT cst_lastname
FROM silver.crm_cust_info 
WHERE cst_lastname <> TRIM(cst_lastname);

SELECT cst_marital_status
FROM silver.crm_cust_info 
WHERE cst_marital_status <> TRIM(cst_marital_status);

SELECT cst_gndr
FROM silver.crm_cust_info 
WHERE cst_gndr <> TRIM(cst_gndr);


-- Check 3: Data Standardization & Consistency
-- Purpose: Identify inconsistent categorical values
SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info;

SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;


-- Check 4: Date Validation
-- Detect invalid or improperly formatted dates
SELECT *
FROM (
    SELECT cst_create_date,
           ISDATE(CAST(cst_create_date AS VARCHAR)) AS CheckDate
    FROM silver.crm_cust_info
) t
WHERE CheckDate <> 1;


-- ====================================================================
--					Checking 'bronze.crm_prd_info'
-- ====================================================================


-- Check 1: Detect NULL or Duplicate Primary Keys
-- Expectation: No results
SELECT 
    prd_id ,
    COUNT(*) 
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;


-- Check 2: Detect Leading or Trailing Spaces in String Fields
-- Expectation: No results
SELECT prd_key
FROM bronze.crm_prd_info
WHERE prd_key <> TRIM(prd_key);

SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm <> TRIM(prd_nm);


-- Check 3: Validate Product Cost
-- Expectation: Cost should be positive and not NULL
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 1 OR prd_cost IS NULL;


-- Check 4: Data Standardization
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info;


-- Check 5: Validate Date Columns
SELECT *
FROM (
    SELECT prd_start_dt,
           ISDATE(CAST(prd_start_dt AS VARCHAR)) AS CheckDate
    FROM bronze.crm_prd_info
) t
WHERE CheckDate <> 1;

SELECT *
FROM (
    SELECT prd_end_dt,
           ISDATE(CAST(prd_end_dt AS VARCHAR)) AS CheckDate
    FROM bronze.crm_prd_info
) t
WHERE CheckDate <> 1;


-- Check 6: Detect Invalid Date Order
-- Expectation: Start date should not be after end date
SELECT 
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info
WHERE prd_start_dt > prd_end_dt



-- ====================================================================
--					Checking ' silver.crm_prd_info'
-- ====================================================================


-- Check 1: Detect NULL or Duplicate Primary Keys
-- Expectation: No results
SELECT 
    prd_id ,
    COUNT(*) 
FROM  silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;


-- Check 2: Detect Leading or Trailing Spaces in String Fields
-- Expectation: No results
SELECT prd_key
FROM  silver.crm_prd_info
WHERE prd_key <> TRIM(prd_key);

SELECT prd_nm
FROM  silver.crm_prd_info
WHERE prd_nm <> TRIM(prd_nm);


-- Check 3: Validate Product Cost
-- Expectation: Cost should be positive and not NULL 
SELECT prd_cost
FROM  silver.crm_prd_info
WHERE prd_cost < 1 OR prd_cost IS NULL;


-- Check 4: Data Standardization
SELECT DISTINCT prd_line
FROM  silver.crm_prd_info;


-- Check 5: Validate Date Columns
SELECT *
FROM (
    SELECT prd_start_dt,
           ISDATE(CAST(prd_start_dt AS VARCHAR)) AS CheckDate
    FROM  silver.crm_prd_info
) t
WHERE CheckDate <> 1;

SELECT *
FROM (
    SELECT prd_end_dt,
           ISDATE(CAST(prd_end_dt AS VARCHAR)) AS CheckDate
    FROM  silver.crm_prd_info
) t
WHERE CheckDate <> 1;


-- Check 6: Detect Invalid Date Order
-- Expectation: Start date should not be after end date
SELECT 
	prd_start_dt,
	prd_end_dt
FROM  silver.crm_prd_info
WHERE prd_start_dt > prd_end_dt


-- ====================================================================
-- 					Checking 'bronze.crm_sales_details'
-- ====================================================================

-- Check 1: Detect Leading or Trailing Spaces
SELECT sls_ord_num
FROM bronze.crm_sales_details
WHERE sls_ord_num <> TRIM(sls_ord_num);

SELECT sls_prd_key
FROM bronze.crm_sales_details
WHERE sls_prd_key <> TRIM(sls_prd_key);


-- Check 2: Referential Integrity Validation
-- Ensure product keys exist in product table
SELECT *
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)

SELECT *
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)


-- Check 3: Validate Customer Reference
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 
OR LEN(sls_order_dt) != 8 
OR sls_order_dt > 20150128 
OR sls_order_dt < 19900128
OR sls_order_dt IS NULL;

SELECT *
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0 
OR LEN(sls_ship_dt) != 8 
OR sls_ship_dt > 20150128 
OR sls_ship_dt < 19900128
OR sls_order_dt > sls_ship_dt;

SELECT *
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
OR LEN(sls_due_dt) != 8 
OR sls_due_dt > 20150128 
OR sls_due_dt < 19900128
OR sls_ship_dt > sls_due_dt;


-- Check 4: Business Rule Validation
-- Sales amount must equal Quantity * Price
SELECT *
FROM bronze.crm_sales_details
WHERE sls_sales <> (sls_quantity * sls_price) 
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
ORDER BY sls_sales, sls_quantity, sls_price


-- ====================================================================
-- 					Checking ' silver.crm_sales_details'
-- ====================================================================

-- Check 1: Detect Leading or Trailing Spaces
SELECT sls_ord_num
FROM  silver.crm_sales_details
WHERE sls_ord_num <> TRIM(sls_ord_num);

SELECT sls_prd_key
FROM  silver.crm_sales_details
WHERE sls_prd_key <> TRIM(sls_prd_key);


-- Check 2: Referential Integrity Validation
-- Ensure product keys exist in product table
SELECT *
FROM  silver.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)

SELECT *
FROM  silver.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)


-- Check 3: Validate Customer Reference
SELECT *
FROM  silver.crm_sales_details
WHERE sls_order_dt <= 0 
OR LEN(sls_order_dt) != 8 
OR sls_order_dt > 20150128 
OR sls_order_dt < 19900128
OR sls_order_dt IS NULL;

SELECT *
FROM  silver.crm_sales_details
WHERE sls_ship_dt <= 0 
OR LEN(sls_ship_dt) != 8 
OR sls_ship_dt > 20150128 
OR sls_ship_dt < 19900128
OR sls_order_dt > sls_ship_dt;

SELECT *
FROM  silver.crm_sales_details
WHERE sls_due_dt <= 0 
OR LEN(sls_due_dt) != 8 
OR sls_due_dt > 20150128 
OR sls_due_dt < 19900128
OR sls_ship_dt > sls_due_dt;


-- Check 4: Business Rule Validation
-- Sales amount must equal Quantity * Price
SELECT *
FROM  silver.crm_sales_details
WHERE sls_sales <> (sls_quantity * sls_price) 
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
ORDER BY sls_sales, sls_quantity, sls_price


-- ====================================================================
--					Checking 'bronze.erp_cust_az12'
-- ====================================================================


-- Check 1: Detect NULL or Duplicate Primary Keys
-- Expectation: No results
SELECT 
	cid,
	COUNT(*)
FROM bronze.erp_cust_az12
GROUP BY cid
HAVING COUNT(*) > 1 OR cid IS NULL;


-- Check 2: Detect Invalid Date Order
SELECT *
FROM (
    SELECT bdate,
           ISDATE(CAST(bdate AS VARCHAR)) AS CheckDate
    FROM bronze.erp_cust_az12
) t
WHERE CheckDate <> 1;

SELECT 
	DISTINCT bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1926-01-01' OR bdate > GETDATE() 


-- Check 3: Data Standardization & Consistency
-- Purpose: Identify inconsistent categorical values
SELECT 
	DISTINCT gen
FROM bronze.erp_cust_az12;


-- ====================================================================
--					Checking ' silver.erp_cust_az12'
-- ====================================================================


-- Check 1: Detect NULL or Duplicate Primary Keys
-- Expectation: No results
SELECT 
	cid,
	COUNT(*)
FROM  silver.erp_cust_az12
GROUP BY cid
HAVING COUNT(*) > 1 OR cid IS NULL;


-- Check 2: Detect Invalid Date Order
SELECT *
FROM (
    SELECT bdate,
           ISDATE(CAST(bdate AS VARCHAR)) AS CheckDate
    FROM  silver.erp_cust_az12
) t
WHERE CheckDate <> 1;

SELECT 
	DISTINCT bdate
FROM  silver.erp_cust_az12
WHERE bdate < '1926-01-01' OR bdate > GETDATE() 


-- Check 3: Data Standardization & Consistency
-- Purpose: Identify inconsistent categorical values
SELECT 
	DISTINCT gen
FROM  silver.erp_cust_az12;


-- ====================================================================
--					Checking '  bronze.erp_loc_a101'
-- ====================================================================


-- Check 1: Detect NULL or Duplicate Primary Keys
-- Expectation: No results
SELECT 
	cid,
	COUNT(*)
FROM  bronze.erp_loc_a101
GROUP BY cid
HAVING COUNT(*) > 1 OR cid IS NULL;


-- Check 2: Column Integrity Verification
SELECT *
FROM  bronze.erp_loc_a101
WHERE cid NOT IN (SELECT cst_key FROM bronze.crm_cust_info)


-- Check 3: Detect Leading or Trailing Spaces in String Fields
-- Expectation: No results
SELECT cntry
FROM  bronze.erp_loc_a101
WHERE cntry <> TRIM(cntry);


-- Check 4: Data Standardization & Consistency
-- Purpose: Identify inconsistent categorical values
SELECT DISTINCT cntry
FROM  bronze.erp_loc_a101;


-- ====================================================================
--					Checking '  silver.erp_loc_a101'
-- ====================================================================


-- Check 1: Detect NULL or Duplicate Primary Keys
-- Expectation: No results
SELECT 
	cid,
	COUNT(*)
FROM  silver.erp_loc_a101
GROUP BY cid
HAVING COUNT(*) > 1 OR cid IS NULL;


-- Check 2: Column Integrity Verification
SELECT *
FROM  silver.erp_loc_a101
WHERE cid NOT IN (SELECT cst_key FROM silver.crm_cust_info)


-- Check 3: Detect Leading or Trailing Spaces in String Fields
-- Expectation: No results
SELECT cntry
FROM  silver.erp_loc_a101
WHERE cntry <> TRIM(cntry);


-- Check 4: Data Standardization & Consistency
-- Purpose: Identify inconsistent categorical values
SELECT DISTINCT cntry
FROM  silver.erp_loc_a101;


-- ====================================================================
--					Checking '  bronze.erp_px_cat_g1v2'
-- ====================================================================


-- Check 1: Detect NULL or Duplicate Primary Keys
-- Expectation: No results
SELECT 
	id,
	COUNT(*)
FROM  bronze.erp_px_cat_g1v2
GROUP BY id
HAVING COUNT(*) > 1 OR id IS NULL;


-- Check 2: Column Integrity Verification
SELECT *
FROM  bronze.erp_px_cat_g1v2
WHERE id NOT IN (SELECT cat_id FROM silver.crm_prd_info)


-- Check 3: Detect Leading or Trailing Spaces in String Fields
-- Expectation: No results
SELECT *
FROM  bronze.erp_px_cat_g1v2
WHERE cat <> TRIM(cat) OR subcat <> TRIM(subcat) OR maintenance <> TRIM(maintenance);


-- Check 4: Data Standardization & Consistency
-- Purpose: Identify inconsistent categorical values
SELECT DISTINCT cat
FROM  bronze.erp_px_cat_g1v2;

SELECT DISTINCT subcat
FROM  bronze.erp_px_cat_g1v2;

SELECT DISTINCT maintenance
FROM  bronze.erp_px_cat_g1v2;


-- ====================================================================
--					Checking '  silver.erp_px_cat_g1v2'
-- ====================================================================


-- Check 1: Detect NULL or Duplicate Primary Keys
-- Expectation: No results
SELECT 
	id,
	COUNT(*)
FROM  silver.erp_px_cat_g1v2
GROUP BY id
HAVING COUNT(*) > 1 OR id IS NULL;


-- Check 2: Column Integrity Verification
SELECT *
FROM  silver.erp_px_cat_g1v2
WHERE id NOT IN (SELECT cat_id FROM silver.crm_prd_info)


-- Check 3: Detect Leading or Trailing Spaces in String Fields
-- Expectation: No results
SELECT *
FROM  silver.erp_px_cat_g1v2
WHERE cat <> TRIM(cat) OR subcat <> TRIM(subcat) OR maintenance <> TRIM(maintenance);


-- Check 4: Data Standardization & Consistency
-- Purpose: Identify inconsistent categorical values
SELECT DISTINCT cat
FROM  silver.erp_px_cat_g1v2;

SELECT DISTINCT subcat
FROM  silver.erp_px_cat_g1v2;

SELECT DISTINCT maintenance
FROM  silver.erp_px_cat_g1v2;
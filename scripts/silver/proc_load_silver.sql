/*
	=============================================================
	Stored Procedure: Load Data into Silver Layer (Bronze -> Silver)
	=============================================================

	Procedure Name:
		silver.load_silver

	Purpose:
		This stored procedure transforms and loads cleaned data from the Bronze layer
		into the Silver layer tables of the Data Warehouse.

		The Silver layer is responsible for applying data cleansing, standardization,
		and transformation rules to prepare data for analytical use.

		Data in this layer is structured, consistent, and ready for further
		business-level transformations in the Gold layer.

	Process Overview:
		1. Truncate existing data in Silver tables to ensure a fresh load.
		2. Extract data from Bronze layer tables.
		3. Apply data cleansing and transformation rules including:
			- Standardizing categorical values
			- Data integration from multiple sources
			- Creating derived columns
			- Fixing invalid or inconsistent dates
			- Correcting inconsistent numeric values
			- Removing duplicate records
			- Handling NULL and missing values
			- Removing unwanted spaces in text fields
			- Applying business rules and logic
			- Casting columns to appropriate data types
			- Filtering irrelevant or invalid records
		4. Insert the transformed data into Silver tables.
		5. Capture the number of rows inserted.
		6. Log load duration for each table.
		7. Display execution progress messages.
		8. Handle errors using TRY...CATCH.

	Source Layer:
		Bronze Tables:
			- bronze.crm_cust_info
			- bronze.crm_prd_info
			- bronze.crm_sales_details
			- bronze.erp_cust_az12
			- bronze.erp_loc_a101
			- bronze.erp_px_cat_g1v2

	Target Layer:
		Silver Tables:
			- silver.crm_cust_info
			- silver.crm_prd_info
			- silver.crm_sales_details
			- silver.erp_cust_az12
			- silver.erp_loc_a101
			- silver.erp_px_cat_g1v2

	Major Transformations Applied:

	CRM Customer Data (crm_cust_info):
		- Remove records with NULL primary keys.
		- Remove duplicate customers by keeping the latest record.
		- Trim unwanted spaces from text fields.
		- Standardize marital status and gender values.

	CRM Product Data (crm_prd_info):
		- Split composite product key into category ID and product key.
		- Replace NULL product cost values with 0.
		- Standardize product line descriptions.
		- Calculate product end dates using the next product start date.

	CRM Sales Data (crm_sales_details):
		- Convert numeric date fields into valid DATE format.
		- Replace invalid or malformed dates with NULL.
		- Ensure sales values match Quantity × Price.
		- Recalculate sales if values are missing or inconsistent.
		- Correct negative or missing price values.

	ERP Customer Data (erp_cust_az12):
		- Remove "NAS" prefix from customer IDs.
		- Validate birth dates and remove future dates.
		- Standardize gender values.

	ERP Location Data (erp_loc_a101):
		- Remove hyphens from customer identifiers.
		- Standardize country names.
		- Replace missing country values with 'n/a'.

	ERP Product Categories (erp_px_cat_g1v2):
		- Load data directly from Bronze layer without transformations.

	Logging & Monitoring:
		The procedure prints:
			- Table being processed
			- Number of rows inserted
			- Load duration for each table
			- Total batch execution time

	Error Handling:
		Errors during execution are handled using TRY...CATCH.
		The procedure prints:
			- Error message
			- Error number
			- Error state

	WARNING:
		This procedure uses TRUNCATE TABLE before loading data.
		Running this procedure will permanently delete existing
		data in the Silver tables before reloading.

		Ensure the Bronze layer has been successfully loaded
		before executing this procedure.

	Parameters:
		None. 
		  This stored procedure does not accept any parameters or return any values.

	Execution Example:
		EXEC silver.load_silver;

	=============================================================
*/
USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @RowCount INT, @batch_start_time DATETIME, @batch_end_time DATETIME; 
	SET @batch_start_time = GETDATE();
	SET NOCOUNT ON;
	BEGIN TRY
		PRINT '================================================================';
		PRINT '                    Loading Silver Layer                        ';
		PRINT '================================================================';
		PRINT '                                                                ';
		PRINT '                                                                ';
		PRINT '----------------------------------------------------------------';
		PRINT '                     Loading CRM Tables                         ';
		PRINT '----------------------------------------------------------------';

		/* =============================================================================
		   Load Cleaned Customer Data into silver.crm_cust_info
		   =============================================================================
		   Transformation Rules:
		   - Remove records where the primary key (cst_id) is NULL.
		   - Handle duplicate primary keys:
			   * Keep the most recent record based on cst_create_date.
			   * Prefer the latest available version of the customer record.
		   - Remove leading and trailing spaces from string columns. 
		   - Standardize categorical values:
			   * cst_marital_status: S → Single, M → Married, others → 'n/a'
			   * cst_gndr: M → Male, F → Female, others → 'n/a'
		   - Insert the cleaned and standardized data into the Silver layer.
		============================================================================= */
		SET @start_time = GETDATE();
		PRINT '>>>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;

		PRINT '>>>> Inserting Data Into: silver.crm_cust_info From bronze.crm_cust_info';

		INSERT INTO silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date)

		SELECT 
			cst_id,             
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				ELSE 'n/a'
			END cst_marital_status,
			CASE
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				ELSE 'n/a'
				END cst_gndr,
			cst_create_date 
		FROM (SELECT 
				*,
				ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL) t
		WHERE flag_last = 1

		SET @RowCount = @@ROWCOUNT;
		PRINT '>>>> ' + FORMAT(@RowCount, 'N0') + ' rows inserted successfully';
		SET @end_time = GETDATE();
		PRINT '>>>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '                                                                ';
	
		/*
		-- Quality Check or Test
		SELECT * FROM silver.crm_cust_info; 
		SELECT COUNT(*) FROM silver.crm_cust_info;
		*/

		/* =============================================================================
		   Load Product Information into silver.crm_prd_info
		   =============================================================================
		   Transformation Rules:

		   - prd_key Structure
			  * The prd_key column is a composite identifier.
			  * First 4–5 characters represent the Category ID 
				from bronze.erp_px_cat_g1v2 (Primary Key).
			  * Remaining characters represent the product key used in
				bronze.crm_sales_details.sls_prd_key.
			  * SUBSTRING is used to split the composite key into:
					cat_id  → category identifier
					prd_key → product identifier

		   - Data Cleaning
			  * Replace NULL values in prd_cost with 0.

		   - Product Line Standardization
				M → Mountain
				R → Road
				S → Other Sales
				T → Touring
				Others → 'n/a'

		   - Product Validity Dates
			  * Ensure prd_start_dt < prd_end_dt.
			  * prd_end_dt is calculated as:
					(next record's prd_start_dt - 1 day)
				within the same product key.
			  * The most recent record keeps prd_end_dt = NULL
				to represent the current active product record.
		============================================================================= */
		SET @start_time = GETDATE();
		PRINT '>>>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;

		PRINT '>>>> Inserting Data Into: silver.crm_prd_info From bronze.crm_prd_info';
		INSERT INTO silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
			)
		SELECT 
			prd_id,
			Replace(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
			prd_nm,
			ISNULL(prd_cost,0) AS prd_cost,
			CASE
				WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
				WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
				WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
				WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
				ELSE 'n/a'
			END prd_line,
			prd_start_dt,
			DATEADD(DAY, -1, LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt ASC)) AS prd_end_dt
		FROM bronze.crm_prd_info

		SET @RowCount = @@ROWCOUNT;
		PRINT '>>>> ' + FORMAT(@RowCount, 'N0') + ' rows inserted successfully';
		SET @end_time = GETDATE();
		PRINT '>>>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '                                                                ';

		/* =============================================================================
		   Load Sales Details into silver.crm_sales_details
		   =============================================================================
		   Transformation Rules:

		   - Date Conversion
			  * Source date columns are stored as numeric values (YYYYMMDD).
			  * Invalid values (0, negative numbers, or incorrect length)
				are replaced with NULL.
			  * Valid values are converted to DATE format.

		   - Sales Calculation Logic
			  * If Sales is NULL, zero, negative, or inconsistent with
				Quantity * Price → recalculate using Quantity * Price.

		   - Price Correction
			  * If Price is NULL or zero → derive using Sales / Quantity.
			  * If Price is negative → convert to a positive value.

		   These rules ensure mathematical consistency between
		   Sales, Quantity, and Price.
		============================================================================= */
		SET @start_time = GETDATE();
		PRINT '>>>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;

		PRINT '>>>> Inserting Data Into: silver.crm_sales_details From bronze.crm_sales_details';
		INSERT INTO silver.crm_sales_details 
		(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
			)

		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE
				WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END sls_order_dt,
			CASE
				WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END sls_ship_dt,
				CASE
				WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END sls_due_dt,
			CASE 
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales <> (sls_quantity * ABS(sls_price))  THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END sls_sales,
			sls_quantity,
			CASE 
				WHEN sls_price IS NULL OR sls_price <= 0   THEN  ABS(sls_sales) / NULLIF(sls_quantity, 0)
				ELSE sls_price
			END sls_price
		FROM bronze.crm_sales_details

		SET @RowCount = @@ROWCOUNT;
		PRINT '>>>> ' + FORMAT(@RowCount, 'N0') + ' rows inserted successfully';
		SET @end_time = GETDATE();
		PRINT '>>>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '                                                                ';

		/*
		-- Quality Check or Test
		SELECT * FROM silver.crm_sales_details; 
		SELECT COUNT(*) FROM silver.crm_sales_details;
		*/

		
		PRINT '                                                                ';
		PRINT '----------------------------------------------------------------';
		PRINT '                     Loading ERP Tables                         ';
		PRINT '----------------------------------------------------------------';

		
		/* =============================================================================
		   Load ERP Customer Information into silver.erp_cust_az12
		   =============================================================================
		   Transformation Rules:

		   - Remove the prefix 'NAS' from the cid column if present.
		   - Ensure birthdate (bdate) is not in the future.
			 Future dates are replaced with NULL.
		   - Standardize gender values:
				M / Male → Male
				F / Female → Female
				Others → 'n/a'
		============================================================================= */
		SET @start_time = GETDATE();
		PRINT '>>>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;

		PRINT '>>>> Inserting Data Into: silver.erp_cust_az12 From bronze.erp_cust_az12';

		INSERT INTO silver.erp_cust_az12 
		(
			cid,
			bdate,
			gen)

		SELECT 
			CASE
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
				ELSE cid
			END cid,
			CASE
				WHEN bdate > GETDATE() THEN NULL
				ELSE bdate
			END bdate,
			CASE
				WHEN UPPER(TRIM(gen)) IN ( 'M', 'Male')  THEN 'Male'
				WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
				ELSE 'n/a'
			END gen 
		FROM bronze.erp_cust_az12

		SET @RowCount = @@ROWCOUNT;
		PRINT '>>>> ' + FORMAT(@RowCount, 'N0') + ' rows inserted successfully';
		SET @end_time = GETDATE();
		PRINT '>>>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '                                                                ';

		/*
		-- Quality Check or Test
		SELECT * FROM silver.erp_cust_az12; 
		SELECT COUNT(*) FROM silver.erp_cust_az12;
		*/

		SET @start_time = GETDATE();
		PRINT '>>>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;

		PRINT '>>>> Inserting Data Into: silver.erp_loc_a101 From bronze.erp_loc_a101';


		/* =============================================================================
		   Load Location Data into silver.erp_loc_a101
		   =============================================================================
		   Transformation Rules:

		   - Remove hyphens from the customer identifier (cid).
		   - Standardize country names to a consistent format.
		   - Replace missing or empty country values with 'n/a'.
		============================================================================= */
		INSERT INTO silver.erp_loc_a101 
		(
			cid,
			cntry)
		SELECT 
			REPLACE(cid, '-', '') AS cid,
			CASE 
				WHEN UPPER(TRIM(cntry)) = 'AUSTRALIA' THEN 'Australia'
				WHEN UPPER(TRIM(cntry)) IN ('DE', 'GERMANY') THEN 'Germany'
				WHEN UPPER(TRIM(cntry)) IN ('UNITED STATES', 'US', 'USA') THEN 'United States' 
				WHEN UPPER(TRIM(cntry)) = 'UNITED KINGDOM' THEN 'United Kingdom'
				WHEN UPPER(TRIM(cntry)) = 'CANADA' THEN 'Canada'
				WHEN UPPER(TRIM(cntry)) = 'FRANCE' THEN 'France'
				WHEN UPPER(TRIM(cntry)) = '' OR  cntry IS NULL THEN 'n/a'
				ELSE cntry
			END cntry
		FROM bronze.erp_loc_a101

		SET @RowCount = @@ROWCOUNT;
		PRINT '>>>> ' + FORMAT(@RowCount, 'N0') + ' rows inserted successfully';
		SET @end_time = GETDATE();
		PRINT '>>>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '                                                                ';

		/*
		-- Quality Check or Test
		SELECT * FROM silver.erp_loc_a101; 
		SELECT COUNT(*) FROM silver.erp_loc_a101;
		*/


		SET @start_time = GETDATE();
		PRINT '>>>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;

		PRINT '>>>> Inserting Data Into: silver.erp_px_cat_g1v2 From bronze.erp_px_cat_g1v2';
		/* =============================================================================
		   Load Product Category Data into silver.erp_px_cat_g1v2
		   =============================================================================
		   Transformation Rules:
		   - No transformations are required.
		   - Data is loaded as-is from the Bronze layer.
		============================================================================= */
		INSERT INTO silver.erp_px_cat_g1v2
		(
			id,
			cat,
			subcat,
			maintenance)

		SELECT 
			id,
			cat,
			subcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2

		SET @RowCount = @@ROWCOUNT;
		PRINT '>>>> ' + FORMAT(@RowCount, 'N0') + ' rows inserted successfully';
		SET @end_time = GETDATE();
		PRINT '>>>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '                                                                ';


		/*
		-- Quality Check or Test
		SELECT * FROM silver.erp_px_cat_g1v2; 
		SELECT COUNT(*) FROM silver.erp_px_cat_g1v2;
		*/
		SET NOCOUNT OFF;
		SET @batch_end_time = GETDATE();
		PRINT '                                                                ';
		PRINT '                                                                ';
		PRINT '================================================================';
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '================================================================';
	
	END TRY
	BEGIN CATCH
		PRINT '================================================================';
		PRINT '           Error Occurred During Loading Silver Layer            ';
		PRINT '================================================================';
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR);
		PRINT 'Error State: ' + CAST(ERROR_STATE() AS VARCHAR);
		PRINT 'Error Severity: ' + CAST(ERROR_SEVERITY() AS VARCHAR);
		PRINT 'Error Line: ' + CAST(ERROR_LINE() AS VARCHAR);
		PRINT '================================================================';
    
		-- Re-throw the error if you want it to be raised to the calling application
		THROW;
	END CATCH


END
GO



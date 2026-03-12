/*
=============================================================
Stored Procedure: Load Data into Bronze Layer
=============================================================

Procedure Name:
    bronze.load_bronze

Purpose:
    This stored procedure loads raw data from external CSV files into the Bronze layer tables of the Data Warehouse.

    The Bronze layer acts as the first landing zone for source system data. Data is loaded exactly as received from 
	source systems with minimal or no transformation.

Process Overview:
    1. Truncate existing data in Bronze tables to ensure a fresh load.
    2. Load data from source CSV files using BULK INSERT.
    3. Capture the number of rows inserted.
    4. Log load duration for each table.
    5. Display execution progress messages.
    6. Handle errors using TRY...CATCH.

Source Systems:
    CRM Data Files:
        - cust_info.csv
        - prd_info.csv
        - sales_details.csv

    ERP Data Files:
        - CUST_AZ12.csv
        - LOC_A101.csv
        - PX_CAT_G1V2.csv

Tables Loaded:
    CRM Tables:
        - bronze.crm_cust_info
        - bronze.crm_prd_info
        - bronze.crm_sales_details

    ERP Tables:
        - bronze.erp_cust_az12
        - bronze.erp_loc_a101
        - bronze.erp_px_cat_g1v2

Logging & Monitoring:
    The procedure prints:
        - Table being loaded
        - Number of rows inserted
        - Load duration per table
        - Total batch execution time

Error Handling:
    Any errors occurring during the load process are captured using TRY...CATCH and printed with the error message,
    error number, and error state.

WARNING:
    This procedure uses TRUNCATE TABLE before loading data. Running this procedure will permanently delete existing
    data in the Bronze tables before reloading.

    Ensure source files are correct and available before executing this procedure.

Execution Example:
    EXEC bronze.load_bronze;

=============================================================
*/



CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @RowCount INT, @batch_start_time DATETIME, @batch_end_time DATETIME; 
	SET @batch_start_time = GETDATE();
	SET NOCOUNT ON;
	SET @start_time = GETDATE();
	BEGIN TRY
		PRINT '================================================================';
		PRINT '                    Loading Bronze Layer                        ';
		PRINT '================================================================';
		PRINT '                                                                ';
		PRINT '                                                                ';
		PRINT '----------------------------------------------------------------';
		PRINT '                     Loading CRM Tables                         ';
		PRINT '----------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>>>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>>>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'F:\ML\Project\Data Warehouse\Datasets\source_crm\cust_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @RowCount = @@ROWCOUNT;
		PRINT '>>>> ' + FORMAT(@RowCount, 'N0') + ' rows inserted successfully';
		SET @end_time = GETDATE();
		PRINT '>>>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '                                                                ';
	
		/*
		-- Quality Check or Test
		SELECT * FROM bronze.crm_cust_info; 
		SELECT COUNT(*) FROM bronze.crm_cust_info;
		*/

		SET @start_time = GETDATE();
		PRINT '>>>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>>>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'F:\ML\Project\Data Warehouse\Datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @RowCount = @@ROWCOUNT;
		PRINT '>>>> ' + FORMAT(@RowCount, 'N0') + ' rows inserted successfully';
		SET @end_time = GETDATE();
		PRINT '>>>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '                                                                ';

		/*
		-- Quality Check or Test
		SELECT * FROM bronze.crm_prd_info; 
		SELECT COUNT(*) FROM bronze.crm_prd_info;
		*/

		SET @start_time = GETDATE();
		PRINT '>>>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>>>> Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'F:\ML\Project\Data Warehouse\Datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @RowCount = @@ROWCOUNT;
		PRINT '>>>> ' + FORMAT(@RowCount, 'N0') + ' rows inserted successfully';
		SET @end_time = GETDATE();
		PRINT '>>>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '                                                                ';

		/*
		-- Quality Check or Test
		SELECT * FROM bronze.crm_sales_details; 
		SELECT COUNT(*) FROM bronze.crm_sales_details;
		*/

		PRINT '                                                                ';
		PRINT '----------------------------------------------------------------';
		PRINT '                     Loading ERP Tables                         ';
		PRINT '----------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>>>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>>>> Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'F:\ML\Project\Data Warehouse\Datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @RowCount = @@ROWCOUNT;
		PRINT '>>>> ' + FORMAT(@RowCount, 'N0') + ' rows inserted successfully';
		SET @end_time = GETDATE();
		PRINT '>>>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '                                                                ';

		/*
		-- Quality Check or Test
		SELECT * FROM bronze.erp_cust_az12; 
		SELECT COUNT(*) FROM bronze.erp_cust_az12;
		*/

		SET @start_time = GETDATE();
		PRINT '>>>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>>>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'F:\ML\Project\Data Warehouse\Datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @RowCount = @@ROWCOUNT;
		PRINT '>>>> ' + FORMAT(@RowCount, 'N0') + ' rows inserted successfully';
		SET @end_time = GETDATE();
		PRINT '>>>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '                                                                ';
		

		/*
		-- Quality Check or Test
		SELECT * FROM bronze.erp_loc_a101; 
		SELECT COUNT(*) FROM bronze.erp_loc_a101;
		*/

		SET @start_time = GETDATE();
		PRINT '>>>> Truncating Table: erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>>>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'F:\ML\Project\Data Warehouse\Datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @RowCount = @@ROWCOUNT;
		PRINT '>>>> ' + FORMAT(@RowCount, 'N0') + ' rows inserted successfully';
		SET @end_time = GETDATE();
		PRINT '>>>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '                                                                ';


		/*
		-- Quality Check or Test
		SELECT * FROM bronze.erp_px_cat_g1v2; 
		SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2;
		*/

		SET @batch_end_time = GETDATE();
		PRINT '                                                                ';
		PRINT '                                                                ';
		PRINT '================================================================';
		PRINT 'Loading Bronze Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '================================================================';
		SET NOCOUNT OFF;
	END TRY
	BEGIN CATCH
		PRINT '================================================================';
		PRINT '           Error Occured During Loading Bronze Layer            ';
		PRINT 'Error Message' + ERROR_Message();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS VARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS VARCHAR);
		PRINT '================================================================';
	END CATCH


END
GO
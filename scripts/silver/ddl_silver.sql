/*
=============================================================
Create  silver Layer Tables
=============================================================

Script Purpose:
    This script creates the tables required for the ' silver' layer
    in the DataWarehouse database.

    The  silver layer stores raw data exactly as it is received from
    source systems with minimal or no transformations. These tables
    act as the initial landing zone for data ingestion.

    The script first checks whether each table already exists.
    If a table exists, it is dropped and recreated to ensure the
    structure is fresh and consistent.

Tables Created:
    CRM Source Tables:
        -  silver.crm_cust_info       : Customer information from CRM system
        -  silver.crm_prd_info        : Product information from CRM system
        -  silver.crm_sales_details   : Sales transaction data from CRM system

    ERP Source Tables:
        -  silver.erp_cust_az12       : Customer demographic data from ERP
        -  silver.erp_loc_a101        : Customer location information
        -  silver.erp_px_cat_g1v2     : Product category and maintenance data

WARNING:
    Running this script will DROP existing  silver tables if they
    already exist. All data inside those tables will be permanently
    deleted.

    Ensure proper backups or staging processes exist before running
    this script in production environments.

=============================================================
*/

USE DataWarehouse;
GO

IF OBJECT_ID(' silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE  silver.crm_cust_info;
GO

CREATE TABLE  silver.crm_cust_info (
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
Go



IF OBJECT_ID(' silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE  silver.crm_prd_info;
GO

CREATE TABLE  silver.crm_prd_info (
	prd_id INT,
	cat_id NVARCHAR(50),
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
Go



IF OBJECT_ID(' silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE  silver.crm_sales_details;
GO

CREATE TABLE  silver.crm_sales_details (
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt DATE,
	sls_ship_dt  DATE,
	sls_due_dt DATE,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
Go


IF OBJECT_ID(' silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE  silver.erp_cust_az12;
GO

CREATE TABLE  silver.erp_cust_az12 (
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
Go


IF OBJECT_ID(' silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE  silver.erp_loc_a101;
GO

CREATE TABLE  silver.erp_loc_a101 (
	cid NVARCHAR(50),
	cntry NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
Go


IF OBJECT_ID(' silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE  silver.erp_px_cat_g1v2;
GO

CREATE TABLE  silver.erp_px_cat_g1v2 (
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance CHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
Go
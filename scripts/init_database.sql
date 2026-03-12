/*
=============================================================
Create Database and Schemas
=============================================================

Script Purpose:
    This script creates a new database named 'DataWarehouse'. 
    It first checks whether the database already exists. 
    If it exists, the script forces all connections to close,
    drops the existing database, and then recreates it.

    After creating the database, the script also creates three
    schemas inside the database:
        - bronze  : Raw data layer
        - silver  : Cleaned and transformed data layer
        - gold    : Business-ready and analytics layer

WARNING:
    Running this script will DROP the existing 'DataWarehouse'
    database if it already exists. This will permanently delete
    all data stored in the database.

    Make sure you have proper backups before executing this script.

=============================================================
*/

-- Switch to master database
USE master;
GO

-- Remove all other users (disconnect them), Set the database to SINGLE_USER mode, Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

-- Create new database
CREATE DATABASE DataWarehouse;
GO

-- Switch to the new database - DataWarehouse
USE DataWarehouse;
GO

-- Create schemas for data layers
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
Go

CREATE SCHEMA gold;
Go
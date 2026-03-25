/*
========================================================================
CREATE DATABASE AND SCHEMAS
========================================================================
Script Purpose;
    This script creates a new database name 'Datawarehouse' after checking if it already exists.
    if the database exists, it is dropped and recreated. additionally,the script sets up three schemas
    within the database ;'bronze','silver','gold'

WARNING:
  Running this script will drop the entire 'datawarehous' database if it exists.
  All data in the database will be permanently deleted.proceed with caution
  and ensure you have a proper backup before running this script
*/
-- 1. Drop the database if it exists (Note: This will fail if there are active connections)
DROP DATABASE IF EXISTS datawarehouse;

-- 2. Create the database
CREATE DATABASE datawarehouse;

-- 3. IMPORTANT: You must connect to 'datawarehouse' before running the lines below.
-- In psql, use: \c datawarehouse

-- 4. Create the schemas for the Medallion Architecture
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- 5. Optional: Verify the schemas were created
SELECT schema_name 
FROM information_schema.schemata 
WHERE schema_name IN ('bronze', 'silver', 'gold');

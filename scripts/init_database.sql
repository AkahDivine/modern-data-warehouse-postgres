/* ============================================
   DATA WAREHOUSE SETUP (PostgreSQL)

   Creates a fresh 'datawarehouse' database
   and sets up bronze, silver, and gold schemas.

   ⚠️ WARNING:
   This will DROP the existing database and delete all data.
   Ensure you have backups before running.
   ============================================ */

-- Drop and recreate database
DROP DATABASE IF EXISTS datawarehouse;
CREATE DATABASE datawarehouse;

-- Connect to database (psql only)
-- \c datawarehouse

-- Create schemas (Medallion Architecture)
CREATE SCHEMA bronze;   -- raw data
CREATE SCHEMA silver;   -- cleaned data
CREATE SCHEMA gold;     -- analytics-ready data

-- Optional: verify schemas
-- SELECT schema_name 
-- FROM information_schema.schemata
-- WHERE schema_name IN ('bronze','silver','gold');

/*
================================================================================
DDL Script: Create Bronze Layer Tables
================================================================================

Purpose:
This script defines the physical table structures for the 'bronze' schema.
It drops existing tables (if they exist) and recreates them.

The bronze layer represents the raw ingestion layer in the data warehouse
architecture. Tables in this layer store data exactly as received from
source systems with minimal or no transformation.

Run this script when:
- Setting up the database for the first time
- Resetting the bronze schema structure
- Rebuilding the data warehouse foundation
================================================================================
*/

-- ============================================================
-- CRM CUSTOMER INFORMATION TABLE
-- ============================================================

-- Recreate table to ensure clean structure
DROP TABLE IF EXISTS bronze.crm_cust_info;

CREATE TABLE bronze.crm_cust_info (
	cst_id INT,
	cst_key VARCHAR(50),
	cst_firstname VARCHAR(50),
	cst_lastname VARCHAR(50),
	cst_material_status VARCHAR(50),
	cst_gndr VARCHAR(50),
	cst_create_date DATE
);

-- ============================================================
-- CRM PRODUCT INFORMATION TABLE
-- ============================================================

DROP TABLE IF EXISTS bronze.crm_prd_info;

CREATE TABLE bronze.crm_prd_info (
    prd_id INT,
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost INT,
    prd_line VARCHAR(50),
    prd_start_dt TIMESTAMP,  
    prd_end_dt TIMESTAMP     
);

-- ============================================================
-- CRM SALES DETAILS TABLE
-- ============================================================

DROP TABLE IF EXISTS bronze.crm_sales_details;

CREATE TABLE bronze.crm_sales_details (
    sls_ord_num VARCHAR(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id INT,
    sls_order_dt INT,  
    sls_ship_dt INT,
    sls_due_dt INT,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);

-- ============================================================
-- ERP LOCATION TABLE
-- ============================================================

DROP TABLE IF EXISTS bronze.erp_loc_a101;

CREATE TABLE bronze.erp_loc_a101 (
    cid    VARCHAR(50),
    cntry  VARCHAR(50)
);

-- ============================================================
-- ERP CUSTOMER DEMOGRAPHICS TABLE
-- ============================================================

DROP TABLE IF EXISTS bronze.erp_cust_az12;

CREATE TABLE bronze.erp_cust_az12 (
    cid     VARCHAR(50),
    bdate   DATE,
    gen     VARCHAR(50)
);

-- ============================================================
-- ERP PRODUCT CATEGORY TABLE
-- ============================================================

DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;

CREATE TABLE bronze.erp_px_cat_g1v2 (
    id            VARCHAR(50),
    cat           VARCHAR(50),
    subcat        VARCHAR(50),
    maintenance   VARCHAR(50)
);

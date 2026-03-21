/*
=========================================================================
DDL Script: Gold Layer Views Creation
=========================================================================

Purpose:
This script defines and creates views for the Gold layer of the data warehouse.
The Gold layer represents the finalized dimension and fact tables (Star Schema),
ready for analytics and reporting.

Overview:
- Each view transforms and integrates data from the Silver layer.
- Ensures data is clean, enriched, and business-ready.
- Supports analytics, reporting, and dashboarding directly on these views.

Usage:
- Query these views for insights without altering underlying Silver tables.
- Automatically updates existing views when using CREATE OR REPLACE VIEW.

Notes:
- Designed to maintain dependencies safely between dimension and fact views.
- Avoids dropping views to prevent breaking dependent objects.

=========================================================================
*/
-- ============================================
-- gold.dim_customers
-- ============================================
CREATE OR REPLACE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    la.cntry AS country,
    ci.cst_marital_status AS marital_status,
    CASE
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the master for gender
        ELSE COALESCE(ca.gen, 'n/a')
    END AS gender,
    ca.bdate AS birthdate,
    ci.cst_create_date AS create_date
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS la
    ON ci.cst_key = la.cid;


-- ============================================
-- gold.dim_products
-- ============================================
CREATE OR REPLACE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pi.prd_start_dt, pi.prd_key) AS product_key,
    pi.prd_id AS product_id,
    pi.prd_key AS product_number,
    pi.prd_nm AS product_name,
    pi.cat_id AS category_id,
    pc.cat AS category,
    pc.subcat AS sub_category,
    pc.maintenance,
    pi.prd_cost AS cost,
    pi.prd_line AS product_line,
    pi.prd_start_dt AS start_date
FROM silver.crm_prd_info AS pi
LEFT JOIN silver.erp_px_cat_g1v2 AS pc
    ON pi.cat_id = pc.id;


-- ============================================
-- gold.fact_sales
-- ============================================
CREATE OR REPLACE VIEW gold.fact_sales AS 
SELECT 
    sd.sls_ord_num AS order_number,
    dp.product_key,
    dc.customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS shipping_date,
    sd.sls_due_dt AS due_date,
    sd.sls_sales AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price AS price
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_products AS dp
    ON sd.sls_prd_key = dp.product_number
LEFT JOIN gold.dim_customers AS dc
    ON sd.sls_cust_id = dc.customer_id;

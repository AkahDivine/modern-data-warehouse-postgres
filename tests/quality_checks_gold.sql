/*
=========================================================================
Quality Checks: Gold Layer
=========================================================================

Purpose:
This script performs quality checks to validate the integrity, consistency,
and accuracy of the Gold Layer. These checks ensure:
- Uniqueness of surrogate keys in dimension tables.
- Referential integrity between fact and dimension tables.
- Validation of relationships in the data model for analytical purposes.

Usage Notes:
- Run these checks after loading data from the Silver Layer.
- Investigate and resolve any discrepancies found during the checks.
- Expected results: Empty sets indicate no issues.

=========================================================================
*/

-- ============================================
-- Checking gold.dim_customers
-- ============================================

/* 1. Check for consistency of gender information
   - Compares CRM master gender with ERP data.
   - Expectation: Distinct records show any mismatches in gender assignment.
*/
SELECT DISTINCT
    ci.cst_gndr,
    ca.gen,
    CASE
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr  -- CRM is the master for gender info
        ELSE COALESCE(ca.gen, 'n/a')
    END AS new_gender
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS la
    ON ci.cst_key = la.cid;

-- ============================================
-- Referential Integrity Checks
-- ============================================

/* 2. Check for missing customers in fact_sales
   - Ensures all customer_keys in fact_sales exist in dim_customers.
   - Expectation: No results (all customer keys must match)
*/
SELECT *
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_customers AS c
    ON f.customer_key = c.customer_key
WHERE c.customer_key IS NULL;

-- 3. Check for missing products in fact_sales
   - Ensures all product_keys in fact_sales exist in dim_products.
   - Expectation: No results (all product keys must match)
SELECT *
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_products AS p
    ON f.product_key = p.product_key
WHERE p.product_key IS NULL;

-- ============================================
-- Checking gold.dim_products
-- ============================================

/* 4. Validate product dimension consistency
   - Combines CRM product info with ERP category info.
   - Filters out historical products (prd_end_dt IS NULL means active products).
   - Expectation: All active products have valid category data.
*/
SELECT
    pi.prd_id,
    pi.cat_id,
    pi.prd_key,
    pi.prd_nm,
    pi.prd_cost,
    pi.prd_line,
    pi.prd_start_dt,
    pc.cat,
    pc.subcat,
    pc.maintenance
FROM silver.crm_prd_info AS pi
LEFT JOIN silver.erp_px_cat_g1v2 AS pc
    ON pi.cat_id = pc.id
WHERE pi.prd_end_dt IS NULL;

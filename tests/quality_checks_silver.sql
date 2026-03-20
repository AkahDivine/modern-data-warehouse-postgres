/*
================================================================================
Quality Checks: Silver Layer Data Validation
================================================================================

Purpose:
This script performs a series of data quality checks on the 'silver' schema
to ensure data consistency, accuracy, and standardization after transformation
from the Bronze layer.

The checks include:
- Detection of null or duplicate primary/unique identifiers.
- Identification of unwanted leading/trailing spaces in string fields.
- Validation of numeric fields (e.g., negative or null values).
- Verification of date integrity and logical date ranges.
- Cross-field consistency checks (e.g., sales = quantity * price).
- Standardization checks for categorical fields.

Usage Notes:
- Run this script after loading the Silver layer.
- Each query is independent and separated by semicolons (;).
- Investigate and resolve any anomalies returned by these queries.
================================================================================
*/

-- ============================================================
-- Check for duplicate or NULL product IDs
-- ============================================================
SELECT
	prd_id,
	COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- ============================================================
-- Check for unwanted spaces in product names
-- ============================================================
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- ============================================================
-- Validate product cost (should not be NULL or negative)
-- ============================================================
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- ============================================================
-- Review distinct product line values for standardization
-- ============================================================
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

-- ============================================================
-- Check for invalid date ranges (end date before start date)
-- ============================================================
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- ============================================================
-- Check for NULL or improperly formatted order numbers
-- ============================================================
SELECT sls_ord_num
FROM silver.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num) OR sls_ord_num IS NULL;

-- ============================================================
-- Check for NULL or improperly formatted product keys
-- ============================================================
SELECT sls_prd_key
FROM silver.crm_sales_details
WHERE sls_prd_key != TRIM(sls_prd_key) OR sls_prd_key IS NULL;

-- ============================================================
-- Validate due dates (invalid format or unrealistic range)
-- ============================================================
SELECT sls_due_dt
FROM silver.crm_sales_details
WHERE sls_due_dt IS NULL
   OR sls_due_dt > '2050-01-01'
   OR sls_due_dt < '1990-01-01';

-- ============================================================
-- Validate sales consistency (sales = quantity * price)
-- ============================================================
SELECT
	sls_sales AS old_sales,
	sls_quantity,
	sls_price
FROM silver.crm_sales_details
WHERE 
	sls_sales != sls_quantity * sls_price
	OR sls_sales IS NULL
	OR sls_quantity IS NULL
	OR sls_price IS NULL
	OR sls_sales <= 0
	OR sls_quantity <= 0
	OR sls_price <= 0
ORDER BY sls_sales;

-- ============================================================
-- Review distinct gender values for standardization
-- ============================================================
SELECT DISTINCT gen
FROM silver.erp_cust_az12;

-- ============================================================
-- Validate birth dates (future dates or unrealistic past dates)
-- ============================================================
SELECT DISTINCT bdate
FROM silver.erp_cust_az12
WHERE bdate < '1994-01-01' OR bdate >= NOW();

-- ============================================================
-- Review country values and transformation logic
-- ============================================================
SELECT DISTINCT
	cntry,
	CASE 
		WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		ELSE TRIM(cntry)
	END AS standardized_cntry
FROM silver.erp_loc_a101;

-- ============================================================
-- Review maintenance values for consistency
-- ============================================================
SELECT DISTINCT maintenance
FROM silver.erp_px_cat_g1v2;

-- ============================================================
-- Check for duplicate product category IDs
-- ============================================================
SELECT
	id,
	COUNT(*)
FROM silver.erp_px_cat_g1v2
GROUP BY id
HAVING COUNT(*) > 1;

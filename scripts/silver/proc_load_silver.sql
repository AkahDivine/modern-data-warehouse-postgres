/*
================================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
================================================================================

Purpose:
This PostgreSQL stored procedure performs the ETL process to populate the 
'silver' schema tables from the 'bronze' schema. 

The Silver layer represents cleaned, transformed, and business-ready data.
This procedure:
- Truncates Silver tables to allow full reload.
- Inserts transformed and cleansed data from Bronze tables.
- Generates derived columns (e.g., cat_id in crm_prd_info) for business use.
- Tracks row counts and load duration for each table.
- Logs overall batch execution time.
- Handles and reports errors.

Important Notes:
- The `cat_id` column in crm_prd_info is **new** in Silver and does not exist 
  in Bronze. It is derived from prd_key for business categorization.
- All tables include a `dwh_create_date` timestamp to track row insertion.

Parameters:
None.

Execution:
CALL silver.load_silver();
================================================================================
*/

CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
	v_rows INTEGER;
	start_time TIMESTAMP;
	end_time TIMESTAMP;
	batch_start_time TIMESTAMP;
	batch_end_time TIMESTAMP;
BEGIN
	batch_start_time := NOW();

	RAISE NOTICE '=============================';
	RAISE NOTICE 'Loading Silver Layer';
	RAISE NOTICE '=============================';

	-- =========================
	-- Load CRM Tables
	-- =========================
	start_time := NOW();
    RAISE NOTICE '>> Truncating Table: silver.crm_cust_info';
    TRUNCATE TABLE silver.crm_cust_info;
    RAISE NOTICE '>> Inserting Data Into: silver.crm_cust_info';
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
        TRIM(cst_firstname),
        TRIM(cst_lastname),
        CASE
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END AS cst_marital_status,
        CASE
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END AS cst_gndr,
        cst_create_date
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY cst_id
                ORDER BY cst_create_date DESC
            ) AS flag_last
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) AS cst_id_check
    WHERE flag_last = 1;
	GET DIAGNOSTICS v_rows = ROW_COUNT;
	RAISE NOTICE '>> silver.crm_cust_info Table loaded: % rows', v_rows;
	end_time := NOW();
	RAISE NOTICE '>> silver.crm_cust_info Table load Duration: % seconds',
		 EXTRACT(EPOCH FROM (end_time - start_time));

	-- =========================
	-- Load CRM Product Table
	-- =========================
	start_time := NOW();
	RAISE NOTICE '>> Truncating Table: silver.crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info;
    RAISE NOTICE '>> Inserting Data Into: silver.crm_prd_info';
	INSERT INTO silver.crm_prd_info (
		prd_id,
		cat_id,          -- New column in Silver, not in Bronze
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt)
	SELECT
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
		SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
		prd_nm,
		COALESCE(prd_cost, 0) AS prd_cost,
		CASE UPPER(TRIM(prd_line))
			WHEN 'M' THEN 'Mountain'
			WHEN 'S' THEN 'Other sales'
			WHEN 'R' THEN 'Road'
			WHEN 'T' THEN 'Touring'
			ELSE 'n/a'
		END AS prd_line,
		CAST(prd_start_dt AS DATE),
		CAST(LEAD(prd_start_dt) OVER (
			PARTITION BY prd_key ORDER BY prd_start_dt ASC) - INTERVAL '1 DAY' AS DATE) AS prd_end_dt
	FROM bronze.crm_prd_info;
	GET DIAGNOSTICS v_rows = ROW_COUNT;
	RAISE NOTICE '>> silver.crm_prd_info Table loaded: % rows', v_rows;
	end_time := NOW();
	RAISE NOTICE '>> silver.crm_prd_info Table load Duration: % seconds',
		 EXTRACT(EPOCH FROM (end_time - start_time));

	-- =========================
	-- Load CRM Sales Details
	-- =========================
	start_time := NOW();
	RAISE NOTICE '>> Truncating Table: silver.crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details;
    RAISE NOTICE '>> Inserting Data Into: silver.crm_sales_details';
	INSERT INTO silver.crm_sales_details (
	    sls_ord_num,
	    sls_prd_key,
	    sls_cust_id,
	    sls_order_dt,
	    sls_ship_dt,
	    sls_due_dt,
	    sls_sales,
	    sls_quantity,
	    sls_price)
	SELECT
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE
			WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
			ELSE CAST(sls_order_dt::TEXT AS DATE) 
		END AS sls_order_dt,
		CASE
			WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
			ELSE CAST(sls_ship_dt::TEXT AS DATE) 
		END AS sls_ship_dt,
		CASE
			WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL
			ELSE CAST(sls_due_dt::TEXT AS DATE) 
		END AS sls_due_dt,
		CASE
			WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
				THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE
			WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / COALESCE(sls_quantity,0)
			ELSE sls_price
		END AS sls_price
	FROM bronze.crm_sales_details;
	GET DIAGNOSTICS v_rows = ROW_COUNT;
	RAISE NOTICE '>> silver.crm_sales_details Table loaded: % rows', v_rows;
	end_time := NOW();
	RAISE NOTICE '>> silver.crm_sales_details Table load Duration: % seconds',
		 EXTRACT(EPOCH FROM (end_time - start_time));

	-- =========================
	-- Load ERP Tables
	-- =========================
	start_time := NOW();
	RAISE NOTICE '>> Truncating Table: silver.erp_cust_az12';
    TRUNCATE TABLE silver.erp_cust_az12;
    RAISE NOTICE '>> Inserting Data Into: silver.erp_cust_az12';
	INSERT INTO silver.erp_cust_az12 (
	    cid,
	    bdate,
	    gen)
	SELECT 
		CASE
			WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
			ELSE cid
		END AS cid,
		CASE 
			WHEN bdate > NOW() THEN NULL
			ELSE bdate
		END AS bdate,
		CASE
			WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
			WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
			ELSE 'n/a'
		END AS gen
	FROM bronze.erp_cust_az12;
	GET DIAGNOSTICS v_rows = ROW_COUNT;
	RAISE NOTICE '>> silver.erp_cust_az12 Table loaded: % rows', v_rows;
	end_time := NOW();
	RAISE NOTICE '>> silver.erp_cust_az12 Table load Duration: % seconds',
		 EXTRACT(EPOCH FROM (end_time - start_time));

	start_time := NOW();
	RAISE NOTICE '>> Truncating Table: silver.erp_loc_a101';
    TRUNCATE TABLE silver.erp_loc_a101;
    RAISE NOTICE '>> Inserting Data Into: silver.erp_loc_a101';
	INSERT INTO silver.erp_loc_a101 (
	    cid,
	    cntry)
	SELECT
		REPLACE(cid, '-', '') AS cid,
		CASE 
			WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry) IN ('US', 'USA' ) THEN 'United States'
			WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
			ELSE TRIM(cntry)
		END cntry
	FROM bronze.erp_loc_a101;
	GET DIAGNOSTICS v_rows = ROW_COUNT;
	RAISE NOTICE '>> silver.erp_loc_a101 Table loaded: % rows', v_rows;
	end_time := NOW();
	RAISE NOTICE '>> silver.erp_loc_a101 Table load Duration: % seconds',
		 EXTRACT(EPOCH FROM (end_time - start_time));

	start_time := NOW();
	RAISE NOTICE '>> Truncating Table: silver.erp_px_cat_g1v2';
    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    RAISE NOTICE '>> Inserting Data Into: silver.erp_px_cat_g1v2';
	INSERT INTO silver.erp_px_cat_g1v2 (
	    id,
	    cat,
	    subcat,
	    maintenance)
	SELECT
		id,
		cat,
		subcat,
		maintenance
	FROM bronze.erp_px_cat_g1v2;
	GET DIAGNOSTICS v_rows = ROW_COUNT;
	RAISE NOTICE '>> silver.erp_px_cat_g1v2 Table loaded: % rows', v_rows;
	end_time := NOW();
	RAISE NOTICE '>> silver.erp_px_cat_g1v2 Table load Duration: % seconds',
		 EXTRACT(EPOCH FROM (end_time - start_time));

	batch_end_time := NOW();
	RAISE NOTICE '------------------------------------------------';
	RAISE NOTICE '>> Loading Silver Layer Is Completed';
	RAISE NOTICE '>> Silver Layer load Duration: % seconds',
		 EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));
	RAISE NOTICE '------------------------------------------------';

	-- =========================
	-- Error Handling
	-- =========================
	EXCEPTION 
	WHEN others THEN
		RAISE NOTICE '============================================';
        RAISE NOTICE 'ERROR OCCURRED DURING LOADING SILVER LAYER';
        RAISE NOTICE 'Error Message: %', SQLERRM;
        RAISE NOTICE 'Error Code (SQLSTATE): %', SQLSTATE;
        RAISE NOTICE '============================================';

END;
$$;

/*
================================================================================
Stored Procedure: bronze.load_bronze
================================================================================

Purpose:
This PostgreSQL stored procedure loads and refreshes all tables in the
'bronze' schema from external CSV data files.

The bronze layer represents the raw ingestion layer of the data warehouse.
This procedure implements a full reload strategy by:

- Truncating each bronze table before loading to ensure clean data refresh.
- Importing data from CSV files using PostgreSQL's COPY command.
- Tracking row counts for each table.
- Measuring load duration per table and for the entire batch.
- Handling errors using an exception block and reporting diagnostic details.

Important:
This procedure requires the corresponding CSV data files to execute successfully.
The required files are included in this repository under the designated data
folder.

Before running this procedure:
- Ensure the CSV files are available on the local system.
- Replace file paths with your local environment paths.
- Ensure PostgreSQL has permission to read the files.

Parameters:
None.

Returns:
This procedure does not return any value.

Execution:
CALL bronze.load_bronze();

================================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze ()
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

	RAISE NOTICE 'Loading Bronze Layer';

	-- ============================================================
	-- CRM TABLES
	-- ============================================================

	start_time := NOW();

	TRUNCATE TABLE bronze.crm_cust_info;

	COPY bronze.crm_cust_info
	FROM 'your_file_path_here'
	WITH (
	    FORMAT csv,
	    HEADER true,
	    DELIMITER ','
	);

	GET DIAGNOSTICS v_rows = ROW_COUNT;

	end_time := NOW();

	RAISE NOTICE 'crm_cust_info loaded: % rows', v_rows;
	RAISE NOTICE 'Load Duration: % seconds',
		EXTRACT(EPOCH FROM (end_time - start_time));

	-- ============================================================

	start_time := NOW();

	TRUNCATE TABLE bronze.crm_prd_info;

	COPY bronze.crm_prd_info
	FROM 'your_file_path_here'
	WITH (
		FORMAT csv,
		HEADER true,
		DELIMITER ','
	);

	GET DIAGNOSTICS v_rows = ROW_COUNT;

	end_time := NOW();

	RAISE NOTICE 'crm_prd_info loaded: % rows', v_rows;
	RAISE NOTICE 'Load Duration: % seconds',
		EXTRACT(EPOCH FROM (end_time - start_time));

	-- ============================================================

	start_time := NOW();

	TRUNCATE TABLE bronze.crm_sales_details;

	COPY bronze.crm_sales_details
	FROM 'your_file_path_here'
	WITH (
		FORMAT csv,
		HEADER true,
		DELIMITER ','
	);

	GET DIAGNOSTICS v_rows = ROW_COUNT;

	end_time := NOW();

	RAISE NOTICE 'crm_sales_details loaded: % rows', v_rows;
	RAISE NOTICE 'Load Duration: % seconds',
		EXTRACT(EPOCH FROM (end_time - start_time));

	-- ============================================================
	-- ERP TABLES
	-- ============================================================

	start_time := NOW();

	TRUNCATE TABLE bronze.erp_cust_az12;

	COPY bronze.erp_cust_az12
	FROM 'your_file_path_here'
	WITH (
		FORMAT csv,
		HEADER true,
		DELIMITER ','
	);

	GET DIAGNOSTICS v_rows = ROW_COUNT;

	end_time := NOW();

	RAISE NOTICE 'erp_cust_az12 loaded: % rows', v_rows;
	RAISE NOTICE 'Load Duration: % seconds',
		EXTRACT(EPOCH FROM (end_time - start_time));

	-- ============================================================

	start_time := NOW();

	TRUNCATE TABLE bronze.erp_loc_a101;

	COPY bronze.erp_loc_a101
	FROM 'your_file_path_here'
	WITH (
		FORMAT csv,
		HEADER true,
		DELIMITER ','
	);

	GET DIAGNOSTICS v_rows = ROW_COUNT;

	end_time := NOW();

	RAISE NOTICE 'erp_loc_a101 loaded: % rows', v_rows;
	RAISE NOTICE 'Load Duration: % seconds',
		EXTRACT(EPOCH FROM (end_time - start_time));

	-- ============================================================

	start_time := NOW();

	TRUNCATE TABLE bronze.erp_px_cat_g1v2;

	COPY bronze.erp_px_cat_g1v2
	FROM 'your_file_path_here'
	WITH (
		FORMAT csv,
		HEADER true,
		DELIMITER ','
	);

	GET DIAGNOSTICS v_rows = ROW_COUNT;

	end_time := NOW();

	RAISE NOTICE 'erp_px_cat_g1v2 loaded: % rows', v_rows;
	RAISE NOTICE 'Load Duration: % seconds',
		EXTRACT(EPOCH FROM (end_time - start_time));

	-- ============================================================

	batch_end_time := NOW();

	RAISE NOTICE 'Bronze Layer Load Completed';
	RAISE NOTICE 'Total Batch Duration: % seconds',
		EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));

EXCEPTION 
	WHEN others THEN
        RAISE NOTICE 'ERROR OCCURRED DURING BRONZE LOAD';
        RAISE NOTICE 'Message: %', SQLERRM;
        RAISE NOTICE 'SQLSTATE: %', SQLSTATE;
END;
$$;

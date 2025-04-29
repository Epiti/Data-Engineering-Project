
/*
**********************************************************************************************************
                                         LOADING PROCEDURE
**********************************************************************************************************
This script loads Data from 'bronze' layer to 'Silver' Layer
It first Truncate the tables and then insert transformed and clean data into silver tables.
How to use it ?: EXEC load_data_from_bronze_layer_to_silver_layer
*/



CREATE OR ALTER PROCEDURE load_data_from_bronze_layer_to_silver_layer
AS
BEGIN

DECLARE @start_time Datetime, @whole_time_start Datetime, @whole_time_end Datetime, @end_time Datetime;


BEGIN TRY

Set @whole_time_start = GETDATE();


	print('=================================================================================');
	print('                    LOADING DATA INTO SILVER LAYER...                            ');
	print('=================================================================================');



	print('*************************************************************************************');
	print('                              Loading CRM Tables                                     ');
	print('*************************************************************************************');


	Set @start_time = GETDATE();
		print('....Truncating Table silver.crm_cust_info');
		TRUNCATE TABLE silver.crm_cust_info;
		print('....Inserting Data into silver.crm_cust_info')
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
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		ELSE 'n/a'
		END cst_marital_status,
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		ELSE 'n/a'
		END cst_gndr,
		cst_create_date

		FROM
		(
		SELECT *,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS most_recent
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL
		)r WHERE most_recent = 1;

	Set @end_time = GETDATE();
		print('Time took to insert data into silver.crm_cust_info :' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds');
		print('--------------------------------------------------');



	Set @start_time = GETDATE();
		print('....Truncating Table silver.crm_prd_info');
		TRUNCATE TABLE silver.crm_prd_info;
		print('....Inserting Data into silver.crm_prd_info')
		INSERT INTO silver.crm_prd_info( 
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
		)

		SELECT
		prd_id,
		REPLACE(SUBSTRING (prd_key,1,5), '-','_') AS cat_id,
		SUBSTRING (prd_key, 7, LEN(prd_key) ) AS prd_key,
		prd_nm,
		ISNULL(prd_cost, 0) AS prd_cost,
		CASE UPPER(TRIM(prd_line))
			WHEN 'M' THEN  'Mountain'
			WHEN 'T' THEN 'Touring'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other Sales'
			ELSE 'n/a'
		END AS prd_line,
		CAST(prd_start_dt AS DATE ) AS prd_start_dt,
		CAST(DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS DATE) AS prd_end_dt
		FROM bronze.crm_prd_info;

	Set @end_time = GETDATE();
		print('Time took to insert data into silver.crm_prd_info :' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds');
		print('--------------------------------------------------');




	Set @start_time = GETDATE();
		print('....Truncating silver.crm_sales_details');
		TRUNCATE TABLE silver.crm_sales_details;
		print('....Inserting Data into silver.crm_sales_details')
		INSERT INTO silver.crm_sales_details(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
		)

		SELECT 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE 
			WHEN sls_order_dt = 0 OR LEN(sls_order_dt) ! = 8 THEN NULL
			ELSE CAST( CAST (sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt,

		CASE 
			WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) ! = 8 THEN NULL
			ELSE CAST( CAST (sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,

		CASE 
			WHEN sls_due_dt = 0 OR LEN(sls_due_dt) ! = 8 THEN NULL
			ELSE CAST( CAST (sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt,

		CASE
			WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE 
			WHEN sls_price IS NULL OR sls_price <= 0
				THEN sls_sales / NULLIF(sls_quantity,0)
			ELSE sls_price
		END AS sls_price

		from bronze.crm_sales_details;

	Set @end_time = GETDATE();
		print('Time took to insert data into silver.crm_sales_details :' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds');
		print('--------------------------------------------------');



	print('*************************************************************************************');
	print('                               Loading ERP Tables                                    ');
	print('*************************************************************************************');


	--- silver.erp_custaz12 loading
	set @end_time = GETDATE()
		print('....Truncating silver.erp_custaz12')
		TRUNCATE TABLE silver.erp_custaz12;
		print('....Inserting Data into silver.erp_custaz12')
		INSERT INTO silver.erp_custaz12 (
		cid,
		bdate,
		gen

		)

		SELECT
		CASE 
			WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
			ELSE cid
		END AS cid,

		CASE 
			WHEN bdate > GETDATE() THEN NULL
			ELSE bdate
		END AS bdate,

		CASE 
			WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
			WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
			ELSE 'n/a'
		END AS gen

		FROM bronze.erp_custaz12;

	set @end_time = GETDATE()
		print('Time took to insert data into silver.erp_custaz12 :' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds');
		print('--------------------------------------------------');



	----silver.erp_loc_a101 loading
	set @end_time = GETDATE()
		print('....Truncating silver.erp_loc_a101')
		TRUNCATE TABLE silver.erp_loc_a101;
		print('....Inserting Data into silver.erp_loc_a101')
		INSERT INTO silver.erp_loc_a101 (
		cid,
		cntry
		)

		SELECT
		REPLACE(cid, '-', '') cid,
		  CASE 
			WHEN UPPER(TRIM(cntry)) IN ('USA','UNITED STATES', 'US') THEN 'United States'
			WHEN UPPER(TRIM(cntry)) IN ('GERMANY','DE') THEN 'Germany'
			WHEN TRIM(cntry) = '' or cntry is null THEN 'n/a'
			ELSE TRIM(cntry)
		END AS cntry
		FROM bronze.erp_loc_a101;

	set @end_time = GETDATE()
		print('Time took to insert data into silver.erp_loc_a101 :' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds');
		print('--------------------------------------------------');




	--silver.erp_px_cat_g1v12 loadinig
	set @start_time = GETDATE()
		print('....Truncating silver.erp_px_cat_g1v12')
		TRUNCATE TABLE silver.erp_px_cat_g1v12;
		print('....Inserting Data into silver.erp_px_cat_g1v12')
		INSERT INTO silver.erp_px_cat_g1v12(
		id,
		sub,
		cat,
		maintenance
		)
		select
		id,
		TRIM(sub),
		TRIM(cat),
		maintenance
		from bronze.erp_px_cat_g1v12

	set @end_time = GETDATE()
		print('Time took to insert data into silver.erp_px_cat_g1v12 :' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds');
		print('--------------------------------------------------');



Set @whole_time_end = GETDATE();

		print('===============================================================================');
		print('                              	LOADING COMPLETED                             ');
		print('Time took to insert all tables :' + CAST(DATEDIFF(second, @whole_time_start, @whole_time_end) AS NVARCHAR) + ' Seconds');
		print('-----------------------------------------------------');
		print('===============================================================================');

END TRY


		BEGIN CATCH

				print('===============================================================================');
				print('      	ERRORS OCCURED DURING LOADING DATA INTO SILVER LAYER                  ');
				print('===============================================================================');
				print('ERROR MESSAGE :' + ERROR_MESSAGE());
				print('ERROR NUMBER :' + CAST(ERROR_NUMBER()AS VARCHAR ));

		END CATCH



END

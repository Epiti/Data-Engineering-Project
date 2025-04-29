/*
**********************************************************************************************************
                                         LOADING PROCEDURE
**********************************************************************************************************
This script loads CSV files into 'bronze' schema.
It first Truncate the table and then insert data into tables.

How to use it ?: EXEC load_data_into_bronze_layer

*/





CREATE OR ALTER PROCEDURE load_data_into_bronze_layer
AS
BEGIN

DECLARE @start_time Datetime, @whole_time_start Datetime, @whole_time_end Datetime, @end_time Datetime;

	BEGIN TRY

	Set @whole_time_start = GETDATE();

	print('=================================================================================');
	print('                    LOADING DATA INTO BRONZE LAYER...                            ');
	print('=================================================================================');




	print('*************************************************************************************');
	print('                              Loading CRM Tables                                     ');
	print('*************************************************************************************');
		
Set @start_time = GETDATE();
		print('....Truncating Table bronze.crm_cust_info');
		TRUNCATE TABLE bronze.crm_cust_info;

		print('....Inserting Data into Table bronze.crm_cust_info')
		BULK INSERT bronze.crm_cust_info
			FROM 'C:\Users\godwin\Documents\data\source_crm\cust_info.csv'
			WITH (
			FORMAT = 'CSV',
			FIELDTERMINATOR  =',',
			FIRSTROW = 2,
			TABLOCK
		);

Set @end_time = GETDATE();
print('Time took to load bronze.crm_cust_info :' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds');
print('--------------------------------------------------');



Set @start_time = GETDATE();
		print('....Truncating Table bronze.crm_prd_info');
		TRUNCATE TABLE bronze.crm_prd_info;

		print('....Inserting Data into Table bronze.crm_prd_info')
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\godwin\Documents\data\source_crm\prd_info.csv'
		WITH (
			FORMAT = 'CSV',
			FIELDTERMINATOR = ',',
			FIRSTROW = 2,
			TABLOCK
		);

Set @end_time = GETDATE();
print('Time took to load bronze.crm_prd_info :' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds');
print('--------------------------------------------------');




Set @start_time =GETDATE();
		print('....Truncating Table bronze.crm_sales_detail');
		TRUNCATE TABLE bronze.crm_sales_details

		print('....Inserting Data into Table bronze.crm_sales_details')
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\godwin\Documents\data\source_crm\sales_details.csv'
		WITH(
		FORMAT = 'CSV',
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);

Set @end_time = GETDATE();
print('Time took to load bronze.crm_sales_detail :' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds');
print('--------------------------------------------------')



	print('*************************************************************************************');
	print('                               Loading ERP Tables                                    ');
	print('*************************************************************************************');


Set @start_time = GETDATE()
		print('....Truncating Table bronze.erp_custaz12');
		TRUNCATE TABLE bronze.erp_custaz12;

		print('....Inserting Data into Table bronze.erp_custaz12')
		BULK INSERT bronze.erp_custaz12
		FROM 'C:\Users\godwin\Documents\data\source_erp\CUST_AZ12.csv'
		WITH (
		FORMAT = 'CSV',
		FIELDTERMINATOR = ',',
		FIRSTROW = 2,
		TABLOCK
		);

Set @end_time = GETDATE();
print('Time took to load bronze.erp_custaz12 :' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds');
print('--------------------------------------------------');




Set @start_time = GETDATE();
		print('....Truncating Table bronze.erp_loc_a101');
		TRUNCATE TABLE bronze.erp_loc_a101;

		print('....Inserting Data into bronze.erp_loc_a101')
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\godwin\Documents\data\source_erp\LOC_A101.csv'
		WITH (
		FORMAT = 'CSV',
		FIELDTERMINATOR = ',',
		FIRSTROW = 2,
		TABLOCK
		);

Set @end_time = GETDATE();
print('Time took to load bronze.erp_loc_a101 :' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds');
print('--------------------------------------------------');



Set @start_time = GETDATE();
		print('....Truncating Table bronze.erp_px_cat_g1v12');
		TRUNCATE TABLE bronze.erp_px_cat_g1v12;

		print('....Inserting Data into Table bronze.erp_px_cat_g1v12')
		BULK INSERT bronze.erp_px_cat_g1v12
		FROM 'C:\Users\godwin\Documents\data\source_erp\PX_CAT_G1V2.csv'
		WITH (
		FORMAT = 'CSV',
		FIELDTERMINATOR = ',',
		FIRSTROW = 2,
		TABLOCK
		);

Set @end_time = GETDATE();
print('Time took to load bronze.erp_px_cat_g1v12 :' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' Seconds');
print('--------------------------------------------------');


Set @whole_time_end = GETDATE();
print('===============================================================================');
print('                              	LOADING COMPLETED                             ');
print('Time took to load the entire batch :' + CAST(DATEDIFF(second, @whole_time_start, @whole_time_end) AS NVARCHAR) + ' Seconds');
print('-----------------------------------------------------');
print('===============================================================================');


	END TRY

				BEGIN CATCH

				print('===============================================================================');
				print('      	ERRORS OCCURED DURING LOADING DATA INTO BRONZE LAYERS                 ');
				print('===============================================================================');
				print('ERROR MESSAGE :' + ERROR_MESSAGE());
				print('ERROR NUMBER :' + CAST(ERROR_NUMBER()AS VARCHAR ));

				END CATCH


END


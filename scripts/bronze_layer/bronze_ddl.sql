/* 
***********************************************************************************
                                DDL SCRIPT
***********************************************************************************
                 This script creates Tables in 'bronze' Schema.
If the table exists and is not null, drop it and create a new one.
WARNING: THIS SCRIPT DROPS THE EXISTING TABLE AND CREATES A NEW ONE.
*/



IF OBJECT_ID('bronze.crm_cust_info','U') IS NOT NULL DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
	cst_id INT,
	cst_key varchar(50),
	cst_firstname varchar(50),
	cst_lastname varchar(50),
	cst_marital_status varchar(25),
	cst_gndr varchar (25),
	cst_create_date date

);


IF OBJECT_ID('bronze.crm_prd_info','U') IS NOT NULL DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
	prd_id INT,
	prd_key varchar(50),
	prd_nm varchar(50),
	prd_cost INT,
	prd_line varchar(50),
	prd_start_dt date,
	prd_end_dt date
);

IF OBJECT_ID('bronze.crm_sales_details','U') IS NOT NULL DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details(
	sls_ord_num varchar(50),
	sls_prd_key varchar(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT

);



IF OBJECT_ID('bronze.erp_custaz12','U') IS NOT NULL DROP TABLE bronze.erp_custaz12;
CREATE TABLE bronze.erp_custaz12(
	cid varchar(50),
	bdate date,
	gen varchar(15)
);


IF OBJECT_ID('bronze.erp_loc_a101','U') IS NOT NULL DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101(
	cid varchar(50),
	cntry varchar(50)
);

IF OBJECT_ID('bronze.erp_px_cat_g1v12','U') IS NOT NULL DROP TABLE bronze.erp_px_cat_g1v12;
CREATE TABLE bronze.erp_px_cat_g1v12(
	id varchar(50),
	cat varchar(50),
	sub varchar (50),
	maintenance varchar(50)

)

/*   
   THIS SCRIPT PERFORM VARIOUS QUALITY CHECK OF DATA.
   PROCESS OF CHECKING ERRORS IN BRONZE LAYER BEFORE LOAD CLEANED DATA INTO SILVER LAYER. 
*/

--FOR bronze.crm_cust_info TO silver.crm_cust_info
--Check for Nulls or Duplicate values
--Expectation: No result

SELECT cst_id, 
count(*)
FROM bronze.crm_cust_info 
GROUP BY cst_id 
HAVING count(*) > 1 OR cst_id IS NULL


 --Unwanted Spaces
 --Expectation : No results

 SELECT cst_firstname
 FROM bronze.crm_cust_info
 WHERE cst_firstname ! = TRIM(cst_firstname)


 --Data Standarization and Consistency
 SELECT DISTINCT cst_gndr
 FROM bronze.crm_cust_info

 SELECT DISTINCT cst_marital_status
 FROM bronze.crm_cust_info


 ------silver.crm_cust_info AFTER TRANSFORMATION------

 SELECT TOP 100 *
FROM silver.crm_cust_info


SELECT cst_id,
count(*)
FROM silver.crm_cust_info 
GROUP BY cst_id 
HAVING count(*) > 1 OR cst_id IS NULL

 SELECT cst_firstname
 FROM silver.crm_cust_info
 WHERE cst_firstname ! = TRIM(cst_firstname)


 SELECT DISTINCT cst_gndr
 FROM silver.crm_cust_info


 SELECT DISTINCT cst_marital_status
 FROM silver.crm_cust_info

 -------------------------------------------------------------------------------
 --FOR bronze.crm_prd_info TO silver.crm_prd_info
 --Cleaning
-- Check for Duplicate and null values
--Expectation: No result


select prd_id, COUNT(*)
FROM bronze.crm_prd_info 
GROUP BY prd_id
HAVING count(*) > 1 OR prd_id IS NULL


 --Product price null or less than zero
 --Expectation : No results

 SELECT prd_cost
FROM bronze.crm_prd_info 
WHERE prd_cost < 0 OR prd_cost IS NULL

--Distinct full product line
 --Expectation : No abreviation or Null
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info 

----Check start Date less than end date
--Expectation : No results
select *
from bronze.crm_prd_info
where prd_start_dt > prd_end_dt

---Check for unwanted space in product name
----- Expectation : No results

select prd_nm 
from bronze.crm_prd_info
where prd_nm ! = TRIM(prd_nm)

------silver.crm_prd_info  AFTER TRANSFORMATION------

select *
from silver.crm_prd_info
where prd_start_dt > prd_end_dt


SELECT prd_cost
FROM silver.crm_prd_info 
WHERE prd_cost < 0 OR prd_cost IS NULL

SELECT DISTINCT prd_line
FROM silver.crm_prd_info 

--------------------------------------------------------------------------------
--Check for Silver.crm sales details

select sls_ord_num,
count(*)
from bronze.crm_sales_details
group by sls_ord_num
having count(*)  > 1 or sls_ord_num is null


select sls_prd_key
from bronze.crm_sales_details
where sls_prd_key ! = trim(sls_prd_key)

select *
from bronze.crm_sales_details
where sls_prd_key not in (select prd_key from silver.crm_prd_info)


select *
from bronze.crm_sales_details
where sls_prd_key not in (select prd_key from silver.crm_prd_info) -- all prd sls from crm.sales datails can be connect to prd_key (silver)


select *
from bronze.crm_sales_details
where sls_cust_id not in (select cst_id from silver.crm_cust_info)


---check for invalid dates
--expectation : No results

select sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <= 0

select sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <= 0

select nullif(sls_order_dt, 0) sls_order_dt -- make it null if there is a zero
from bronze.crm_sales_details
where sls_order_dt <= 0 
or sls_order_dt > 20500101
or sls_order_dt < 19000101
or len(sls_order_dt) ! = 8


--Perfect for sls_ship_dt
select nullif(sls_ship_dt, 0) sls_ship_dt -- make it null if there is a zero
from bronze.crm_sales_details
where sls_ship_dt <= 0 
or sls_ship_dt > 20500101
or sls_ship_dt < 19000101
or len(sls_ship_dt) ! = 8

--check for invalid orders dates
select *
from bronze.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt --perfect


---Check sales integrety
select  sls_sales, sls_quantity, sls_price
from bronze.crm_sales_details
where sls_sales <= 0 or sls_sales is null
or sls_sales ! = (sls_quantity) * (sls_price)

select *
from bronze.crm_sales_details
where sls_quantity <= 0 or sls_quantity is null

select *
from bronze.crm_sales_details
where sls_price <= 0 or sls_price is null

---------------------------------------------------------------------------------
--Check errors for bronze.erp_custaz12

 select distinct gen 
 from bronze.erp_custaz12

 select distinct bdate
 from bronze.erp_custaz12
 where bdate = '1025-01-01' or bdate > GETDATE()

 select distinct gen 
 from silver.erp_custaz12

  select * 
 from silver.erp_custaz12

 select * from bronze.erp_custaz12
 --------------------------------------------------------------------------------------------------
 --Check errors for bronze.erp_loc_a101 :DONE

------Check errors fro bronze.px_cat

select sub
from bronze.erp_px_cat_g1v12
where sub ! = TRIM(sub)

select cat
from bronze.erp_px_cat_g1v12
where cat ! = TRIM(cat)

select distinct maintenance
from bronze.erp_px_cat_g1v12

----Seems OK

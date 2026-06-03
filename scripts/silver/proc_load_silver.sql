/*
==============================================================================
Stored Procedure:Load SILVER Layer (Bronze->SILVER)
==============================================================================
Script Purpose:
	This stored procedure performs the (ETL)process to populate the 'silver' schema tables from the 'bronze' schema
	It performs the following actions:
	- Truncate the silver tables before loading data.
  -Inserts transformed and cleaned data from the Bronze into silver

Parameters:
	None.
	This stored procedure does not accept ant parameters or retern any values.

Usage Example:
	EXEC silver.load_silver;
================================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN 
	DECLARE @start_time DATETIME,@end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME
	BEGIN TRY
	    PRINT '==============================================================';
		PRINT 'LOADING BRONZE LAYER';
		PRINT '==============================================================';
	
		PRINT '--------------------------------------------------------------';
		PRINT 'LOADING CRM TABLES';
		PRINT '--------------------------------------------------------------';
    
	    --- crm Customer Info insert
		SET @batch_start_time=GETDATE();
		SET @start_time=GETDATE();

		PRINT'>> TRUNCATING TABLE:silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT'>> INSERTING TABLE:silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info(cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_material_status,
		cst_gndr,
		cst_create_date
		)
		SELECT
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname ,
		TRIM(cst_lastname) AS cst_lastname,
		CASE WHEN UPPER(TRIM(cst_material_status))='S' THEN 'Single'
			 WHEN UPPER(TRIM(cst_material_status))='M' THEN 'Married'
			 ELSE 'n/a'
		END cst_material_status,
		CASE WHEN UPPER(TRIM(cst_gndr))='F' THEN 'Female'
			 WHEN UPPER(TRIM(cst_gndr))='M' THEN 'Male'
			 ELSE 'n/a'
		END cst_gndr,
		cst_create_date
		FROM
		(SELECT *,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL)t
		WHERE flag_last =1 

		SET @end_time=GETDATE();
		PRINT '>>LOAD DURATION :'+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+' seconds'
		PRINt '---------------------------------------->'

		--- crm product Info insert
		SET @start_time=GETDATE()
		PRINT'>> TRUNCATING TABLE:silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT'>> INSERTING TABLE:silver.crm_prd_info';
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
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id, --- Extract category ID
		SUBSTRING(prd_key,7,LEN(prd_key)) As prd_key,      --- Extract product key
		prd_nm,
		ISNULL(prd_cost,0) AS prd_cost,
		CASE UPPER(TRIM(prd_line))
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other Sales '
			WHEN 'T' THEN 'Touring'
			ELSE 'n/a'
		END prd_line,-- Map product line code to descriptive values
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(LEAD(prd_start_dt) OVER( PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt -- calculate end date as one day before the next start date (Its Data enrichment)
		FROM bronze.crm_prd_info
		SET @end_time=GETDATE()
		PRINT '>>LOAD DURATION :'+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+' seconds'
		PRINt '---------------------------------------->'

		--- crm sales details insert
		SET @start_time=GETDATE()
		PRINT '>>Truncating Table:bronze.crm_sales_details';
		PRINT'>> TRUNCATING TABLE:silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT'>> INSERTING TABLE:silver.crm_sales_details';
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
		CASE WHEN sls_order_dt =0 OR LEN(sls_order_dt)!=8 THEN NULL
			 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END sls_order_dt,
		CASE WHEN sls_ship_dt =0 OR LEN(sls_ship_dt)!=8 THEN NULL
			 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END sls_ship_dt,
		CASE WHEN sls_due_dt =0 OR LEN(sls_due_dt)!=8 THEN NULL
			 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END sls_due_dt,
		CASE WHEN sls_sales IS NULL OR sls_sales<=0 OR sls_sales!=sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price)
			 ELSE sls_sales
		END sls_sales,              --sales =quantity * price
		sls_quantity,
		CASE WHEN sls_price IS NULL OR sls_price <=0
				THEN sls_sales/NULLIF(sls_quantity,0)
			 ELSE sls_price
		END sls_price
		FROM bronze.crm_sales_details
		SET @end_time=GETDATE()
		PRINT '>>LOAD DURATION :'+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+' seconds'
        
		PRINT '--------------------------------------------------------------';
		PRINT 'LOADING ERP TABLES';
		PRINT '--------------------------------------------------------------';

		--- erp cust_az12
		SET @start_time=GETDATE()
		PRINT'>> TRUNCATING TABLE:silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT'>> INSERTING TABLE:silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12(
		cid,
		bdate,
		gen)
		SELECT 
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
			 ELSE cid
		END AS cid,
		CASE WHEN bdate > GETDATE() THEN NULL
			 ELSE bdate
		END AS bdate,
		CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
			 WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
			 ELSE 'n/a'
		END AS gen
		FROM bronze.erp_cust_az12
		SET @end_time=GETDATE()
		PRINT '>>LOAD DURATION :'+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+' seconds'
		PRINt '---------------------------------------->'

		--- erp loc_a101 insert
		SET @start_time=GETDATE()
		PRINT'>> TRUNCATING TABLE:silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT'>> INSERTING TABLE:silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101
		(cid,cntry)
		SELECT 
		REPLACE(cid,'-','') AS cid,
		CASE WHEN TRIM(cntry)='DE' THEN 'Germany'
			 WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
			 WHEN TRIM(cntry)='' OR cntry IS NULL THEN 'n/a'
			 ELSE TRIM(cntry)
		END AS cntry
		FROM bronze.erp_loc_a101
		SET @end_time=GETDATE()
		PRINT '>>LOAD DURATION :'+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+' seconds'
		PRINt '---------------------------------------->'

		--- erp px_cat_g1v2 insert
		SET @start_time=GETDATE()

		PRINT'>> TRUNCATING TABLE:silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT'>> INSERTING TABLE:silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2
		(
		id,
		cat,
		subcat,
		maintenance
		)
		select 
		id,
		cat,
		subcat,
		maintenance
		from bronze.erp_px_cat_g1v2
		SET @end_time=GETDATE()
		PRINT '>>LOAD DURATION :'+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+' seconds'
		PRINt '---------------------------------------->'
		SET @batch_end_time=GETDATE();
		PRINT '==============================================================';
		PRINT 'LOADING SILVER LAYER IS COMPLETED';
		PRINT ' - TOTAL LOAD DURATION :'+CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time) AS NVARCHAR)+ ' seconds';
		PRINT '==============================================================';
	END TRY
	BEGIN CATCH 
	PRINT '==============================================================';
	PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
	PRINT 'ERROR MESSAGE'+ ERROR_MESSAGE();
	PRINT 'ERROR MESSAGE'+ CAST(ERROR_NUMBER() AS NVARCHAR);
	PRINT 'ERROR MESSAGE'+ CAST(ERROR_STATE() AS NVARCHAR);
	PRINT '==============================================================';
	END CATCH
END






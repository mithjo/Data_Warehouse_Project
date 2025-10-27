/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/
CREATE OR ALTER   PROCEDURE silver.load_silver AS
BEGIN 
    DECLARE @start_time DATETIME,@end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;
BEGIN TRY
    SET @batch_start_time = GETDATE();

    SET @start_time = GETDATE();
    PRINT '>> Truncating Table: silver.crm_cust_info';
    TRUNCATE TABLE silver.crm_cust_info;
    PRINT '>> Inserting Data Into : silver.crm_cust_info';
    INSERT INTO silver.crm_cust_info
    (cst_id,
     cst_key,
     cst_firstname,
     cst_lastname,
     cst_marital_status,
     cst_gndr,
     cst_create_date)
    SELECT cst_id
          ,cst_key
          ,TRIM(cst_firstname) AS cst_firstname
          ,TRIM(cst_lastname) AS cst_lastname
          ,CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                ELSE 'n/a'
           END AS cst_marital_status
          ,CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'n/a'
           END AS cst_gndr
          ,cst_create_date
      FROM 
      (
        SELECT *,
          ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date) as rank_list
        FROM bronze.crm_cust_info
        WHERE cst_id is not null
       ) AS t 
       WHERE  rank_list = 1 ;
       SET @end_time =GETDATE();
       PRINT 'INSERT TIME : ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR ) + 'SECONDS';
    PRINT '>> Insertion Completed for table :silver.crm_cust_info';

    SET @start_time = GETDATE();
    PRINT '>> Truncating Table: silver.crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info;
    PRINT '>> Inserting Data Into : silver.crm_prd_info';
    INSERT INTO silver.crm_prd_info
    (
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
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
        SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
        prd_num,
        ISNULL(prd_cost, 0) AS prd_cost,
        CASE UPPER(TRIM(prd_line))
             WHEN 'M' THEN 'Mountain'
             WHEN 'R' THEN 'Road'
             WHEN 'S' THEN 'Other Sales'
             WHEN 'T' THEN 'Touring'
             ELSE 'n/a'
        END AS prd_line,
        CAST(prd_start_dt AS DATE) AS prd_start_dt,
        CAST(
        DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt))
        AS DATE
    ) AS prd_end_dt
    FROM [datawarehouse].[bronze].[crm_prd_info];
    SET @end_time =GETDATE();
    PRINT 'INSERT TIME : ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR ) + 'SECONDS';
    PRINT '>> Insertion Completed for table :silver.crm_prd_info';

    SET @start_time = GETDATE();
    PRINT '>> Truncating Table: silver.crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details;
    PRINT '>> Inserting Data Into : silver.crm_sales_details';
    INSERT INTO silver.crm_sales_details
    (
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
    SELECT sls_ord_num
          ,sls_prd_key
          ,sls_cust_id
          ,CASE WHEN sls_order_dt <=0 or Len(sls_order_dt) !=8 THEN NULL
                ELSE cast(cast(sls_order_dt AS varchar) AS DATE) 
           END as sls_order_dt
          ,sls_ship_dt
          ,sls_due_dt
          ,case when sls_sales is null or sls_sales < = 0 or sls_sales != sls_quantity * abs(sls_price)
                then sls_quantity * abs(sls_price)
                else sls_sales
           end as sls_sales
          ,sls_quantity
          ,case when sls_price <= 0 or sls_price is null 
                then sls_sales / nullif(sls_quantity,0)
                else abs(sls_sales)
            end as sls_price
      FROM [datawarehouse].[bronze].[crm_sales_details]
    SET @end_time =GETDATE();
    PRINT 'INSERT TIME : ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR ) + 'SECONDS';
    PRINT '>> Insertion Completed for table :silver.crm_sales_details';

    SET @start_time = GETDATE();
    PRINT '>> Truncating Table: silver.erp_cust_az12';
    TRUNCATE TABLE silver.erp_cust_az12;
    PRINT '>> Inserting Data Into : silver.erp_cust_az12';
      INSERT INTO silver.erp_cust_az12
    ( cid,
      bdate,
      gen
    )
    SELECT 
          case when cid like 'NAS%' then substring(cid,4,len(cid))
                else cid
            end as cid 
          ,case when bdate > getdate() then null
                else bdate
            end as bdate
          ,case when gen = 'F' then 'Female'
                when gen = 'M' then 'Male'
                when gen is null or gen = ' 'then 'n/a'
                else gen
            end as gen
    FROM [datawarehouse].[bronze].[erp_cust_az12];
    SET @end_time =GETDATE();
    PRINT 'INSERT TIME : ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR ) + 'SECONDS';
    PRINT '>> Insertion Completed for table :silver.erp_cust_az12';

    SET @start_time = GETDATE();
    PRINT '>> Truncating Table: silver.erp_loc_a101';
    TRUNCATE TABLE silver.erp_loc_a101;
    PRINT '>> Inserting Data Into : silver.erp_loc_a101';
    Insert into silver.erp_loc_a101
    (
     cid,
     cntry
     )
    SELECT REPLACE(cid,'-','') cid
          ,case when upper(trim(cntry)) in ('US','United States','USA') then 'United States'
                when upper(trim(cntry)) = 'DE' then 'Germany'
                when upper(trim(cntry)) is null or CNTRY = ' ' then 'n/a'
                else cntry
           end as cntry
      FROM [datawarehouse].[bronze].[erp_loc_a101];
    SET @end_time =GETDATE();
    PRINT 'INSERT TIME : ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR ) + 'SECONDS';
    PRINT '>> Insertion Completed for table :silver.erp_loc_a101';

    SET @start_time = GETDATE();
    PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    PRINT '>> Inserting Data Into : silver.erp_px_cat_g1v2';
    Insert into silver.erp_px_cat_g1v2
    ( 
          [ID]
          ,[CAT]
          ,[SUBCAT]
          ,[MAINTENANCE]
    )
    SELECT [ID]
          ,[CAT]
          ,[SUBCAT]
          ,[MAINTENANCE]
      FROM [datawarehouse].[bronze].[erp_px_cat_g1v2];
      SET @end_time =GETDATE();
      PRINT 'INSERT TIME : ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR ) + 'SECONDS';
      PRINT '>> Insertion Completed for table :silver.erp_px_cat_g1v2';
 SET @batch_end_time = GETDATE();
 PRINT 'TOTAL BATCH TIME: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
  END TRY
  BEGIN CATCH
    PRINT'ERROR IN INSERTING DATA';
    PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
  END CATCH
END;


GO



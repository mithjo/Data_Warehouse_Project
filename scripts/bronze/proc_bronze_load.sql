/*
========================================================================================================
Stored Procedure: Load Bronze Layer(Source -> Bronze)
========================================================================================================
Script Purpose
 This stored procedure loads data into the bronze schema from external CSV files.
 It performs the following actions:
  - Truncates the bronze tables before laoding data.
  - Uses the BULK INSERT command to load data from csv files to bronze tables. 

Parameters:
  None
  This stored procedure does not accept any parameters or return any values.

Usage Example:
  EXEC bronze.load_bronz;
=======================================================================================================
*/

CREATE OR ALTER   PROCEDURE [bronze].[load_bronze] AS
BEGIN
	BEGIN TRY
		-- Declare variables to find the loading time of each table and also the whole batch loading time 
		DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME,@batch_end_time DATETIME;
		SET @batch_start_time = GETDATE();
		PRINT '====================================================';
		PRINT 'lOADING BRONZE LAYER';
		PRINT '====================================================';

		PRINT '-----------------------------------------------------';
		PRINT 'lOADING CRM DATAS'
		PRINT '-----------------------------------------------------';

		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';
		SET @start_time = GETDATE();
		PRINT '>> TRUNCACTE TABLE : crm_cust_info >>';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '>> INSERT DATA INTO: crm_cust_info >>';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\minnu\Desktop\2025 job hunting journey\Datawarehouse\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
				FIRSTROW =2,
				FIELDTERMINATOR = ',',
				TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'LOAD TIME :' + CAST (DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'SECONDS';
		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';

		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';
		SET @start_time = GETDATE();
		PRINT '>> TRUNCACTE TABLE : crm_prd_info >>';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>> INSERT DATA INTO: crm_prd_info >>';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\minnu\Desktop\2025 job hunting journey\Datawarehouse\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'LOAD TIME :' + CAST (DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'SECONDS';
		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';

		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';
		SET @start_time = GETDATE();
		PRINT '>> TRUNCACTE TABLE : crm_sales_details >>';
		TRUNCATE TABLE bronze.crm_sales_details
		PRINT '>> INSERT DATA INTO: crm_sales_details >>';
		BULK INSERT bronze.crm_sales_details 
		FROM 'C:\Users\minnu\Desktop\2025 job hunting journey\Datawarehouse\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'LOAD TIME :' + CAST (DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'SECONDS';
		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';

		PRINT '-----------------------------------------------------';
		PRINT 'lOADING CRM DATAS'
		PRINT '-----------------------------------------------------'

		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';
		SET @start_time = GETDATE();
		PRINT '>> TRUNCACTE TABLE : erp_cust_az12 >>';
		TRUNCATE TABLE bronze.erp_cust_az12
		PRINT '>> INSERT DATA INTO: erp_cust_az12 >>';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\minnu\Desktop\2025 job hunting journey\Datawarehouse\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'LOAD TIME :' + CAST (DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'SECONDS';
		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';

		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';
		SET @start_time = GETDATE();
		PRINT '>> TRUNCACTE TABLE : erp_loc_a101 >>';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>> INSERT DATA INTO: erp_loc_a101 >>';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\minnu\Desktop\2025 job hunting journey\Datawarehouse\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'LOAD TIME :' + CAST (DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'SECONDS';
		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';

		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';
		SET @start_time = GETDATE();
		PRINT '>> TRUNCACTE TABLE : erp_px_cat_g1v2 >>';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '>> INSERT DATA INTO: erp_px_cat_g1v2 >>';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\minnu\Desktop\2025 job hunting journey\Datawarehouse\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'LOAD TIME :' + CAST (DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'SECONDS';
		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';

		SET @batch_end_time = GETDATE();
		PRINT 'BATCH LOAD TIME: ' + CAST(DATEDIFF(SECOND,@batch_start_time, @batch_end_time) AS NVARCHAR) + 'SECONDS';
	END TRY
	BEGIN CATCH
		PRINT '====================================================';
		PRINT 'ERROR IN LOADING DATA';
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT '====================================================';
	END CATCH
END;


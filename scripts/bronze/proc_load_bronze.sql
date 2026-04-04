/* 
============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
============================================================================
Script Purpose:
 This stored procedure loads dta into the 'bronze' schema from external CSV files.
  It performs the following actions:
  - Truncates the bronze tables before loading data.
  - Uses the 'BULK INSEERT' command to load data from csv Files to bronze tables.

Parameters: 
  None.
 This stored procedure does not accept any parameters or return any values.

Usage exmaple:
  EXEC bronze.load_bronze;
============================================================================
*/





-- Develop SQL Load Scripts

-- Bulk Insert -> Loading massive amount of data from files to the database
-- one operation on one GO

-- File Delimiters:   , ; | # "

-- Truncate delete all rows from a table, resetting it to an empty state
-- “Empty the table so we can load fresh data”


CREATE OR ALTER PROCEDURE bronze.load_bronze AS  -- Added stored procedure
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, 
	@batch_start_time DATETIME, @batch_end_time DATETIME;
	

	BEGIN TRY
	SET @batch_start_time = GETDATE();
		PRINT '===================================';
		PRINT 'Loading Bronze Layer';
		PRINT '===================================';


		PRINT '===================================';
		PRINT 'Loading CRM Tables';
		PRINT '===================================';

			SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\User\Desktop\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2, -- skip the first row
			FIELDTERMINATOR = ',', -- columns if the CSV are separated by commas
			TABLOCK -- optimize insert by locking the table during insert
			-- no one else can access table while you loading it 
		);
			SET @end_time = GETDATE();
			-- Datediff calcualtes the difference between two dates, returns days, months, or years
			PRINT '-----------------';
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '-----------------';
		-- Quality check
		-- Check that the data has not shifted and is in the correct columns 



		-- Write SQL BULK Insert to load ALL CSV files into your bronze Tables

		-- crm prd info
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info
		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\User\Desktop\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '-----------------';
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '-----------------';

		-- crm sales details
		SET @start_time = GETDATE();
		PRINT '>> bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details
		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\User\Desktop\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '-----------------';
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '-----------------';


		-- erp Cust az12
		SET @start_time = GETDATE();
		PRINT '==================================='
		PRINT 'Loading ERP'
		PRINT '==================================='

		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12
		PRINT '>> Inserting Data Into:  bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\User\Desktop\sql-data-warehouse-project-main\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '-----------------';
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '-----------------';

		-- erp loc a101
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101
		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\User\Desktop\sql-data-warehouse-project-main\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '-----------------';
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '-----------------';

		-- erp px cat g1v2
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2
		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\User\Desktop\sql-data-warehouse-project-main\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '-----------------';
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '-----------------';

		SET @batch_end_time = GETDATE();
		PRINT '-==-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-'
		PRINT 'Loading Bronze Layer is Completed'
		PRINT '  - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds'
		PRINT '-==-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-'


	END TRY
	BEGIN CATCH
		PRINT '=============================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '=============================================';

		
	END CATCH
END

-- Create stored procedure

-- Hint: Save frequently used SQL code in stored procedures in database



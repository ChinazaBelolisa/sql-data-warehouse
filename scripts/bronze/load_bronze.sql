/*
===============================================================================
Bronze Load Script — CSV -> Bronze
===============================================================================
How to run:
  mysql -u root -p --local-infile=1 --silent --skip-column-names datawarehouse < proc_load_bronze.sql
===============================================================================
*/

USE datawarehouse;

SELECT '================================================' AS msg;
SELECT 'Loading Bronze Layer' AS msg;
SELECT '================================================' AS msg;

-- ------------------------------------------------------------
-- CRM TABLES
-- ------------------------------------------------------------
SELECT '------------------------------------------------' AS msg;
SELECT 'Loading CRM Tables' AS msg;
SELECT '------------------------------------------------' AS msg;

SELECT '>> Truncating Table: bronze__crm_cust_info' AS msg;
TRUNCATE TABLE bronze__crm_cust_info;

SELECT '>> Loading: cust_info.csv -> bronze__crm_cust_info' AS msg;
LOAD DATA LOCAL INFILE '/Users/chinazabelolisa/sql-data-warehouse/datasets/source_crm/cust_info.csv'
INTO TABLE bronze__crm_cust_info
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date);

SELECT CONCAT('>> Rows loaded: ', (SELECT COUNT(*) FROM bronze__crm_cust_info)) AS msg;
SELECT '>> -------------' AS msg;


SELECT '>> Truncating Table: bronze__crm_prd_info' AS msg;
TRUNCATE TABLE bronze__crm_prd_info;

SELECT '>> Loading: prd_info.csv -> bronze__crm_prd_info' AS msg;
LOAD DATA LOCAL INFILE '/Users/chinazabelolisa/sql-data-warehouse/datasets/source_crm/prd_info.csv'
INTO TABLE bronze__crm_prd_info
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt);

SELECT CONCAT('>> Rows loaded: ', (SELECT COUNT(*) FROM bronze__crm_prd_info)) AS msg;
SELECT '>> -------------' AS msg;


SELECT '>> Truncating Table: bronze__crm_sales_details' AS msg;
TRUNCATE TABLE bronze__crm_sales_details;

SELECT '>> Loading: sales_details.csv -> bronze__crm_sales_details' AS msg;
LOAD DATA LOCAL INFILE '/Users/chinazabelolisa/sql-data-warehouse/datasets/source_crm/sales_details.csv'
INTO TABLE bronze__crm_sales_details
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price);

SELECT CONCAT('>> Rows loaded: ', (SELECT COUNT(*) FROM bronze__crm_sales_details)) AS msg;
SELECT '>> -------------' AS msg;


-- ------------------------------------------------------------
-- ERP TABLES
-- ------------------------------------------------------------
SELECT '------------------------------------------------' AS msg;
SELECT 'Loading ERP Tables' AS msg;
SELECT '------------------------------------------------' AS msg;

SELECT '>> Truncating Table: bronze__erp_loc_a101' AS msg;
TRUNCATE TABLE bronze__erp_loc_a101;

SELECT '>> Loading: LOC_A101.csv -> bronze__erp_loc_a101' AS msg;
LOAD DATA LOCAL INFILE '/Users/chinazabelolisa/sql-data-warehouse/datasets/source_erp/LOC_A101.csv'
INTO TABLE bronze__erp_loc_a101
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(cid, cntry);

SELECT CONCAT('>> Rows loaded: ', (SELECT COUNT(*) FROM bronze__erp_loc_a101)) AS msg;
SELECT '>> -------------' AS msg;


SELECT '>> Truncating Table: bronze__erp_cust_az12' AS msg;
TRUNCATE TABLE bronze__erp_cust_az12;

SELECT '>> Loading: CUST_AZ12.csv -> bronze__erp_cust_az12' AS msg;
LOAD DATA LOCAL INFILE '/Users/chinazabelolisa/sql-data-warehouse/datasets/source_erp/CUST_AZ12.csv'
INTO TABLE bronze__erp_cust_az12
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(cid, bdate, gen);

SELECT CONCAT('>> Rows loaded: ', (SELECT COUNT(*) FROM bronze__erp_cust_az12)) AS msg;
SELECT '>> -------------' AS msg;


SELECT '>> Truncating Table: bronze__erp_px_cat_g1v2' AS msg;
TRUNCATE TABLE bronze__erp_px_cat_g1v2;

SELECT '>> Loading: PX_CAT_G1V2.csv -> bronze__erp_px_cat_g1v2' AS msg;
LOAD DATA LOCAL INFILE '/Users/chinazabelolisa/sql-data-warehouse/datasets/source_erp/PX_CAT_G1V2.csv'
INTO TABLE bronze__erp_px_cat_g1v2
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(id, cat, subcat, maintenance);

SELECT CONCAT('>> Rows loaded: ', (SELECT COUNT(*) FROM bronze__erp_px_cat_g1v2)) AS msg;
SELECT '>> -------------' AS msg;


SELECT '==========================================' AS msg;
SELECT 'Loading Bronze Layer is Completed' AS msg;
SELECT '==========================================' AS msg;
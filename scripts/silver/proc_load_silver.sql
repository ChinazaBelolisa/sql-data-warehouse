/*
===============================================================================
Stored Procedure: load_silver (Bronze -> Silver) 
===============================================================================
Script Purpose:
    This stored procedure performs the ETL process to populate the Silver layer
    tables from the Bronze layer.
Actions Performed:
    - Truncates Silver tables.
    - Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
    None.

Usage Example:
	mysql -u root -p datawarehouse < proc_load_silver.sql (Create/Update procedure file)
    mysql -u root -p -N -B datawarehouse -e "CALL load_silver();" (Execute procedure)
===============================================================================
*/

USE datawarehouse;

DROP PROCEDURE IF EXISTS load_silver;

DELIMITER $$

CREATE PROCEDURE load_silver()
BEGIN
    DECLARE v_batch_start DATETIME;
    DECLARE v_batch_end   DATETIME;
    DECLARE v_start       DATETIME;
    DECLARE v_end         DATETIME;
    DECLARE v_err_msg     TEXT;

    -- Error handler (prints message and exits)
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        GET DIAGNOSTICS CONDITION 1 v_err_msg = MESSAGE_TEXT;
        SELECT '==========================================' ;
        SELECT 'ERROR OCCURRED DURING LOADING SILVER LAYER';
        SELECT v_err_msg;
        SELECT '==========================================';
    END;

    SET v_batch_start = NOW();

    SELECT '================================================';
    SELECT 'Loading Silver Layer';
    SELECT '================================================';

    SELECT '------------------------------------------------';
    SELECT 'Loading CRM Tables';
    SELECT '------------------------------------------------';

    -- ============================================================
    -- silver__crm_cust_info
    -- ============================================================
    SET v_start = NOW();
    SELECT '>> Truncating Table: silver__crm_cust_info';
    TRUNCATE TABLE silver__crm_cust_info;

    SELECT '>> Inserting Data Into: silver__crm_cust_info';
    INSERT INTO silver__crm_cust_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date
    )
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname)  AS cst_lastname,
        CASE
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END AS cst_marital_status,
        CASE
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END AS cst_gndr,
        cst_create_date
    FROM (
        SELECT
            b.*,
            ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
        FROM bronze__crm_cust_info b
        WHERE cst_id IS NOT NULL
    ) t
    WHERE flag_last = 1;

    SET v_end = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, v_start, v_end), ' seconds');
    SELECT '>> -------------';


    -- ============================================================
    -- silver__crm_prd_info
    -- ============================================================
    SET v_start = NOW();
    SELECT '>> Truncating Table: silver__crm_prd_info';
    TRUNCATE TABLE silver__crm_prd_info;

    SELECT '>> Inserting Data Into: silver__crm_prd_info';
    INSERT INTO silver__crm_prd_info (
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
        SUBSTRING(prd_key, 7) AS prd_key,
        prd_nm,
        IFNULL(prd_cost, 0) AS prd_cost,
        CASE
            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            ELSE 'n/a'
        END AS prd_line,
        DATE(prd_start_dt) AS prd_start_dt,
        DATE_SUB(
            DATE(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)),
            INTERVAL 1 DAY
        ) AS prd_end_dt
    FROM bronze__crm_prd_info;

    SET v_end = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, v_start, v_end), ' seconds');
    SELECT '>> -------------';


    -- ============================================================
    -- silver__crm_sales_details
    -- ============================================================
    SET v_start = NOW();
    SELECT '>> Truncating Table: silver__crm_sales_details';
    TRUNCATE TABLE silver__crm_sales_details;

    SELECT '>> Inserting Data Into: silver__crm_sales_details';
    INSERT INTO silver__crm_sales_details (
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
        x.sls_ord_num,
        x.sls_prd_key,
        x.sls_cust_id,
        x.sls_order_dt,
        x.sls_ship_dt,
        x.sls_due_dt,
        x.sls_sales_corrected AS sls_sales,
        x.sls_quantity,
        x.sls_price_corrected AS sls_price
    FROM (
        SELECT
            b.sls_ord_num,
            b.sls_prd_key,
            b.sls_cust_id,

            CASE
                WHEN b.sls_order_dt = 0 OR CHAR_LENGTH(CAST(b.sls_order_dt AS CHAR)) <> 8 THEN NULL
                ELSE STR_TO_DATE(CAST(b.sls_order_dt AS CHAR), '%Y%m%d')
            END AS sls_order_dt,

            CASE
                WHEN b.sls_ship_dt = 0 OR CHAR_LENGTH(CAST(b.sls_ship_dt AS CHAR)) <> 8 THEN NULL
                ELSE STR_TO_DATE(CAST(b.sls_ship_dt AS CHAR), '%Y%m%d')
            END AS sls_ship_dt,

            CASE
                WHEN b.sls_due_dt = 0 OR CHAR_LENGTH(CAST(b.sls_due_dt AS CHAR)) <> 8 THEN NULL
                ELSE STR_TO_DATE(CAST(b.sls_due_dt AS CHAR), '%Y%m%d')
            END AS sls_due_dt,

            b.sls_quantity,

            CASE
                WHEN b.sls_sales IS NULL
                  OR b.sls_sales <= 0
                  OR b.sls_sales <> b.sls_quantity * ABS(b.sls_price)
                THEN b.sls_quantity * ABS(b.sls_price)
                ELSE b.sls_sales
            END AS sls_sales_corrected,

            CASE
                WHEN b.sls_price IS NULL OR b.sls_price <= 0
                THEN (
                    CASE
                        WHEN b.sls_sales IS NULL
                          OR b.sls_sales <= 0
                          OR b.sls_sales <> b.sls_quantity * ABS(b.sls_price)
                        THEN b.sls_quantity * ABS(b.sls_price)
                        ELSE b.sls_sales
                    END
                ) / NULLIF(b.sls_quantity, 0)
                ELSE b.sls_price
            END AS sls_price_corrected

        FROM bronze__crm_sales_details b
    ) x;

    SET v_end = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, v_start, v_end), ' seconds');
    SELECT '>> -------------';


    -- ============================================================
    -- silver__erp_cust_az12
    -- ============================================================
    SET v_start = NOW();
    SELECT '>> Truncating Table: silver__erp_cust_az12';
    TRUNCATE TABLE silver__erp_cust_az12;

    SELECT '>> Inserting Data Into: silver__erp_cust_az12';
    INSERT INTO silver__erp_cust_az12 (
        cid,
        bdate,
        gen
    )
    SELECT
        CASE
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4)
            ELSE cid
        END AS cid,
        CASE
            WHEN bdate > CURRENT_DATE THEN NULL
            ELSE bdate
        END AS bdate,
        CASE
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'n/a'
        END AS gen
    FROM bronze__erp_cust_az12;

    SET v_end = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, v_start, v_end), ' seconds');
    SELECT '>> -------------';


    SELECT '------------------------------------------------';
    SELECT 'Loading ERP Tables';
    SELECT '------------------------------------------------';

    -- ============================================================
    -- silver__erp_loc_a101
    -- ============================================================
    SET v_start = NOW();
    SELECT '>> Truncating Table: silver__erp_loc_a101';
    TRUNCATE TABLE silver__erp_loc_a101;

    SELECT '>> Inserting Data Into: silver__erp_loc_a101';
    INSERT INTO silver__erp_loc_a101 (
        cid,
        cntry
    )
    SELECT
        REPLACE(cid, '-', '') AS cid,
        CASE
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
            WHEN cntry IS NULL OR TRIM(cntry) = '' THEN 'n/a'
            ELSE TRIM(cntry)
        END AS cntry
    FROM bronze__erp_loc_a101;

    SET v_end = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, v_start, v_end), ' seconds');
    SELECT '>> -------------';


    -- ============================================================
    -- silver__erp_px_cat_g1v2
    -- ============================================================
    SET v_start = NOW();
    SELECT '>> Truncating Table: silver__erp_px_cat_g1v2';
    TRUNCATE TABLE silver__erp_px_cat_g1v2;

    SELECT '>> Inserting Data Into: silver__erp_px_cat_g1v2';
    INSERT INTO silver__erp_px_cat_g1v2 (
        id,
        cat,
        subcat,
        maintenance
    )
    SELECT
        id,
        cat,
        subcat,
        maintenance
    FROM bronze__erp_px_cat_g1v2;

    SET v_end = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, v_start, v_end), ' seconds');
    SELECT '>> -------------';


    SET v_batch_end = NOW();
    SELECT '==========================================';
    SELECT 'Loading Silver Layer is Completed';
    SELECT CONCAT('   - Total Load Duration: ', TIMESTAMPDIFF(SECOND, v_batch_start, v_batch_end), ' seconds');
    SELECT '==========================================';
END$$

DELIMITER ;

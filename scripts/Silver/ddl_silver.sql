/*
====================================================================
DDL Script : create silver tables
====================================================================
Script Purpose;
   This script creates table in the 'silver schema',dropping existing tables
   they already exists.
   run the script to re-define the DDL structure of 'bronze' tables
====================================================================
/*

drop table silver.crm_custi_info;
create table silver.crm_custi_info(
	cst_id int,
	cst_key NCHAR(50),
	cst_firstname NCHAR(50),
	cst_lastname NCHAR(50),
	CST_material_status NCHAR(50),
	CST_gndr nchar(50),
	cst_create_date DATE,
	dwh_create_date timestamp default current_timestamp
);

drop table  silver.crm_prd_info;
create table silver.crm_prd_info(
	prd_id    INT,
	cat_i
	prd_key   NCHAR(50),
	prd_nm    NCHAR(50),
	prd_cost  INT,
	prd_line  NCHAR(50),
	prd_start_dt  TIMESTAMP,
	prd_end_dt    TIMESTAMP,
	dwh_create_date timestamp default current_timestamp
);
drop table silver.crm_sales_details;
create table silver.crm_sales_details(
	sls_ord_num NCHAR(50),
	sls_prd_key NCHAR(50),
	sls_cust_key INT,
	sls_order_dt date,
	sls_ship_dt  date,
	sls_due_dt date,
	sls_sales   INT,
	sls_quantity INT,
	sls_price INT,
	dwh_create_date timestamp default current_timestamp
);

drop table silver.erp_loc_a101;
create table silver.erp_loc_a101(
	 cid NCHAR(50),
	 cntry NCHAR(50),
	dwh_create_date timestamp default current_timestamp
);

drop table silver.cust_az12;
create table silver.cust_az12(
	cid  NCHAR(50),
	bdate DATE,
	gen  NCHAR(50),
	dwh_create_date timestamp default current_timestamp
);

drop table silver.erp_px_cat_g1v2;
create table silver.erp_px_cat_g1v2 (
	id   NCHAR(50),
	cat  NCHAR(50),
	subcat NCHAR(50),
	maintenance NCHAR(50),
	dwh_create_date timestamp default current_timestamp
);



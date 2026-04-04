/*
===========================================================================
DDL Script : Create gold views
===========================================================================
script purpose;
 This script creates views for the gold layer in the data warehouse
This Gold layer represents the final dimensions and fact tables( star schema)

Each view performs transformation and combines data from the silver layer to produce
 a clean,enriched , and business-ready dataset.

Usage;
  -These views can be queried directly for analytics and reporting
=============================================================================
*/

 --  =======================================================================
 --  Create Dimensions: gold.dim_customers
===========================================================================

create view gold.dim_customers as
SELECT 
 row_number() over (order by cst_id) as customer_key,
ci.cst_id as customer_id,
ci.cst_key as customer_number,
ci.cst_firstname as first_name,
ci.cst_lastname as lastname,
la.cntry as country,
ci.cst_material_status  marital_status,
case when ci.cst_gndr != 'n/a' then ci.cst_gndr
  else coalesce (ca.gen,'n/a')
 end as gender,
ca.bdate as birth_date,
ci.cst_create_date as create_date
from silver.crm_custi_info ci
left join silver.cust_az12 ca
on      ci.cst_key=ca.cid
left join silver.erp_loc_a101 la
on   ci.cst_key = la.cid

-- =============================================================================
--  Create Dimension: gold.dim_product
-- =============================================================================

create view gold.dim_product as
select
  row_number() over (order by pn.prd_start_dt,pn.prd_key) as product_key,
	pn.prd_id as product_id ,  
	pn.prd_key as product_number,
	pn.prd_nm as product_name,
	pn.cat_id as category_id,
	pc.cat as category,
	pc.subcat as subcategory,
	pc.maintenance,
	pn.prd_cost as cost,
	pn.prd_line as product_line,  
	pn.prd_start_dt as start_date	
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id=pc.id
where prd_end_dt is null  --Filter out all historical data

-- ========================================================================
--  create view : gold.fact_sales
-- =========================================================================

create view gold.fact_sales as
select 
sd.sls_ord_num as order_number,
pr.product_key ,
cu.customer_key,
sd.sls_order_dt  as order_date,
sd.sls_ship_dt as shipping_date,
sd.sls_due_dt as due_date,
sd.sls_sales as sales_amount,
sd.sls_quantity as quantity,
sd.sls_price
from silver.crm_sales_details sd
left join gold.dim_product pr
on sd.sls_prd_key = pr.product_number
left join gold.dim_customers cu
on sd.sls_cust_key = cu.customer_id

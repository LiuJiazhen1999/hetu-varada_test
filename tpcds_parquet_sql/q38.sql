 
SELECT "count"(*)
FROM
  (
   SELECT DISTINCT
     "c_last_name"
   , "c_first_name"
   , "d_date"
   FROM
     hive.tpcds_parquet_1000.store_sales
   , hive.tpcds_parquet_1000.date_dim
   , hive.tpcds_parquet_1000.customer
   WHERE ("store_sales"."ss_sold_date_sk" = "date_dim"."d_date_sk")
      AND ("store_sales"."ss_customer_sk" = "customer"."c_customer_sk")
      AND ("d_month_seq" BETWEEN 1200 AND (1200 + 11))
INTERSECT    SELECT DISTINCT
     "c_last_name"
   , "c_first_name"
   , "d_date"
   FROM
     hive.tpcds_parquet_1000.catalog_sales
   , hive.tpcds_parquet_1000.date_dim
   , hive.tpcds_parquet_1000.customer
   WHERE ("catalog_sales"."cs_sold_date_sk" = "date_dim"."d_date_sk")
      AND ("catalog_sales"."cs_bill_customer_sk" = "customer"."c_customer_sk")
      AND ("d_month_seq" BETWEEN 1200 AND (1200 + 11))
INTERSECT    SELECT DISTINCT
     "c_last_name"
   , "c_first_name"
   , "d_date"
   FROM
     hive.tpcds_parquet_1000.web_sales
   , hive.tpcds_parquet_1000.date_dim
   , hive.tpcds_parquet_1000.customer
   WHERE ("web_sales"."ws_sold_date_sk" = "date_dim"."d_date_sk")
      AND ("web_sales"."ws_bill_customer_sk" = "customer"."c_customer_sk")
      AND ("d_month_seq" BETWEEN 1200 AND (1200 + 11))
)  hot_cust
LIMIT 100;

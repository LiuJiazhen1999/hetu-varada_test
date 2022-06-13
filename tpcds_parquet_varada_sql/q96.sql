 
SELECT "count"(*)
FROM
  varada.tpcds_parquet_100.store_sales
, varada.tpcds_parquet_100.household_demographics
, varada.tpcds_parquet_100.time_dim
, varada.tpcds_parquet_100.store
WHERE ("ss_sold_time_sk" = "time_dim"."t_time_sk")
   AND ("ss_hdemo_sk" = "household_demographics"."hd_demo_sk")
   AND ("ss_store_sk" = "s_store_sk")
   AND ("time_dim"."t_hour" = 20)
   AND ("time_dim"."t_minute" >= 30)
   AND ("household_demographics"."hd_dep_count" = 7)
   AND ("store"."s_store_name" = 'ese')
ORDER BY "count"(*) ASC
LIMIT 100;

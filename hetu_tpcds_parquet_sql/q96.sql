 
SELECT "count"(*)
FROM
  hive.hetu_tpcds_parquet_1000.store_sales
, hive.hetu_tpcds_parquet_1000.household_demographics
, hive.hetu_tpcds_parquet_1000.time_dim
, hive.hetu_tpcds_parquet_1000.store
WHERE ("ss_sold_time_sk" = "time_dim"."t_time_sk")
   AND ("ss_hdemo_sk" = "household_demographics"."hd_demo_sk")
   AND ("ss_store_sk" = "s_store_sk")
   AND ("time_dim"."t_hour" = 20)
   AND ("time_dim"."t_minute" >= 30)
   AND ("household_demographics"."hd_dep_count" = 7)
   AND ("store"."s_store_name" = 'ese')
ORDER BY "count"(*) ASC
LIMIT 100;

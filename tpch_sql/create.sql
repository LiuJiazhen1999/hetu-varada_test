create schema if not exists tpch_parquet_1000;
use hive.tpch_parquet_1000;
CREATE TABLE hive.tpch_parquet_1000.lineitem WITH (format = 'parquet') AS SELECT * FROM tpch.sf1000.lineitem;
CREATE TABLE hive.tpch_parquet_1000.lineitemsub WITH (format = 'parquet') AS SELECT * FROM hive.tpch_parquet_1000.lineitem where shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY;
CREATE TABLE hive.tpch_parquet_1000.lineitemsub14 WITH (format = 'parquet') AS SELECT * FROM hive.tpch_parquet_1000.lineitemsub where shipdate >= DATE '1995-09-01' AND shipdate < DATE '1995-09-01' + INTERVAL '1' MONTH;
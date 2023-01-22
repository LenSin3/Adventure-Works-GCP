-- create database sales_bronze_db
-- save to bucket_name
CREATE DATABASE IF NOT EXISTS sales_bronze_db
LOCATION '${bucket_name}/sales_bronze.db';
-- use bronze_db database
USE sales_bronze_db;

-- create temporary view of table from csv file
CREATE OR REPLACE TEMPORARY VIEW ${table_name}_v
USING CSV
OPTIONS (
    path '${bucket_name}/transformed/${table_name}.csv',
    header true
);

-- create table in parquet format
CREATE TABLE IF NOT EXISTS ${table_name}
USING PARQUET
SELECT * FROM ${table_name}_v;

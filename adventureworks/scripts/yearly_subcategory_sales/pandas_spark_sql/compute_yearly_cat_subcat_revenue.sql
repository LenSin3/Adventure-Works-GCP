-- create database sales_gold_db
CREATE DATABASE IF NOT EXISTS sales_gold_db
LOCATION '${bucket_name}/sales_gold.db';

USE sales_gold_db;

CREATE TABLE IF NOT EXISTS yearly_sales_revenue (
    order_year INT,
    category STRING,
    subcategory STRING,
    quantity INT,
    sales_revenue FLOAT
) USING PARQUET;
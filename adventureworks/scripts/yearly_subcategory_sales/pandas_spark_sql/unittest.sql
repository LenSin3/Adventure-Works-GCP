SHOW databases;

USE sales_bronze_db;

SHOW tables;

!gsutil ls gs://lpkadvworks/sales_bronze.db;

SELECT 'Displaying count for customers';
SELECT count(*) FROM customers;

SELECT 'Displaying count for territories';
SELECT count(*) FROM territories;

SELECT 'Displaying count for categories';
SELECT count(*) FROM categories;

SELECT 'Displaying count for subcategories';
SELECT count(*) FROM subcategories;

SELECT 'Displaying count for products';
SELECT count(*) FROM products;

SELECT 'Displaying count for returns';
SELECT count(*) FROM returns;

SELECT 'Displaying count for sales';
SELECT count(*) FROM sales;
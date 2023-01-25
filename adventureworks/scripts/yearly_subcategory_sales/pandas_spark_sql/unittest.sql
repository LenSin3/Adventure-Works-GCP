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

USE sales_gold_db;

SHOW tables;

!gsutil ls gs://lpkadvworks/sales_gold.db;

SELECT 'Displaying count for daily product revenue';
SELECT count(*) FROM daily_product_revenue;

SELECT 'Displaying first 5 rows for daily product revenue';
SELECT * FROM daily_product_revenue LIMIT 5;

USE returns_gold_db;

SHOW tables;

!gsutil ls gs://lpkadvworks/returns_gold.db;

SELECT 'Displaying count for daily returns cost';
SELECT count(*) FROM daily_returns_cost;

SELECT 'Displaying first 5 rows of daily returns cost';
SELECT * FROM daily_returns_cost LIMIT 5;
-- create database sales_gold_db
CREATE DATABASE IF NOT EXISTS sales_gold_db
LOCATION '${bucket_name}/sales_gold.db';

USE sales_gold_db;

CREATE TABLE IF NOT EXISTS daily_product_revenue (
    OrderDate DATE,
    CategoryName STRING,
    ProductSubcategoryKey STRING,
    SubcategoryName STRING,
    TotalOrders INT,
    SalesTotal FLOAT
) USING PARQUET;

INSERT INTO daily_product_revenue
WITH combined_sales AS (
    SELECT s.OrderDate,
        s.ProductKey,
        p.ProductSubCategoryKey,
        c.CategoryName,
        sc.SubCategoryName,
        s.OrderQuantity,
        p.ProductPrice,
        s.OrderQuantity * p.ProductPrice as Sales
    FROM sales_bronze_db.sales s
    LEFT JOIN sales_bronze_db.products p
    USING(ProductKey)
    JOIN sales_bronze_db.subcategories sc
    USING(ProductSubcategoryKey)
    JOIN sales_bronze_db.categories c
    USING(ProductCategoryKey)
)
SELECT OrderDate,
    CategoryName,
    ProductSubcategoryKey,
    SubcategoryName,
    SUM(OrderQuantity) as TotalOrders,
    ROUND(SUM(Sales), 2) as SalesTotal
FROM combined_sales
GROUP BY 1, 2, 3, 4\
ORDER BY 1 DESC, 6 DESC;
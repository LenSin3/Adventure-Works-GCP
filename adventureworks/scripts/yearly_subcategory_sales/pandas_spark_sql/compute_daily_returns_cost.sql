-- create database sales_gold_db
CREATE DATABASE IF NOT EXISTS returns_gold_db
LOCATION '${bucket_name}/returns_gold.db';

USE returns_gold_db;

CREATE TABLE IF NOT EXISTS daily_returns_cost (
    ReturnDate DATE,
    CategoryName STRING,
    ProductSubcategoryKey STRING,
    SubcategoryName STRING,
    TotalReturns INT,
    TotalReturnsCost FLOAT
) USING PARQUET;

INSERT INTO daily_returns_cost 
WITH combined_returns AS (
    SELECT r.ReturnDate,
        r.ProductKey,
        p.ProductSubCategoryKey,
        c.CategoryName,
        sc.SubCategoryName,
        r.ReturnQuantity,
        p.ProductPrice,
        r.ReturnQuantity * p.ProductPrice as ReturnsCost
    FROM sales_bronze_db.returns r
    LEFT JOIN sales_bronze_db.products
    USING(ProductKey)
    JOIN sales_bronze_db.subcategories sc
    USING(ProductSubcategoryKey)\
    JOIN sales_bronze_db.categories
    USING(ProductCategoryKey)
)
SELECT ReturnDate,
    CategoryName,
    ProductSubcategoryKey,
    SubcategoryName,
    SUM(ReturnQuantity) as TotalReturns,
    ROUND(SUM(ReturnsCost), 2) as TotalReturnsCost
FROM combined_returns
GROUP BY 1, 2, 3, 4
ORDER BY 1 DESC, 6 DESC;
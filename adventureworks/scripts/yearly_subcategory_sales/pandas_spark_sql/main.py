import app
bucket_name = 'lpkadvworks'
dest_dir_name = 'transformed'

customers_uri = 'gs://lpkadvworks/data/other_data/AdventureWorks_Customers.csv'
categories_uri = 'gs://lpkadvworks/data/other_data/AdventureWorks_Product_Categories.csv'
subcategories_uri = 'gs://lpkadvworks/data/other_data/AdventureWorks_Product_Subcategories.csv'
products_uri = 'gs://lpkadvworks/data/other_data/AdventureWorks_Products.csv'
returns_uri = 'gs://lpkadvworks/data/other_data/AdventureWorks_Returns.csv'
territories_uri = 'gs://lpkadvworks/data/other_data/AdventureWorks_Territories.csv'
sales_dir_uri = 'gs://lpkadvworks/data/sales_stage/'

# customers
app.transform_customers(customers_uri, bucket_name, dest_dir_name, 'customers')
# territories
app.save_no_transform(territories_uri, bucket_name, dest_dir_name, 'territories')
# categories
app.save_no_transform(categories_uri, bucket_name, dest_dir_name, 'categories')
# subcategories
app.save_no_transform(subcategories_uri, bucket_name, dest_dir_name, 'subcategories')
# products
app.transform_products(products_uri, bucket_name, dest_dir_name, 'products')
# returns
app.transform_returns(returns_uri, bucket_name, dest_dir_name, 'returns')
# sales
app.transform_sales(bucket_name, dest_dir_name, 'sales')
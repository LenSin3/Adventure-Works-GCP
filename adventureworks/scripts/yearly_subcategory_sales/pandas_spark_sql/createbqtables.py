from google.cloud import bigquery
import pandas as pd

# project id
project_id = 'dataengongcp'

# instantiate bigquery client
client = bigquery.Client()



# table_id = 'dataengongcp.advworks.daily_product_revenue'
# table = bigquery.Table(table_id, schema=schema_daily_product_revenue)
# table = client.create_table(table)  # Make an API request.
# print(
#     "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
# )

# function to create table in bigquery
def create_bq_schema(table_name, schema, project_id='dataengongcp', dataset='advworks'):
    table_id = f'{project_id}.{dataset}.{table_name}'
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )
    
# create daily_product_revenue table
schema_daily_sales_revenue=[
        bigquery.SchemaField('OrderDate', 'STRING'),
        bigquery.SchemaField('CategoryName', 'STRING'),
        bigquery.SchemaField('ProductSubcategoryKey', 'STRING'),
        bigquery.SchemaField('SubcategoryName', 'STRING'),
        bigquery.SchemaField('TotalOrders', 'INTEGER'),
        bigquery.SchemaField('SalesTotal', 'FLOAT'),
    ]
    
# create daily_returns_cost table
schema_daily_returns_cost=[
        bigquery.SchemaField('ReturnDate', 'STRING'),
        bigquery.SchemaField('CategoryName', 'STRING'),
        bigquery.SchemaField('ProductSubcategoryKey', 'STRING'),
        bigquery.SchemaField('SubcategoryName', 'STRING'),
        bigquery.SchemaField('TotalReturns', 'INTEGER'),
        bigquery.SchemaField('TotalReturnsCost', 'FLOAT'),
    ]

# daily_product_revenue
create_bq_schema('daily_sales_revenue', schema_daily_sales_revenue)

# daily returns cost
create_bq_schema('daily_returns_cost', schema_daily_returns_cost)
    

    


    
    
import os
from pyspark.sql import SparkSession

spark = SparkSession. \
    builder. \
    getOrCreate()
    
    
data_uri = os.environ.get('DATA_URI')
project_id = os.environ.get('PROJECT_ID')
dateset_name = os.environ.get('DATASET_NAME')
table_name = os.environ.get('TABLE_NAME')
gcs_temp_bucket = os.environ.get('GCS_TEMP_BUCKET')

# daily_product_revenue
revenue_or_cost = spark. \
    read. \
    parquet(data_uri)
    
spark.conf.set('temporaryGcsBucket', gcs_temp_bucket)

revenue_or_cost. \
    write. \
    mode('overwrite').format('bigquery'). \
    option('table', f'{project_id}:{dateset_name}.{table_name}'). \
    save()
import datetime 

from airflow import models
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitSparkSqlJobOperator,
    DataprocSubmitPySparkJobOperator,
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator
)
from airflow.utils.dates import days_ago

project_id = Variable.get('project_id')
region = Variable.get('region')
bucket_name = Variable.get('bucket_name')
# cluster_name = Variable.get('cluster_name')

default_args ={
    'project_id': project_id,
    'region': region,
    # 'cluster_name': cluster_name,
    'start_date': days_ago(1)
}

with models.DAG(
    'dynamic_dataproc_daily_sales_and_returns_jobs_dag_v3',
    default_args=default_args,
    schedule_interval=datetime.timedelta(days=1)
) as dag:
    task_create_cluster = DataprocCreateClusterOperator(
        task_id = 'create_cluster',
        project_id = project_id,
        cluster_name = 'lpkadvworks-dsr-drc-cluster-{{ ds_nodash }}',
        num_workers = 2,
        worker_machine_type = 'n1-standard-2',
        storage_bucket = bucket_name,
        zone = 'us-central1-f',
    )
    task_clean_up = DataprocSubmitSparkSqlJobOperator(
        task_id = 'run_cleanup',
        query_uri=f'gs://{bucket_name}/scripts/yearly_subcategory_sales/pandas_spark_sql/cleanup.sql',
        cluster_name = 'lpkadvworks-dsr-drc-cluster-{{ ds_nodash }}',
    )
    task_convert_customers = DataprocSubmitSparkSqlJobOperator(
        task_id = 'convert_customers',
        query_uri= f'gs://{bucket_name}/scripts/yearly_subcategory_sales/pandas_spark_sql/file_format_converter.sql',
        cluster_name = 'lpkadvworks-dsr-drc-cluster-{{ ds_nodash }}',
        variables={
            'bucket_name':f'gs://{bucket_name}',
            'table_name': 'customers'
        },
    )
    task_convert_territories = DataprocSubmitSparkSqlJobOperator(
        task_id = 'convert_territories',
        query_uri= f'gs://{bucket_name}/scripts/yearly_subcategory_sales/pandas_spark_sql/file_format_converter.sql',
        cluster_name = 'lpkadvworks-dsr-drc-cluster-{{ ds_nodash }}',
        variables={
            'bucket_name':f'gs://{bucket_name}',
            'table_name': 'territories'
        },
    )
    task_convert_categories = DataprocSubmitSparkSqlJobOperator(
        task_id = 'convert_categories',
        query_uri= f'gs://{bucket_name}/scripts/yearly_subcategory_sales/pandas_spark_sql/file_format_converter.sql',
        cluster_name = 'lpkadvworks-dsr-drc-cluster-{{ ds_nodash }}',
        variables={
            'bucket_name':f'gs://{bucket_name}',
            'table_name': 'categories'
        },
    )
    task_convert_subcategories = DataprocSubmitSparkSqlJobOperator(
        task_id = 'convert_subcategories',
        query_uri= f'gs://{bucket_name}/scripts/yearly_subcategory_sales/pandas_spark_sql/file_format_converter.sql',
        cluster_name = 'lpkadvworks-dsr-drc-cluster-{{ ds_nodash }}',
        variables={
            'bucket_name':f'gs://{bucket_name}',
            'table_name': 'subcategories'
        },
    )
    task_convert_products = DataprocSubmitSparkSqlJobOperator(
        task_id = 'convert_products',
        query_uri= f'gs://{bucket_name}/scripts/yearly_subcategory_sales/pandas_spark_sql/file_format_converter.sql',
        cluster_name = 'lpkadvworks-dsr-drc-cluster-{{ ds_nodash }}',
        variables={
            'bucket_name':f'gs://{bucket_name}',
            'table_name': 'products'
        },
    )
    task_convert_returns = DataprocSubmitSparkSqlJobOperator(
        task_id = 'convert_returns',
        query_uri= f'gs://{bucket_name}/scripts/yearly_subcategory_sales/pandas_spark_sql/file_format_converter.sql',
        cluster_name = 'lpkadvworks-dsr-drc-cluster-{{ ds_nodash }}',
        variables={
            'bucket_name':f'gs://{bucket_name}',
            'table_name': 'returns'
        },
    )
    task_convert_sales = DataprocSubmitSparkSqlJobOperator(
        task_id = 'convert_sales',
        query_uri= f'gs://{bucket_name}/scripts/yearly_subcategory_sales/pandas_spark_sql/file_format_converter.sql',
        cluster_name = 'lpkadvworks-dsr-drc-cluster-{{ ds_nodash }}',
        variables={
            'bucket_name':f'gs://{bucket_name}',
            'table_name': 'sales'
        },
    )
    task_compute_daily_sales_revenue = DataprocSubmitSparkSqlJobOperator(
        task_id = 'compute_daily_sales_revenue',
        query_uri=f'gs://{bucket_name}/scripts/yearly_subcategory_sales/pandas_spark_sql/compute_daily_product_revenue.sql',
        cluster_name = 'lpkadvworks-dsr-drc-cluster-{{ ds_nodash }}',
        variables={
            'bucket_name':f'gs://{bucket_name}'
        },
    )
    task_compute_daily_returns_cost = DataprocSubmitSparkSqlJobOperator(
        task_id = 'compute_daily_returns_cost',
        query_uri=f'gs://{bucket_name}/scripts/yearly_subcategory_sales/pandas_spark_sql/compute_daily_returns_cost.sql',
        cluster_name = 'lpkadvworks-dsr-drc-cluster-{{ ds_nodash }}',
        variables={
            'bucket_name':f'gs://{bucket_name}'
        },
    )
    
    task_load_daily_sales_revenue_bq = DataprocSubmitPySparkJobOperator(
        task_id='run_load_dsr_bq',
        main=f'gs://{bucket_name}/apps/daily_sales_and_returns_bq/app.py',
        dataproc_jars=['gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.28.0.jar'],
        cluster_name = 'lpkadvworks-dsr-drc-cluster-{{ ds_nodash }}',
        dataproc_properties={
            'spark.app.name': 'BigQuery Loader - Daily Sales Revenue',
            'spark.submit.deployMode': 'cluster',
            'spark.yarn.appMasterEnv.DATA_URI': f'gs://{bucket_name}/sales_gold.db/daily_product_revenue',
            'spark.yarn.appMasterEnv.PROJECT_ID': project_id,
            'spark.yarn.appMasterEnv.DATASET_NAME': 'advworks',
            'spark.yarn.appMasterEnv.GCS_TEMP_BUCKET': bucket_name,
            'spark.yarn.appMasterEnv.TABLE_NAME': 'daily_sales_revenue'
        },
    )
    task_load_daily_returns_cost = DataprocSubmitPySparkJobOperator(
        task_id='run_load_drc_bq',
        main=f'gs://{bucket_name}/apps/daily_sales_and_returns_bq/app.py',
        dataproc_jars=['gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.28.0.jar'],
        cluster_name = 'lpkadvworks-dsr-drc-cluster-{{ ds_nodash }}',
        dataproc_properties={
            'spark.app.name': 'BigQuery Loader - Daily Returns Cost',
            'spark.submit.deployMode': 'cluster',
            'spark.yarn.appMasterEnv.DATA_URI': f'gs://lpkadvworks/returns_gold.db/daily_returns_cost',
            'spark.yarn.appMasterEnv.PROJECT_ID': project_id,
            'spark.yarn.appMasterEnv.DATASET_NAME': 'advworks',
            'spark.yarn.appMasterEnv.GCS_TEMP_BUCKET': bucket_name,
            'spark.yarn.appMasterEnv.TABLE_NAME': 'daily_returns_cost'
        },
    )
    task_delete_cluster = DataprocDeleteClusterOperator(
        task_id = 'delete_cluster',
        project_id=project_id,
        cluster_name = 'lpkadvworks-dsr-drc-cluster-{{ ds_nodash }}',
        trigger_rule = 'all_done',
    )
    
    task_create_cluster >> task_clean_up
    
    task_clean_up >> task_convert_customers
    task_clean_up >> task_convert_territories
    task_clean_up >> task_convert_categories
    task_clean_up >> task_convert_subcategories
    task_clean_up >> task_convert_products
    task_clean_up >> task_convert_returns
    task_clean_up >> task_convert_sales
    
    task_convert_customers >> task_compute_daily_sales_revenue
    task_convert_territories >> task_compute_daily_sales_revenue
    task_convert_categories >> task_compute_daily_sales_revenue
    task_convert_subcategories >> task_compute_daily_sales_revenue
    task_convert_products >> task_compute_daily_sales_revenue
    task_convert_returns >> task_compute_daily_sales_revenue
    task_convert_sales >> task_compute_daily_sales_revenue
    
    task_convert_customers >> task_compute_daily_returns_cost
    task_convert_territories >> task_compute_daily_returns_cost
    task_convert_categories >> task_compute_daily_returns_cost
    task_convert_subcategories >> task_compute_daily_returns_cost
    task_convert_products >> task_compute_daily_returns_cost
    task_convert_returns >> task_compute_daily_returns_cost
    task_convert_sales >> task_compute_daily_returns_cost
    
    task_compute_daily_sales_revenue >> task_load_daily_sales_revenue_bq
    task_compute_daily_returns_cost >> task_load_daily_returns_cost
    
    task_load_daily_sales_revenue_bq >> task_delete_cluster
    task_load_daily_returns_cost >> task_delete_cluster
    
    
    
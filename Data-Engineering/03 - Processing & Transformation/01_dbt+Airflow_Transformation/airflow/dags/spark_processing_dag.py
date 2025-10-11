"""
Spark processing DAG for heavy transformations
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import time

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def measure_spark_performance(**context):
    """Measure Spark job performance metrics"""
    task_instance = context['task_instance']
    
    # Get execution time from previous task
    spark_start = context['ti'].xcom_pull(
        task_ids='customer_aggregation',
        key='start_time'
    )
    
    if spark_start:
        duration = time.time() - spark_start
        
        metrics = {
            'job_name': 'customer_aggregation',
            'duration_seconds': duration,
            'duration_minutes': round(duration / 60, 2),
            'records_processed': 10000,  # Would get from Spark metrics
            'throughput_records_per_sec': 10000 / duration
        }
        
        print(f"Spark job metrics: {metrics}")
        return metrics
    
    return {}

with DAG(
    'spark_transformation_pipeline',
    default_args=default_args,
    description='Spark-based data processing for large-scale transformations',
    schedule_interval='0 */6 * * *',  # Every 6 hours
    start_date=days_ago(1),
    catchup=False,
    tags=['spark', 'big-data', 'transformation'],
) as dag:
    
    # Customer aggregation Spark job
    customer_aggregation = SparkSubmitOperator(
        task_id='customer_aggregation',
        application='/opt/airflow/spark_jobs/transformations/customer_aggregations.py',
        name='customer-aggregation-{{ ds }}',
        conn_id='spark_default',
        conf={
            'spark.executor.memory': '4g',
            'spark.executor.cores': '2',
            'spark.executor.instances': '3',
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
        },
        application_args=[
            '--input-path', 's3://data-lake/raw/customers/',
            '--output-path', 's3://data-lake/processed/customer_aggregations/',
            '--date', '{{ ds }}'
        ],
        verbose=True,
        driver_memory='2g'
    )
    
    # Order enrichment Spark job
    order_enrichment = SparkSubmitOperator(
        task_id='order_enrichment',
        application='/opt/airflow/spark_jobs/transformations/order_enrichment.py',
        name='order-enrichment-{{ ds }}',
        conn_id='spark_default',
        conf={
            'spark.executor.memory': '4g',
            'spark.executor.cores': '2',
            'spark.executor.instances': '4',
            'spark.sql.shuffle.partitions': '200',
        },
        application_args=[
            '--orders-path', 's3://data-lake/raw/orders/',
            '--products-path', 's3://data-lake/raw/products/',
            '--output-path', 's3://data-lake/processed/enriched_orders/',
            '--date', '{{ ds }}'
        ],
        verbose=True
    )
    
    # Incremental processing job
    incremental_processor = SparkSubmitOperator(
        task_id='incremental_processor',
        application='/opt/airflow/spark_jobs/transformations/incremental_processor.py',
        name='incremental-processor-{{ ds }}',
        conn_id='spark_default',
        conf={
            'spark.executor.memory': '4g',
            'spark.dynamicAllocation.enabled': 'true',
            'spark.sql.adaptive.enabled': 'true',
        },
        application_args=[
            '--input-path', 's3://data-lake/raw/events/',
            '--checkpoint-path', 's3://data-lake/checkpoints/events/',
            '--output-path', 's3://data-lake/processed/events_aggregated/',
            '--date', '{{ ds }}'
        ],
        verbose=True
    )
    
    measure_performance = PythonOperator(
        task_id='measure_performance',
        python_callable=measure_spark_performance,
        provide_context=True
    )
    
    # Dependencies
    [customer_aggregation, order_enrichment] >> incremental_processor >> measure_performance
"""
Advanced dbt Transformation DAG with monitoring and testing
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import datetime, timedelta
import json
import time

# Custom operators (would be in plugins/operators/)
from airflow.providers.dbt.cloud.operators.dbt import (
    DbtRunOperator,
    DbtTestOperator,
    DbtSeedOperator
)

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email': ['data-eng@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

def record_start_metrics(**context):
    """Record pipeline start time and context"""
    metrics = {
        'dag_id': context['dag'].dag_id,
        'execution_date': str(context['execution_date']),
        'start_time': datetime.now().isoformat(),
        'run_id': context['run_id']
    }
    
    # Store in XCom for later use
    context['ti'].xcom_push(key='start_metrics', value=metrics)
    print(f"Pipeline started: {json.dumps(metrics, indent=2)}")
    
    return metrics

def record_end_metrics(**context):
    """Calculate and record pipeline metrics"""
    start_metrics = context['ti'].xcom_pull(
        task_ids='record_start', 
        key='start_metrics'
    )
    
    end_time = datetime.now()
    start_time = datetime.fromisoformat(start_metrics['start_time'])
    duration_seconds = (end_time - start_time).total_seconds()
    
    end_metrics = {
        **start_metrics,
        'end_time': end_time.isoformat(),
        'duration_seconds': duration_seconds,
        'duration_minutes': round(duration_seconds / 60, 2),
        'status': 'success'
    }
    
    # In production, send to monitoring system
    print(f"Pipeline completed: {json.dumps(end_metrics, indent=2)}")
    
    # Store metrics for cost calculation
    context['ti'].xcom_push(key='end_metrics', value=end_metrics)
    
    return end_metrics

def calculate_pipeline_cost(**context):
    """Calculate estimated pipeline cost"""
    end_metrics = context['ti'].xcom_pull(
        task_ids='record_end',
        key='end_metrics'
    )
    
    duration_hours = end_metrics['duration_seconds'] / 3600
    
    # Cost assumptions (adjust based on your infrastructure)
    costs = {
        'compute_cost_per_hour': 2.50,  # e.g., medium instance
        'storage_read_cost': 0.10,      # per GB
        'storage_write_cost': 0.15,     # per GB
    }
    
    # Estimate based on duration
    estimated_cost = {
        'compute_cost': duration_hours * costs['compute_cost_per_hour'],
        'storage_cost': 0.50,  # Placeholder - would calculate actual
        'total_cost': (duration_hours * costs['compute_cost_per_hour']) + 0.50,
        'currency': 'USD'
    }
    
    print(f"Estimated pipeline cost: ${estimated_cost['total_cost']:.2f}")
    
    return estimated_cost

def validate_data_freshness(**context):
    """Validate that source data is fresh enough"""
    # In production, query source systems
    current_time = datetime.now()
    last_updated = current_time - timedelta(hours=2)  # Simulated
    
    freshness_hours = (current_time - last_updated).total_seconds() / 3600
    max_freshness_hours = 24
    
    if freshness_hours > max_freshness_hours:
        raise ValueError(
            f"Data too stale: {freshness_hours:.1f}h old "
            f"(max: {max_freshness_hours}h)"
        )
    
    print(f"Data freshness check passed: {freshness_hours:.1f}h old")
    return {'freshness_hours': freshness_hours}

with DAG(
    'dbt_transformation_pipeline',
    default_args=default_args,
    description='Complete dbt transformation pipeline with testing and monitoring',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    start_date=days_ago(1),
    catchup=False,
    tags=['dbt', 'transformation', 'analytics'],
    max_active_runs=1,
    doc_md="""
    # dbt Transformation Pipeline
    
    This DAG orchestrates the complete data transformation process:
    1. Validates data freshness
    2. Loads seed data
    3. Runs staging models
    4. Runs intermediate models
    5. Runs marts models
    6. Runs incremental models
    7. Executes data quality tests
    8. Generates documentation
    9. Records metrics
    
    ## SLA
    - Expected duration: 15-30 minutes
    - Max duration: 2 hours
    - Freshness requirement: Data < 24h old
    
    ## Alerts
    - Failures: Email + Slack
    - Duration > 1h: Warning alert
    - Test failures: Immediate notification
    """
) as dag:
    
    # Start monitoring
    record_start = PythonOperator(
        task_id='record_start',
        python_callable=record_start_metrics,
        provide_context=True
    )
    
    # Validate prerequisites
    check_data_freshness = PythonOperator(
        task_id='check_data_freshness',
        python_callable=validate_data_freshness,
        provide_context=True
    )
    
    # dbt seed (load CSV files)
    dbt_seed = BashOperator(
        task_id='dbt_seed',
        bash_command='cd /opt/airflow/dbt_project && dbt seed --profiles-dir .',
        env={
            'DBT_PROFILES_DIR': '/opt/airflow/dbt_project',
            'DBT_TARGET': '{{ var.value.dbt_target }}'
        }
    )
    
    # Task group for staging models
    with TaskGroup('staging_models', tooltip='Run staging layer models') as staging:
        
        run_stg_customers = BashOperator(
            task_id='run_stg_customers',
            bash_command='cd /opt/airflow/dbt_project && dbt run --models stg_customers',
        )
        
        run_stg_orders = BashOperator(
            task_id='run_stg_orders',
            bash_command='cd /opt/airflow/dbt_project && dbt run --models stg_orders',
        )
        
        run_stg_products = BashOperator(
            task_id='run_stg_products',
            bash_command='cd /opt/airflow/dbt_project && dbt run --models stg_products',
        )
        
        test_staging = BashOperator(
            task_id='test_staging',
            bash_command='cd /opt/airflow/dbt_project && dbt test --models tag:staging',
        )
        
        # Staging models can run in parallel
        [run_stg_customers, run_stg_orders, run_stg_products] >> test_staging
    
    # Task group for intermediate models
    with TaskGroup('intermediate_models', tooltip='Run intermediate layer') as intermediate:
        
        run_int_customer_orders = BashOperator(
            task_id='run_int_customer_orders',
            bash_command='cd /opt/airflow/dbt_project && dbt run --models int_customer_orders',
        )
        
        run_int_order_items = BashOperator(
            task_id='run_int_order_items',
            bash_command='cd /opt/airflow/dbt_project && dbt run --models int_order_items',
        )
        
        # Intermediate models run in parallel
        [run_int_customer_orders, run_int_order_items]
    
    # Task group for marts models
    with TaskGroup('marts_models', tooltip='Run marts layer models') as marts:
        
        run_dim_customers = BashOperator(
            task_id='run_dim_customers',
            bash_command='cd /opt/airflow/dbt_project && dbt run --models dim_customers',
        )
        
        run_fct_orders = BashOperator(
            task_id='run_fct_orders',
            bash_command='cd /opt/airflow/dbt_project && dbt run --models fct_orders',
        )
        
        test_marts = BashOperator(
            task_id='test_marts',
            bash_command='cd /opt/airflow/dbt_project && dbt test --models tag:marts',
        )
        
        [run_dim_customers, run_fct_orders] >> test_marts
    
    # Task group for incremental metrics
    with TaskGroup('metrics_models', tooltip='Run incremental metrics') as metrics:
        
        run_customer_ltv = BashOperator(
            task_id='run_customer_ltv',
            bash_command='cd /opt/airflow/dbt_project && dbt run --models customer_lifetime_value',
        )
        
        run_daily_revenue = BashOperator(
            task_id='run_daily_revenue',
            bash_command='cd /opt/airflow/dbt_project && dbt run --models daily_revenue',
        )
        
        test_metrics = BashOperator(
            task_id='test_metrics',
            bash_command='cd /opt/airflow/dbt_project && dbt test --models tag:metrics',
        )
        
        [run_customer_ltv, run_daily_revenue] >> test_metrics
    
    # Generate documentation
    generate_docs = BashOperator(
        task_id='generate_docs',
        bash_command='cd /opt/airflow/dbt_project && dbt docs generate',
    )
    
    # Custom data quality checks
    def run_custom_quality_checks(**context):
        """Run additional custom data quality checks"""
        checks_passed = []
        checks_failed = []
        
        # Check 1: Revenue reconciliation
        # In production, would query actual tables
        expected_revenue = 5000
        actual_revenue = 4980  # Simulated query result
        variance = abs(expected_revenue - actual_revenue) / expected_revenue
        
        if variance < 0.05:  # 5% tolerance
            checks_passed.append('revenue_reconciliation')
        else:
            checks_failed.append(f'revenue_reconciliation: {variance:.1%} variance')
        
        # Check 2: Row count validation
        expected_min_orders = 10
        actual_orders = 15  # Simulated
        
        if actual_orders >= expected_min_orders:
            checks_passed.append('row_count_validation')
        else:
            checks_failed.append(
                f'row_count_validation: only {actual_orders} orders'
            )
        
        results = {
            'passed': checks_passed,
            'failed': checks_failed,
            'total_checks': len(checks_passed) + len(checks_failed),
            'pass_rate': len(checks_passed) / (len(checks_passed) + len(checks_failed))
        }
        
        if checks_failed:
            print(f"Quality checks failed: {checks_failed}")
            raise ValueError(f"Data quality checks failed: {checks_failed}")
        
        print(f"All quality checks passed: {checks_passed}")
        return results
    
    custom_quality_checks = PythonOperator(
        task_id='custom_quality_checks',
        python_callable=run_custom_quality_checks,
        provide_context=True
    )
    
    # End monitoring and metrics
    record_end = PythonOperator(
        task_id='record_end',
        python_callable=record_end_metrics,
        provide_context=True
    )
    
    calculate_cost = PythonOperator(
        task_id='calculate_cost',
        python_callable=calculate_pipeline_cost,
        provide_context=True
    )
    
    # Success notification (would use actual Slack webhook in production)
    success_notification = EmptyOperator(
        task_id='success_notification',
        trigger_rule='all_success'
    )
    
    # Failure notification
    failure_notification = EmptyOperator(
        task_id='failure_notification',
        trigger_rule='one_failed'
    )
    
    # Define dependencies
    (
        record_start >> 
        check_data_freshness >> 
        dbt_seed >> 
        staging >> 
        intermediate >> 
        marts >> 
        metrics >> 
        generate_docs >>
        custom_quality_checks >>
        record_end >>
        calculate_cost >>
        success_notification
    )
    
    # Failure path
    [
        check_data_freshness,
        dbt_seed,
        staging,
        intermediate,
        marts,
        metrics,
        custom_quality_checks
    ] >> failure_notification
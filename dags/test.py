from functions.transform_data import transform_initial_data
from functions.extract_data import extract_initial_data
from functions.load_data import load_data
import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Add the parent directory of 'dags' to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


with DAG(
    'etl_redshift_initial_load_dag',
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
    },
    description='ETL pipeline to extract, transform, and load initial data into\
        Redshift (runs only once)',
    schedule_interval=None,
    start_date=days_ago(1),
    # end_date=datetime(2024, 10, 1),
    catchup=False,
) as dag:

    # Task 1: Extract data
    extract_task = PythonOperator(
        task_id='extract_initial_data',
        python_callable=extract_initial_data,
        provide_context=True,
    )

    # Task 2: Transform data
    transform_task = PythonOperator(
        task_id='transform_initial_data',
        python_callable=transform_initial_data,
        provide_context=True,
    )

    # Task 3: Load data into Redshift
    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        provide_context=True,
        op_kwargs={
            "table_name_devolutions": "devolutions",
            "table_name_pdf": "pdf_devolutions",
        },
    )

    # Set task dependencies
    extract_task >> transform_task >> load_task

import sys
import os

# Add the parent directory of 'dags' to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from functions.transform_data import transform_data  # noqa: E402
from functions.extract_data import extract_data  # noqa: E402
from functions.load_data import load_data  # noqa: E402
from airflow import DAG  # noqa: E402
from airflow.operators.python import PythonOperator, BranchPythonOperator  # noqa: E402
from airflow.operators.dummy import DummyOperator  # noqa: E402
from airflow.utils.dates import days_ago  # noqa: E402


def check_dataframes(**context):
    ti = context['ti']
    df, df_tables = ti.xcom_pull(task_ids='extract_data')

    # If both DataFrames are empty, abort
    if df.empty and df_tables.empty:
        return 'skip_transform_load'
    return 'transform_data'  # Continue with the normal flow


with DAG(
    'etl_redshift_load_dag',
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
    },
    description='ETL pipeline to extract, transform, and load data into Redshift (runs daily at 4 AM)',
    schedule_interval='0 4 * * *',  # This cron expression means "At 4:00 AM, every day"
    start_date=days_ago(1),
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        provide_context=True,
        op_kwargs={
            # For testing purposes, we provide a list of dates with data
            # to avoid running the task for all dates (not all days have data)
            # 1/07/2024 | 26/06/2024 | 8/09/2024 | DD/MM/2024
            "custom_date": "10/10/2024",
        },
    )

    check_task = BranchPythonOperator(
        task_id='check_dataframes',
        python_callable=check_dataframes,
        provide_context=True,
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        provide_context=True,
        op_kwargs={
            "table_name_devolutions": "devolutions",
            "table_name_pdf": "pdf_devolutions",
        },
    )

    skip_task = DummyOperator(
        task_id='skip_transform_load'
    )

    extract_task >> check_task
    check_task >> transform_task >> load_task
    check_task >> skip_task

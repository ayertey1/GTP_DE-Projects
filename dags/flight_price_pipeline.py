from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import sys
sys.path.append('/opt/airflow/scripts')
from datetime import datetime, timedelta

# Import scripts
from validate_data import validate_data
from transform_data import transform_data
from compute_kpis import compute_kpis
from load_csv_to_mysql import load_csv_to_mysql
from load_to_postgres import load_to_postgres


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 16) - timedelta(minutes=5), 
    'retries': 2,
    'retry_delay': timedelta(minutes=1),  # Optional: wait 1 min before retry
    'depends_on_past': False,
}

with DAG(
    dag_id='flight_price_pipeline_Peter_Caleb_Ayertey',
    default_args=default_args,
    # schedule_interval='*/2 * * * *',  # Every 2 minutes CRON expression for testing
    schedule_interval='0 */3 * * *', # At minute 0, every 3rd hour 
    catchup=False,
    description='Flight Price Analysis Pipeline for Bangladesh'
) as dag:

    task_load_csv_to_mysql = PythonOperator(
        task_id='load_csv_to_mysql',
        python_callable=load_csv_to_mysql
    )

    task_validate_data = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data
    )

    task_transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    task_compute_kpis = PythonOperator(
        task_id='compute_kpis',
        python_callable=compute_kpis
    )

    task_load_to_postgres = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres
    )

    # Define pipeline order
    task_load_csv_to_mysql >> task_validate_data >> task_transform_data >> task_compute_kpis >> task_load_to_postgres

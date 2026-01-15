from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

sys.path.append("/opt/airflow/src")
from extract import extract_data

with DAG(
    "psg_extract",
    start_date=datetime(2026, 1, 15),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    
    t1_extract = PythonOperator(
        task_id="extract_psg_data",
        python_callable=extract_data,
    )


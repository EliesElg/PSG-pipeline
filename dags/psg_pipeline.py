from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

sys.path.append("/opt/airflow/src")
from extract import extract_data
from load import  from_s3_to_bq
with DAG(
    "psg_extract",
    start_date=datetime(2026, 1, 15),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    
    t1_extract = PythonOperator(
        task_id="extract_psg_data",
        python_callable=extract_data,
        op_kwargs={"date_dag": '{{ ds }}'}
    )

    t2_load = PythonOperator(
        task_id="load_data_to_bq",
        python_callable=from_s3_to_bq,
        op_kwargs={"date_dag": '{{ ds }}'}
    )
    
    t1_extract >> t2_load
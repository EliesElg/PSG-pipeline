from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import sys

sys.path.append("/opt/airflow/src")
from extract import extract_data
from load import from_s3_to_bq

with DAG(
    "psg_extract",
    start_date=datetime(2026, 1, 15),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    t1_extract = PythonOperator(
        task_id="extract_psg_data",
        python_callable=extract_data,
        op_kwargs={"date_dag": "{{ ds }}"},
    )

    t2_load = PythonOperator(
        task_id="load_data_to_bq",
        python_callable=from_s3_to_bq,
        op_kwargs={"date_dag": "{{ ds }}"},
    )

    t3_dbt_run = BashOperator(
        task_id="dbt_run_models",
        bash_command="cd /opt/airflow/transformation && dbt run --profiles-dir .",
    )

    t4_dbt_test = BashOperator(
        task_id="dbt_test_quality",
        bash_command="cd /opt/airflow/transformation && dbt test --profiles-dir .",
    )

    t1_extract >> t2_load >> t3_dbt_run >> t4_dbt_test

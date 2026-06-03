from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import sys

sys.path.append('/opt/airflow/scripts')

def trigger_producer():
    from producers.load_bmkg_to_kafka import run_producer
    run_producer()

def trigger_sheets_producer():
    from producers.load_google_sheets_to_kafka import run_producer
    run_producer()

with DAG(
    dag_id='flow_pipeline_medallion',
    start_date=datetime(2025, 5, 11),
    schedule='@hourly',
    catchup=False,
    tags=['kafka', 'spark', 'medallion']
) as dag:

    ingest_task_bmkg = PythonOperator(
        task_id='run_scraping_producer',
        python_callable=trigger_producer
    )

    ingest_task_resepsi = PythonOperator(
        task_id='run_sheets_incremental_producer',
        python_callable=trigger_sheets_producer
    )

ingest_task_bmkg >> ingest_task_resepsi
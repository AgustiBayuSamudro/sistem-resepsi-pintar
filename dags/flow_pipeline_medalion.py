from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import sys

# Menambahkan path ke folder scripts Airflow
sys.path.append('/opt/airflow/scripts')

with DAG(
    dag_id='flow_pipeline_medallion_batch',
    start_date=datetime(2025, 5, 11),
    # Scheduler diubah ke Cron Expression untuk jalan setiap 1 menit
    schedule='*/1 * * * *', 
    catchup=False,
    max_active_runs=1,
    tags=['spark', 'minio', 'medallion', 'batch']
) as dag:

    ingest_task_bmkg = BashOperator(
        task_id='run_bmkg_batch_ingestion',
        bash_command='python /opt/airflow/scripts/ingestion/load_bmkg_to_minio.py'
    )

    ingest_task_resepsi = BashOperator(
        task_id='run_sheets_batch_ingestion',
        bash_command='python /opt/airflow/scripts/ingestion/load_google_sheets_to_minio.py'
    )
    
ingest_task_bmkg
ingest_task_resepsi
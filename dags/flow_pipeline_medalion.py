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
    
    bronze_task_bmkg = BashOperator(
        task_id='run_bmkg_batch_bronze',
        bash_command='python /opt/airflow/scripts/spark/bronze/bmkg_bronze.py'
    )

    bronze_task_undangan = BashOperator(
        task_id='run_sheets_batch_undangan_bronze',
        bash_command='python /opt/airflow/scripts/spark/bronze/undangan_bronze.py'
    )

    bronze_task_tamu = BashOperator(
        task_id='run_sheets_batch_tamu_bronze',
        bash_command='python /opt/airflow/scripts/spark/bronze/tamu_bronze.py'
    )

    silver_task_cuaca = BashOperator(
        task_id='run_cuaca_batch_silver',
        bash_command='python /opt/airflow/scripts/spark/silver/cuaca_silver.py'
    )

    silver_task_lokasi = BashOperator(
        task_id='run_lokasi_batch_silver',
        bash_command='python /opt/airflow/scripts/spark/silver/lokasi_silver.py'
    )

    silver_task_undangan = BashOperator(
        task_id='run_sheets_batch_undangan_silver',
        bash_command='python /opt/airflow/scripts/spark/silver/undangan_silver.py'
    )

    silver_task_tamu = BashOperator(
        task_id='run_sheets_batch_tamu_silver',
        bash_command='python /opt/airflow/scripts/spark/silver/tamu_silver.py'
    )

    gold_task_dimtime = BashOperator(
        task_id='run_dimtime_batch_gold',
        bash_command='python /opt/airflow/scripts/spark/gold/dim_time_gold.py'
    )

    gold_task_fact_kehadiran = BashOperator(
        task_id='run_fact_kehadiran_batch_gold',
        bash_command='python /opt/airflow/scripts/spark/gold/fact_kehadiran_gold.py'
    )

    gold_task_dim_tamu = BashOperator(
        task_id='run_dim_tamu_batch_gold',
        bash_command='python /opt/airflow/scripts/spark/gold/dim_tamu_gold.py'
    )
    gold_task_dim_undangan = BashOperator(
        task_id='run_dim_undangan_batch_gold',
        bash_command='python /opt/airflow/scripts/spark/gold/dim_undangan_gold.py'
    )
    ingest_task_bmkg >> bronze_task_bmkg >> [silver_task_lokasi, silver_task_cuaca]  
    ingest_task_resepsi >> [bronze_task_undangan, bronze_task_tamu] >> silver_task_undangan >> silver_task_tamu >> gold_task_dimtime >> gold_task_fact_kehadiran >> gold_task_dim_tamu >> gold_task_dim_undangan
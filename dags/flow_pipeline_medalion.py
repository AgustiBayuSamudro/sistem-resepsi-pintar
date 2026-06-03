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

    bmkg_bronze_task = BashOperator(
        task_id='transform_data_kafka_bmkg_to_bronze',
        bash_command="""
        docker exec -t spark-master spark-submit \
        --master spark://spark-master:7077 \
        --driver-memory 512m \
        --executor-memory 512m \
        --conf spark.jars.ivy=/tmp/.ivy \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.apache.hadoop:hadoop-aws:3.3.4 \
        /opt/spark/scripts/spark/bronze/bmkg_bronze.py
        """
    )

    resepsi_bronze_task = BashOperator(
        task_id='transform_data_kafka_resepsi_to_bronze',
        bash_command="""
        docker exec -t spark-master spark-submit \
        --master spark://spark-master:7077 \
        --driver-memory 512m \
        --executor-memory 512m \
        --conf spark.jars.ivy=/tmp/.ivy \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-core_2.12:2.4.0 \
        /opt/spark/scripts/spark/bronze/resepsi_bronze.py
        """
    )

ingest_task_bmkg >> ingest_task_resepsi >> [bmkg_bronze_task, resepsi_bronze_task]
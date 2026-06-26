import airflow as af
from airflow import DAG
from airflow.operators import PythonOperator
from airflow.operators import BashOperator
from datetime import datetime

from src.gold.data_processing_gold import process_data_per_month, load_data, save_data
from src.bronze.data_processing_bronze import process_data_per_month, load_data, save_data
from src.feature_sql_loader.load_to_sql import load_data_to_sql
from src.ingestion.download_kaggle import download_kaggle, spark_ingest_data

default_args = {
    "owner": "Krishna", 
    "start_date": datetime(2026, 6, 1)
}

with DAG(
    dag_id="retail_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
) as dag:
    ingest_data = PythonOperator(
        task_id="ingest_data",  
        python_callable=download_kaggle 
    )

    bronze= PythonOperator(
        task_id="bronze",   
        python_callable=data_processing_bronze
    )

    silver = PythonOperator(
        task_id="silver",   
        python_callable=data_processing_gold
    )

    gold = PythonOperator(
        task_id="gold", 
        python_callable=load_to_sql
    )

    ingest_data >> bronze >> silver >> gold
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os
import pandas as pd

# Ensure weather.py is in the path
sys.path.append(os.path.dirname(__file__))
from weather import extract, transform, load

default_args = {
    'start_date': datetime(2024, 1, 1),
    'catchup': False
}

with DAG("my_weather_dag", schedule_interval="@daily", default_args=default_args, tags=["weather"], description="ETL for weather data") as dag:

    t1 = PythonOperator(
        task_id="extract",
        python_callable=extract
    )

    def transform_task(ti):
        json_data = ti.xcom_pull(task_ids='extract')
        df = transform(json_data)
        ti.xcom_push(key='transformed', value=df.to_json(orient='records'))

    t2 = PythonOperator(
        task_id="transform",
        python_callable=transform_task
    )

    def load_task(ti):
        json_df = ti.xcom_pull(task_ids='transform', key='transformed')
        df = pd.read_json(json_df, orient='records')
        load(df)

    t3 = PythonOperator(
        task_id="load",
        python_callable=load_task
    )

    t1 >> t2 >> t3

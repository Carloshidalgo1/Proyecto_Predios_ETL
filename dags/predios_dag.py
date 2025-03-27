from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator

import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import etl  # Importacion de las funciones de etl.py

# Initiating the default_args
default_args = {
        'owner' : 'airflow',
        'start_date' : datetime(2023, 11, 12),
}
dag = DAG(
    'dag_predios',
    default_args=default_args,
    description='Proceso ETL proyecto_predios',
    schedule_interval='@daily',
    catchup=False
)

extract_predios = PythonOperator(

    task_id='extract_task',

    python_callable=etl.extract,

    dag=dag

)

transform_predios = PythonOperator(

    task_id='transform_task',

    python_callable=etl.transform,

    dag=dag

)

merge_predios = PythonOperator(

    task_id='merge_task',

    python_callable=etl.merge,

    dag=dag

)

load_predios = PythonOperator(

    task_id='load_task',

    python_callable=etl.load,

    dag=dag

)

extract_predios >> transform_predios >> merge_predios >> load_predios
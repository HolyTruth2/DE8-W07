from os import makedirs
from pendulum import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def write_result(ds):
    makedirs('/opt/airflow/logs/backfill', exist_ok=True)
    with open(f'/opt/airflow/logs/backfill/{ds}.txt', 'w') as f:
        f.write(ds)

with DAG(
    dag_id='backfill_demo_leesungjin',
    start_date=datetime(2026, 11, 7, tz='Asia/Seoul'),
    schedule='@daily',
    catchup=True,
) as dag:
    PythonOperator(
        task_id='write_result',
        python_callable=write_result,
    )

from pendulum import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

def calculate(ti):
    if ti.try_number == 1:
        raise ValueError('재시도 확인을 위한 첫 번째 실패')
    return 1 + 1

def receive(ti):
    value = ti.xcom_pull(task_ids='calculate')
    print(f'전달받은 값: {value}')

with DAG(
    dag_id='xcom_demo_leesungjin',
    start_date=datetime(2026, 11, 14, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:
    first = PythonOperator(
        task_id='calculate',
        python_callable=calculate,
        retries=2,
        retry_delay=10
    )
    second = PythonOperator(
        task_id='receive',
        python_callable=receive,
    )

    first >> second
from pendulum import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

def first_message():  return '첫 번째 작업 완료'
def second_message(): return '두 번째 작업 완료'

with DAG(
    dag_id='sample_dag',
    start_date=datetime(2026, 11, 14, tz='Asia/Seoul'),
    schedule='@daily',
    catchup=False,
) as dag:
    start = EmptyOperator(task_id='start')
    first = PythonOperator(
        task_id='first',
        python_callable=first_message,
    )
    second = PythonOperator(
        task_id='second',
        python_callable=second_message,
    )
    end = EmptyOperator(task_id="end")

    start >> first >> second >> end

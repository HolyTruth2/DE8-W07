import os, pendulum, boto3
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

S3 = boto3.client('s3')

def download():
    os.makedirs('/opt/airflow/data', exist_ok=True)
    S3.download_file(
        os.environ['S3_BUCKET'],
        'bronze/netflix_titles.csv',
        '/opt/airflow/data/netflix_titles.csv'
    )

def upload():
    bucket = os.environ['S3_BUCKET']
    today = pendulum.now('Asia/Seoul').to_date_string()
    prefix = f'silver/{today}/'

    for file in Path('/opt/airflow/data/silver').glob('*.parquet'):
        S3.upload_file(str(file), bucket, prefix + file.name)

    count = 0
    for page in S3.get_paginator('list_objects_v2').paginate(
        Bucket=bucket, Prefix=prefix
    ):
        for content in page.get('Contents', []):
            print(content['Key'], content['Size'])
            count += 1

    print('객체 개수:', count)

with DAG(
    dag_id='weekly_pipeline_leesungjin',
    start_date=pendulum.datetime(2026, 11, 14, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:
    download_csv = PythonOperator(
        task_id='download_csv',
        python_callable=download
    )
    transform = BashOperator(
        task_id='transform',
        bash_command='spark-submit --master "local[2]" /opt/airflow/dags/jobs/transform.py 2015'
    )
    upload_silver = PythonOperator(
        task_id='upload_silver',
        python_callable=upload
    )

    download_csv >> transform >> upload_silver

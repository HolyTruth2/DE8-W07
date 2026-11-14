import csv

import os
os.makedirs('data', exist_ok=True)

import sys
bucket = sys.argv[1]

import boto3
s3 = boto3.client('s3')

for page in s3.get_paginator('list_objects_v2').paginate(
    Bucket=bucket, Prefix='bronze/'
):
    for content in page.get('Contents', []):
        print(content['Key'], content['Size'])

s3.download_file(bucket, 'bronze/netflix_titles.csv', 'data/netflix_titles.csv')

with open('data/netflix_titles.csv', encoding='UTF-8') as file:
    print('Rows:', sum(1 for _ in csv.reader(file)) - 1)

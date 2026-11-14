import sys
year = int(sys.argv[1]) if len(sys.argv) > 1 else 2015

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName('NetflixTransform').getOrCreate()

df = spark.read.csv(
    '/opt/airflow/data/netflix_titles.csv',
    header=True,
    multiLine=True,
    escape='"'
)

result = (
    df.filter(F.col('release_year').cast('int') >= year)
    .withColumn('genre', F.explode(F.split('listed_in', ',')))
    .withColumn('genre', F.trim('genre'))
    .groupBy('type', 'genre')
    .count()
)

print('집계 행 수:', result.count())

result.write.mode('overwrite').option('compression', 'snappy').parquet(
    '/opt/airflow/data/silver'
)

spark.stop()
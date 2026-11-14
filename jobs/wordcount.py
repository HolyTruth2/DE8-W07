from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('WordCount').getOrCreate()

words = spark.sparkContext.textFile(
    '/opt/spark/data/wordcount.txt'
).flatMap(lambda line: line.split())

counts = words.map(
    lambda word: (word, 1)
).reduceByKey(lambda a, b: a + b)

result = counts.toDF(['word','count'])
result.orderBy(result['count'].desc(), result['word'].asc()).show(20)

spark.stop()

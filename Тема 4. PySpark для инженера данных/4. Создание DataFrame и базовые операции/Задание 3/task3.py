from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName("Saving DataFrames") \
    .getOrCreate()

df = spark.read.parquet("/user/master/data/snapshots/channels/actual")

df.write.partitionBy("channel_type").mode("append").parquet("/user/s24268544/analytics/test")

df = spark.read.parquet("/user/s24268544/analytics/test")

df.select("channel_type").orderBy("channel_type").distinct().show()

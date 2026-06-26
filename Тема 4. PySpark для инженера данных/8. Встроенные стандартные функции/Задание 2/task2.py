import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder \
                    .master("local") \
                    .appName("Learning DataFrames") \
                    .getOrCreate()

events = spark.read.json("/user/master/data/events/date=2022-05-31")

count = events.filter(F.col('event.message_to').isNotNull()).count()

print(count)

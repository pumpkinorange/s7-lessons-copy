import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder \
                    .master("local") \
                    .appName("Learning DataFrames") \
                    .getOrCreate()

events = spark.read.json("/user/master/data/events/date=2022-05-31")

events = events.withColumn('hour', F.hour('event.datetime')) \
               .withColumn('minute', F.minute('event.datetime')) \
               .withColumn('second', F.second('event.datetime'))

events = events.orderBy(F.col('event.datetime').desc())

events.show(10, True)

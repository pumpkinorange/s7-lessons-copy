import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder \
                    .master("local") \
                    .appName("Learning DataFrames") \
                    .getOrCreate()

events = spark.read.json("/user/master/data/events/date=2022-05-25")

events.filter(F.col('event_type') == 'reaction') \
      .groupby('event.reaction_from') \
      .count() \
      .agg(F.max('count')) \
      .show()

import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder \
                    .master("yarn") \
                    .appName("Learning DataFrames") \
                    .getOrCreate()

# читаем сырые данные (JSON) из слоя Raw
events = spark.read.json("/user/master/data/events")

# складываем в слой ODS в формате Parquet, партицируя по дате и типу события
events.write \
      .partitionBy("date", "event_type") \
      .mode("overwrite") \
      .parquet("/user/s24268544/data/events")

# читаем записанный датафрейм из ODS и выводим последние 10 строк по времени
events_ods = spark.read.parquet("/user/s24268544/data/events")

events_ods.orderBy(F.desc("event.datetime")).show(10)

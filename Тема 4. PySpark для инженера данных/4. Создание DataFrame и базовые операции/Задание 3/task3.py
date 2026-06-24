# импортируем класс SparkSession — точку входа для работы с DataFrame API
from pyspark.sql import SparkSession

# создаём (или получаем уже существующую) сессию Spark
spark = SparkSession.builder \
    .master("local") \
    .appName("Saving DataFrames") \
    .getOrCreate()

# читаем снапшот channels (формат Parquet) — получаем DataFrame с информацией о каналах
df = spark.read.parquet("/user/master/data/snapshots/channels/actual")

# сохраняем DataFrame в Parquet, разбивая файлы по папкам согласно channel_type;
# mode("append") дозаписывает данные, не перезаписывая то, что уже лежит в директории
df.write.partitionBy("channel_type").mode("append").parquet("/user/s24268544/analytics/test")

# читаем то, что только что сохранили, из той же директории — получаем новый DataFrame
df = spark.read.parquet("/user/s24268544/analytics/test")

# оставляем только столбец channel_type, сортируем его, убираем повторы и выводим результат
df.select("channel_type").orderBy("channel_type").distinct().show()

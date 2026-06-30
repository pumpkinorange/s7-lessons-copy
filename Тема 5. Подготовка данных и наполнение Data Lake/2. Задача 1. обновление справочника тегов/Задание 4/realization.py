from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("yarn") \
    .appName("candidates_d7_describe") \
    .getOrCreate()

# результат тестовой партиции за 7 дней
df = spark.read.parquet("/user/s24268544/data/analytics/candidates_d7_pyspark")

# статистика по suggested_count: count -> количество тегов, min, max
df.describe().show()

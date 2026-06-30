from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("yarn") \
    .appName("candidates_d84_describe") \
    .getOrCreate()

# результат тестовой партиции за 84 дня
df = spark.read.parquet("/user/s24268544/data/analytics/candidates_d84_pyspark")

# статистика по suggested_count: count -> количество тегов, min, max
df.describe().show()

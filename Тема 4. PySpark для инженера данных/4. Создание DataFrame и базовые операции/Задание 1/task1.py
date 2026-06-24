from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName("Creating DataFrames") \
    .getOrCreate()

data = [("Max", 55), 
        ("Yan", 53), 
        ("Dmitry", 54), 
        ("Ann", 25)]

df = spark.createDataFrame(data, schema=["Name", "Age"])
df.printSchema()
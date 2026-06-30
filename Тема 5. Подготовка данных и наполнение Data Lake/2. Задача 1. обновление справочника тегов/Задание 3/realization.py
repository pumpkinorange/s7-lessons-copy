from datetime import datetime, timedelta

import pyspark.sql.functions as F
from pyspark.sql import SparkSession


def input_paths(date, depth):
    dt = datetime.strptime(date, '%Y-%m-%d')
    return [
        f"/user/s24268544/data/events/"
        f"date={(dt - timedelta(days=offset)).strftime('%Y-%m-%d')}/"
        f"event_type=message"
        for offset in range(depth)
    ]


spark = SparkSession.builder \
    .master("yarn") \
    .appName("candidates_d84_pyspark") \
    .getOrCreate()

# сообщения за 84 дня, начиная с 2022-05-31
paths = input_paths('2022-05-31', 84)
messages = spark.read.parquet(*paths)

# общедоступные (проверенные) теги
verified_tags = spark.read.parquet("/user/master/data/snapshots/tags_verified/actual")

candidates = messages \
    .where("event.message_channel_to is not null") \
    .selectExpr("event.message_from as user", "explode(event.tags) as tag") \
    .groupBy("tag") \
    .agg(F.countDistinct("user").alias("suggested_count")) \
    .where("suggested_count >= 100") \
    .join(verified_tags, "tag", "left_anti")

candidates.write \
    .mode("overwrite") \
    .parquet("/user/s24268544/data/analytics/candidates_d84_pyspark")

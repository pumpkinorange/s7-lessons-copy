import sys
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def input_paths(date, depth, events_base_path):
    dt = datetime.strptime(date, '%Y-%m-%d')
    return [
        f"{events_base_path}/"
        f"date={(dt - timedelta(days=offset)).strftime('%Y-%m-%d')}/"
        f"event_type=message"
        for offset in range(depth)
    ]


def main():
    date = sys.argv[1]
    depth = int(sys.argv[2])
    threshold = int(sys.argv[3])
    events_base_path = sys.argv[4]
    verified_tags_path = sys.argv[5]
    output_path = sys.argv[6]

    spark = SparkSession.builder \
        .master("yarn") \
        .appName(f"VerifiedTagsCandidatesJob-{date}-d{depth}-cut{threshold}") \
        .getOrCreate()

    # входные данные
    messages = spark.read.parquet(*input_paths(date, depth, events_base_path))
    verified_tags = spark.read.parquet(verified_tags_path)

    # вычисление выходного DataFrame
    candidates = messages \
        .where("event.message_channel_to is not null") \
        .selectExpr("event.message_from as user", "explode(event.tags) as tag") \
        .groupBy("tag") \
        .agg(F.countDistinct("user").alias("suggested_count")) \
        .where(f"suggested_count >= {threshold}") \
        .join(verified_tags, "tag", "left_anti") \
        .withColumn("date", F.lit(date))

    # запись с партиционированием по дате -> output_path/date=<date>
    candidates.write \
        .mode("overwrite") \
        .partitionBy("date") \
        .parquet(output_path)


if __name__ == "__main__":
    main()

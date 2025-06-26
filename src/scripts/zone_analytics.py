import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.window import Window

def main():
    input_path = sys.argv[1]  # путь хранения event_city
    output_path = sys.argv[2] # путь для создания zone_analytics

    conf = SparkConf().setAppName("ZoneAnalytics")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

# 1 загрузка данных с городами
    event_cities = sql.read.parquet(input_path)

# 2 добавляем поля неделя и месяц
    df = event_cities.withColumn(
        "month", F.date_format(F.col("date"), "yyyy-MM"))\
        .withColumn("week", F.date_format(F.col("date"), "yyyy-'W'ww"))

# 3 через оконную функицю считаем количество различных событий за неделю
    weekly_stats = df.groupBy("week", "month", "nearest_city").agg(
        F.sum(F.when(F.col("event_type") == "message", 1).otherwise(0)).alias("week_message"),
        F.sum(F.when(F.col("event_type") == "reaction", 1).otherwise(0)).alias("week_reaction"),
        F.sum(F.when(F.col("event_type") == "subscription", 1).otherwise(0)).alias("week_subscription"),
        F.sum(F.when(F.col("event_type") == "user", 1).otherwise(0)).alias("week_user")
    )

# 4 через оконную функицю считаем количество различных событий за месяц
    monthly_stats = df.groupBy("month", "nearest_city").agg(
        F.sum(F.when(F.col("event_type") == "message", 1).otherwise(0)).alias("month_message"),
        F.sum(F.when(F.col("event_type") == "reaction", 1).otherwise(0)).alias("month_reaction"),
        F.sum(F.when(F.col("event_type") == "subscription", 1).otherwise(0)).alias("month_subscription"),
        F.sum(F.when(F.col("event_type") == "user", 1).otherwise(0)).alias("month_user")
    )

# 5 объединеняем результаты
    result = weekly_stats.join(monthly_stats, ["month", "nearest_city"], "left")\
    .select(
        "month",
        "week",
        F.col("nearest_city").alias("zone_id"),
        "week_message",
        "week_reaction",
        "week_subscription",
        "week_user",
        "month_message",
        "month_reaction",
        "month_subscription",
        "month_user")

# 6 сохранение результатов
    result.write.mode("overwrite").parquet(output_path)

if __name__ == "__main__":
    main()
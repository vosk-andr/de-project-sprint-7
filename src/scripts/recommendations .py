import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.window import Window

def main():
    input_path_events_city = sys.argv[1] # путь хранения event_city
    input_path_user_cities = sys.argv[2] # путь хранения user_cities
    output_path = sys.argv[3] # путь для создания recommendations

    conf = SparkConf().setAppName("Recommendations")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

# 1 загрузжаем данные
    events_city = sql.read.parquet(input_path_events_city)
    user_cities = sql.read.parquet(input_path_user_cities)

# 2 находим подписки юзеров с их актуальными городами
    subscriptions = events_city.filter(F.col("event_type") == "subscription")\
        .select(F.col("event.user").alias("user_id"), F.col("event.subscription_channel").alias("channel_id"))\
        .join(user_cities.select("user_id", "act_city", "local_time"), "user_id", "left")

# 3 находим пользователей в одних каналах
    channel_pairs = subscriptions.alias("s1")\
        .join(subscriptions.alias("s2"), 
        (F.col("s1.channel_id") == F.col("s2.channel_id")) & (F.col("s1.user_id") < F.col("s2.user_id")))\
        .select(
            F.col("s1.user_id").alias("user_left"),
            F.col("s2.user_id").alias("user_right"),
            F.col("s1.act_city").alias("city_left"),
            F.col("s2.act_city").alias("city_right"),
            F.col("s1.local_time").alias("local_time"))

# 4 отфильтровываем по одному городу
    same_city_pairs = channel_pairs.filter(F.col("city_left") == F.col("city_right"))\
    .withColumnRenamed("city_left", "zone_id")

# 5 находим тех кто друг другу уже писал и исключаем их
    message_pairs = events_city.filter(F.col("event_type") == "message")\
        .select(
            F.least(F.col("event.message_from"), F.col("event.message_to")).alias("user1"),
            F.greatest(F.col("event.message_from"), F.col("event.message_to")).alias("user2")
            ).distinct()

    recommendations = same_city_pairs.join(message_pairs,
        (F.col("user_left") == F.col("user1")) & (F.col("user_right") == F.col("user2")), "left_anti")

# 6 Добавляем дату расчёта витрины и выаодим необходимые поля
    final_recommendations = recommendations.withColumn("processed_dttm", F.current_timestamp())\
    .select("user_left", "user_right", "processed_dttm", "zone_id", "local_time")

# 7 записываем результат
    final_recommendations.write.mode("overwrite").parquet(output_path)

if __name__ == "__main__":
    main()
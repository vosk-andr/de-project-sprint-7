import sys
 
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.window import Window
 
def main():
        input_path = sys.argv[1] # путь хранения event_city
        output_path = sys.argv[2] # путь для создания user_city

        conf = SparkConf().setAppName(f"UserSity")
        sc = SparkContext(conf=conf)
        sql = SQLContext(sc)

# 1 загрузка подготовленных данных сообщений с городами
        event_with_cities = sql.read.parquet(input_path)

# 2 определение актуального города (последнее событие) (плюс добавлено поле event_time для последующего жобавления таймзоны)
        window_spec_last = Window.partitionBy("user_id").orderBy(F.col("date").desc(), F.col("event_time").desc())
        last_event_city = (event_with_cities
                .withColumn("rn", F.row_number().over(window_spec_last))
                .filter(F.col("rn") == 1)
                .select(F.col("user_id"), F.col("nearest_city").alias("act_city"), F.col("event_time")))
        
# 3 определение домашнего адреса
        # 3.1 через оконные функции находим непрерывную цпочку событий, высчитываем ее продолжительность, и отфилтровываем те что меньше 27 дней 
        window_spec_city = Window.partitionBy("user_id", "nearest_city").orderBy("date")
        home_candidates = (
                event_with_cities
                .select("user_id", "nearest_city", "date")
                .distinct()
                .withColumn("prev_date", F.lag("date").over(window_spec_city))
                .withColumn("date_diff", F.datediff("date", "prev_date"))
                .groupBy("user_id", "nearest_city")
                .agg(F.count("date").alias("days_in_city"), F.max("date_diff").alias("max_gap"))
                .filter((F.col("days_in_city") >= 27) & (F.col("max_gap") <= 1)))
        
        # 3.2 определяем домашний адрес как место где события соверщались дольше всего
        window_spec_longest = Window.partitionBy("user_id").orderBy(F.col("days_in_city").desc())
        home_city = (home_candidates
                .withColumn("rn", F.row_number().over(window_spec_longest))
                .filter(F.col("rn") == 1)
                .select(F.col("user_id"), F.col("nearest_city").alias("home_city")))

# 4 высчитываем цепочку городов и считаем ее длину, при этом если города повторяются подрад то они в цепочке не учитываются
        travel_window = Window.partitionBy("user_id").orderBy("date", "TIME_UTC")
        user_travel_stats = (event_with_cities
                .withColumn("prev_city", F.lag("nearest_city").over(travel_window))
                .filter(F.col("nearest_city") != F.col("prev_city"))
                .groupBy("user_id")
                .agg(F.count("nearest_city").alias("travel_count"), F.collect_list("nearest_city").alias("travel_array")))
# 5 определение таймзоны
        last_event_city_local_time = (last_event_city
                .withColumn("timezone", F.concat(F.lit("Australia/"), F.col("act_city")))
                .withColumn("local_time", F.from_utc_timestamp(F.col("event_time"), F.col("timezone")))
                .select("user_id", "act_city", "local_time"))

# 6 создание витрины user_cities и ее сохранение
        user_cities = (last_event_city_local_time
                .join(home_city, "user_id", "left")
                .join(user_travel_stats, "user_id", "left")
                .select("user_id", "act_city", "home_city", "travel_count", "travel_array", "local_time"))

        user_cities.write.mode("overwrite").parquet(output_path)

if __name__ == "__main__":
        main()
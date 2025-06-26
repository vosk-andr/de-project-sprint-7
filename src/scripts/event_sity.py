import sys
 
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
 
def main():
        input_path_event = sys.argv[1] # путь хранения event
        input_path_sity = sys.argv[2] # путь хранения geo.csv
        output_path = sys.argv[3] # путь для создания event_city
        fraction = float(sys.argv[4]) # т.к. большой объем данных берем такой процент от всех
        seed = int(sys.argv[5]) # сид отбора, для повторяемости резултатов

        conf = SparkConf().setAppName(f"EventSity")
        sc = SparkContext(conf=conf)
        sql = SQLContext(sc)
# 1 загрузка данных events с геоданными, геоданные плохо прописую тип данных, также создаю алиасы т.к. эти поля повторяются в файле geo
        events = sql.read.parquet(input_path_event)\
        .sample(withReplacement=False, fraction=fraction, seed=seed)\
        .select("event", "event_type", 
                F.col("lat").cast("float").alias("event_lat"), 
                F.col("lon").cast("float").alias("event_lon"),
                "date")
# 2 загрузка данных geo, геоданные цифры с ",", очень плохо распознаются спарком поэтому явно прописую тип данных и заменяю запятую на точку
        cities = (sql.read
          .option("delimiter", ";")
          .option("header", "true")
          .csv(input_path_sity)
          .select(
              "city",
              F.expr("CAST(replace(lat, ',', '.') AS FLOAT)").alias("geo_lat"),
              F.expr("CAST(replace(lng, ',', '.') AS FLOAT)").alias("geo_lng")))
# 3 кросджойном объединяю тадлицу эвент и гео, расчитываю дистанцию каждого события относительно каждого города (одновременно перевожу градусы в радианы) затем вывожу первый город с минимальным растоянием        
        result = events.crossJoin(F.broadcast(cities)).withColumn("distance_km",
                2 * 6371 * F.asin(
                        F.sqrt(
                                F.pow(F.sin(F.radians(F.col("geo_lat") - F.col("event_lat")) / 2), 2) +
                                F.cos(F.radians(F.col("event_lat"))) * 
                                F.cos(F.radians(F.col("geo_lat"))) * 
                                F.pow(F.sin(F.radians(F.col("geo_lng") - F.col("event_lon")) / 2), 2)))) \
                                .groupBy("event", "event_type", "date") \
                                .agg(
                                        F.first("city").alias("nearest_city"),
                                        F.min("distance_km").alias("distance_to_city_km"))
# 4 сохранение результатов        
        result.write.partitionBy('date').mode("overwrite").parquet(f"{output_path}")

if __name__ == "__main__":
        main()
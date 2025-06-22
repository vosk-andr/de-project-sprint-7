import sys
 
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
 
def main():
        input_path_event = sys.argv[1]
        input_path_sity = sys.argv[1]
        output_path = sys.argv[2]

        conf = SparkConf().setAppName(f"EventSity")
        sc = SparkContext(conf=conf)
        sql = SQLContext(sc)

        events = sql.read.parquet(f"{input_path_event}").select("event", "event_type", 
            F.radians(F.col("lat").cast("float")).alias("event_lat"), 
            F.radians(F.col("lon").cast("float")).alias("event_lon"),
            "date")

        cities = (sql.read
          .option("delimiter", ";")
          .option("header", "true")
          .csv(f"{input_path_sity}")
          .select(
              "city",
              F.radians(F.expr("CAST(replace(lat, ',', '.') AS FLOAT)")).alias("geo_lat"),
              F.radians(F.expr("CAST(replace(lng, ',', '.') AS FLOAT)")).alias("geo_lng")))
        
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
        
        result.write.mode("overwrite").parquet(f"{output_path}")

if __name__ == "__main__":
        main()
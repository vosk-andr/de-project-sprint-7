import sys
 
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
 
def main():
        input_path = sys.argv[1]
        output_path = sys.argv[2]
        fraction = sys.argv[3]
        seed = sys.argv[4]

        conf = SparkConf().setAppName(f"OverWrite")
        sc = SparkContext(conf=conf)
        sql = SQLContext(sc)
        events = sql.read.parquet(f"{input_path}").sample(withReplacement=False, fraction=fraction, seed=seed)
        events.write.mode("overwrite").format('parquet').save(f"{output_path}")


if __name__ == "__main__":
        main()
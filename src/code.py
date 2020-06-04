from pyspark.sql import SparkSession
from pyspark import SparkConf


def main():

    conf = SparkConf().setAppName("Football results").set("spark.executor.memory", "8g")

    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    football_df = spark.read.csv('./data/df_matches.csv', header=True, sep=',')

    football_df.show()

main()

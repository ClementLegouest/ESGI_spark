from pyspark.sql import SparkSession
from pyspark import SparkConf


def main():

    conf = SparkConf().setAppName("Football results").set("spark.executor.memory", "8g")

    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    football_df = spark.read.csv('./data/df_matches.csv', header=True, sep=',')

    clean_football_df = football_df.withColumn('match', football_df.X4)\
        .withColumn('competition', football_df.X6)\
        .drop(football_df.X4)\
        .drop(football_df.X6)

    clean_football_df.show()
    clean_football_df.printSchema()

main()

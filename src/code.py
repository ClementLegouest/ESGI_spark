from pyspark.sql.functions import udf
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import IntegerType
from datetime import datetime


def shift_na_to_zero(penalty):
    if penalty == "NA":
        return 0
    else:
        return 1


clean_penalty_udf = udf(lambda penalty: shift_na_to_zero(penalty), IntegerType())


def main():

    conf = SparkConf().setAppName("Football results").set("spark.executor.memory", "8g")

    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    football_df = spark.read.csv('./data/df_matches.csv', header=True, sep=',')

    clean_football_df = football_df.filter(football_df.date >= datetime.fromisoformat('1980-03-01'))\
        .withColumn('match', football_df.X4)\
        .withColumn('competition', football_df.X6)\
        .withColumn('penalty_france', clean_penalty_udf(football_df.penalty_france))\
        .withColumn('penalty_adversaire', clean_penalty_udf(football_df.penalty_adversaire))\
        .drop(football_df.X4)\
        .drop(football_df.X6)\
        .drop(football_df.X2)\
        .drop(football_df.X5)\
        .drop(football_df.year)\
        .drop(football_df.outcome)\
        .drop(football_df.no)\

    clean_football_df.show(200)
    clean_football_df.printSchema()

main()

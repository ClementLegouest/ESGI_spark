from pyspark.sql.functions import udf, col, sum, count, min, max, avg
from pyspark.sql.types import IntegerType, BooleanType
from datetime import datetime, date
from pyspark.sql import SparkSession
from pyspark import SparkConf


class Application():

    def main(self, argv):

        conf = SparkConf().setAppName("Football results").set("spark.executor.memory", "8g")

        spark = SparkSession.builder.config(conf=conf).getOrCreate()

        clean_football_df = get_data_frame(spark).cache()

        clean_football_df.show(50)
        clean_football_df.printSchema()

        matches_statistics_df = compute_statistics(clean_football_df).cache()
        matches_statistics_df.write.mode("overwrite").parquet("stats.parquet")

        matches_statistics_df.show(200)
        matches_statistics_df.printSchema()

        jointed_df = clean_football_df.join(
            matches_statistics_df,
            clean_football_df.adversaire == matches_statistics_df.adversaire,
            'full_outer'
        )

        jointed_df.show(400)
    

    def shift_na_to_zero(penalty):
        if penalty == "NA":
            return 0
        else:
            try:
                return int(penalty)
            except:
                return 0


    clean_penalty_udf = udf(lambda penalty: shift_na_to_zero(penalty), IntegerType())


    def get_data_frame(spark):

        football_df = spark.read.csv('./data/df_matches.csv', header=True, sep=',')

        clean_football_df = football_df.filter(football_df.date >= date(1980, 3, 1))\
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
            .drop(football_df.no)
        
        return clean_football_df


    def compute_statistics(df):
        total_matches = df.count()

        print("total_matches : " + str(total_matches))
        
        statistics_df = df.withColumn("has_been_played_at_home", has_been_played_at_home_udf(df.match))\
            .withColumn("world_cup", world_cup_udf(df.competition))

        statistics_df.show()

        return statistics_df\
        .groupBy("adversaire")\
        .agg(
            avg(statistics_df.score_france.cast("int")).alias("avg_goal_fr"),
            avg(statistics_df.score_adversaire.cast("int")).alias("avg_goal_adv"),
            count(statistics_df.adversaire).alias("matches_played"),
            (sum(statistics_df.has_been_played_at_home.cast("int")) * 100 /  count(statistics_df.adversaire)).alias("%_played_at_home"),
            sum(statistics_df.world_cup.cast("int")).alias("world_cup_matches"),
            max(statistics_df.penalty_france).alias("max_penalty_france"),
            (sum(statistics_df.penalty_france) - sum(statistics_df.penalty_adversaire)).alias("diff_penalty")
        ).drop("world_cup_matches")


    def world_cup(competition):
        if competition[:14] == "Coupe du monde":
            return True
        else:
            return False


    world_cup_udf = udf(lambda competition: world_cup(competition), BooleanType())


    def has_been_played_at_home(match):
        if match[:6] == "France":
            return True
        else:
            return False


    has_been_played_at_home_udf = udf(lambda match: has_been_played_at_home(match), BooleanType())

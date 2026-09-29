from pyspark.sql import SparkSession
from pyspark.sql import functions as F



spark = SparkSession.builder.appName("NBA_Player_Clutch_Stats").getOrCreate()


CLUTCH_EVENTS_PATH = "processed_data/clutch_events.parquet"
OUTPUT_PATH = "processed_data/player_clutch_stats.parquet"


df_clutch = spark.read.parquet(CLUTCH_EVENTS_PATH)

df_clutch = df_clutch.filter(F.col("player_id").isNotNull())

df_agg = (
    df_clutch
    .groupBy("player_id", "player_name")
    .agg(
        F.sum("points").alias("clutch_points"),
        F.sum("fg_make").alias("clutch_fg_make"),
        F.sum("fg_miss").alias("clutch_fg_miss"),
        F.sum("ft_make").alias("clutch_ft_make"),
        F.sum("ft_miss").alias("clutch_ft_miss"),
        F.sum("assist").alias("clutch_assists"),
        F.sum("rebound").alias("clutch_rebounds"),
        F.sum("steal").alias("clutch_steals"),
        F.sum("turnover").alias("clutch_turnovers"),
        F.countDistinct("game_id").alias("clutch_games_played")
    )
)

df_stats = (
    df_agg
    .withColumn(
        "clutch_fg_attempts",
        F.col("clutch_fg_make") + F.col("clutch_fg_miss")
    )
    .withColumn(
        "clutch_ft_attempts",
        F.col("clutch_ft_make") + F.col("clutch_ft_miss")
    )
    .withColumn(
        "clutch_fg_pct",
        F.when(F.col("clutch_fg_attempts") > 0,
               F.col("clutch_fg_make") / F.col("clutch_fg_attempts"))
         .otherwise(F.lit(None).cast("double"))
    )
    .withColumn(
        "clutch_ft_pct",
        F.when(F.col("clutch_ft_attempts") > 0,
               F.col("clutch_ft_make") / F.col("clutch_ft_attempts"))
         .otherwise(F.lit(None).cast("double"))
    )
    .withColumn(
        "clutch_possessions_used",
        F.col("clutch_fg_attempts") +
        F.col("clutch_ft_attempts") +
        F.col("clutch_turnovers")
    )
)

df_stats.orderBy(F.desc("clutch_points")).show(20, truncate=False)

(
    df_stats
    .select(
        "player_id",
        "player_name",
        "clutch_games_played",
        "clutch_possessions_used",
        "clutch_points",
        "clutch_fg_make", "clutch_fg_miss", "clutch_fg_attempts", "clutch_fg_pct",
        "clutch_ft_make", "clutch_ft_miss", "clutch_ft_attempts", "clutch_ft_pct",
        "clutch_assists", "clutch_rebounds", "clutch_steals", "clutch_turnovers"
    )
    .write
    .mode("overwrite")
    .parquet(OUTPUT_PATH)
)

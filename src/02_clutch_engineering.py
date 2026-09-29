from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("NBA_Clutch_Engineering").getOrCreate()

PBP_PATH = "processed_data/nba_pbp_cleaned.parquet"
EVENTS_PATH = "processed_data/events_clean.parquet"
OUTPUT_PATH = "processed_data/clutch_events.parquet"

df_pbp = spark.read.parquet(PBP_PATH)
df_events = spark.read.parquet(EVENTS_PATH)

df_pbp_time = df_pbp.withColumn(
    "clock_str", 
    F.substring(F.col("time_remaining").cast("string"), 12, 5) 
).withColumn(
    "seconds_left",
    (F.split(F.col("clock_str"), ":")[0].cast("int") * 60) + 
    F.split(F.col("clock_str"), ":")[1].cast("int")
)

print("Applying Clutch Filters (4th Qtr, +/- 5 pts, < 5 mins)...")
df_clutch_pbp = df_pbp_time.filter(
    (F.col("period") == 4) &
    (F.abs(F.col("score_margin")) <= 5) &
    (F.col("seconds_left") <= 300) 
)


df_final = df_clutch_pbp.alias("pbp").join(
    df_events.alias("stats"),
    (F.col("pbp.game_id") == F.col("stats.game_id")) &
    (F.col("pbp.eventnum") == F.col("stats.eventnum")),
    "inner"
).select(
    F.col("pbp.game_id"),
    F.col("pbp.season_id"),
    F.col("pbp.eventnum"),
    F.col("pbp.seconds_left"),
    F.col("pbp.score_margin"),
    F.col("stats.player1_id").alias("player_id"),
    F.col("stats.player_name_full").alias("player_name"),
    F.col("stats.points"),
    F.col("stats.fg_make"),
    F.col("stats.fg_miss"),
    F.col("stats.ft_make"),
    F.col("stats.ft_miss"),
    F.col("stats.rebound"),
    F.col("stats.assist"),
    F.col("stats.steal"),
    F.col("stats.turnover")
)


df_final.write.mode("overwrite").parquet(OUTPUT_PATH)

import os
import kagglehub
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType


print("Downloading raw data from Kaggle...")
path = kagglehub.dataset_download("wyattowalsh/basketball")
csv_folder = path
for root, dirs, files in os.walk(path):
    if "play_by_play.csv" in files:
        csv_folder = root
        break


spark = SparkSession.builder.appName("NBA_Setup").getOrCreate()


df_pbp_raw = spark.read.csv(os.path.join(csv_folder, "play_by_play.csv"), header=True, inferSchema=True)
df_game_raw = spark.read.csv(os.path.join(csv_folder, "game.csv"), header=True, inferSchema=True)
df_player_raw = spark.read.csv(os.path.join(csv_folder, "common_player_info.csv"), header=True, inferSchema=True)

def clean_game_data(df):
    return df.select(
        F.col("game_id").cast(StringType()),
        F.col("season_id").cast(StringType()),
        F.col("game_date"),
        F.col("team_id_home"),
        F.col("team_id_away")
    ).distinct()

def clean_pbp_data(df):
    df = df.withColumn("scoreMargin_clean", 
        F.when(F.col("scoreMargin") == "TIE", 0)
         .otherwise(F.col("scoreMargin"))
    ).withColumn("scoreMargin_clean", F.col("scoreMargin_clean").cast(IntegerType()))
    
    return df.select(
        F.col("game_id").cast(StringType()),
        F.col("eventnum").cast(IntegerType()),
        F.col("eventmsgtype").cast(IntegerType()), 
        F.col("eventmsgactiontype").cast(IntegerType()),
        F.col("period").cast(IntegerType()),
        F.col("pctimestring").alias("time_remaining"),
        F.col("scoreMargin_clean").alias("score_margin"),
        F.col("person1type"),
        F.col("player1_id"), 
        F.col("player2_id"),
        F.col("player3_id"),
        F.col("homedescription"),
        F.col("visitordescription")
    ).na.fill(0, subset=["score_margin"])

def clean_player_data(df):
    return df.select(
        F.col("person_id").cast(IntegerType()),
        F.col("display_first_last").alias("player_name_full")
    ).distinct()

df_game = clean_game_data(df_game_raw)
df_pbp = clean_pbp_data(df_pbp_raw)
df_player = clean_player_data(df_player_raw)

df_pbp_season = df_pbp.join(df_game, on="game_id", how="inner")

df_pbp_final = df_pbp_season.join(
    df_player, 
    df_pbp_season.player1_id == df_player.person_id, 
    how="left"
).drop("person_id")

OUTPUT_PBP = "processed_data/nba_pbp_cleaned.parquet"
df_pbp_final.write.mode("overwrite").parquet(OUTPUT_PBP)

def contains(col, text):
    return F.instr(F.lower(col), text.lower()) > 0

df_events = df_pbp_final \
    .withColumn("is_miss", (contains("homedescription", "MISS") | contains("visitordescription", "MISS"))) \
    .withColumn("is_three", (contains("homedescription", "3PT") | contains("visitordescription", "3PT"))) \
    .withColumn("fg_make", (F.col("eventmsgtype") == 1).cast("int")) \
    .withColumn("fg_miss", (F.col("eventmsgtype") == 2).cast("int")) \
    .withColumn("ft_make", ((F.col("eventmsgtype") == 3) & (~F.col("is_miss"))).cast("int")) \
    .withColumn("ft_miss", ((F.col("eventmsgtype") == 3) & (F.col("is_miss"))).cast("int")) \
    .withColumn("points", 
        F.when(F.col("fg_make") == 1, F.when(F.col("is_three"), 3).otherwise(2))
         .when(F.col("ft_make") == 1, 1)
         .otherwise(0)
    ) \
    .withColumn("rebound", (contains("homedescription", "rebound") | contains("visitordescription", "rebound")).cast("int")) \
    .withColumn("assist", (contains("homedescription", " ast") | contains("visitordescription", " ast")).cast("int")) \
    .withColumn("steal", (contains("homedescription", "steal") | contains("visitordescription", "steal")).cast("int")) \
    .withColumn("block", (contains("homedescription", "block") | contains("visitordescription", "block")).cast("int")) \
    .withColumn("turnover", (F.col("eventmsgtype") == 5).cast("int")) \
    .withColumn("foul", (F.col("eventmsgtype") == 6).cast("int"))

OUTPUT_EVENTS = "processed_data/events_clean.parquet"
df_events.write.mode("overwrite").parquet(OUTPUT_EVENTS)

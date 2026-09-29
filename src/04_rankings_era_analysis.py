from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = SparkSession.builder.appName("NBA_Clutch_Rankings_and_Bias").getOrCreate()

INPUT_PATH = "processed_data/player_clutch_stats.parquet"
RANKINGS_PATH = "processed_data/clutch_player_rankings.parquet"

print("Loading player-level clutch stats...")
df_stats_raw = spark.read.parquet(INPUT_PATH)

# Drop rows with missing player names (data limitation)
df_stats = df_stats_raw.filter(F.col("player_name").isNotNull())

# Core derived metrics used everywhere
df_stats = (
    df_stats
    .withColumn(
        "clutch_possessions_used",
        F.col("clutch_fg_make") + F.col("clutch_fg_miss") +
        F.col("clutch_ft_make") + F.col("clutch_ft_miss") +
        F.col("clutch_turnovers")
    )
    .withColumn(
        "points_per_poss",
        F.when(F.col("clutch_possessions_used") > 0,
               F.col("clutch_points") / F.col("clutch_possessions_used"))
         .otherwise(F.lit(None).cast("double"))
    )
    .withColumn(
        "usage_per_game",
        F.when(F.col("clutch_games_played") > 0,
               F.col("clutch_possessions_used") / F.col("clutch_games_played"))
         .otherwise(F.lit(None).cast("double"))
    )
)

# Save the enriched full table for future analysis
df_stats.write.mode("overwrite").parquet(RANKINGS_PATH)
print(f"Saved full rankings table to {RANKINGS_PATH}")





#Top 20 Clutch Scorers of All Time (by total clutch points)

top20_scorers = (
    df_stats
    .orderBy(F.desc("clutch_points"))
    .limit(20)
    .select(
        "player_id",
        "player_name",
        "clutch_points",
        "clutch_games_played",
        "clutch_possessions_used",
        "points_per_poss"
    )
)

print("\nTop 20 Clutch Scorers (by total clutch points):")
top20_scorers.show(truncate=False)

top20_scorers.write.mode("overwrite").parquet("processed_data/top20_clutch_scorers.parquet")



#Top 20 Clutch FG% (Minimum Attempts Threshold)

MIN_FGA_FOR_FG_LEADER = 50 

top20_fg_pct = (
    df_stats
    .filter(F.col("clutch_fg_attempts") >= MIN_FGA_FOR_FG_LEADER)
    .orderBy(F.desc("clutch_fg_pct"))
    .limit(20)
    .select(
        "player_id",
        "player_name",
        "clutch_fg_attempts",
        "clutch_fg_make",
        "clutch_fg_miss",
        "clutch_fg_pct",
        "clutch_points",
        "points_per_poss"
    )
)

print("\nTop 20 Clutch FG% (min 50 FGA):")
top20_fg_pct.show(truncate=False)

top20_fg_pct.write.mode("overwrite").parquet("processed_data/top20_clutch_fg_pct.parquet")



#High Usage, Low Efficiency Players (Overrated Clutch)

HIGH_USAGE_MIN = 3.0      # possessions per game
MIN_POSS_HIGH = 50        # total clutch possessions
MIN_FGA_HIGH = 30         # at least 30 clutch FGA
MAX_PPP_OVER = 0.9        # low efficiency cutoff (points per possession)

high_usage_low_eff = (
    df_stats
    .filter(F.col("clutch_games_played") >= 20)
    .filter(F.col("clutch_possessions_used") >= MIN_POSS_HIGH)
    .filter(F.col("clutch_fg_attempts") >= MIN_FGA_HIGH)
    .filter(F.col("usage_per_game") >= HIGH_USAGE_MIN)
    .filter(F.col("points_per_poss") <= MAX_PPP_OVER)
    .orderBy(F.desc("usage_per_game"), F.asc("points_per_poss"))
    .limit(20)
    .select(
        "player_id",
        "player_name",
        "clutch_games_played",
        "clutch_possessions_used",
        "usage_per_game",
        "clutch_points",
        "points_per_poss",
        "clutch_fg_attempts",
        "clutch_fg_pct"
    )
)

print("\nHigh Usage, Low Efficiency Players (Overrated Clutch):")
high_usage_low_eff.show(truncate=False)

high_usage_low_eff.write.mode("overwrite").parquet("processed_data/high_usage_low_eff.parquet")



# Low Usage, High Efficiency Players (Underrated Clutch)

LOW_USAGE_MAX = 2.0       # possessions per game
MIN_POSS_LOW = 20         # total clutch possessions
MIN_FGA_LOW = 10          # at least 10 FGA
MIN_PPP_UNDER = 1.1       # high efficiency cutoff

low_usage_high_eff = (
    df_stats
    .filter(F.col("clutch_games_played") >= 20)
    .filter(F.col("clutch_possessions_used") >= MIN_POSS_LOW)
    .filter(F.col("clutch_fg_attempts") >= MIN_FGA_LOW)
    .filter(F.col("usage_per_game") <= LOW_USAGE_MAX)
    .filter(F.col("points_per_poss") >= MIN_PPP_UNDER)
    .orderBy(F.desc("points_per_poss"))
    .limit(20)
    .select(
        "player_id",
        "player_name",
        "clutch_games_played",
        "clutch_possessions_used",
        "usage_per_game",
        "clutch_points",
        "points_per_poss",
        "clutch_fg_attempts",
        "clutch_fg_pct"
    )
)

print("\nLow Usage, High Efficiency Players (Underrated Clutch):")
low_usage_high_eff.show(truncate=False)

low_usage_high_eff.write.mode("overwrite").parquet("processed_data/low_usage_high_eff.parquet")



#ERA BIAS ANALYSIS (90s vs 2000s vs 2010s vs 2020s)

print("\n=== ERA BIAS ANALYSIS (90s vs 2000s vs 2010s vs 2020s) ===")

CLUTCH_EVENTS_PATH = "processed_data/clutch_events.parquet"

# Load event-level clutch data
df_clutch_events = spark.read.parquet(CLUTCH_EVENTS_PATH)

# Extract a reasonable starting year from season_id.
# NBA stats convention: season_id like 22014 -> 2014-15 season,
# so `season_id % 10000` gives 2014.
df_clutch_era = (
    df_clutch_events
    .withColumn("season_int", F.col("season_id").cast("int"))
    .withColumn("season_start_year", (F.col("season_int") % F.lit(10000)))
    .withColumn(
        "era",
        F.when((F.col("season_start_year") >= 1990) & (F.col("season_start_year") <= 1999), "1990s")
         .when((F.col("season_start_year") >= 2000) & (F.col("season_start_year") <= 2009), "2000s")
         .when((F.col("season_start_year") >= 2010) & (F.col("season_start_year") <= 2019), "2010s")
         .when((F.col("season_start_year") >= 2020) & (F.col("season_start_year") <= 2029), "2020s")
         .otherwise("Other/Unknown")
    )
)

# Aggregate clutch performance by era
df_era_raw = (
    df_clutch_era
    .groupBy("era")
    .agg(
        F.count("*").alias("clutch_events"),
        F.countDistinct("game_id").alias("clutch_games"),
        F.countDistinct("player_id").alias("unique_players"),
        F.sum("points").alias("total_points"),
        F.sum("fg_make").alias("total_fg_make"),
        F.sum("fg_miss").alias("total_fg_miss"),
        F.sum("ft_make").alias("total_ft_make"),
        F.sum("ft_miss").alias("total_ft_miss"),
        F.sum("turnover").alias("total_turnovers")
    )
)

df_era_stats = (
    df_era_raw
    .withColumn(
        "fg_attempts",
        F.col("total_fg_make") + F.col("total_fg_miss")
    )
    .withColumn(
        "ft_attempts",
        F.col("total_ft_make") + F.col("total_ft_miss")
    )
    .withColumn(
        "fg_pct",
        F.when(F.col("fg_attempts") > 0,
               F.col("total_fg_make") / F.col("fg_attempts"))
         .otherwise(F.lit(None).cast("double"))
    )
    .withColumn(
        "ft_pct",
        F.when(F.col("ft_attempts") > 0,
               F.col("total_ft_make") / F.col("ft_attempts"))
         .otherwise(F.lit(None).cast("double"))
    )
    .withColumn(
        "points_per_event",
        F.when(F.col("clutch_events") > 0,
               F.col("total_points") / F.col("clutch_events"))
         .otherwise(F.lit(None).cast("double"))
    )
    .withColumn(
        "points_per_game",
        F.when(F.col("clutch_games") > 0,
               F.col("total_points") / F.col("clutch_games"))
         .otherwise(F.lit(None).cast("double"))
    )
    .withColumn(
        "turnovers_per_game",
        F.when(F.col("clutch_games") > 0,
               F.col("total_turnovers") / F.col("clutch_games"))
         .otherwise(F.lit(None).cast("double"))
    )
)

#ordered eras
era_order = F.when(F.col("era") == "1990s", 1) \
             .when(F.col("era") == "2000s", 2) \
             .when(F.col("era") == "2010s", 3) \
             .when(F.col("era") == "2020s", 4) \
             .otherwise(99)

df_era_final = df_era_stats.orderBy(era_order)

print("Clutch Performance by Era:")
df_era_final.select(
    "era",
    "clutch_events",
    "clutch_games",
    "unique_players",
    "total_points",
    "fg_pct",
    "ft_pct",
    "points_per_event",
    "points_per_game",
    "turnovers_per_game"
).show(truncate=False)

ERA_OUTPUT_PATH = "processed_data/era_bias_summary.parquet"
df_era_final.write.mode("overwrite").parquet(ERA_OUTPUT_PATH)

from pathlib import Path
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

FIGURES = Path("figures")
FIGURES.mkdir(exist_ok=True)

spark = SparkSession.builder.appName("NBA_Clutch_Visualizations").getOrCreate()

# Final two minutes vs. earlier portion of the already-filtered 5-minute clutch window
df_final = spark.read.parquet("processed_data/clutch_events.parquet")
df_window = df_final.withColumn("final_two_minutes", F.when(F.col("seconds_left") <= 120, 1).otherwise(0))
stats = df_window.groupBy("final_two_minutes").agg(F.avg("points").alias("avg_points")).orderBy("final_two_minutes").collect()
labels = ["Earlier Clutch Window" if r["final_two_minutes"] == 0 else "Final 2 Minutes" for r in stats]
values = [r["avg_points"] for r in stats]
plt.figure()
plt.bar(labels, values)
plt.title("Average Points Within the 5-Minute Clutch Window")
plt.xlabel("Game Situation")
plt.ylabel("Average Points per Event")
plt.tight_layout()
plt.savefig(FIGURES / "clutch_window_comparison.png", dpi=160)
plt.close()

# Era comparison
df_era = spark.read.parquet("processed_data/era_bias_summary.parquet").collect()
eras = [r["era"] for r in df_era]
fg_pct = [r["fg_pct"] for r in df_era]
plt.figure(figsize=(6,5))
plt.bar(eras, fg_pct)
plt.title("Clutch FG% Across Eras")
plt.ylabel("FG%")
plt.xlabel("Era")
plt.tight_layout()
plt.savefig(FIGURES / "clutch_fg_pct_by_era.png", dpi=160)
plt.close()

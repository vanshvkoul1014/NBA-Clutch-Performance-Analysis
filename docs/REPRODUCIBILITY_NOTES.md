# Reproducibility and Scope Notes

## Implemented in the included notebook

The code in this repository implements:

- Kaggle dataset download and PySpark ingestion
- Data cleaning and joins
- Event-level feature extraction
- Clutch-event filtering
- Player-level clutch aggregation
- Clutch scoring and FG% rankings
- Usage-vs-efficiency slices
- Era-level summaries
- Two visualizations

## MVP modeling scope

The final report/presentation describe an objective of using MLlib to predict official MVP vote share. The included notebook does **not** contain a complete trained/evaluated MVP vote-share model. This repository therefore describes that piece as future/modeling work rather than a completed result.

## Source-reported counts

The original notebook output reports `1,352,230` rows after the clutch-event filtering step. The course report also reports roughly 1.35M total play-by-play events. Because those counts are unusually close, anyone reproducing the work should validate the time parsing and clutch filter against the current Kaggle dataset version before treating the clutch-event count as an authoritative basketball statistic.

## Visualization semantics

The notebook's first bar chart is generated *after* loading `clutch_events.parquet`. It compares events with <=2:00 remaining against events earlier in the already-filtered 5-minute clutch window; it is not a full-season clutch-versus-regular comparison.

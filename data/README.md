# Data

This repository does **not** include the raw NBA dataset because it is large and is distributed separately on Kaggle.

The project uses the **NBA Basketball** dataset by Wyatt Walsh (`wyattowalsh/basketball`) and downloads it programmatically with `kagglehub`.

Expected source files used by the pipeline include:

- `play_by_play.csv`
- `game.csv`
- `common_player_info.csv`

The notebook/scripts write intermediate and derived Parquet datasets to `processed_data/`. Those generated files are intentionally ignored by Git.

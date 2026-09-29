#!/usr/bin/env bash
set -euo pipefail
python src/01_data_setup.py
python src/02_clutch_engineering.py
python src/03_player_clutch_stats.py
python src/04_rankings_era_analysis.py
python src/05_visualizations.py

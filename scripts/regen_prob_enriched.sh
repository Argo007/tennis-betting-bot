#!/usr/bin/env bash
set -euo pipefail

echo "[regen] start"

# 0) Folders
mkdir -p data/raw data/raw/odds outputs results/backtests

# 1) Pull fresh match metadata (stub-safe)
python scripts/fetch_tennis_data.py --outdir data/raw

# 2) Get close odds (uses sample if provider unavailable)
python scripts/fetch_close_odds.py --odds oddsportal --outdir data/raw/odds

# 3) Add synthetic live odds so we always have some rows
python scripts/fill_with_synthetic_live.py --outdir data/raw/odds

# 4) Build unified raw files (joins odds + matches)
python scripts/build_from_raw.py

# 5) Assemble model dataset (dedup, normalize)
python scripts/build_dataset.py

# 6) Guarantee presence of minimal CSVs (no-crash)
python scripts/ensure_dataset.py

# 7) Compute vig-free probabilities (Shin by default)
python scripts/compute_prob_vigfree.py \
  --input data/raw/historical_matches.csv \
  --output data/raw/vigfree_matches.csv \
  --method shin

# 8) Sanity check + small enrich; writes the file we need:
python scripts/check_probabilities.py \
  --input data/raw/vigfree_matches.csv \
  --output outputs/prob_enriched.csv

# 9) Optional: extra edges & features for the dashboard
python scripts/edge_smith_enrich.py \
  --input outputs/prob_enriched.csv \
  --output outputs/edge_enriched.csv

echo "[regen] done → outputs/prob_enriched.csv"
